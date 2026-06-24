package walrusds

import (
	"context"
	"database/sql"
	"log"
	"time"
)

// renewBatchSize bounds how many expiring blobs are processed per scan so a
// single tick cannot monopolize the publisher. With block-packing this counts
// distinct blobs, so one entry can cover many IPFS blocks.
const renewBatchSize = 256

// startRenewalWorker launches the background goroutine that keeps Walrus
// blobs alive. Walrus storage is paid per epoch; without renewal a blob (and
// therefore the IPFS block it holds) silently disappears at expiry. The
// worker periodically asks Postgres for blobs whose expires_at is within
// RenewLead of now, re-uploads their bytes for a fresh epoch window, and
// updates the index transactionally per key.
func (w *WalrusDatastore) startRenewalWorker(ctx context.Context) {
	lead := w.conf.RenewLead
	if lead <= 0 {
		lead = w.conf.EpochDuration
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(w.conf.RenewInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := w.renewExpiring(ctx, lead); err != nil {
					log.Printf("walrusds: renewal scan failed: %v", err)
				}
			}
		}
	}()
}

func (w *WalrusDatastore) renewExpiring(ctx context.Context, lead time.Duration) error {
	deadline := time.Now().Add(lead)
	items, err := w.index.DueForRenewal(ctx, deadline, renewBatchSize)
	if err != nil {
		return err
	}

	for _, it := range items {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := w.renewOneBlob(ctx, it); err != nil {
			log.Printf("walrusds: renewing blob %s: %v", it.BlobID, err)
		}
	}
	return nil
}

// renewOneBlob refreshes the paid storage of a single Walrus blob and points
// every index row that referenced it at the renewed blob. The blob may be a
// plain block, a concatenated pack, or a quilt — in every case it is one
// content-addressed Walrus blob identified by it.BlobID (for a quilt, its quilt
// ID). We read the whole blob from the aggregator and store the identical bytes
// again for a fresh epoch window.
//
// Because Walrus blob IDs are content-derived, re-uploading the same bytes
// yields the same blob ID; for a quilt the member QuiltPatchIDs (quilt ID +
// position) are therefore unchanged too, so the existing patch_id rows stay
// valid and only end_epoch/expires_at are updated. This needs only the HTTP API
// (no Sui key) at the cost of one round-trip per blob near expiry, regardless
// of how many IPFS blocks the blob holds.
//
// This background worker is deliberately HTTP-only and holds no Sui key, so it
// always renews by re-upload. Operators who want to avoid re-downloading and
// re-uploading can instead extend blobs in place with a funded Sui key using
// the external renew.js tool (Walrus SDK `extend`), which uses the object_id
// recorded per row at store time. See js/renew.js.
func (w *WalrusDatastore) renewOneBlob(ctx context.Context, it RenewItem) error {
	return renewBlob(ctx, w.client, w.index, it.BlobID, w.conf.Epochs, w.conf.Deletable, w.conf.EpochDuration)
}

// renewBlob refreshes one Walrus blob's paid storage and repoints every index
// row that referenced it. It is the shared core used both by the optional
// background worker and by out-of-band renewal (see Renewer): read the whole
// blob, store the identical bytes for a fresh epoch window, then update the
// index. epochDuration, when > 0, is used to compute the new expires_at;
// otherwise expires_at is left NULL (the operator tracks timing externally).
//
// A single blob may back many keys (a quilt or a concat pack), so one call here
// renews all of them at once. For a quilt, blobID is the quilt ID and the
// member QuiltPatchIds are unchanged by re-upload, so existing patch_id rows
// stay valid.
func renewBlob(ctx context.Context, client *Client, index Index, blobID string, epochs int, deletable bool, epochDuration time.Duration) error {
	data, err := client.Read(ctx, blobID)
	if err != nil {
		return err
	}
	res, err := client.Store(ctx, data, epochs, deletable)
	if err != nil {
		return err
	}

	var expiresAt sql.NullTime
	if epochDuration > 0 {
		expiresAt = sql.NullTime{
			Time:  time.Now().Add(time.Duration(epochs) * epochDuration),
			Valid: true,
		}
	}
	return index.UpdateBlobAfterRenewal(ctx, blobID, res.BlobID, res.ObjectID, res.EndEpoch, expiresAt)
}
