package walrusds

import (
	"context"
	"database/sql"
	"log"
	"time"
)

// renewBatchSize bounds how many expiring blobs are processed per scan so a
// single tick cannot monopolize the publisher.
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
		if err := w.renewOne(ctx, it); err != nil {
			log.Printf("walrusds: renewing key %s (blob %s): %v", it.Key, it.BlobID, err)
		}
	}
	return nil
}

// renewOne re-uploads a single blob's bytes and points the index at the new
// blob ID. We read the existing bytes from the aggregator and store them
// again; this needs only the HTTP API (no Sui key) at the cost of one
// round-trip per blob near expiry.
func (w *WalrusDatastore) renewOne(ctx context.Context, it RenewItem) error {
	data, err := w.client.Read(ctx, it.BlobID)
	if err != nil {
		return err
	}
	res, err := w.client.Store(ctx, data, w.conf.Epochs, w.conf.Deletable)
	if err != nil {
		return err
	}

	var expiresAt sql.NullTime
	if w.conf.EpochDuration > 0 {
		expiresAt = sql.NullTime{
			Time:  time.Now().Add(time.Duration(w.conf.Epochs) * w.conf.EpochDuration),
			Valid: true,
		}
	}
	return w.index.UpdateAfterRenewal(ctx, it.Key, res.BlobID, res.EndEpoch, expiresAt)
}
