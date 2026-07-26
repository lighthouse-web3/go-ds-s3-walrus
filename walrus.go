// Package walrusds implements a Kubo (IPFS) datastore backed by Walrus, a
// decentralized storage network built on Sui, using a shared Postgres
// database as the durable key -> blob index.
//
// Walrus is content-addressed (blobs are addressed by a content-derived blob
// ID, not by an arbitrary key), exposes no list/query API, and treats blobs
// as immutable with a finite, epoch-based lifetime. To present it as an IPFS
// datastore we keep the bytes on Walrus and the mapping
//
//	ds.Key -> { blobId, size, deletable, endEpoch, expiresAt }
//
// in Postgres. Postgres is the source of truth for what the node "knows":
// it is shared across upload and retrieval nodes, survives local disk loss,
// and supports point-in-time recovery. Has/GetSize/Query are answered purely
// from Postgres so they never incur a Walrus round-trip; only Get touches the
// Walrus aggregator.
package walrusds

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

const (
	// defaultWorkers is the pack flusher's upload concurrency. Each in-flight
	// pack holds its bytes in memory while uploading, so peak memory scales
	// with workers × PackTargetSize; 16 × the 256 MiB default is ≈4 GiB peak,
	// so lower workers on smaller hosts. Raise it only alongside more RAM
	// (and a matching maxOpenConns).
	defaultWorkers = 16
	// defaultMaxOpenConns bounds the Postgres connection pool. Commit fans out
	// to `workers` goroutines that each run a PutMany, plus reads/renewal, so
	// the pool must comfortably exceed Workers; 32 covers the default worker
	// count with headroom. An unbounded pool (the old behaviour) can exhaust
	// Postgres' max_connections under load.
	defaultMaxOpenConns   = 32
	defaultEpochs         = 1
	defaultRequestTimeout = 60 * time.Second
	defaultMaxRetries     = 3
	// defaultPackTargetSize is the target size of a packed Walrus blob. The
	// staging flusher accumulates blocks across commits until this many bytes
	// (or quiltMaxPatches) are ready, amortizing Walrus's fixed ~64 MB
	// per-blob metadata overhead. 256 MiB is large enough that typical
	// multi-tens-of-MB files land in one quilt. Public Walrus services cap
	// requests near 10 MiB, so packs this large require a self-hosted
	// publisher/aggregator (expected on mainnet anyway).
	defaultPackTargetSize = 256 << 20 // 256 MiB
	// defaultBlobCacheBytes is the default in-memory budget for caching whole
	// blobs to serve range reads of packed blocks. Sized so a single
	// default-target pack (256 MiB) stays cacheable (per-entry cap is a quarter
	// of the budget).
	defaultBlobCacheBytes = 1 << 30 // 1 GiB
	// quiltMaxPatches is the maximum number of member blobs in a single
	// QuiltV1. It is fixed by the protocol (derived from the 1000-shard
	// committee: 667 secondary columns minus one reserved for the index).
	// A pack destined to become a quilt may therefore hold at most this many
	// blocks regardless of PackTargetSize.
	quiltMaxPatches = 666
	// concatMaxBlocks bounds a claimed concat pack (DisableQuilt) so a single
	// flush cannot build an unboundedly long index transaction. There is no
	// protocol limit for concat packs; PackTargetSize is the real bound.
	concatMaxBlocks = 10000
	// defaultPackMaxAge is how long a block may sit in the Postgres staging
	// buffer before it is flushed to Walrus even though a full pack has not
	// accumulated. Staged blocks are already durable (Postgres), so this only
	// bounds how long bytes are served from Postgres instead of Walrus. It is
	// the backstop for continuous trickle ingest, where the idle trigger
	// (defaultPackIdleFlush) never fires.
	defaultPackMaxAge = 5 * time.Minute
	// defaultPackIdleFlush is how long the staging buffer must receive no new
	// blocks before its partial tail is flushed to Walrus. Quiescence is the
	// signal that an upload has finished: the tail of an `ipfs add` reaches
	// Walrus seconds after the last block instead of waiting out PackMaxAge,
	// while a stream of back-to-back writes keeps accumulating full packs.
	defaultPackIdleFlush = 30 * time.Second
	// defaultPackFlushInterval is how often the flusher re-checks the staging
	// buffer, independently of the kicks it gets after every Commit. It also
	// bounds how quickly the idle trigger is noticed, so it stays below
	// defaultPackIdleFlush.
	defaultPackFlushInterval = 5 * time.Second
	// stageLease is how long a flusher's claim on staged rows lasts. It must
	// comfortably exceed the worst-case pack upload (RequestTimeout × retries
	// × publishers); an expired lease simply lets another flusher retry, which
	// is safe because Walrus uploads are content-addressed (idempotent).
	stageLease = 15 * time.Minute
)

var (
	_ ds.Datastore = (*WalrusDatastore)(nil)
	_ ds.Batching  = (*WalrusDatastore)(nil)
)

// Config holds everything needed to construct a WalrusDatastore.
type Config struct {
	// PublisherURLs are Walrus publisher (write) base URLs (comma-separated
	// values are split by the plugin). At least one is required.
	PublisherURLs []string
	// AggregatorURLs are Walrus aggregator (read) base URLs. At least one is
	// required.
	AggregatorURLs []string

	// PostgresURL is the database/sql connection string for the shared index,
	// e.g. "postgres://user:pass@host:5432/db?sslmode=require".
	PostgresURL string
	// Table is the index table name. Defaults to "walrus_index".
	Table string

	// Epochs is how many storage epochs new blobs are paid for. Defaults to 1.
	Epochs int
	// NShards is the Walrus committee shard count, used only to compute a
	// blob's encoded ("datacap") size when the publisher response omits it
	// (e.g. a deduplicated/already-certified blob). Defaults to DefaultNShards
	// (1000, Walrus mainnet). It does not affect what is stored on Walrus.
	NShards int
	// Deletable registers blobs as deletable on Walrus. Defaults to false.
	Deletable bool
	// Workers is how many packs the flusher uploads to Walrus in parallel when
	// the staging buffer holds more than one pack's worth of blocks. Defaults
	// to defaultWorkers. Peak upload memory is roughly Workers ×
	// PackTargetSize, so raise both Workers and host RAM together.
	Workers int
	// MaxOpenConns bounds the Postgres connection pool. Defaults to
	// defaultMaxOpenConns. Keep it >= Workers so committing packs does not
	// starve on connections; a value <= 0 applies the default.
	MaxOpenConns int

	// PackTargetSize is the target size (in bytes) of a packed Walrus blob.
	// During Batch.Commit, blocks are grouped into packs up to this size and
	// uploaded as a single Walrus quilt (or, with DisableQuilt, a concatenated
	// blob), amortizing the per-blob Walrus cost (Sui gas + WAL minimums) across
	// many IPFS blocks. A block larger than this gets its own blob. Defaults to
	// defaultPackTargetSize.
	PackTargetSize int64
	// DisableQuilt reverts batch packing to the legacy scheme: blocks are
	// concatenated into one opaque blob and read back by byte range. By default
	// (false) batches are stored as Walrus quilts, which share blob overhead
	// natively and are read back per-member by QuiltPatchID. Existing rows
	// written under either scheme keep working regardless of this setting.
	DisableQuilt bool
	// PackMaxAge bounds how long a block may wait in the Postgres staging
	// buffer for a pack to fill before it is flushed to Walrus anyway.
	// Blocks are durable from the moment they are staged; this only trades
	// slightly longer Postgres-served reads for fuller (cheaper) packs.
	// Defaults to defaultPackMaxAge.
	PackMaxAge time.Duration
	// PackIdleFlush flushes the partial tail of the staging buffer once no new
	// blocks have been staged for this long — i.e. the ingest has finished —
	// so an upload's last pack reaches Walrus promptly instead of waiting out
	// PackMaxAge. Uploads with pauses longer than this flush a pack per pause;
	// raise it if your ingest is bursty and packing density matters more than
	// tail latency. Defaults to defaultPackIdleFlush.
	PackIdleFlush time.Duration
	// PackFlushInterval is how often the background flusher checks the staging
	// buffer (it is additionally kicked after every Commit). Defaults to
	// defaultPackFlushInterval.
	PackFlushInterval time.Duration
	// BlobCacheBytes is the byte budget for the in-memory LRU of whole blobs
	// used to serve range reads of packed blocks. Defaults to
	// defaultBlobCacheBytes; a negative value disables the cache.
	BlobCacheBytes int64

	// RequestTimeout bounds a single Walrus HTTP attempt. Defaults to 60s.
	RequestTimeout time.Duration
	// MaxRetries is the number of retries per Walrus request. Defaults to 3.
	MaxRetries int

	// EpochDuration is the wall-clock length of one Walrus storage epoch. When
	// non-zero (together with RenewInterval) it enables the renewal worker,
	// which re-uploads blobs before their paid storage expires. Operators set
	// this to match the target network (e.g. ~14 days on mainnet).
	EpochDuration time.Duration
	// RenewInterval is how often the renewal worker scans for expiring blobs.
	// Zero disables renewal.
	RenewInterval time.Duration
	// RenewLead is how far ahead of expiry a blob is renewed. Defaults to one
	// EpochDuration when zero and renewal is enabled.
	RenewLead time.Duration
}

// WalrusDatastore stores values on Walrus and keeps the durable key -> blob
// mapping in Postgres.
type WalrusDatastore struct {
	conf   Config
	client *Client
	index  Index

	// flushKick nudges the pack flusher right after a Commit stages new
	// blocks, so full packs upload immediately instead of waiting for the
	// next ticker interval.
	flushKick chan struct{}

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewWalrusDatastore validates the configuration, connects to Postgres
// (creating the index table if needed), prepares the Walrus client and, if
// configured, starts the background renewal worker.
func NewWalrusDatastore(conf Config) (*WalrusDatastore, error) {
	if len(conf.PublisherURLs) == 0 {
		return nil, fmt.Errorf("walrusds: at least one publisher URL is required")
	}
	if len(conf.AggregatorURLs) == 0 {
		return nil, fmt.Errorf("walrusds: at least one aggregator URL is required")
	}
	if conf.PostgresURL == "" {
		return nil, fmt.Errorf("walrusds: postgres URL is required")
	}
	if conf.Epochs <= 0 {
		conf.Epochs = defaultEpochs
	}
	if conf.NShards <= 0 {
		conf.NShards = DefaultNShards
	}
	if conf.Workers <= 0 {
		conf.Workers = defaultWorkers
	}
	if conf.MaxOpenConns <= 0 {
		conf.MaxOpenConns = defaultMaxOpenConns
	}
	if conf.RequestTimeout <= 0 {
		conf.RequestTimeout = defaultRequestTimeout
	}
	if conf.MaxRetries < 0 {
		conf.MaxRetries = defaultMaxRetries
	}
	if conf.PackTargetSize <= 0 {
		conf.PackTargetSize = defaultPackTargetSize
	}
	if conf.PackMaxAge <= 0 {
		conf.PackMaxAge = defaultPackMaxAge
	}
	if conf.PackIdleFlush <= 0 {
		conf.PackIdleFlush = defaultPackIdleFlush
	}
	if conf.PackFlushInterval <= 0 {
		conf.PackFlushInterval = defaultPackFlushInterval
	}
	// A negative BlobCacheBytes explicitly disables caching; zero means default.
	cacheBytes := conf.BlobCacheBytes
	switch {
	case cacheBytes == 0:
		cacheBytes = defaultBlobCacheBytes
	case cacheBytes < 0:
		cacheBytes = 0
	}

	ctx, cancel := context.WithCancel(context.Background())

	index, err := newPostgresIndex(ctx, conf.PostgresURL, conf.Table, conf.MaxOpenConns)
	if err != nil {
		cancel()
		return nil, err
	}

	client := NewClient(ClientConfig{
		PublisherURLs:  conf.PublisherURLs,
		AggregatorURLs: conf.AggregatorURLs,
		RequestTimeout: conf.RequestTimeout,
		MaxRetries:     conf.MaxRetries,
		BlobCacheBytes: cacheBytes,
	})

	w := &WalrusDatastore{
		conf:      conf,
		client:    client,
		index:     index,
		flushKick: make(chan struct{}, 1),
		cancel:    cancel,
	}

	w.startFlushWorker(ctx)

	if conf.RenewInterval > 0 && conf.EpochDuration > 0 {
		w.startRenewalWorker(ctx)
	}

	return w, nil
}

// expiry returns the wall-clock time at which a freshly stored blob's paid
// storage runs out, or a null time when renewal tracking is disabled.
func (w *WalrusDatastore) expiry() sql.NullTime {
	if w.conf.EpochDuration <= 0 {
		return sql.NullTime{}
	}
	return sql.NullTime{
		Time:  time.Now().Add(time.Duration(w.conf.Epochs) * w.conf.EpochDuration),
		Valid: true,
	}
}

// Put durably stages value in Postgres and returns; the background flusher
// packs staged blocks from many Puts/Commits into full-size Walrus blobs.
// Staging is what lets packs fill across commits: uploading synchronously per
// Put/Commit produced one under-filled blob per commit, each paying Walrus's
// fixed ~64 MB per-blob metadata overhead. A value too large to ever share a
// pack skips staging and goes straight to Walrus as its own blob.
func (w *WalrusDatastore) Put(ctx context.Context, k ds.Key, value []byte) error {
	if int64(len(value)) >= w.conf.PackTargetSize {
		recs, err := w.uploadPack(ctx, []blockEntry{{key: k.String(), val: value}})
		if err != nil {
			return err
		}
		return w.index.PutMany(ctx, recs)
	}
	if err := w.index.StagePutMany(ctx, []StageEntry{{Key: k.String(), Value: value}}); err != nil {
		return err
	}
	w.kickFlusher()
	return nil
}

// encodedSize returns the blob's Walrus encoded ("datacap") size to record.
// It prefers the value the publisher reported (reported, exact) and falls back
// to computing it from the unencoded length when the publisher omitted it
// (e.g. a deduplicated/already-certified blob, whose response carries no size).
func (w *WalrusDatastore) encodedSize(reported, unencoded int64) int64 {
	if reported > 0 {
		return reported
	}
	return EncodedBlobLength(unencoded, w.conf.NShards)
}

func (w *WalrusDatastore) Sync(ctx context.Context, prefix ds.Key) error {
	return nil
}

// Get serves k from the Postgres staging buffer if it has not been packed
// onto Walrus yet; otherwise it resolves the blob ID in Postgres and fetches
// the bytes from the Walrus aggregator. Returns ds.ErrNotFound when k is
// unknown.
//
// Staging is checked first deliberately: promotion moves a key atomically
// from staging to the index, so probing in that order can never miss a block
// mid-promote (the reverse order could miss both sides of the move).
func (w *WalrusDatastore) Get(ctx context.Context, k ds.Key) ([]byte, error) {
	if val, err := w.index.StageGet(ctx, k.String()); err == nil {
		return val, nil
	} else if err != ds.ErrNotFound {
		return nil, err
	}

	rec, err := w.index.Get(ctx, k.String())
	if err != nil {
		return nil, err
	}

	// A non-empty PatchID means the block is a quilt member; read it by patch.
	// Otherwise it is a plain/concatenated blob addressed by byte range.
	var data []byte
	if rec.PatchID != "" {
		data, err = w.client.ReadQuiltPatch(ctx, rec.PatchID)
	} else {
		data, err = w.client.ReadRange(ctx, rec.BlobID, rec.Offset, rec.Size)
	}
	if err != nil {
		if err == ErrBlobNotFound {
			return nil, ds.ErrNotFound
		}
		return nil, fmt.Errorf("walrusds: reading blob %s for key %s: %w", rec.BlobID, k.String(), err)
	}
	return data, nil
}

// Has reports whether k is present (staged or indexed), answered entirely
// from Postgres. Staging is probed first for the same mid-promote reason as
// Get.
func (w *WalrusDatastore) Has(ctx context.Context, k ds.Key) (bool, error) {
	_, err := w.index.StageGetSize(ctx, k.String())
	switch err {
	case nil:
		return true, nil
	case ds.ErrNotFound:
	default:
		return false, err
	}

	_, err = w.index.Get(ctx, k.String())
	switch err {
	case nil:
		return true, nil
	case ds.ErrNotFound:
		return false, nil
	default:
		return false, err
	}
}

// GetSize returns the stored size for k, answered entirely from Postgres.
func (w *WalrusDatastore) GetSize(ctx context.Context, k ds.Key) (int, error) {
	if sz, err := w.index.StageGetSize(ctx, k.String()); err == nil {
		return int(sz), nil
	} else if err != ds.ErrNotFound {
		return -1, err
	}

	rec, err := w.index.Get(ctx, k.String())
	if err != nil {
		return -1, err
	}
	return int(rec.Size), nil
}

// Delete removes k from the index and from the staging buffer (so an
// in-flight pack upload cannot resurrect it). It does not delete the
// underlying Walrus blob: on-chain deletion requires a Sui key and is out of
// scope for this datastore. The blob becomes unreferenced and eventually
// expires. Delete is idempotent.
func (w *WalrusDatastore) Delete(ctx context.Context, k ds.Key) error {
	return w.index.Delete(ctx, k.String())
}

// Query enumerates keys from Postgres. Orders and Filters are unsupported,
// matching the S3 datastore. When KeysOnly is false each value is fetched
// lazily from Walrus.
func (w *WalrusDatastore) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	if q.Orders != nil || q.Filters != nil {
		return nil, fmt.Errorf("walrusds: filters or orders are not supported")
	}

	prefix := normalizePrefix(q.Prefix)
	items, err := w.index.List(ctx, prefix, q.Limit, q.Offset)
	if err != nil {
		return nil, err
	}

	i := 0
	nextValue := func() (dsq.Result, bool) {
		if i >= len(items) {
			return dsq.Result{}, false
		}
		it := items[i]
		i++

		entry := dsq.Entry{Key: it.Key, Size: int(it.Size)}
		if !q.KeysOnly {
			val, err := w.Get(ctx, ds.NewKey(it.Key))
			if err != nil {
				return dsq.Result{Error: err}, true
			}
			entry.Value = val
			entry.Size = len(val)
		}
		return dsq.Result{Entry: entry}, true
	}

	return dsq.ResultsFromIterator(q, dsq.Iterator{
		Next:  nextValue,
		Close: func() error { return nil },
	}), nil
}

// Batch buffers Put/Delete operations and applies them concurrently on Commit.
func (w *WalrusDatastore) Batch(_ context.Context) (ds.Batch, error) {
	return &walrusBatch{
		w:   w,
		ops: make(map[string]batchOp),
	}, nil
}

// Close stops the renewal worker and closes the Postgres connection.
func (w *WalrusDatastore) Close() error {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
	return w.index.Close()
}

// normalizePrefix converts a datastore query prefix into the canonical key
// form used for prefix matching ("/blocks", "/", ...).
func normalizePrefix(prefix string) string {
	if prefix == "" {
		return "/"
	}
	return ds.NewKey(prefix).String()
}

type walrusBatch struct {
	w   *WalrusDatastore
	ops map[string]batchOp
}

type batchOp struct {
	val      []byte
	isDelete bool
}

func (b *walrusBatch) Put(ctx context.Context, k ds.Key, val []byte) error {
	b.ops[k.String()] = batchOp{val: val, isDelete: false}
	return nil
}

func (b *walrusBatch) Delete(ctx context.Context, k ds.Key) error {
	b.ops[k.String()] = batchOp{val: nil, isDelete: true}
	return nil
}

// blockEntry is one block staged for packing.
type blockEntry struct {
	key string
	val []byte
}

// Commit durably stages the buffered Puts in Postgres and applies the
// buffered Deletes, then nudges the pack flusher. Blocks are NOT uploaded to
// Walrus here: a single Commit rarely carries enough bytes to fill a pack
// (Kubo's importer commits every few MiB), and uploading per commit is what
// produced thousands of under-filled blobs each paying Walrus's fixed ~64 MB
// per-blob metadata overhead. Durability is preserved — the staging table is
// in the same Postgres that already holds the index — while the flusher
// accumulates staged blocks across commits into full PackTargetSize packs.
// Only a block too large to ever share a pack is uploaded directly.
func (b *walrusBatch) Commit(ctx context.Context) error {
	var (
		stage    []StageEntry
		oversize []blockEntry
		deletes  []string
	)
	for key, op := range b.ops {
		switch {
		case op.isDelete:
			deletes = append(deletes, key)
		case int64(len(op.val)) >= b.w.conf.PackTargetSize:
			oversize = append(oversize, blockEntry{key: key, val: op.val})
		default:
			stage = append(stage, StageEntry{Key: key, Value: op.val})
		}
	}

	var errs []string
	if err := b.w.index.StagePutMany(ctx, stage); err != nil {
		errs = append(errs, err.Error())
	}
	for _, e := range oversize {
		recs, err := b.w.uploadPack(ctx, []blockEntry{e})
		if err == nil {
			err = b.w.index.PutMany(ctx, recs)
		}
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(deletes) > 0 {
		if err := b.w.index.DeleteMany(ctx, deletes); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("walrusds: failed batch operation:\n%s", strings.Join(errs, "\n"))
	}

	if len(stage) > 0 {
		b.w.kickFlusher()
	}
	return nil
}

// kickFlusher nudges the pack flusher without blocking; a full channel means
// a check is already pending, which is just as good.
func (w *WalrusDatastore) kickFlusher() {
	select {
	case w.flushKick <- struct{}{}:
	default:
	}
}

// startFlushWorker launches the background goroutine that turns staged blocks
// into full-size Walrus packs. It runs on every node sharing the index; the
// staging claim lease keeps concurrent flushers from packing the same blocks,
// and a crashed flusher's lease simply expires (re-uploading is idempotent:
// Walrus blob IDs are content-derived).
func (w *WalrusDatastore) startFlushWorker(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(w.conf.PackFlushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				w.flushStaged(ctx)
			case <-w.flushKick:
				w.flushStaged(ctx)
			}
		}
	}()
}

// flushStaged uploads staged blocks as packs while a pack is "ripe": a full
// PackTargetSize of bytes has accumulated, no new blocks have arrived for
// PackIdleFlush (the ingest finished, so its tail should reach Walrus now),
// or the oldest staged block has waited PackMaxAge (backstop for continuous
// trickle ingest). It claims up to Workers packs at a time (leasing them against
// concurrent flushers) and uploads them in parallel; each pack is then
// atomically moved from staging to the index. On upload failure the claim is
// released for a later retry — the blocks stay durable in staging throughout.
// Peak memory is bounded by Workers × PackTargetSize.
func (w *WalrusDatastore) flushStaged(ctx context.Context) {
	maxCount := quiltMaxPatches
	if w.conf.DisableQuilt {
		maxCount = concatMaxBlocks
	}

	for {
		var packs [][]StageEntry
		for len(packs) < w.conf.Workers {
			st, err := w.index.StageStats(ctx)
			if err != nil {
				log.Printf("walrusds: staging stats failed: %v", err)
				break
			}
			if st.Count == 0 {
				break
			}
			aged := st.Oldest.Valid && time.Since(st.Oldest.Time) >= w.conf.PackMaxAge
			idle := st.Newest.Valid && time.Since(st.Newest.Time) >= w.conf.PackIdleFlush
			if st.Bytes < w.conf.PackTargetSize && !aged && !idle {
				break
			}

			entries, err := w.index.StageClaim(ctx, maxCount, w.conf.PackTargetSize, stageLease)
			if err != nil {
				log.Printf("walrusds: staging claim failed: %v", err)
				break
			}
			if len(entries) == 0 {
				break
			}
			packs = append(packs, entries)
		}
		if len(packs) == 0 {
			return
		}

		var (
			wg     sync.WaitGroup
			failed bool
			mu     sync.Mutex
		)
		for _, entries := range packs {
			entries := entries
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := w.flushOnePack(ctx, entries); err != nil {
					log.Printf("walrusds: %v", err)
					mu.Lock()
					failed = true
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if failed {
			// Back off until the next tick/kick instead of hot-looping on a
			// failing publisher; the released claims retry then.
			return
		}
	}
}

// flushOnePack uploads one claimed pack and promotes its members from staging
// to the index, releasing the claim on failure.
func (w *WalrusDatastore) flushOnePack(ctx context.Context, entries []StageEntry) error {
	pack := make([]blockEntry, len(entries))
	keys := make([]string, len(entries))
	for i, e := range entries {
		pack[i] = blockEntry{key: e.Key, val: e.Value}
		keys[i] = e.Key
	}

	recs, err := w.uploadPack(ctx, pack)
	if err == nil {
		err = w.index.PromoteStaged(ctx, recs)
	}
	if err != nil {
		if rerr := w.index.StageRelease(ctx, keys); rerr != nil {
			log.Printf("walrusds: releasing %d staged blocks failed: %v", len(keys), rerr)
		}
		return fmt.Errorf("flushing pack of %d blocks: %w", len(pack), err)
	}
	return nil
}

// uploadPack stores one pack of blocks on Walrus and returns the index rows
// to persist, keeping the Walrus-first ordering invariant (blob stored before
// index rows, so a failure leaks a recoverable blob rather than leaving a
// dangling row). A single-block pack is stored as a plain blob; a multi-block
// pack as a Walrus quilt, or as a legacy concatenated blob when DisableQuilt
// is set.
func (w *WalrusDatastore) uploadPack(ctx context.Context, pack []blockEntry) ([]KeyRecord, error) {
	switch {
	case len(pack) == 0:
		return nil, nil
	case len(pack) == 1:
		return w.uploadSingleBlock(ctx, pack[0])
	case w.conf.DisableQuilt:
		return w.uploadConcatPack(ctx, pack)
	default:
		return w.uploadQuiltPack(ctx, pack)
	}
}

// uploadSingleBlock stores one block as its own Walrus blob (a quilt of one
// is not worthwhile). The row has an empty PatchID and Offset 0.
func (w *WalrusDatastore) uploadSingleBlock(ctx context.Context, e blockEntry) ([]KeyRecord, error) {
	res, err := w.client.Store(ctx, e.val, w.conf.Epochs, w.conf.Deletable)
	if err != nil {
		return nil, fmt.Errorf("walrusds: storing block %s: %w", e.key, err)
	}
	return []KeyRecord{{
		Key: e.key,
		Rec: Record{
			BlobID:      res.BlobID,
			ObjectID:    res.ObjectID,
			Offset:      0,
			Size:        int64(len(e.val)),
			EncodedSize: w.encodedSize(res.EncodedSize, int64(len(e.val))),
			Cost:        res.Cost,
			Deletable:   w.conf.Deletable,
			EndEpoch:    res.EndEpoch,
			ExpiresAt:   w.expiry(),
		},
	}}, nil
}

// uploadQuiltPack stores a pack of blocks as a single Walrus quilt and
// returns each member's row with its QuiltPatchID. Member identifiers are the
// block's position in the pack, which the store response echoes back so we
// can map each returned patch ID to its key.
func (w *WalrusDatastore) uploadQuiltPack(ctx context.Context, pack []blockEntry) ([]KeyRecord, error) {
	parts := make([]QuiltPart, len(pack))
	keyByID := make(map[string]string, len(pack))
	sizeByID := make(map[string]int64, len(pack))
	for i, e := range pack {
		id := strconv.Itoa(i)
		parts[i] = QuiltPart{Identifier: id, Data: e.val}
		keyByID[id] = e.key
		sizeByID[id] = int64(len(e.val))
	}

	var totalUnencoded int64
	for _, e := range pack {
		totalUnencoded += int64(len(e.val))
	}

	res, err := w.client.StoreQuilt(ctx, parts, w.conf.Epochs, w.conf.Deletable)
	if err != nil {
		return nil, fmt.Errorf("walrusds: storing quilt of %d blocks: %w", len(pack), err)
	}
	if len(res.Patches) != len(pack) {
		return nil, fmt.Errorf("walrusds: quilt stored %d patches, expected %d", len(res.Patches), len(pack))
	}

	// The encoded size and cost are quilt-level (one Walrus blob backs every
	// member); record the same figures on each member row so per-file datacap
	// is summed over DISTINCT blob_id. The fallback for a deduplicated quilt is
	// approximate (it ignores the quilt's framing overhead), which is rare.
	encoded := w.encodedSize(res.EncodedSize, totalUnencoded)

	expiry := w.expiry()
	recs := make([]KeyRecord, 0, len(res.Patches))
	for _, p := range res.Patches {
		key, ok := keyByID[p.Identifier]
		if !ok {
			return nil, fmt.Errorf("walrusds: quilt returned unknown identifier %q", p.Identifier)
		}
		recs = append(recs, KeyRecord{
			Key: key,
			Rec: Record{
				BlobID:      res.QuiltID,
				ObjectID:    res.ObjectID,
				PatchID:     p.QuiltPatchID,
				Offset:      0,
				Size:        sizeByID[p.Identifier],
				EncodedSize: encoded,
				Cost:        res.Cost,
				Deletable:   w.conf.Deletable,
				EndEpoch:    res.EndEpoch,
				ExpiresAt:   expiry,
			},
		})
	}
	return recs, nil
}

// uploadConcatPack is the legacy packing scheme: concatenate the pack's
// blocks into one blob, upload it, and return each block's byte-range row.
// Used only when DisableQuilt is set.
func (w *WalrusDatastore) uploadConcatPack(ctx context.Context, pack []blockEntry) ([]KeyRecord, error) {
	var total int64
	for _, e := range pack {
		total += int64(len(e.val))
	}

	buf := make([]byte, 0, total)
	recs := make([]KeyRecord, len(pack))
	var offset int64
	for i, e := range pack {
		buf = append(buf, e.val...)
		recs[i] = KeyRecord{
			Key: e.key,
			Rec: Record{
				Offset: offset,
				Size:   int64(len(e.val)),
			},
		}
		offset += int64(len(e.val))
	}

	res, err := w.client.Store(ctx, buf, w.conf.Epochs, w.conf.Deletable)
	if err != nil {
		return nil, fmt.Errorf("walrusds: storing pack of %d blocks: %w", len(pack), err)
	}

	// One blob backs the whole concat pack: record its encoded size and cost on
	// every block's row (summed per file over DISTINCT blob_id). total is the
	// blob's exact unencoded length, so the fallback is exact here too.
	encoded := w.encodedSize(res.EncodedSize, total)

	expiry := w.expiry()
	for i := range recs {
		recs[i].Rec.BlobID = res.BlobID
		recs[i].Rec.ObjectID = res.ObjectID
		recs[i].Rec.EncodedSize = encoded
		recs[i].Rec.Cost = res.Cost
		recs[i].Rec.Deletable = w.conf.Deletable
		recs[i].Rec.EndEpoch = res.EndEpoch
		recs[i].Rec.ExpiresAt = expiry
	}
	return recs, nil
}
