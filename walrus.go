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
	"strconv"
	"strings"
	"sync"
	"time"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

const (
	// defaultWorkers is the Batch.Commit() upload concurrency. Each in-flight
	// pack holds its bytes in memory while uploading, so peak memory scales
	// with workers × PackTargetSize; 16 keeps that bounded (≈1 GiB at the 64
	// MiB default) while still saturating a self-hosted publisher. Raise it
	// only alongside more RAM (and a matching maxOpenConns).
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
	// defaultPackTargetSize is the target (maximum) size of a packed Walrus
	// blob. It is a ceiling, not a floor: Batch.Commit flushes whatever it has
	// buffered, so a small file still uploads immediately as a smaller blob.
	// 64 MiB amortizes the per-blob Walrus cost (Sui gas + WAL minimums +
	// erasure-coding overhead) across many blocks while staying well within
	// memory and read-amplification limits. Public Walrus services cap
	// requests near 10 MiB, so packs this large require a self-hosted
	// publisher/aggregator (expected on mainnet anyway).
	defaultPackTargetSize = 64 << 20 // 64 MiB
	// defaultBlobCacheBytes is the default in-memory budget for caching whole
	// blobs to serve range reads of packed blocks. Sized so a single
	// default-target pack (64 MiB) stays cacheable (per-entry cap is a quarter
	// of the budget).
	defaultBlobCacheBytes = 256 << 20 // 256 MiB
	// quiltMaxPatches is the maximum number of member blobs in a single
	// QuiltV1. It is fixed by the protocol (derived from the 1000-shard
	// committee: 667 secondary columns minus one reserved for the index).
	// A pack destined to become a quilt may therefore hold at most this many
	// blocks regardless of PackTargetSize.
	quiltMaxPatches = 666
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
	// Deletable registers blobs as deletable on Walrus. Defaults to false.
	Deletable bool
	// Workers is the Batch.Commit() concurrency. Defaults to defaultWorkers.
	// Peak upload memory is roughly Workers × PackTargetSize, so raise both
	// Workers and host RAM together.
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
		conf:   conf,
		client: client,
		index:  index,
		cancel: cancel,
	}

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

// Put uploads value to Walrus and records the resulting blob ID and metadata
// in Postgres. The Walrus upload happens first: if the index write then
// fails, the blob exists but is unreferenced (a recoverable leak), which is
// strictly safer than an index row pointing at a blob that was never stored.
func (w *WalrusDatastore) Put(ctx context.Context, k ds.Key, value []byte) error {
	res, err := w.client.Store(ctx, value, w.conf.Epochs, w.conf.Deletable)
	if err != nil {
		return fmt.Errorf("walrusds: storing %s: %w", k.String(), err)
	}

	rec := Record{
		BlobID:    res.BlobID,
		ObjectID:  res.ObjectID,
		Offset:    0,
		Size:      int64(len(value)),
		Deletable: w.conf.Deletable,
		EndEpoch:  res.EndEpoch,
		ExpiresAt: w.expiry(),
	}
	if err := w.index.Put(ctx, k.String(), rec); err != nil {
		return err
	}
	return nil
}

func (w *WalrusDatastore) Sync(ctx context.Context, prefix ds.Key) error {
	return nil
}

// Get resolves the blob ID for k in Postgres and fetches the bytes from the
// Walrus aggregator. Returns ds.ErrNotFound when k is unknown to the index.
func (w *WalrusDatastore) Get(ctx context.Context, k ds.Key) ([]byte, error) {
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

// Has reports whether k is present, answered entirely from Postgres.
func (w *WalrusDatastore) Has(ctx context.Context, k ds.Key) (bool, error) {
	_, err := w.index.Get(ctx, k.String())
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
	rec, err := w.index.Get(ctx, k.String())
	if err != nil {
		return -1, err
	}
	return int(rec.Size), nil
}

// Delete removes the index entry for k. It does not delete the underlying
// Walrus blob: on-chain deletion requires a Sui key and is out of scope for
// this datastore. The blob becomes unreferenced and eventually expires.
// Delete is idempotent.
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
		w:          w,
		ops:        make(map[string]batchOp),
		numWorkers: w.conf.Workers,
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
	w          *WalrusDatastore
	ops        map[string]batchOp
	numWorkers int
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

// Commit groups the buffered Puts into packed Walrus blobs (concatenating
// blocks up to PackTargetSize and uploading each pack as a single blob), then
// applies the buffered Deletes. Packs are uploaded concurrently; each pack
// keeps the Walrus-first ordering invariant: the blob is stored before its
// index rows are written, so a failure leaks a recoverable blob rather than
// leaving a dangling index row.
func (b *walrusBatch) Commit(ctx context.Context) error {
	var (
		puts    []blockEntry
		deletes []string
	)
	for key, op := range b.ops {
		if op.isDelete {
			deletes = append(deletes, key)
		} else {
			puts = append(puts, blockEntry{key: key, val: op.val})
		}
	}

	maxPerPack := 0
	if !b.w.conf.DisableQuilt {
		maxPerPack = quiltMaxPatches
	}
	packs := buildPacks(puts, b.w.conf.PackTargetSize, maxPerPack)

	jobs := make([]func() error, 0, len(packs)+1)
	for _, pack := range packs {
		pack := pack
		jobs = append(jobs, func() error { return b.w.storePack(ctx, pack) })
	}
	if len(deletes) > 0 {
		jobs = append(jobs, func() error { return b.w.index.DeleteMany(ctx, deletes) })
	}
	if len(jobs) == 0 {
		return nil
	}

	numWorkers := b.numWorkers
	if numWorkers <= 0 {
		numWorkers = defaultWorkers
	}
	if len(jobs) < numWorkers {
		numWorkers = len(jobs)
	}

	jobCh := make(chan func() error, len(jobs))
	results := make(chan error, len(jobs))
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for j := range jobCh {
				results <- j()
			}
		}()
	}
	for _, j := range jobs {
		jobCh <- j
	}
	close(jobCh)
	wg.Wait()
	close(results)

	var errs []string
	for err := range results {
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("walrusds: failed batch operation:\n%s", strings.Join(errs, "\n"))
	}
	return nil
}

// buildPacks greedily groups blocks into packs no larger than target bytes and,
// when maxCount > 0, no more than maxCount blocks each (the quilt member limit).
// A single block bigger than target gets its own pack (it cannot be split).
func buildPacks(puts []blockEntry, target int64, maxCount int) [][]blockEntry {
	if target <= 0 {
		target = defaultPackTargetSize
	}
	var (
		packs   [][]blockEntry
		cur     []blockEntry
		curSize int64
	)
	for _, e := range puts {
		sz := int64(len(e.val))
		overSize := curSize+sz > target
		overCount := maxCount > 0 && len(cur) >= maxCount
		if len(cur) > 0 && (overSize || overCount) {
			packs = append(packs, cur)
			cur = nil
			curSize = 0
		}
		cur = append(cur, e)
		curSize += sz
	}
	if len(cur) > 0 {
		packs = append(packs, cur)
	}
	return packs
}

// storePack persists one pack of blocks, keeping the Walrus-first ordering
// invariant (blob stored before index rows). A single-block pack is stored as
// a plain blob; a multi-block pack is stored as a Walrus quilt unless quilting
// is disabled, in which case it falls back to the legacy concatenated blob.
func (w *WalrusDatastore) storePack(ctx context.Context, pack []blockEntry) error {
	switch {
	case len(pack) == 0:
		return nil
	case len(pack) == 1:
		return w.storeSingleBlock(ctx, pack[0])
	case w.conf.DisableQuilt:
		return w.storeConcatPack(ctx, pack)
	default:
		return w.storeQuiltPack(ctx, pack)
	}
}

// storeSingleBlock stores one block as its own Walrus blob (a quilt of one is
// not worthwhile). The row has an empty PatchID and Offset 0.
func (w *WalrusDatastore) storeSingleBlock(ctx context.Context, e blockEntry) error {
	res, err := w.client.Store(ctx, e.val, w.conf.Epochs, w.conf.Deletable)
	if err != nil {
		return fmt.Errorf("walrusds: storing block %s: %w", e.key, err)
	}
	rec := Record{
		BlobID:    res.BlobID,
		ObjectID:  res.ObjectID,
		Offset:    0,
		Size:      int64(len(e.val)),
		Deletable: w.conf.Deletable,
		EndEpoch:  res.EndEpoch,
		ExpiresAt: w.expiry(),
	}
	return w.index.Put(ctx, e.key, rec)
}

// storeQuiltPack stores a pack of blocks as a single Walrus quilt and records
// each member's QuiltPatchID in the index in one transaction. Member
// identifiers are the block's position in the pack, which the store response
// echoes back so we can map each returned patch ID to its key.
func (w *WalrusDatastore) storeQuiltPack(ctx context.Context, pack []blockEntry) error {
	parts := make([]QuiltPart, len(pack))
	keyByID := make(map[string]string, len(pack))
	sizeByID := make(map[string]int64, len(pack))
	for i, e := range pack {
		id := strconv.Itoa(i)
		parts[i] = QuiltPart{Identifier: id, Data: e.val}
		keyByID[id] = e.key
		sizeByID[id] = int64(len(e.val))
	}

	res, err := w.client.StoreQuilt(ctx, parts, w.conf.Epochs, w.conf.Deletable)
	if err != nil {
		return fmt.Errorf("walrusds: storing quilt of %d blocks: %w", len(pack), err)
	}
	if len(res.Patches) != len(pack) {
		return fmt.Errorf("walrusds: quilt stored %d patches, expected %d", len(res.Patches), len(pack))
	}

	expiry := w.expiry()
	recs := make([]KeyRecord, 0, len(res.Patches))
	for _, p := range res.Patches {
		key, ok := keyByID[p.Identifier]
		if !ok {
			return fmt.Errorf("walrusds: quilt returned unknown identifier %q", p.Identifier)
		}
		recs = append(recs, KeyRecord{
			Key: key,
			Rec: Record{
				BlobID:    res.QuiltID,
				ObjectID:  res.ObjectID,
				PatchID:   p.QuiltPatchID,
				Offset:    0,
				Size:      sizeByID[p.Identifier],
				Deletable: w.conf.Deletable,
				EndEpoch:  res.EndEpoch,
				ExpiresAt: expiry,
			},
		})
	}
	return w.index.PutMany(ctx, recs)
}

// storeConcatPack is the legacy packing scheme: concatenate the pack's blocks
// into one blob, upload it, and record each block's byte range. Used only when
// DisableQuilt is set.
func (w *WalrusDatastore) storeConcatPack(ctx context.Context, pack []blockEntry) error {
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
		return fmt.Errorf("walrusds: storing pack of %d blocks: %w", len(pack), err)
	}

	expiry := w.expiry()
	for i := range recs {
		recs[i].Rec.BlobID = res.BlobID
		recs[i].Rec.ObjectID = res.ObjectID
		recs[i].Rec.Deletable = w.conf.Deletable
		recs[i].Rec.EndEpoch = res.EndEpoch
		recs[i].Rec.ExpiresAt = expiry
	}
	return w.index.PutMany(ctx, recs)
}
