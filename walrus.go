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
	"strings"
	"sync"
	"time"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

const (
	defaultWorkers        = 100
	defaultEpochs         = 1
	defaultRequestTimeout = 60 * time.Second
	defaultMaxRetries     = 3
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
	Workers int

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
	if conf.RequestTimeout <= 0 {
		conf.RequestTimeout = defaultRequestTimeout
	}
	if conf.MaxRetries < 0 {
		conf.MaxRetries = defaultMaxRetries
	}

	ctx, cancel := context.WithCancel(context.Background())

	index, err := newPostgresIndex(ctx, conf.PostgresURL, conf.Table)
	if err != nil {
		cancel()
		return nil, err
	}

	client := NewClient(ClientConfig{
		PublisherURLs:  conf.PublisherURLs,
		AggregatorURLs: conf.AggregatorURLs,
		RequestTimeout: conf.RequestTimeout,
		MaxRetries:     conf.MaxRetries,
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
	data, err := w.client.Read(ctx, rec.BlobID)
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

func (b *walrusBatch) Commit(ctx context.Context) error {
	numJobs := len(b.ops)
	if numJobs == 0 {
		return nil
	}

	jobs := make(chan func() error, numJobs)
	results := make(chan error, numJobs)

	numWorkers := b.numWorkers
	if numWorkers <= 0 {
		numWorkers = defaultWorkers
	}
	if numJobs < numWorkers {
		numWorkers = numJobs
	}

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	defer wg.Wait()
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for j := range jobs {
				results <- j()
			}
		}()
	}

	for key, op := range b.ops {
		k := ds.NewKey(key)
		if op.isDelete {
			jobs <- func() error { return b.w.Delete(ctx, k) }
		} else {
			val := op.val
			jobs <- func() error { return b.w.Put(ctx, k, val) }
		}
	}
	close(jobs)

	var errs []string
	for i := 0; i < numJobs; i++ {
		if err := <-results; err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("walrusds: failed batch operation:\n%s", strings.Join(errs, "\n"))
	}
	return nil
}
