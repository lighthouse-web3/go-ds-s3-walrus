// Package walrusds implements a Kubo (IPFS) datastore backed by Walrus,
// a decentralized storage network built on Sui.
//
// Walrus is content-addressed (blobs are addressed by blob ID, not by an
// arbitrary key), has no list/query API, and treats blobs as effectively
// immutable. To make Walrus usable as an IPFS datastore we keep a small
// local LevelDB "index" that maps IPFS keys → Walrus blob IDs. The index
// is the source of truth for what this node "knows about"; Walrus is the
// underlying byte store.
package walrusds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	ldb "github.com/ipfs/go-ds-leveldb"
	walrus "github.com/namihq/walrus-go"
)

const (
	defaultWorkers = 100
	defaultEpochs  = 1
)

var (
	_ ds.Datastore = (*WalrusBucket)(nil)
	_ ds.Batching  = (*WalrusBucket)(nil)
)

// Config holds the configuration needed to construct a WalrusBucket.
type Config struct {
	// AggregatorURL is the Walrus aggregator (read) endpoint, e.g.
	// "https://aggregator.testnet.walrus.space".
	AggregatorURL string
	// PublisherURL is the Walrus publisher (write) endpoint, e.g.
	// "https://publisher.testnet.walrus.space".
	PublisherURL string
	// Epochs is the number of storage epochs to persist new blobs for.
	// Defaults to 1 when zero.
	Epochs int
	// IndexPath is the on-disk directory used by the local LevelDB index
	// that maps IPFS keys to Walrus blob IDs.
	IndexPath string
	// Workers is the size of the goroutine pool used by Batch.Commit().
	// Defaults to defaultWorkers when zero.
	Workers int
}

// WalrusBucket is a ds.Datastore / ds.Batching implementation that stores
// values on Walrus and keeps a local LevelDB index of key → blob ID.
type WalrusBucket struct {
	Config
	walrus *walrus.Client
	index  ds.Datastore
}

// IndexEntry is the JSON-encoded record we store in the local index for
// each IPFS key. It maps the key to the Walrus blob ID that holds the data.
type IndexEntry struct {
	BlobID string `json:"blobId"`
}

// NewWalrusDatastore constructs a WalrusBucket. It opens the local LevelDB
// index at conf.IndexPath (creating it if necessary) and prepares a Walrus
// HTTP client pointed at the supplied aggregator and publisher URLs.
func NewWalrusDatastore(conf Config) (*WalrusBucket, error) {
	if conf.Epochs == 0 {
		conf.Epochs = defaultEpochs
	}
	if conf.Workers == 0 {
		conf.Workers = defaultWorkers
	}

	var client *walrus.Client
    if conf.AggregatorURL == "" && conf.PublisherURL == "" {
        client = walrus.NewClient() // use SDK defaults
    } else {
        client = walrus.NewClient(
            walrus.WithAggregatorURLs([]string{conf.AggregatorURL}),
            walrus.WithPublisherURLs([]string{conf.PublisherURL}),
        )
    }

	index, err := ldb.NewDatastore(conf.IndexPath, nil)
	if err != nil {
		return nil, fmt.Errorf("walrusds: failed to open level index at %q: %w", conf.IndexPath, err)
	}

	return &WalrusBucket{
		Config: conf,
		walrus: client,
		index:  index,
	}, nil
}

// Put writes value to Walrus and records the resulting blob ID under key in
// the local index. Put is not atomic: if the index write fails after a
// successful upload, the blob still exists on Walrus but this node will not
// be aware of it. Walrus has no delete API, so we accept that minor leak.
func (w *WalrusBucket) Put(ctx context.Context, k ds.Key, value []byte) error {
	resp, err := w.walrus.Store(value, &walrus.StoreOptions{Epochs: w.Epochs})
	if err != nil {
		return fmt.Errorf("walrusds: store failed for key %s: %w", k.String(), err)
	}

	blobID, err := extractBlobID(resp)
	if err != nil {
		return fmt.Errorf("walrusds: invalid store response for key %s: %w", k.String(), err)
	}

	encoded, err := json.Marshal(IndexEntry{BlobID: blobID})
	if err != nil {
		return fmt.Errorf("walrusds: failed to encode index entry for key %s: %w", k.String(), err)
	}

	if err := w.index.Put(ctx, k, encoded); err != nil {
		return fmt.Errorf("walrusds: failed to write index entry for key %s: %w", k.String(), err)
	}
	return nil
}

func (w *WalrusBucket) Sync(ctx context.Context, prefix ds.Key) error {
	return nil
}

// Get returns the value previously associated with k. It first consults the
// local index to obtain the blob ID and then fetches the bytes from Walrus.
// Returns ds.ErrNotFound if k is unknown to the local index.
func (w *WalrusBucket) Get(ctx context.Context, k ds.Key) ([]byte, error) {
	entry, err := w.lookup(ctx, k)
	if err != nil {
		return nil, err
	}
	data, err := w.walrus.Read(entry.BlobID, nil)
	if err != nil {
		return nil, fmt.Errorf("walrusds: read failed for blob %s (key %s): %w", entry.BlobID, k.String(), err)
	}
	return data, nil
}

func (w *WalrusBucket) Has(ctx context.Context, k ds.Key) (bool, error) {
	_, err := w.GetSize(ctx, k)
	switch err {
	case nil:
		return true, nil
	case ds.ErrNotFound:
		return false, nil
	default:
		return false, err
	}
}

// GetSize returns the size of the value associated with k. We prefer the
// cheap Walrus HEAD request, but transparently fall back to a full Read if
// the aggregator does not return a usable Content-Length.
func (w *WalrusBucket) GetSize(ctx context.Context, k ds.Key) (int, error) {
	entry, err := w.lookup(ctx, k)
	if err != nil {
		return -1, err
	}

	if md, herr := w.walrus.Head(entry.BlobID); herr == nil && md != nil && md.ContentLength >= 0 {
		return int(md.ContentLength), nil
	}

	data, rerr := w.walrus.Read(entry.BlobID, nil)
	if rerr != nil {
		return -1, fmt.Errorf("walrusds: failed to size blob %s (key %s): %w", entry.BlobID, k.String(), rerr)
	}
	return len(data), nil
}

// Delete removes the local index entry for k. It does NOT attempt to delete
// the underlying Walrus blob: Walrus blobs are public/immutable and there
// is no reliable delete API. Removing only the index makes the key
// effectively invisible to this IPFS node.
func (w *WalrusBucket) Delete(ctx context.Context, k ds.Key) error {
	if _, err := w.lookup(ctx, k); err != nil {
		if err == ds.ErrNotFound {
			return nil
		}
		return err
	}

	if err := w.index.Delete(ctx, k); err != nil {
		return fmt.Errorf("walrusds: failed to delete index entry %s: %w", k.String(), err)
	}
	return nil
}

// Query enumerates keys known to the local index. Walrus is never consulted
// for the listing itself; if the caller asked for values (KeysOnly == false)
// we fetch each value lazily via Get. Orders and Filters are not supported,
// matching the S3 implementation's behaviour.
func (w *WalrusBucket) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	if q.Orders != nil || q.Filters != nil {
		return nil, fmt.Errorf("walrusds: filters or orders are not supported")
	}

	indexQuery := dsq.Query{
		Prefix:   q.Prefix,
		Limit:    q.Limit,
		Offset:   q.Offset,
		KeysOnly: true,
	}
	indexResults, err := w.index.Query(ctx, indexQuery)
	if err != nil {
		return nil, fmt.Errorf("walrusds: index query failed: %w", err)
	}

	nextValue := func() (dsq.Result, bool) {
		res, ok := indexResults.NextSync()
		if !ok {
			return dsq.Result{}, false
		}
		if res.Error != nil {
			return dsq.Result{Error: res.Error}, true
		}
		entry := dsq.Entry{Key: res.Entry.Key}
		if !q.KeysOnly {
			val, err := w.Get(ctx, ds.NewKey(res.Entry.Key))
			if err != nil {
				return dsq.Result{Error: err}, true
			}
			entry.Value = val
			entry.Size = len(val)
		}
		return dsq.Result{Entry: entry}, true
	}

	return dsq.ResultsFromIterator(q, dsq.Iterator{
		Next: nextValue,
		Close: func() error {
			return indexResults.Close()
		},
	}), nil
}

// Batch returns a ds.Batch that buffers Put/Delete operations and applies
// them concurrently via a worker pool on Commit.
func (w *WalrusBucket) Batch(_ context.Context) (ds.Batch, error) {
	return &walrusBatch{
		s:          w,
		ops:        make(map[string]batchOp),
		numWorkers: w.Workers,
	}, nil
}

// Close shuts down the local LevelDB index. Walrus does not require an
// explicit client shutdown.
func (w *WalrusBucket) Close() error {
	return w.index.Close()
}

func (w *WalrusBucket) lookup(ctx context.Context, k ds.Key) (*IndexEntry, error) {
	raw, err := w.index.Get(ctx, k)
	if err != nil {
		if err == ds.ErrNotFound {
			return nil, ds.ErrNotFound
		}
		return nil, fmt.Errorf("walrusds: failed to read index for key %s: %w", k.String(), err)
	}
	var entry IndexEntry
	if err := json.Unmarshal(raw, &entry); err != nil {
		return nil, fmt.Errorf("walrusds: corrupt index entry for key %s: %w", k.String(), err)
	}
	if entry.BlobID == "" {
		return nil, fmt.Errorf("walrusds: index entry for key %s has empty blob ID", k.String())
	}
	return &entry, nil
}

func extractBlobID(resp *walrus.StoreResponse) (string, error) {
	if resp == nil {
		return "", errors.New("nil store response")
	}
	if resp.NewlyCreated != nil && resp.NewlyCreated.BlobObject.BlobID != "" {
		return resp.NewlyCreated.BlobObject.BlobID, nil
	}
	if resp.AlreadyCertified != nil && resp.AlreadyCertified.BlobID != "" {
		return resp.AlreadyCertified.BlobID, nil
	}
	if resp.Blob.BlobID != "" {
		return resp.Blob.BlobID, nil
	}
	return "", errors.New("store response missing blob ID")
}

type walrusBatch struct {
	s          *WalrusBucket
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
			worker(jobs, results)
		}()
	}

	for key, op := range b.ops {
		k := ds.NewKey(key)
		if op.isDelete {
			jobs <- b.newDeleteJob(ctx, k)
		} else {
			jobs <- b.newPutJob(ctx, k, op.val)
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

func (b *walrusBatch) newPutJob(ctx context.Context, k ds.Key, value []byte) func() error {
	return func() error {
		return b.s.Put(ctx, k, value)
	}
}

func (b *walrusBatch) newDeleteJob(ctx context.Context, k ds.Key) func() error {
	return func() error {
		return b.s.Delete(ctx, k)
	}
}

func worker(jobs <-chan func() error, results chan<- error) {
	for j := range jobs {
		results <- j()
	}
}
