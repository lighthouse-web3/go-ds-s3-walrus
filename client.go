package walrusds

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ErrBlobNotFound is returned by the Walrus client when the aggregator has no
// blob for the requested blob ID.
var ErrBlobNotFound = errors.New("walrusds: blob not found")

// StoreResult is the subset of a Walrus publisher "store" response that we
// care about: the resulting blob ID and the epoch at which the blob's paid
// storage ends.
type StoreResult struct {
	BlobID   string
	EndEpoch uint64
}

// Client is a small, context-aware HTTP client for the Walrus publisher
// (write) and aggregator (read) HTTP APIs. It supports multiple endpoints
// for failover and retries transient failures with exponential backoff.
//
// We deliberately implement this directly instead of depending on a
// third-party SDK so that every request honours the caller's context
// (cancellation/timeouts) and so retry/failover behaviour is under our
// control.
type Client struct {
	publishers  []string
	aggregators []string
	http        *http.Client
	maxRetries  int
	cache       *blobCache
}

// ClientConfig configures a Walrus Client.
type ClientConfig struct {
	// PublisherURLs are Walrus publisher (write) base URLs. At least one is
	// required for Put to work.
	PublisherURLs []string
	// AggregatorURLs are Walrus aggregator (read) base URLs. At least one is
	// required for Get to work.
	AggregatorURLs []string
	// RequestTimeout bounds a single HTTP attempt. Zero means no per-attempt
	// timeout beyond the caller's context.
	RequestTimeout time.Duration
	// MaxRetries is the number of additional attempts (per endpoint set) on
	// transient failures. Zero means a single attempt.
	MaxRetries int
	// BlobCacheBytes is the byte budget for the in-memory LRU of whole blobs
	// used to satisfy range reads of packed blocks. Zero disables the cache.
	BlobCacheBytes int64
}

// NewClient builds a Walrus client from the supplied configuration.
func NewClient(conf ClientConfig) *Client {
	return &Client{
		publishers:  normalizeURLs(conf.PublisherURLs),
		aggregators: normalizeURLs(conf.AggregatorURLs),
		http:        &http.Client{Timeout: conf.RequestTimeout},
		maxRetries:  conf.MaxRetries,
		cache:       newBlobCache(conf.BlobCacheBytes),
	}
}

func normalizeURLs(in []string) []string {
	out := make([]string, 0, len(in))
	for _, u := range in {
		u = strings.TrimRight(strings.TrimSpace(u), "/")
		if u != "" {
			out = append(out, u)
		}
	}
	return out
}

// Store uploads value to a Walrus publisher, keeping it alive for the given
// number of epochs. When deletable is true the blob is registered as
// deletable so it can later be removed on-chain.
func (c *Client) Store(ctx context.Context, value []byte, epochs int, deletable bool) (StoreResult, error) {
	if len(c.publishers) == 0 {
		return StoreResult{}, errors.New("walrusds: no publisher URLs configured")
	}

	q := url.Values{}
	if epochs > 0 {
		q.Set("epochs", strconv.Itoa(epochs))
	}
	if deletable {
		q.Set("deletable", "true")
	}

	var lastErr error
	for _, base := range c.publishers {
		endpoint := base + "/v1/blobs"
		if enc := q.Encode(); enc != "" {
			endpoint += "?" + enc
		}

		res, err := c.doWithRetry(ctx, func() (*http.Response, error) {
			req, rerr := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(value))
			if rerr != nil {
				return nil, rerr
			}
			req.Header.Set("Content-Type", "application/octet-stream")
			req.ContentLength = int64(len(value))
			return c.http.Do(req)
		})
		if err != nil {
			lastErr = err
			continue
		}

		result, perr := parseStoreResponse(res)
		if perr != nil {
			lastErr = perr
			continue
		}
		return result, nil
	}
	return StoreResult{}, fmt.Errorf("walrusds: store failed on all publishers: %w", lastErr)
}

// Read fetches the bytes of the blob identified by blobID from a Walrus
// aggregator. It returns ErrBlobNotFound if the aggregator returns 404.
func (c *Client) Read(ctx context.Context, blobID string) ([]byte, error) {
	if len(c.aggregators) == 0 {
		return nil, errors.New("walrusds: no aggregator URLs configured")
	}

	var lastErr error
	for _, base := range c.aggregators {
		endpoint := base + "/v1/blobs/" + url.PathEscape(blobID)

		res, err := c.doWithRetry(ctx, func() (*http.Response, error) {
			req, rerr := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
			if rerr != nil {
				return nil, rerr
			}
			return c.http.Do(req)
		})
		if err != nil {
			lastErr = err
			continue
		}

		data, rerr := readBlobResponse(res)
		if rerr != nil {
			if errors.Is(rerr, ErrBlobNotFound) {
				return nil, ErrBlobNotFound
			}
			lastErr = rerr
			continue
		}
		return data, nil
	}
	return nil, fmt.Errorf("walrusds: read failed on all aggregators: %w", lastErr)
}

// ReadRange fetches the bytes [offset, offset+length) of the blob identified
// by blobID. It first issues an HTTP Range request so only the requested
// block travels over the wire. If the aggregator ignores the Range header and
// returns the whole blob (HTTP 200), the full body is cached and sliced
// locally, so packed blocks remain cheap to read even without range support.
func (c *Client) ReadRange(ctx context.Context, blobID string, offset, length int64) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}
	if len(c.aggregators) == 0 {
		return nil, errors.New("walrusds: no aggregator URLs configured")
	}

	if full, ok := c.cache.get(blobID); ok {
		return sliceBlob(full, offset, length)
	}

	rangeHdr := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)

	var lastErr error
	for _, base := range c.aggregators {
		endpoint := base + "/v1/blobs/" + url.PathEscape(blobID)

		res, err := c.doWithRetry(ctx, func() (*http.Response, error) {
			req, rerr := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
			if rerr != nil {
				return nil, rerr
			}
			req.Header.Set("Range", rangeHdr)
			return c.http.Do(req)
		})
		if err != nil {
			lastErr = err
			continue
		}

		data, rerr := c.readRangeResponse(res, blobID, offset, length)
		if rerr != nil {
			if errors.Is(rerr, ErrBlobNotFound) {
				return nil, ErrBlobNotFound
			}
			lastErr = rerr
			continue
		}
		return data, nil
	}
	return nil, fmt.Errorf("walrusds: range read failed on all aggregators: %w", lastErr)
}

func (c *Client) readRangeResponse(res *http.Response, blobID string, offset, length int64) ([]byte, error) {
	defer res.Body.Close()
	switch {
	case res.StatusCode == http.StatusNotFound:
		return nil, ErrBlobNotFound
	case res.StatusCode == http.StatusPartialContent:
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}
		if int64(len(body)) != length {
			return nil, fmt.Errorf("walrusds: aggregator returned %d partial bytes, want %d", len(body), length)
		}
		return body, nil
	case res.StatusCode >= 200 && res.StatusCode < 300:
		// Range header ignored: the aggregator returned the whole blob.
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}
		c.cache.add(blobID, body)
		return sliceBlob(body, offset, length)
	default:
		body, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		return nil, fmt.Errorf("walrusds: aggregator returned %d: %s", res.StatusCode, strings.TrimSpace(string(body)))
	}
}

// sliceBlob returns a copy of full[offset:offset+length], clamped to the blob
// length. It copies so callers can't mutate cached blob bytes.
func sliceBlob(full []byte, offset, length int64) ([]byte, error) {
	if offset < 0 || offset > int64(len(full)) {
		return nil, fmt.Errorf("walrusds: offset %d out of range for %d-byte blob", offset, len(full))
	}
	end := offset + length
	if end > int64(len(full)) {
		end = int64(len(full))
	}
	out := make([]byte, end-offset)
	copy(out, full[offset:end])
	return out, nil
}

func readBlobResponse(res *http.Response) ([]byte, error) {
	defer res.Body.Close()
	switch {
	case res.StatusCode == http.StatusNotFound:
		return nil, ErrBlobNotFound
	case res.StatusCode >= 200 && res.StatusCode < 300:
		return io.ReadAll(res.Body)
	default:
		body, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		return nil, fmt.Errorf("walrusds: aggregator returned %d: %s", res.StatusCode, strings.TrimSpace(string(body)))
	}
}

// doWithRetry performs an HTTP attempt with exponential backoff on transient
// errors (network failures and 5xx / 429 responses).
func (c *Client) doWithRetry(ctx context.Context, attempt func() (*http.Response, error)) (*http.Response, error) {
	var lastErr error
	for i := 0; i <= c.maxRetries; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		res, err := attempt()
		if err != nil {
			lastErr = err
		} else if isRetryableStatus(res.StatusCode) {
			body, _ := io.ReadAll(io.LimitReader(res.Body, 512))
			res.Body.Close()
			lastErr = fmt.Errorf("walrusds: transient status %d: %s", res.StatusCode, strings.TrimSpace(string(body)))
		} else {
			return res, nil
		}

		if i < c.maxRetries {
			if err := sleepWithContext(ctx, backoff(i)); err != nil {
				return nil, err
			}
		}
	}
	return nil, lastErr
}

func isRetryableStatus(code int) bool {
	return code == http.StatusTooManyRequests || (code >= 500 && code <= 599)
}

func backoff(attempt int) time.Duration {
	base := 200 * time.Millisecond
	d := base * time.Duration(1<<attempt)
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	// add up to 25% jitter
	jitter := time.Duration(rand.Int63n(int64(d/4) + 1))
	return d + jitter
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// storeResponse mirrors the Walrus publisher JSON response. A successful
// upload returns either a "newlyCreated" object (first time the blob is
// seen) or "alreadyCertified" (the content already existed on Walrus).
type storeResponse struct {
	NewlyCreated *struct {
		BlobObject struct {
			BlobID  string `json:"blobId"`
			Size    int64  `json:"size"`
			Storage struct {
				StartEpoch uint64 `json:"startEpoch"`
				EndEpoch   uint64 `json:"endEpoch"`
			} `json:"storage"`
		} `json:"blobObject"`
	} `json:"newlyCreated"`
	AlreadyCertified *struct {
		BlobID   string `json:"blobId"`
		EndEpoch uint64 `json:"endEpoch"`
	} `json:"alreadyCertified"`
}

func parseStoreResponse(res *http.Response) (StoreResult, error) {
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		return StoreResult{}, fmt.Errorf("walrusds: publisher returned %d: %s", res.StatusCode, strings.TrimSpace(string(body)))
	}

	var sr storeResponse
	if err := json.NewDecoder(res.Body).Decode(&sr); err != nil {
		return StoreResult{}, fmt.Errorf("walrusds: decoding store response: %w", err)
	}

	switch {
	case sr.NewlyCreated != nil && sr.NewlyCreated.BlobObject.BlobID != "":
		return StoreResult{
			BlobID:   sr.NewlyCreated.BlobObject.BlobID,
			EndEpoch: sr.NewlyCreated.BlobObject.Storage.EndEpoch,
		}, nil
	case sr.AlreadyCertified != nil && sr.AlreadyCertified.BlobID != "":
		return StoreResult{
			BlobID:   sr.AlreadyCertified.BlobID,
			EndEpoch: sr.AlreadyCertified.EndEpoch,
		}, nil
	default:
		return StoreResult{}, errors.New("walrusds: store response missing blob ID")
	}
}
