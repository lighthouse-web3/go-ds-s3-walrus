package walrusds

import (
	"container/list"
	"sync"
)

// blobCache is a small, byte-bounded LRU of whole Walrus blobs keyed by blob
// ID. With block-packing, many IPFS blocks share one blob, so a single cached
// packfile can satisfy many subsequent range reads without re-fetching from
// the aggregator. It also makes the "aggregator ignored our Range header"
// fallback cheap on repeated access.
//
// Only fully-downloaded blob bodies are cached (never partial range
// responses). Blobs larger than maxEntryBytes are not cached so a single huge
// packfile cannot evict everything.
type blobCache struct {
	mu           sync.Mutex
	ll           *list.List
	items        map[string]*list.Element
	curBytes     int64
	maxBytes     int64
	maxEntrySize int64
}

type cacheEntry struct {
	key  string
	data []byte
}

func newBlobCache(maxBytes int64) *blobCache {
	if maxBytes <= 0 {
		return nil
	}
	// Don't let one entry consume more than a quarter of the budget.
	maxEntry := maxBytes / 4
	if maxEntry <= 0 {
		maxEntry = maxBytes
	}
	return &blobCache{
		ll:           list.New(),
		items:        make(map[string]*list.Element),
		maxBytes:     maxBytes,
		maxEntrySize: maxEntry,
	}
}

func (c *blobCache) get(key string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(el)
	return el.Value.(*cacheEntry).data, true
}

func (c *blobCache) add(key string, data []byte) {
	if c == nil {
		return
	}
	size := int64(len(data))
	if size > c.maxEntrySize {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		old := el.Value.(*cacheEntry)
		c.curBytes += size - int64(len(old.data))
		old.data = data
		c.ll.MoveToFront(el)
	} else {
		el := c.ll.PushFront(&cacheEntry{key: key, data: data})
		c.items[key] = el
		c.curBytes += size
	}

	for c.curBytes > c.maxBytes {
		back := c.ll.Back()
		if back == nil {
			break
		}
		ent := back.Value.(*cacheEntry)
		c.ll.Remove(back)
		delete(c.items, ent.key)
		c.curBytes -= int64(len(ent.data))
	}
}
