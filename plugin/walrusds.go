package plugin

import (
	"fmt"
	"strings"
	"time"

	"github.com/ipfs/kubo/plugin"
	"github.com/ipfs/kubo/repo"
	"github.com/ipfs/kubo/repo/fsrepo"
	walrusds "github.com/lighthouse-web3/go-ds-s3-walrus"
)

// Plugins is the symbol Kubo looks up when loading this shared object.
var Plugins = []plugin.Plugin{
	&WalrusPlugin{},
}

// WalrusPlugin registers the "walrusds" datastore type: a Walrus-backed
// datastore that keeps its key -> blob index in a shared Postgres database.
type WalrusPlugin struct{}

func (WalrusPlugin) Name() string {
	return "walrus-datastore-plugin"
}

func (WalrusPlugin) Version() string {
	return "0.4.0"
}

func (WalrusPlugin) Init(*plugin.Environment) error {
	return nil
}

func (WalrusPlugin) DatastoreTypeName() string {
	return "walrusds"
}

// DatastoreConfigParser parses the "walrusds" mount config from the Kubo repo
// config.
//
// Required keys:
//   - "publisherURL"  (string; comma-separated for failover)
//   - "aggregatorURL" (string; comma-separated for failover)
//   - "postgresURL"   (string; database/sql connection string)
//
// Optional keys:
//   - "table"                 (string; index table, default "walrus_index")
//   - "epochs"                (number; storage epochs to buy, default 1)
//   - "nShards"               (number; Walrus committee shard count for the
//     encoded-size/datacap fallback, default 1000)
//   - "deletable"             (bool;   register blobs as deletable)
//   - "workers"               (number; batch concurrency, default 16)
//   - "maxOpenConns"          (number; Postgres pool size, default 32)
//   - "requestTimeoutSeconds" (number; per-attempt Walrus HTTP timeout)
//   - "maxRetries"            (number; retries per Walrus request)
//   - "packTargetSizeBytes"   (number; target packed-blob size, default 64 MiB)
//   - "disableQuilt"          (bool;   pack batches as concatenated blobs instead of quilts)
//   - "blobCacheBytes"        (number; LRU budget for range reads, default 256 MiB)
//   - "epochDurationSeconds"  (number; wall-clock length of one epoch; enables renewal)
//   - "renewIntervalSeconds"  (number; how often to scan for expiring blobs)
//   - "renewLeadSeconds"      (number; how far ahead of expiry to renew)
func (WalrusPlugin) DatastoreConfigParser() fsrepo.ConfigFromMap {
	return func(m map[string]interface{}) (fsrepo.DatastoreConfig, error) {
		publisher, err := requiredString(m, "publisherURL")
		if err != nil {
			return nil, err
		}
		aggregator, err := requiredString(m, "aggregatorURL")
		if err != nil {
			return nil, err
		}
		postgresURL, err := requiredString(m, "postgresURL")
		if err != nil {
			return nil, err
		}

		cfg := walrusds.Config{
			PublisherURLs:  splitURLs(publisher),
			AggregatorURLs: splitURLs(aggregator),
			PostgresURL:    postgresURL,
		}

		if v, ok := m["table"]; ok {
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("walrusds: table not a string")
			}
			cfg.Table = s
		}

		if v, ok := m["deletable"]; ok {
			b, ok := v.(bool)
			if !ok {
				return nil, fmt.Errorf("walrusds: deletable not a boolean")
			}
			cfg.Deletable = b
		}

		if v, ok := m["disableQuilt"]; ok {
			b, ok := v.(bool)
			if !ok {
				return nil, fmt.Errorf("walrusds: disableQuilt not a boolean")
			}
			cfg.DisableQuilt = b
		}

		if cfg.Epochs, err = optionalPositiveInt(m, "epochs"); err != nil {
			return nil, err
		}
		if cfg.NShards, err = optionalPositiveInt(m, "nShards"); err != nil {
			return nil, err
		}
	if cfg.Workers, err = optionalPositiveInt(m, "workers"); err != nil {
		return nil, err
	}
	if cfg.MaxOpenConns, err = optionalPositiveInt(m, "maxOpenConns"); err != nil {
		return nil, err
	}
	if cfg.MaxRetries, err = optionalPositiveInt(m, "maxRetries"); err != nil {
		return nil, err
	}

		packBytes, err := optionalPositiveInt(m, "packTargetSizeBytes")
		if err != nil {
			return nil, err
		}
		cfg.PackTargetSize = int64(packBytes)

		cacheBytes, err := optionalPositiveInt(m, "blobCacheBytes")
		if err != nil {
			return nil, err
		}
		cfg.BlobCacheBytes = int64(cacheBytes)

		secs, err := optionalPositiveInt(m, "requestTimeoutSeconds")
		if err != nil {
			return nil, err
		}
		cfg.RequestTimeout = time.Duration(secs) * time.Second

		if secs, err = optionalPositiveInt(m, "epochDurationSeconds"); err != nil {
			return nil, err
		}
		cfg.EpochDuration = time.Duration(secs) * time.Second

		if secs, err = optionalPositiveInt(m, "renewIntervalSeconds"); err != nil {
			return nil, err
		}
		cfg.RenewInterval = time.Duration(secs) * time.Second

		if secs, err = optionalPositiveInt(m, "renewLeadSeconds"); err != nil {
			return nil, err
		}
		cfg.RenewLead = time.Duration(secs) * time.Second

		return &WalrusConfig{cfg: cfg}, nil
	}
}

func requiredString(m map[string]interface{}, key string) (string, error) {
	v, ok := m[key].(string)
	if !ok || v == "" {
		return "", fmt.Errorf("walrusds: no %s specified", key)
	}
	return v, nil
}

// optionalPositiveInt parses an optional JSON number that, if present, must be
// a positive integer. Missing keys yield 0 so the package defaults apply.
func optionalPositiveInt(m map[string]interface{}, key string) (int, error) {
	v, ok := m[key]
	if !ok {
		return 0, nil
	}
	f, ok := v.(float64)
	if !ok {
		return 0, fmt.Errorf("walrusds: %s not a number", key)
	}
	n := int(f)
	switch {
	case n <= 0:
		return 0, fmt.Errorf("walrusds: %s <= 0: %f", key, f)
	case float64(n) != f:
		return 0, fmt.Errorf("walrusds: %s is not an integer: %f", key, f)
	}
	return n, nil
}

func splitURLs(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// WalrusConfig adapts walrusds.Config to fsrepo.DatastoreConfig.
type WalrusConfig struct {
	cfg walrusds.Config
}

// DiskSpec uniquely identifies where this datastore's data lives, for Kubo's
// repo fingerprinting. It deliberately omits the Postgres connection string
// (which carries credentials) and runtime-only knobs; the publisher,
// aggregator and table are enough to detect a backing-store change.
func (wc *WalrusConfig) DiskSpec() fsrepo.DiskSpec {
	table := wc.cfg.Table
	if table == "" {
		table = "walrus_index"
	}
	return fsrepo.DiskSpec{
		"publisherURL":  strings.Join(wc.cfg.PublisherURLs, ","),
		"aggregatorURL": strings.Join(wc.cfg.AggregatorURLs, ","),
		"table":         table,
	}
}

// Create instantiates the WalrusDatastore. The repo path is ignored: durable
// state lives in Postgres and on Walrus, not under the repo root.
func (wc *WalrusConfig) Create(path string) (repo.Datastore, error) {
	return walrusds.NewWalrusDatastore(wc.cfg)
}
