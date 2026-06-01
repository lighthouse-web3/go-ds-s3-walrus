// Package plugin registers the Walrus datastore as a Kubo (IPFS) plugin.
//
// It is the bridge between Kubo's datastore plugin contract and the
// walrusds package: it parses the relevant fields from the repo config map,
// produces a fsrepo.DiskSpec for repo fingerprinting, and constructs a
// concrete *walrusds.WalrusBucket on demand.
package plugin

import (
	"fmt"

	"github.com/ipfs/kubo/plugin"
	"github.com/ipfs/kubo/repo"
	"github.com/ipfs/kubo/repo/fsrepo"
	walrusds "github.com/lighthouse-web3/go-ds-s3"
)

// Plugins is the symbol Kubo looks up when loading this shared object.
var Plugins = []plugin.Plugin{
	&WalrusPlugin{},
}

// WalrusPlugin satisfies plugin.PluginDatastore for the "walrusds" type.
type WalrusPlugin struct{}

func (WalrusPlugin) Name() string {
	return "walrus-datastore-plugin"
}

func (WalrusPlugin) Version() string {
	return "0.0.1"
}

func (WalrusPlugin) Init(*plugin.Environment) error {
	return nil
}

func (WalrusPlugin) DatastoreTypeName() string {
	return "walrusds"
}

// DatastoreConfigParser returns a parser that extracts the Walrus datastore
// configuration from the Kubo repo config map.
//
// Required keys:
//   - "aggregatorURL" (string)
//   - "publisherURL"  (string)
//   - "indexPath"     (string)
//
// Optional keys:
//   - "epochs"  (number, defaults to 1, must be a positive integer)
//   - "workers" (number, batch concurrency; defaults to the walrusds package
//     default when zero/omitted; must be a positive integer if specified)
func (WalrusPlugin) DatastoreConfigParser() fsrepo.ConfigFromMap {
	return func(m map[string]interface{}) (fsrepo.DatastoreConfig, error) {
		aggregatorURL, ok := m["aggregatorURL"].(string)
		if !ok || aggregatorURL == "" {
			return nil, fmt.Errorf("walrusds: aggregatorURL is required")
		}

		publisherURL, ok := m["publisherURL"].(string)
		if !ok || publisherURL == "" {
			return nil, fmt.Errorf("walrusds: publisherURL is required")
		}

		indexPath, ok := m["indexPath"].(string)
		if !ok || indexPath == "" {
			return nil, fmt.Errorf("walrusds: indexPath is required")
		}

		epochs := 1
		if v, ok := m["epochs"]; ok {
			epochsf, ok := v.(float64)
			if !ok {
				return nil, fmt.Errorf("walrusds: epochs is not a number")
			}
			epochs = int(epochsf)
			switch {
			case epochs <= 0:
				return nil, fmt.Errorf("walrusds: epochs must be > 0: %f", epochsf)
			case float64(epochs) != epochsf:
				return nil, fmt.Errorf("walrusds: epochs is not an integer: %f", epochsf)
			}
		}

		workers := 0
		if v, ok := m["workers"]; ok {
			workersf, ok := v.(float64)
			if !ok {
				return nil, fmt.Errorf("walrusds: workers is not a number")
			}
			workers = int(workersf)
			switch {
			case workers <= 0:
				return nil, fmt.Errorf("walrusds: workers must be > 0: %f", workersf)
			case float64(workers) != workersf:
				return nil, fmt.Errorf("walrusds: workers is not an integer: %f", workersf)
			}
		}

		return &WalrusConfig{
			cfg: walrusds.Config{
				AggregatorURL: aggregatorURL,
				PublisherURL:  publisherURL,
				IndexPath:     indexPath,
				Epochs:        epochs,
				Workers:       workers,
			},
		}, nil
	}
}

// WalrusConfig wraps the walrusds.Config so it satisfies
// fsrepo.DatastoreConfig.
type WalrusConfig struct {
	cfg walrusds.Config
}

// DiskSpec returns the fields that uniquely identify this datastore on
// disk. Kubo uses this to detect config drift; we exclude epochs and workers
// because they only affect runtime behaviour, not where blobs live.
func (wc *WalrusConfig) DiskSpec() fsrepo.DiskSpec {
	return fsrepo.DiskSpec{
		"aggregatorURL": wc.cfg.AggregatorURL,
		"publisherURL":  wc.cfg.PublisherURL,
		"indexPath":     wc.cfg.IndexPath,
	}
}

// Create instantiates the underlying WalrusBucket. The path argument is the
// repo path supplied by Kubo and is intentionally ignored: our on-disk
// state lives at the configured IndexPath, not under the repo root.
func (wc *WalrusConfig) Create(path string) (repo.Datastore, error) {
	return walrusds.NewWalrusDatastore(wc.cfg)
}
