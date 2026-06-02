package plugin

import (
	"reflect"
	"testing"
	"time"

	walrusds "github.com/lighthouse-web3/go-ds-s3-walrus"
)

func TestWalrusPluginDatastoreConfigParser(t *testing.T) {
	testcases := []struct {
		Name   string
		Input  map[string]interface{}
		Want   *WalrusConfig
		HasErr bool
	}{
		{
			Name: "required fields only",
			Input: map[string]interface{}{
				"publisherURL":  "https://pub",
				"aggregatorURL": "https://agg",
				"postgresURL":   "postgres://u:p@h:5432/db",
			},
			Want: &WalrusConfig{cfg: walrusds.Config{
				PublisherURLs:  []string{"https://pub"},
				AggregatorURLs: []string{"https://agg"},
				PostgresURL:    "postgres://u:p@h:5432/db",
			}},
		},
		{
			Name: "comma-separated endpoints split for failover",
			Input: map[string]interface{}{
				"publisherURL":  "https://pub1, https://pub2",
				"aggregatorURL": "https://agg1,https://agg2",
				"postgresURL":   "postgres://u:p@h:5432/db",
			},
			Want: &WalrusConfig{cfg: walrusds.Config{
				PublisherURLs:  []string{"https://pub1", "https://pub2"},
				AggregatorURLs: []string{"https://agg1", "https://agg2"},
				PostgresURL:    "postgres://u:p@h:5432/db",
			}},
		},
		{
			Name: "all optional fields",
			Input: map[string]interface{}{
				"publisherURL":          "https://pub",
				"aggregatorURL":         "https://agg",
				"postgresURL":           "postgres://u:p@h:5432/db",
				"table":                 "blocks_idx",
				"epochs":                12.0,
				"deletable":             true,
				"workers":               8.0,
				"maxRetries":            5.0,
				"requestTimeoutSeconds": 30.0,
				"epochDurationSeconds":  1209600.0,
				"renewIntervalSeconds":  3600.0,
				"renewLeadSeconds":      604800.0,
			},
			Want: &WalrusConfig{cfg: walrusds.Config{
				PublisherURLs:  []string{"https://pub"},
				AggregatorURLs: []string{"https://agg"},
				PostgresURL:    "postgres://u:p@h:5432/db",
				Table:          "blocks_idx",
				Epochs:         12,
				Deletable:      true,
				Workers:        8,
				MaxRetries:     5,
				RequestTimeout: 30 * time.Second,
				EpochDuration:  1209600 * time.Second,
				RenewInterval:  3600 * time.Second,
				RenewLead:      604800 * time.Second,
			}},
		},
		{
			Name:   "missing publisherURL",
			Input:  map[string]interface{}{"aggregatorURL": "a", "postgresURL": "p"},
			HasErr: true,
		},
		{
			Name:   "missing aggregatorURL",
			Input:  map[string]interface{}{"publisherURL": "p", "postgresURL": "p"},
			HasErr: true,
		},
		{
			Name:   "missing postgresURL",
			Input:  map[string]interface{}{"publisherURL": "p", "aggregatorURL": "a"},
			HasErr: true,
		},
		{
			Name: "epochs not a number",
			Input: map[string]interface{}{
				"publisherURL": "p", "aggregatorURL": "a", "postgresURL": "d",
				"epochs": "ten",
			},
			HasErr: true,
		},
		{
			Name: "epochs non-positive",
			Input: map[string]interface{}{
				"publisherURL": "p", "aggregatorURL": "a", "postgresURL": "d",
				"epochs": 0.0,
			},
			HasErr: true,
		},
		{
			Name: "epochs non-integer",
			Input: map[string]interface{}{
				"publisherURL": "p", "aggregatorURL": "a", "postgresURL": "d",
				"epochs": 1.5,
			},
			HasErr: true,
		},
		{
			Name: "deletable not a bool",
			Input: map[string]interface{}{
				"publisherURL": "p", "aggregatorURL": "a", "postgresURL": "d",
				"deletable": "yes",
			},
			HasErr: true,
		},
	}

	parser := WalrusPlugin{}.DatastoreConfigParser()
	for _, tc := range testcases {
		t.Run(tc.Name, func(t *testing.T) {
			cfg, err := parser(tc.Input)
			if err != nil {
				if !tc.HasErr {
					t.Fatalf("unexpected error: %s", err)
				}
				return
			}
			if tc.HasErr {
				t.Fatalf("expected error, got config: %+v", cfg)
			}
			got, ok := cfg.(*WalrusConfig)
			if !ok {
				t.Fatalf("wrong config type returned: %T", cfg)
			}
			if !reflect.DeepEqual(got, tc.Want) {
				t.Fatalf("got: %+v\nwant: %+v", got, tc.Want)
			}
		})
	}
}

func TestWalrusConfigDiskSpec(t *testing.T) {
	wc := &WalrusConfig{cfg: walrusds.Config{
		PublisherURLs:  []string{"https://pub"},
		AggregatorURLs: []string{"https://agg"},
		PostgresURL:    "postgres://u:secret@h:5432/db",
		Table:          "blocks_idx",
	}}

	spec := wc.DiskSpec()

	want := map[string]string{
		"publisherURL":  "https://pub",
		"aggregatorURL": "https://agg",
		"table":         "blocks_idx",
	}
	for k, v := range want {
		if got, ok := spec[k].(string); !ok || got != v {
			t.Errorf("DiskSpec[%q] = %v; want %q", k, spec[k], v)
		}
	}
	if _, ok := spec["postgresURL"]; ok {
		t.Error("DiskSpec must not include postgresURL (it carries credentials)")
	}
}
