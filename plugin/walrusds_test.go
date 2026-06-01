package plugin

import (
	"reflect"
	"testing"

	walrusds "github.com/lighthouse-web3/go-ds-s3"
)

func TestWalrusPluginDatastoreConfigParser(t *testing.T) {
	testcases := []struct {
		Name   string
		Input  map[string]interface{}
		Want   *WalrusConfig
		HasErr bool
	}{
		{
			Name: "required fields only - epochs defaults to 1",
			Input: map[string]interface{}{
				"aggregatorURL": "https://aggregator.testnet.walrus.space",
				"publisherURL":  "https://publisher.testnet.walrus.space",
				"indexPath":     "/var/walrus/index",
			},
			Want: &WalrusConfig{cfg: walrusds.Config{
				AggregatorURL: "https://aggregator.testnet.walrus.space",
				PublisherURL:  "https://publisher.testnet.walrus.space",
				IndexPath:     "/var/walrus/index",
				Epochs:        1,
			}},
		},
		{
			Name: "explicit epochs",
			Input: map[string]interface{}{
				"aggregatorURL": "https://agg",
				"publisherURL":  "https://pub",
				"indexPath":     "/var/walrus/index",
				"epochs":        5.0,
			},
			Want: &WalrusConfig{cfg: walrusds.Config{
				AggregatorURL: "https://agg",
				PublisherURL:  "https://pub",
				IndexPath:     "/var/walrus/index",
				Epochs:        5,
			}},
		},
		{
			Name:   "missing aggregatorURL",
			Input:  map[string]interface{}{"publisherURL": "p", "indexPath": "/i"},
			HasErr: true,
		},
		{
			Name:   "missing publisherURL",
			Input:  map[string]interface{}{"aggregatorURL": "a", "indexPath": "/i"},
			HasErr: true,
		},
		{
			Name:   "missing indexPath",
			Input:  map[string]interface{}{"aggregatorURL": "a", "publisherURL": "p"},
			HasErr: true,
		},
		{
			Name: "epochs not a number",
			Input: map[string]interface{}{
				"aggregatorURL": "a", "publisherURL": "p", "indexPath": "/i",
				"epochs": "five",
			},
			HasErr: true,
		},
		{
			Name: "epochs non-positive",
			Input: map[string]interface{}{
				"aggregatorURL": "a", "publisherURL": "p", "indexPath": "/i",
				"epochs": 0.0,
			},
			HasErr: true,
		},
		{
			Name: "epochs non-integer",
			Input: map[string]interface{}{
				"aggregatorURL": "a", "publisherURL": "p", "indexPath": "/i",
				"epochs": 1.5,
			},
			HasErr: true,
		},
		{
			Name: "explicit workers",
			Input: map[string]interface{}{
				"aggregatorURL": "https://agg",
				"publisherURL":  "https://pub",
				"indexPath":     "/var/walrus/index",
				"workers":       4.0,
			},
			Want: &WalrusConfig{cfg: walrusds.Config{
				AggregatorURL: "https://agg",
				PublisherURL:  "https://pub",
				IndexPath:     "/var/walrus/index",
				Epochs:        1,
				Workers:       4,
			}},
		},
		{
			Name: "workers not a number",
			Input: map[string]interface{}{
				"aggregatorURL": "a", "publisherURL": "p", "indexPath": "/i",
				"workers": "many",
			},
			HasErr: true,
		},
		{
			Name: "workers non-positive",
			Input: map[string]interface{}{
				"aggregatorURL": "a", "publisherURL": "p", "indexPath": "/i",
				"workers": 0.0,
			},
			HasErr: true,
		},
		{
			Name: "workers non-integer",
			Input: map[string]interface{}{
				"aggregatorURL": "a", "publisherURL": "p", "indexPath": "/i",
				"workers": 2.5,
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
		AggregatorURL: "https://agg",
		PublisherURL:  "https://pub",
		IndexPath:     "/var/walrus/index",
		Epochs:        7,
		Workers:       4,
	}}

	spec := wc.DiskSpec()

	want := map[string]string{
		"aggregatorURL": "https://agg",
		"publisherURL":  "https://pub",
		"indexPath":     "/var/walrus/index",
	}
	for k, v := range want {
		if got, ok := spec[k].(string); !ok || got != v {
			t.Errorf("DiskSpec[%q] = %v; want %q", k, spec[k], v)
		}
	}
	if _, ok := spec["epochs"]; ok {
		t.Errorf("DiskSpec must not include epochs (it does not change blob location)")
	}
	if _, ok := spec["workers"]; ok {
		t.Errorf("DiskSpec must not include workers (runtime-only knob)")
	}
}
