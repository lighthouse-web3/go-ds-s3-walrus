package walrusds_test

import (
	"context"
	"testing"

	datastore "github.com/ipfs/go-datastore"
	walrusds "github.com/lighthouse-web3/go-ds-s3" // update this to your actual module path
)

func TestIntegration_PutGet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	dir := t.TempDir()
	store, err := walrusds.NewWalrusDatastore(walrusds.Config{
		Epochs:        1,
		IndexPath:     dir,
	})
	if err != nil {
		t.Fatalf("NewWalrusDatastore: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	key := datastore.NewKey("/test/smoketest")
	val := []byte("hello walrus from ipfs")

	// 1. Put
	t.Log("--- PUT ---")
	if err := store.Put(ctx, key, val); err != nil {
		t.Fatalf("Put: %v", err)
	}
	t.Log("Put OK")

	// 2. Get
	t.Log("--- GET ---")
	got, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(val) {
		t.Fatalf("value mismatch: got %q want %q", got, val)
	}
	t.Log("Get OK, value matches")

	// 3. Has existing key
	t.Log("--- HAS (existing) ---")
	exists, err := store.Has(ctx, key)
	if err != nil {
		t.Fatalf("Has: %v", err)
	}
	if !exists {
		t.Fatal("Has returned false for existing key")
	}
	t.Log("Has OK")

	// 4. Get missing key → must return ErrNotFound
	t.Log("--- GET (missing key) ---")
	_, err = store.Get(ctx, datastore.NewKey("/does/not/exist"))
	if err != datastore.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
	t.Log("ErrNotFound OK")

	// 5. Delete
	t.Log("--- DELETE ---")
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// After delete, Has should return false
	exists, err = store.Has(ctx, key)
	if err != nil {
		t.Fatalf("Has after Delete: %v", err)
	}
	if exists {
		t.Fatal("key still exists after Delete")
	}
	t.Log("Delete OK")

	t.Log("=== ALL CHECKS PASSED ===")
}
