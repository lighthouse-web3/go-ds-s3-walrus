# Project Context & Memory

This file captures the design discussion and decisions behind this repo so work can continue
seamlessly. Read this first when resuming.

## Goal

Build a Kubo (IPFS) datastore plugin that stores block bytes on **Walrus** (Sui-based
decentralized blob storage) instead of S3, using a **shared Postgres database** as the
durable `key -> blobId` index. Derived from `go-ds-s3`.

## Why this architecture (the discussion)

We worked through several options before landing here:

1. **Walrus as a drop-in S3 replacement?** No. The IPFS datastore interface needs prefix
   listing, mutability, and cheap small writes — none of which Walrus provides natively
   (content-addressed, immutable, no list API, finite epoch lifetime).
2. **Do it at the "pinning layer" instead?** Considered (one CAR blob per pinned DAG, mapped
   `CID -> blobId`). Cleaner economically, but the user wants it at the datastore layer.
3. **MinIO with a Walrus backend?** Not possible — MinIO removed gateway/backend modes in
   2022. The viable shape (S3↔Walrus bridge) is the same idea we built, just inside the
   plugin instead of a separate service.
4. **Index store choice:**
   - Local SQLite/LevelDB → rejected: not shareable across nodes, dies with the disk.
   - DynamoDB → viable (managed, PITR) but per-request cost + sharded-key design needed for
     prefix listing.
   - **Postgres → chosen.** Shared across upload + retrieval nodes, durable, PITR, trivial
     1-writer/N-reader. User will operate it themselves.
5. **Role split (uploader/retriever)?** Decided **no** — every node has full read/write.

## Key decisions (locked)

- Bytes on **Walrus** via HTTP publisher (write) / aggregator (read).
- Index in **Postgres** via `database/sql` + `github.com/lib/pq` (pure Go, no CGO).
- Index record: `{ blob_id, blob_offset, size, deletable, end_epoch, expires_at }` per `ds.Key`.
- `Has` / `GetSize` / `Query` answered from Postgres only; only `Get` hits Walrus.
- Walrus-first, index-second write ordering (a failure leaks a recoverable blob, never a
  dangling index row).
- **Block packing (v2):** `Batch.Commit` concatenates blocks into packfiles up to
  `PackTargetSize` (default **64 MiB**) and uploads each as one Walrus blob. Many keys can share
  a `blob_id`; each row stores the block's byte range `[blob_offset, blob_offset+size)`. `Get`
  fetches only that range via an HTTP `Range` request, with a whole-blob fallback + a
  byte-bounded LRU cache (`BlobCacheBytes`, default **256 MiB**, sized so a 64 MiB pack stays
  cacheable). Single (non-batch) `Put` still writes one blob per block (`blob_offset = 0`).
  Fully backward-compatible: legacy rows are `blob_offset = 0`, `size` = whole blob.
- **`PackTargetSize` is a ceiling, not a floor:** `Commit` flushes whatever is buffered, so a
  small file uploads immediately as a smaller blob — it never waits to fill. The plugin can only
  pack what arrives in one `Commit`; we deliberately do **not** coalesce across commits because
  the datastore contract requires a block to be durable when `Commit` returns (buffering past it
  would risk data loss on crash). The realized pack size is therefore bounded by Kubo's
  `Import.BatchMaxSize` (hardcoded ~8 MiB on Kubo <0.33; configurable on v0.33+, divided by
  `runtime.NumCPU()` in the buffered DAG). To hit 64 MiB packs via `ipfs add`, set
  `Import.BatchMaxSize ≈ 64 MiB × NumCPU`, `Import.BatchMaxNodes` high, and
  `Import.UnixFSChunker = size-1048576` (1 MiB). Sweet spot is ~32–64 MiB (diminishing per-blob
  cost savings vs. rising memory/read-amplification beyond that); Walrus max blob size is
  13.6 GiB.
- **Epoch renewal via re-upload** (HTTP-only, no Sui key): background worker re-uploads blobs
  near `expires_at`, **per distinct `blob_id`** (so a packed blob is renewed once, not once per
  block), then repoints all member rows. Enabled when `epochDurationSeconds` +
  `renewIntervalSeconds` are set.
- `deletable` defaults to `false`. `Delete` is logical (removes index row only); no on-chain
  Walrus deletion (would need a Sui key).
- `postgresURL` kept OUT of `DiskSpec` (it carries credentials).
- **Kubo-version-agnostic build.** The plugin is not tied to one Kubo release: `go.mod` keeps
  a baseline (currently `kubo v0.30.0`, `go 1.22`), and the build is retargeted to any Kubo
  via `set-target.sh` (driven by the `IPFS_VERSION` make var) or, when preloading, an explicit
  `go mod edit -require=...@v0.0.0` + `go mod tidy`. Use the Go toolchain the chosen Kubo tag
  requires (newer Kubo lines, e.g. v0.41.x, need Go 1.26+). For block-packing to actually engage
  via `ipfs add`, target **kubo v0.33+** (configurable `Import.BatchMaxSize`/`BatchMaxNodes`) —
  **v0.40+** preferred (clean 1 MiB chunks). On kubo <0.33 the write-batch is hardcoded ~8 MiB,
  so packs stay small regardless of `packTargetSizeBytes`.

## Repo layout

```
go-ds-s3-walrus/
  walrus.go      # WalrusDatastore: ds.Datastore + ds.Batching; packing in Batch.Commit (package walrusds, at root)
  client.go      # context-aware Walrus HTTP client (store/read, range read, retries, failover)
  cache.go       # byte-bounded LRU of whole blobs to serve range reads of packed blocks
  index.go       # Index interface + postgresIndex (auto-migrate, upsert, put-many, get, delete[-many], list, per-blob renewal)
  renewal.go     # background epoch-renewal worker (per blob)
  plugin/
    walrusds.go        # WalrusPlugin + config parser + Plugins var (package plugin)
    walrusds_test.go   # config-parser + DiskSpec tests (passing)
    main/main.go       # buildmode=plugin entrypoint
  Makefile, set-target.sh, go.mod, README.md, CONTEXT.md, LICENSE, version.json
```

Module path: `github.com/lighthouse-web3/go-ds-s3-walrus`.
Datastore type name registered with Kubo: `walrusds`.

## Status

- Implemented and compiling. `go build ./...`, `go vet ./...`, and the plugin config tests
  pass against the baseline kubo (v0.30.0) and the build also links cleanly when preloaded
  into newer Kubo (verified against the v0.41.x line via the retarget flow above).
- **Not yet tested live** against a real Walrus endpoint + Postgres (no credentials available
  during development).

## Important caveats / things to verify next

1. **Walrus HTTP API shapes** — `client.go` codes the publisher store response as
   `newlyCreated.blobObject` / `alreadyCertified` and read as `GET /v1/blobs/{blobId}`.
   Verify against the actual publisher/aggregator you target; public gateway response shapes
   can vary.
2. **Live integration test** — add an env-guarded test exercising Put/Get/Has/GetSize/Delete/
   Query against a real Postgres + Walrus testnet.
3. **Epoch math for renewal** — `epochDurationSeconds` is operator-supplied wall-clock; the
   real expiry is on-chain/epoch-based, so keep a safety margin (`renewLeadSeconds`).
4. **Aggregator Range support** — `Get` issues HTTP `Range` requests so only the requested
   block transfers. If the target aggregator ignores `Range` and returns the whole blob, the
   client caches it (LRU) and slices locally — correct, but verify Range is honored for best
   read efficiency on packed blobs.
5. **Delete fragments packed blobs** — logical `Delete` drops the index row but the bytes stay
   inside the shared blob; reclaiming space needs a future compaction/GC pass (read live
   blocks, repack, let the old blob expire). Renewal currently re-uploads whole packfiles
   including any dead bytes.
6. **Pack sizing** — `packTargetSizeBytes` defaults to 8 MiB to stay under the 10 MiB public
   limit. Self-hosted publishers/aggregators can raise it; bigger packs amortize cost more but
   waste more transfer on partial reads (without Range) and carry more dead weight after
   deletes. Pairs well with raising the IPFS chunk size to 1 MiB.

## How it was split from go-ds-s3

Originally added into the `go-ds-s3` repo as a second plugin, then moved here into its own
module so the two plugins don't share one repo. The `go-ds-s3` repo was restored to its
original S3-only state.
