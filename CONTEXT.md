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
- **Block packing (v3 — Walrus Quilt):** `Batch.Commit` groups blocks into packs up to
  `PackTargetSize` (default **64 MiB**) **and** at most **666** blocks (the QuiltV1 member limit),
  then stores each pack as a single Walrus **quilt** via the publisher's `PUT /v1/quilts`
  (multipart, one file part per block; identifier = block's index in the pack). The store
  response returns the quilt's own `blobId` plus a `quiltPatchId` per member. Each index row
  records `blob_id` = quilt ID (for renewal grouping) and `patch_id` = `QuiltPatchID`. `Get`
  dispatches on `patch_id`: non-empty → read the member via the aggregator's
  `GET /v1/blobs/by-quilt-patch-id/{id}` (only that member's slivers transfer), empty → legacy
  byte-range read. A byte-bounded LRU (`BlobCacheBytes`, default **256 MiB**) caches patch bytes
  (keyed by patch ID) and whole blobs (keyed by blob ID). Quilt is the **native** equivalent of
  the old manual concat packing and is dramatically cheaper for small blobs (docs cite >400x for
  ~10 KiB files) because it shares the per-blob overhead (Sui storage object, gas,
  erasure-coding metadata) across all members.
  - Single (non-batch) `Put`, and any pack that ends up with exactly **one** block, still write
    one plain blob (`patch_id = ''`, `blob_offset = 0`) — a quilt of one isn't worth it.
  - **Legacy concat packing is retained** behind `disableQuilt: true` (`Config.DisableQuilt`):
    blocks are concatenated into one opaque blob and read back by byte range. Fully
    backward-compatible: pre-quilt rows (`patch_id = ''`, with a byte range) and pre-packing
    rows (`blob_offset = 0`, `size` = whole blob) keep working unchanged.
  - Quilts are **immutable** and quilt-level only: `delete`/`extend` can't target a single
    member. `QuiltPatchId` is composition-dependent (not content-addressed), so it changes if a
    block is repacked into a different quilt. Retrieval is one member per request (no bulk read).
- **Cross-commit staging (v4) — packs now fill regardless of Kubo's commit size.** The old
  design packed only what arrived in one `Batch.Commit`, and Kubo commits every few MiB (or per
  small file), so real workloads produced thousands of under-filled blobs — measured live:
  **1766 blobs at 7.2 blocks/blob avg**, each paying Walrus's fixed **~64 MB per-blob metadata
  floor** (encoded size ≈ 4.5×raw + ~64 MB/blob ⇒ 4.97 MiB of blocks billed as 9.31 GiB, ~68 MB
  × 147 blobs for one file). Fix: `Commit`/`Put` no longer upload; they durably write block
  bytes to a Postgres **staging table** (`<table>_staging`) and return — the durability contract
  holds because Postgres *is* the durable store. A background **flusher** (`PackFlushInterval`
  ticker + a kick after every commit) claims staged blocks FIFO once ≥ `PackTargetSize` bytes
  (or ≤666 for quilts) have accumulated — or once the oldest staged block is older than
  `PackMaxAge` (default 5 min) — and uploads full packs, up to `Workers` packs in parallel.
  Claims use a `leased_until` lease (15 min) so multiple nodes sharing the DB never pack the
  same blocks; a crashed flusher's lease expires and the claim is retried (safe: Walrus uploads
  are content-addressed/idempotent). `PromoteStaged` then atomically moves rows staging→index,
  inserting only keys still present in staging so a concurrent `Delete` wins (no resurrection).
  Reads probe **staging first, then index** (that order can't miss a block mid-promote);
  `Query`/`List` UNIONs both tables; `Delete` purges both. Oversized blocks
  (≥ `PackTargetSize`) skip staging and upload directly as plain blobs. Kubo's
  `Import.BatchMaxSize` no longer matters for pack size (any Kubo version fills 64 MiB packs);
  1 MiB chunking (`Import.UnixFSChunker=size-1048576`, Kubo v0.40+) still helps by reducing
  block count. Walrus max blob size is 13.6 GiB; sweet spot for packs remains ~32–64 MiB.
- **Epoch renewal — per distinct `blob_id`** (so a packed blob / quilt is renewed once, not once
  per block). Two mechanisms and two ways to drive them:
  - **Mechanisms:**
    1. *Extend (preferred, no data movement):* extend the blob's lifetime on-chain via the Walrus
       TS SDK (`executeExtendBlobTransaction`) using the blob's Sui **object id**. To make this
       possible the plugin now records `object_id` per row at store time
       (`newlyCreated.blobObject.id` from the publisher store/quilt response → `Record.ObjectID` →
       `object_id` column). Needs a funded Sui key (SUI gas + WAL) that **owns** the blob, and the
       blob must not be expired. Only `js/renew.js` does this (the Go side holds no key).
    2. *Re-upload (fallback, HTTP-only):* read the whole blob (`GET /v1/blobs/{id}`) and re-store
       the identical bytes (`PUT /v1/blobs`). Because Walrus blob IDs are content-derived, the
       re-uploaded quilt keeps the **same** quilt ID and member `QuiltPatchId`s, so `patch_id` rows
       stay valid and only `end_epoch`/`expires_at`/`object_id` change. A re-upload creates a new
       Sui object, so `UpdateBlobAfterRenewal` also rewrites `object_id`.
  - **Automatic (opt-in):** background worker (`renewal.go`), enabled only when
    `epochDurationSeconds` + `renewIntervalSeconds` are set. It scans `expires_at` and renews
    everything due **by re-upload** (deliberately HTTP-only, no Sui key). **Leave these unset to
    disable auto-renewal** (the common choice when renewing selectively, e.g. only paying users).
  - **Manual / external (default for selective renewal):** the Node.js script `js/renew.js`
    (`pg` + `multiformats` + `@mysten/walrus`/`@mysten/sui`). It expands each root CID to its whole
    block DAG itself (via the Kubo API), resolves blocks to distinct Walrus blobs, then per blob
    tries *extend* (when `--sui-key`/`SUI_PRIVATE_KEY` is set and `object_id` is present) and falls
    back to *re-upload* (unless `--no-fallback`). `--epochs N` means "add N epochs" for extend and
    "fresh N-epoch window" for re-upload. Extend txs run serially (one signer → avoid gas-coin
    conflicts); re-upload honours `--concurrency`. (The former Go `Renewer`/`cmd/walrus-renew` were
    removed in favor of these JS scripts, which live in `js/` — a sibling of this Go module.)
    CID→key mapping is base32 of the multihash with a configurable `--key-prefix` (default "",
    i.e. blocks datastore mounted at `/blocks`; use `/blocks` if walrusds is the root datastore).
  - **Packing caveat for per-user billing:** renewing one key renews its *whole* backing blob,
    including any other users' blocks packed in the same quilt/pack. For strict per-user
    accounting, keep each user's blocks in separate commits so they land in separate quilts.
- **Lifecycle / safe deletion (JS scripts in `js/`).** Besides `renew.js`: `inspect.js` shows a
  file's keys/blobs/expiry; `register.js` records `file_blocks(root_cid, key)` edges (idempotent,
  via `ipfs refs -r`); `forget.js` deletes a file's index rows **without corrupting shared files**.
  Because IPFS de-dups, two files can share a block (one key/row), so forget deletes only blocks
  unique to the forgotten file, in one of two modes: *keep-set* (subtract the DAGs of `--keep` /
  `--keep-pinned` files) or *reference-count* (`--use-refcounts`: delete a block only when no other
  registered file references it, via an atomic CTE that tests `root_cid <> ALL($targets)` so the
  orphan check is correct under Postgres' data-modifying-CTE snapshot semantics). The edge table is
  thus an index-level pinset; it must be backfilled (`register.js --from-pinned`) for refcount mode
  to be safe.
- **Per-blob billing / datacap (encoded size).** Walrus bills by a blob's *encoded* size
  (RedStuff erasure coding ~5x + a fixed ~64 MiB/blob metadata overhead on a 1000-shard
  committee), **not** the raw byte count IPFS reports. The plugin now records this per blob:
  the publisher store/quilt response's `storage.storageSize` (== `resourceOperation.encodedLength`)
  → `StoreResult.EncodedSize`/`QuiltStoreResult.EncodedSize` → `Record.EncodedSize` → `encoded_size`
  column, plus the WAL actually paid (`newlyCreated.cost`, FROST) → `cost` column. Both are
  **blob-level**: every row sharing a `blob_id` (all quilt members, all concat-pack blocks) gets
  the same numbers, so per-file datacap = sum over **DISTINCT `blob_id`**. For an
  already-certified (deduplicated) blob the response omits the size and pays nothing, so
  `encoded_size` is filled in by computing `EncodedBlobLength(unencoded, nShards)` (RS2 formula in
  `encoding.go`, verified exact: 17 B → 66,034,000 B) and `cost` is 0. `nShards` config defaults
  to `DefaultNShards` (1000). Per-file query (needs the `file_blocks` edge table from
  `register.js`):
  `SELECT SUM(encoded_size) FROM (SELECT DISTINCT wi.blob_id, wi.encoded_size FROM file_blocks fb JOIN walrus_index wi ON wi.key=fb.key WHERE fb.root_cid=$1) t;`
  Caveat: a quilt shared across users attributes the whole quilt's datacap to each file that uses
  it, so for strict per-user accounting keep each user's blocks in their own commit (→ own quilt),
  as already noted under packing.
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
  walrus.go      # WalrusDatastore: ds.Datastore + ds.Batching; Commit/Put stage to Postgres, background flusher packs staged blocks into full quilts (package walrusds, at root)
  client.go      # context-aware Walrus HTTP client (blob store/read, range read, quilt store + patch read, retries, failover)
  cache.go       # byte-bounded LRU of whole blobs / quilt patches to serve packed reads
  index.go       # Index interface + postgresIndex (auto-migrate, upsert, put-many, get, delete[-many], list, per-blob renewal; rows carry blob_id + object_id + patch_id) + staging table (stage put/get/claim-lease/release/promote)
  renewal.go     # opt-in background epoch-renewal worker + shared renewBlob core (per blob, re-upload)
  client_test.go # quilt multipart streaming tests (length invariant + httptest round-trip)
  plugin/
    walrusds.go        # WalrusPlugin + config parser + Plugins var (package plugin)
    walrusds_test.go   # config-parser + DiskSpec tests (passing)
    main/main.go       # buildmode=plugin entrypoint
  Makefile, set-target.sh, go.mod, README.md, CONTEXT.md, LICENSE, version.json

js/                # operator scripts, repo root (sibling of the Go module above)
  README.md                # install, config, usage, large-file notes
  common.js                # shared helpers (CID→key, normalizeCid, pg pool, IPFS API, edges table)
  renew.js                 # selective renewal: roots → DAG → distinct blobs → extend (Sui SDK) or re-upload → index
  inspect.js               # show a file's keys/blobs/patch-ids/expiry
  register.js              # record file_blocks(root_cid,key) edges (+ --from-pinned backfill)
  forget.js                # safe deletion: keep-set (default) or --use-refcounts
  .env.example             # dotenv template (Postgres/IPFS/Walrus/Sui config)
  package.json             # bins: walrus-renew/inspect/register/forget (pg + multiformats + @mysten/walrus + @mysten/sui)
```

Module path: `github.com/lighthouse-web3/go-ds-s3-walrus`.
Datastore type name registered with Kubo: `walrusds`.

## Status

- Implemented and compiling, **now including Walrus Quilt batch packing** (default) with the
  legacy concat packer behind `disableQuilt`, **plus cross-commit staging (v4)** so packs
  actually fill to `PackTargetSize` instead of one under-filled blob per Kubo commit.
  `go build ./...`, `go vet ./...`, and the tests pass against the baseline kubo (v0.30.0) and
  the build also links cleanly when preloaded into newer Kubo (verified against the v0.41.x
  line via the retarget flow above).
- **Not yet tested live** against a real Walrus endpoint + Postgres (no credentials available
  during development).

## Important caveats / things to verify next

1. **Walrus HTTP API shapes** — `client.go` codes the publisher blob-store response as
   `newlyCreated.blobObject` / `alreadyCertified`, blob read as `GET /v1/blobs/{blobId}`, the
   quilt store as `PUT /v1/quilts` (multipart) returning `{blobStoreResult, storedQuiltBlobs[]
   {identifier, quiltPatchId}}`, and quilt member read as
   `GET /v1/blobs/by-quilt-patch-id/{id}`. Verify against the actual publisher/aggregator you
   target; public gateway response shapes can vary, and public services cap requests at ~10 MiB
   so large quilts/packs require a self-hosted publisher+aggregator (which also must support the
   quilt endpoints — quilt needs a reasonably recent Walrus version).
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
   blocks, repack, let the old blob expire). Re-upload renewal carries any dead bytes along; the
   `renew.js` *extend* path leaves the blob untouched (so it keeps the same dead weight too).
6. **Pack sizing** — `packTargetSizeBytes` defaults to 64 MiB (requires a self-hosted
   publisher/aggregator; public services cap requests near 10 MiB). Bigger packs amortize the
   ~64 MB per-blob metadata floor further but carry more dead weight after deletes and cost
   more memory per in-flight upload (`Workers × PackTargetSize`). Pairs well with raising the
   IPFS chunk size to 1 MiB.
7. **Staging table operations** — `<table>_staging` churns (insert → claim → delete per block);
   monitor autovacuum/bloat under heavy ingest. Blocks not yet flushed live only in Postgres
   (still durable, served from staging on reads) for at most `packMaxAgeSeconds` + one flush;
   size Postgres accordingly (steady-state staging ≈ ingest rate × PackMaxAge, bounded below by
   PackTargetSize).

## How it was split from go-ds-s3

Originally added into the `go-ds-s3` repo as a second plugin, then moved here into its own
module so the two plugins don't share one repo. The `go-ds-s3` repo was restored to its
original S3-only state.
