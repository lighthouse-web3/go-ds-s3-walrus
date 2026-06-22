# Walrus Datastore for Kubo (`walrusds`)

An implementation of the IPFS/Kubo datastore interface backed by
[Walrus](https://www.walrus.xyz/) (a Sui-based decentralized blob store), using a **shared
Postgres database** as the durable `key -> blobId` index.

It is derived from [`go-ds-s3`](https://github.com/lighthouse-web3/go-ds-s3) and keeps the
same plugin shape, so it installs the same way.

**NOTE:** Plugins only work on Linux and MacOS at the moment. See
https://github.com/golang/go/issues/19282

## Why this design

Walrus is content-addressed (blobs are addressed by a content-derived blob ID, not an
arbitrary key), exposes no list/query API, and treats blobs as immutable with a finite,
epoch-based lifetime. A datastore therefore needs an external index. We keep that index in
Postgres so it is:

- **Shared** — separate upload and retrieval nodes point at the same database and see the
  same mapping.
- **Durable** — it does not live on the node's local disk, so disk failure does not lose data.
- **Recoverable** — enable Postgres Point-in-Time Recovery (PITR) for accidental-delete
  protection.

`Has`, `GetSize`, and `Query` are answered entirely from Postgres (no Walrus round-trip);
only `Get` fetches bytes from the Walrus aggregator.

```
Kubo ──ds.Datastore──▶ walrusds ──┬── bytes ───────────▶ Walrus publisher / aggregator
                                   └── key→blobId+meta ─▶ Postgres (walrus_index table)
```

## Index schema

Created automatically on first start:

```sql
CREATE TABLE walrus_index (
  key         TEXT PRIMARY KEY,   -- ds.Key string, e.g. "/blocks/CIQ..."
  blob_id     TEXT NOT NULL,      -- Walrus blob ID (for a quilt, the quilt's own blob ID)
  patch_id    TEXT NOT NULL DEFAULT '', -- QuiltPatchID when the block is a quilt member; '' otherwise
  blob_offset BIGINT NOT NULL DEFAULT 0, -- byte offset within the blob (concat/plain rows only)
  size        BIGINT NOT NULL,    -- block length
  deletable   BOOLEAN NOT NULL DEFAULT FALSE,
  end_epoch   BIGINT NOT NULL DEFAULT 0,
  expires_at  TIMESTAMPTZ,        -- used by the renewal worker / tooling
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Block packing (Walrus Quilt).** To amortize Walrus's per-blob cost (Sui gas + WAL minimums +
erasure-coding overhead), `Batch.Commit` groups many IPFS blocks (up to `packTargetSizeBytes`,
default 64 MiB, and at most 666 blocks) and stores each group as a single Walrus
[**quilt**](https://docs.wal.app/docs/system-overview/quilt) — Walrus's native batch-storage
primitive, which can cut small-blob storage cost by >400x. Each `key` row records `blob_id` (the
quilt's own blob ID) and `patch_id` (its `QuiltPatchID`); `Get` reads just that member via the
aggregator's `by-quilt-patch-id` endpoint, so only the requested block transfers.

Set `disableQuilt: true` to fall back to the **legacy concat scheme** instead: blocks are
concatenated into one opaque blob and addressed by `blob_offset`/`size`, read back via HTTP
`Range` (with a cached whole-blob fallback). A single (non-batch) `Put`, or any group that ends
up with exactly one block, is always stored as a plain blob (`patch_id = ''`). Existing repos
upgrade transparently: the `patch_id`/`blob_offset` columns default to `''`/`0`, so legacy rows
keep working unchanged.

`packTargetSizeBytes` is a **ceiling, not a floor** — a small file uploads immediately as a
smaller blob and never waits to fill. The plugin can only pack the blocks delivered in one
`Batch.Commit`, which Kubo bounds by `Import.BatchMaxSize` (default ~8 MiB on Kubo <0.33;
configurable on v0.33+, where it is additionally divided by `runtime.NumCPU()`). To get large
packs, set `Import.BatchMaxSize ≈ packTargetSizeBytes × NumCPU` and raise `Import.BatchMaxNodes`.
Pairs well with raising the IPFS chunk size to 1 MiB (the max interoperable block size; clean on
Kubo v0.40+). Measure the realized ratio with
`SELECT count(*), count(DISTINCT blob_id) FROM walrus_index;`.

## Building and Installing

This plugin is **not pinned to a single Kubo release.** The `go.mod` carries a baseline
version, but the build is retargeted to whatever Kubo you point it at — so pick the
`KUBO_VERSION` you need and the tooling aligns the dependency graph to match.

Build the plugin with the _exact_ Go version used to build your Kubo binary, against the
matching Kubo version. Substitute the tag you want for `${KUBO_VERSION}` below (and use the
Go toolchain that Kubo's own `go.mod` requires for that tag — newer Kubo lines need newer Go):

```dockerfile
ARG KUBO_VERSION=v0.30.0

RUN git clone https://github.com/ipfs/kubo && \
    cd kubo && \
    git checkout ${KUBO_VERSION} && \
    go get github.com/lighthouse-web3/go-ds-s3-walrus/plugin@latest

RUN cd kubo && \
    echo "\nwalrusds github.com/lighthouse-web3/go-ds-s3-walrus/plugin 0" >> plugin/loader/preload_list && \
    go mod edit -require=github.com/lighthouse-web3/go-ds-s3-walrus@v0.0.0 && \
    go mod tidy && \
    make build && \
    cp cmd/ipfs/ipfs /usr/local/bin/ipfs
```

Notes:
- The preload `name` token (`walrusds`) is just a label; the import path is what matters.
- Kubo needs the plugin module both `require`d and `replace`d/`get`-resolved; the explicit
  `go mod edit -require=...@v0.0.0` + `go mod tidy` avoids the
  "is replaced but not required" build error.
- Pure Go (Postgres driver `lib/pq`); no CGO required.

To build/install the `.so` locally instead: `make install` (drops `walrusplugin.so` into
`$IPFS_PATH/plugins/go-ds-s3-walrus.so`). Retarget the Kubo version with the `IPFS_VERSION`
variable, which rewrites `go.mod`/`go.sum` to that release via `set-target.sh`:

```bash
make install IPFS_VERSION=v0.30.0       # build against a published Kubo tag
make install IPFS_VERSION=/path/to/kubo # build against a local Kubo checkout
```

## Provisioning Postgres

```sql
CREATE DATABASE walrusidx;
CREATE USER ipfs WITH PASSWORD 'CHANGE_ME';
GRANT ALL PRIVILEGES ON DATABASE walrusidx TO ipfs;
-- the walrus_index table is created automatically on first run
```

Recommended:
- Turn on **PITR / continuous backups** — this is what protects you from accidental deletes.
- Use TLS (`sslmode=require`) if Postgres is reachable over a network.

Connection string (standard `database/sql` + `lib/pq`):

```
postgres://ipfs:CHANGE_ME@db-host:5432/walrusidx?sslmode=require
```

> The `postgresURL` (with the password) is **not** written into the repo `datastore_spec`;
> only `publisherURL`, `aggregatorURL`, and `table` are used as the disk fingerprint.

## Configuration

In `$IPFS_DIR/config`, set the `/blocks` mount to `walrusds`:

```json
{
  "Datastore": {
    "Spec": {
      "mounts": [
        {
          "child": {
            "type": "walrusds",
            "publisherURL": "https://publisher.walrus-testnet.walrus.space",
            "aggregatorURL": "https://aggregator.walrus-testnet.walrus.space",
            "postgresURL": "postgres://ipfs:CHANGE_ME@127.0.0.1:5432/walrusidx? sslmode=disable",
            "table": "walrus_index",
            "epochs": 53,
            "deletable": false,
            "workers": 16
          },
          "mountpoint": "/blocks",
          "prefix": "walrus.datastore",
          "type": "measure"
        },
        {
          "child": { "type": "levelds", "path": "datastore", "compression": "none" },
          "mountpoint": "/",
          "prefix": "leveldb.datastore",
          "type": "measure"
        }
      ],
      "type": "mount"
    }
  }
}
```

Matching `$IPFS_DIR/datastore_spec` (brand-new repo only — **do not** do this on a repo with
existing data):

```json
{"mounts":[{"aggregatorURL":"https://aggregator.walrus-testnet.walrus.space","mountpoint":"/blocks","publisherURL":"https://publisher.walrus-testnet.walrus.space","table":"walrus_index"},{"mountpoint":"/","path":"datastore","type":"levelds"}],"type":"mount"}
```

Multiple nodes (e.g. an upload node and a retrieval node) share data by pointing the same
`postgresURL`, `publisherURL`, and `aggregatorURL` at all of them.

### Setting the config from the CLI / Dockerfile

If you configure the node from a Dockerfile or script (like the S3 plugin's
`ipfs config --json Datastore.Spec ...` approach), use these two commands after `ipfs init`.
They assume the following build-time variables are available:

```dockerfile
ARG WALRUS_PUBLISHER_URL=https://publisher.walrus-mainnet.walrus.space
ARG WALRUS_AGGREGATOR_URL=https://aggregator.walrus-mainnet.walrus.space
ARG WALRUS_POSTGRES_URL=postgres://ipfs:CHANGE_ME@db-host:5432/walrusidx?sslmode=require
ENV IPFS_PATH=/data/ipfs
```

Set `Datastore.Spec` (the live config):

```dockerfile
RUN ipfs config --json Datastore.Spec "{\"mounts\":[{\"child\":{\"type\":\"walrusds\",\"publisherURL\":\"${WALRUS_PUBLISHER_URL}\",\"aggregatorURL\":\"${WALRUS_AGGREGATOR_URL}\",\"postgresURL\":\"${WALRUS_POSTGRES_URL}\",\"table\":\"walrus_index\",\"epochs\":53},\"mountpoint\":\"/blocks\",\"prefix\":\"walrus.datastore\",\"type\":\"measure\"},{\"child\":{\"compression\":\"none\",\"path\":\"datastore\",\"type\":\"levelds\"},\"mountpoint\":\"/\",\"prefix\":\"leveldb.datastore\",\"type\":\"measure\"}],\"type\":\"mount\"}"
```

Overwrite `datastore_spec` (the on-disk fingerprint). **Only on a brand-new repo with no
data** — overwriting this on a populated repo orphans existing blocks:

```dockerfile
RUN echo "{\"mounts\":[{\"aggregatorURL\":\"${WALRUS_AGGREGATOR_URL}\",\"mountpoint\":\"/blocks\",\"publisherURL\":\"${WALRUS_PUBLISHER_URL}\",\"table\":\"walrus_index\"},{\"mountpoint\":\"/\",\"path\":\"datastore\",\"type\":\"levelds\"}],\"type\":\"mount\"}" > $IPFS_PATH/datastore_spec
```

> **Critical:** the `datastore_spec` entry for `/blocks` must contain exactly the datastore's
> `DiskSpec` keys — `publisherURL`, `aggregatorURL`, and `table` — and **must not** include
> `postgresURL` (it carries credentials and is deliberately excluded from the fingerprint).
> If the spec doesn't match what the plugin computes, Kubo refuses to start with a
> "datastore configuration does not match what is on disk" error.
>
> **Key order matters.** Kubo computes the expected spec by JSON-marshaling a Go `map`, which
> always emits keys in **alphabetical order**, and compares it against the raw bytes of this
> file. So every object's keys must be alphabetized: the `/blocks` mount is
> `aggregatorURL, mountpoint, publisherURL, table` (note `mountpoint` is injected by the mount
> wrapper and sorts in), and the root mount is `mountpoint, path, type`. The simplest way to
> avoid mistakes is to **let `ipfs init` generate `datastore_spec` for you** and only hand-write
> it when scripting a brand-new repo (as above), copying the exact string Kubo reports as the
> expected value in any mismatch error.

Notes:
- Run these after `ipfs init` and with `IPFS_PATH` set.
- Build-time `ARG`/`ENV` values are baked into the image. To avoid baking the Postgres
  password (and to keep `epochs`/endpoints flexible), prefer injecting these at container
  start instead — e.g. an entrypoint script that runs the same `ipfs config` command using
  runtime environment variables before `ipfs daemon`.

### Config keys

| Key | Required | Default | Description |
|---|---|---|---|
| `publisherURL` | yes | — | Walrus publisher (write) base URL(s). Comma-separated for failover. |
| `aggregatorURL` | yes | — | Walrus aggregator (read) base URL(s). Comma-separated for failover. |
| `postgresURL` | yes | — | `database/sql` connection string for the shared index. |
| `table` | no | `walrus_index` | Index table name. |
| `epochs` | no | `1` | Storage epochs to purchase per blob. **Set this high** (see below). |
| `deletable` | no | `false` | Register blobs as deletable on Walrus. |
| `workers` | no | `16` | Concurrency for `Batch().Commit()` (packfile uploads run in parallel). Peak upload memory ≈ `workers × packTargetSizeBytes`, so raise this together with host RAM (and `maxOpenConns`). |
| `maxOpenConns` | no | `32` | Upper bound on the Postgres connection pool. Keep it ≥ `workers` so committing packs does not starve on connections; an unbounded pool can exhaust Postgres' `max_connections`. |
| `packTargetSizeBytes` | no | `67108864` (64 MiB) | **Ceiling** for a packed Walrus blob: blocks in one `Batch.Commit` are grouped up to this size (and ≤666 blocks) and stored as one quilt (a smaller commit uploads immediately — it never waits to fill). Packs >10 MiB require a self-hosted publisher/aggregator (public services cap requests near 10 MiB). The realized pack size is also bounded by Kubo's `Import.BatchMaxSize` (see below). |
| `disableQuilt` | no | `false` | When `true`, pack batches as legacy concatenated blobs (read by byte range) instead of Walrus quilts. Existing rows of either kind keep working regardless. |
| `blobCacheBytes` | no | `268435456` (256 MiB) | In-memory LRU budget for whole blobs, used to serve range reads of packed blocks. Per-entry cap is ¼ of this, so the default keeps a 64 MiB pack cacheable. A negative value disables the cache. |
| `requestTimeoutSeconds` | no | `60` | Per-attempt Walrus HTTP timeout. |
| `maxRetries` | no | `3` | Retries per Walrus request (exponential backoff). |
| `epochDurationSeconds` | no | `0` | Wall-clock length of one Walrus epoch. Enables the renewal worker when set. |
| `renewIntervalSeconds` | no | `0` | How often to scan for expiring blobs. Enables renewal when set. |
| `renewLeadSeconds` | no | one epoch | How far ahead of expiry to renew. |

## Durability: epochs and renewal (read this)

Walrus blobs are paid for a finite number of epochs and are **deleted when they expire** — if
that happens the IPFS block is gone even though the Postgres row survives. Stay durable by one of:

1. **Buying a long lifetime up front:** set `epochs` high enough for your retention window
   (mainnet epoch ≈ 14 days, so `epochs: 53` ≈ ~2 years).
2. **Enabling the automatic renewal worker:** set both `epochDurationSeconds` (the network's
   epoch length, e.g. `1209600` for ~14 days) and `renewIntervalSeconds` (e.g. `86400`). The
   worker finds blobs nearing `expires_at`, re-uploads their bytes for a fresh window, and
   updates the index. HTTP-only (no Sui key required). This renews **everything** — leave these
   unset if you only want to renew selected content.
3. **Renewing selected content yourself:** keep auto-renewal **off** (don't set
   `epochDurationSeconds`/`renewIntervalSeconds`) and run the bundled **JS scripts** against the
   content you choose — e.g. only paying users' CIDs. See below.

The default `epochs: 1` expires quickly — fine for testing, **not** for production.

### Operator scripts (`../js`)

Renewal, inspection, registration, and deletion are handled by Node.js scripts that talk straight
to Postgres + Walrus (no running Kubo node or Sui key required for renewal; the others use the Kubo
HTTP API to expand DAGs). They live in the [`js/`](../js) directory at the repo root (a sibling of
this Go module). Install once:

```bash
cd ../js && npm install   # needs Node >= 18
```

All scripts share these conventions: endpoints/DSN read from `WALRUS_PUBLISHER_URL`,
`WALRUS_AGGREGATOR_URL`, `WALRUS_POSTGRES_URL`, `WALRUS_TABLE`; the Kubo API defaults to
`http://127.0.0.1:5001` (`--api` / `IPFS_API_URL`); pass `--key-prefix /blocks` only if walrusds is
your **root** datastore rather than the `/blocks` mount; set `WALRUS_PG_SSL_NO_VERIFY=1` for
self-signed Postgres TLS.

#### Selective renewal (`renew.js`)

Renews only the CIDs (or raw datastore keys) you give it. Many keys can share one blob (a
quilt/pack), so each underlying blob is renewed at most once. A file is a DAG of many blocks, so
expand each root CID to all of its block CIDs first:

```bash
( echo "$ROOT_CID"; ipfs refs -r "$ROOT_CID" ) | node renew.js \
  --publisher "$WALRUS_PUBLISHER_URL" \
  --aggregator "$WALRUS_AGGREGATOR_URL" \
  --postgres "$WALRUS_POSTGRES_URL" \
  --epochs 53
# prints: requested=N missing=N blobs_renewed=N blobs_failed=N
```

Drive it from your billing system: collect the active (paying) users' root CIDs, expand them, and
feed the combined list. Anything you don't feed simply expires. Extra flags: `--input cids|keys`
(default `cids`), `--concurrency` (parallel blob renewals, default 8), `--dry-run`,
`--epoch-duration-seconds` (set `expires_at` on renewed rows), `--max-retries`.

### Inspecting a file (`js/inspect.js`)

Before renewing or forgetting, see exactly which keys and Walrus blobs a file occupies and when it
expires. `inspect.js` expands the file's DAG via the Kubo HTTP API, maps each block CID to its
datastore key, and looks each key up in the index:

```bash
node inspect.js "$ROOT_CID" \
  --postgres "$WALRUS_POSTGRES_URL" \
  --api http://127.0.0.1:5001
# per-block table: present?, size, blob_id (quilt), patch_id, key
# summary: blocks present/missing, distinct Walrus blobs, total size, expires_at range
```

Add `--dag` to also dump the root node (dag-json) and `--json` for machine-readable output.

### Registering files for reference-counted deletion (`js/register.js`)

`register.js` records which blocks belong to which file in an edge table
(`walrus_file_blocks(root_cid, key)`, created automatically) so deletion can be made safe by
**reference count** instead of by enumerating a keep-set. Run it whenever you add/pin a file (it is
idempotent — re-running inserts nothing new):

```bash
node register.js "$ROOT_CID" \
  --postgres "$WALRUS_POSTGRES_URL" --api http://127.0.0.1:5001
# prints: roots=N edges=N new_edges=N failed=N

# Backfill the table from everything currently pinned (do this once before
# first using --use-refcounts):
node register.js --from-pinned \
  --postgres "$WALRUS_POSTGRES_URL" --api http://127.0.0.1:5001
```

The edge table is the source of truth for who references a block, so `--use-refcounts` deletion is
only safe if **every file you keep has been registered**. Use `--dry-run` to preview, `--json` for
machine output, and `--edges-table` (`WALRUS_EDGES_TABLE`) to rename the table.

### Forgetting a file safely (`js/forget.js`)

To stop paying for a file you won't renew, delete its index rows. The catch: IPFS de-duplicates,
so two files can **share** a block (one row), and deleting it would corrupt the other file.
`forget.js` deletes only blocks **unique** to the file(s) you name, in one of two modes. It edits
Postgres only; freed Walrus blobs then expire on their own (a removed member's bytes linger inside a
shared quilt until the whole blob expires). It is a **dry run by default** — pass `--confirm` to
actually delete.

**Keep-set mode (default).** Expands the forgotten file's DAG, subtracts the DAG of every file you
keep (`--keep` / `--keep-pinned`), and deletes the difference. Stateless, but you must name what to
keep:

```bash
# Protect everything still pinned on the node, unpin the target, then delete its unique rows:
node forget.js "$ROOT_CID" \
  --postgres "$WALRUS_POSTGRES_URL" \
  --api http://127.0.0.1:5001 \
  --keep-pinned --unpin --confirm

# Or protect an explicit keep-list instead of the pinset:
node forget.js bafyTARGET --keep bafyKEEP1,bafyKEEP2 \
  --postgres "$WALRUS_POSTGRES_URL" --confirm
# prints: targets=… target_blocks=… shared_protected=… deletable=… rows_deleted=…
```

Always supply `--keep-pinned` and/or `--keep` so shared blocks are protected; with neither, nothing
is treated as protected and the tool warns you.

**Reference-count mode (`--use-refcounts`).** Uses the `register.js` edge table to decide safety: a
block is deleted only when **no other registered file** still references it. This scales without
enumerating keepers and does not need the Kubo node (keys come from the edge table). The whole
decide-and-delete runs as one atomic SQL statement:

```bash
node forget.js "$ROOT_CID" --use-refcounts \
  --postgres "$WALRUS_POSTGRES_URL" --confirm
# prints: mode=refcounts targets=… target_blocks=… shared_protected=… deletable=…
#         rows_deleted=… blobs_fully_freed=… blobs_still_shared=…
```

This is only safe if every file you keep was registered (see `register.js --from-pinned` for
backfill). For strict per-user accounting in either mode, keep each user's blocks in separate
`Batch.Commit`s (separate quilts), since renewal and blob expiry operate per backing blob.

## Limitations

- `Delete` removes the Postgres row only; it does not delete the blob on Walrus (on-chain
  deletion needs a Sui key, out of scope). Unreferenced blobs simply expire. With packing, a
  deleted block's bytes also remain inside its shared blob until the whole blob expires —
  reclaiming that space would need a future compaction/GC pass. To drop a whole file's rows
  without breaking files that share blocks, use [`forget.js`](../js/forget.js) (with
  [`register.js`](../js/register.js) for reference-counted deletion).
- Block-packing batches blocks written through `Batch().Commit()` (e.g. `ipfs add`). A single
  `Put` outside a batch still writes one blob per block, since a lone `Put` must be durable on
  return.
- Read efficiency on packed blobs depends on the aggregator honoring HTTP `Range`; otherwise
  the whole blob is fetched once and cached (`blobCacheBytes`).
- `Query` does not support `Orders` or `Filters` (same as the S3 datastore).

## License

MIT
