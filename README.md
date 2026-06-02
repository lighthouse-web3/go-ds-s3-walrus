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
  blob_id     TEXT NOT NULL,      -- Walrus blob ID (shared across packed blocks)
  blob_offset BIGINT NOT NULL DEFAULT 0, -- byte offset of this block within the blob
  size        BIGINT NOT NULL,    -- block length; the block is blob[offset : offset+size]
  deletable   BOOLEAN NOT NULL DEFAULT FALSE,
  end_epoch   BIGINT NOT NULL DEFAULT 0,
  expires_at  TIMESTAMPTZ,        -- used by the renewal worker
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Block packing.** To amortize Walrus's per-blob cost (Sui gas + WAL minimums + erasure-coding
overhead), `Batch.Commit` concatenates many IPFS blocks into one Walrus blob (a "packfile") up
to `packTargetSizeBytes` (default 64 MiB), so several `key` rows share a `blob_id` and are
distinguished by `blob_offset`/`size`. `Get` reads only its slice via an HTTP `Range` request
(falling back to a cached whole-blob slice if the aggregator ignores `Range`). Existing repos
upgrade transparently: the `blob_offset` column is added with default `0`, so legacy
one-blob-per-block rows keep working unchanged.

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
| `workers` | no | `100` | Concurrency for `Batch().Commit()` (packfile uploads run in parallel). |
| `packTargetSizeBytes` | no | `67108864` (64 MiB) | **Ceiling** for a packed Walrus blob: blocks in one `Batch.Commit` are concatenated up to this size and stored as one blob (a smaller commit uploads immediately as a smaller blob — it never waits to fill). Packs >10 MiB require a self-hosted publisher/aggregator (public services cap requests near 10 MiB). The realized pack size is also bounded by Kubo's `Import.BatchMaxSize` (see below). |
| `blobCacheBytes` | no | `268435456` (256 MiB) | In-memory LRU budget for whole blobs, used to serve range reads of packed blocks. Per-entry cap is ¼ of this, so the default keeps a 64 MiB pack cacheable. A negative value disables the cache. |
| `requestTimeoutSeconds` | no | `60` | Per-attempt Walrus HTTP timeout. |
| `maxRetries` | no | `3` | Retries per Walrus request (exponential backoff). |
| `epochDurationSeconds` | no | `0` | Wall-clock length of one Walrus epoch. Enables the renewal worker when set. |
| `renewIntervalSeconds` | no | `0` | How often to scan for expiring blobs. Enables renewal when set. |
| `renewLeadSeconds` | no | one epoch | How far ahead of expiry to renew. |

## Durability: epochs and renewal (read this)

Walrus blobs are paid for a finite number of epochs and are **deleted when they expire** — if
that happens the IPFS block is gone even though the Postgres row survives. Stay durable by
either:

1. **Buying a long lifetime up front:** set `epochs` high enough for your retention window
   (mainnet epoch ≈ 14 days, so `epochs: 53` ≈ ~2 years).
2. **Enabling the renewal worker:** set both `epochDurationSeconds` (the network's epoch
   length, e.g. `1209600` for ~14 days) and `renewIntervalSeconds` (e.g. `86400`). The worker
   finds blobs nearing `expires_at`, re-uploads their bytes for a fresh window, and updates
   the index. HTTP-only (no Sui key required).

The default `epochs: 1` expires quickly — fine for testing, **not** for production.

## Limitations

- `Delete` removes the Postgres row only; it does not delete the blob on Walrus (on-chain
  deletion needs a Sui key, out of scope). Unreferenced blobs simply expire. With packing, a
  deleted block's bytes also remain inside its shared blob until the whole blob expires —
  reclaiming that space would need a future compaction/GC pass.
- Block-packing batches blocks written through `Batch().Commit()` (e.g. `ipfs add`). A single
  `Put` outside a batch still writes one blob per block, since a lone `Put` must be durable on
  return.
- Read efficiency on packed blobs depends on the aggregator honoring HTTP `Range`; otherwise
  the whole blob is fetched once and cached (`blobCacheBytes`).
- `Query` does not support `Orders` or `Filters` (same as the S3 datastore).

## License

MIT
