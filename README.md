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
  key        TEXT PRIMARY KEY,   -- ds.Key string, e.g. "/blocks/CIQ..."
  blob_id    TEXT NOT NULL,      -- Walrus blob ID
  size       BIGINT NOT NULL,
  deletable  BOOLEAN NOT NULL DEFAULT FALSE,
  end_epoch  BIGINT NOT NULL DEFAULT 0,
  expires_at TIMESTAMPTZ,        -- used by the renewal worker
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

## Building and Installing

Build the plugin with the _exact_ Go version used to build your Kubo binary, against the
matching Kubo version. Example Dockerfile flow (Kubo v0.30.0):

```dockerfile
RUN git clone https://github.com/ipfs/kubo && \
    cd kubo && \
    git checkout v0.30.0 && \
    go get github.com/lighthouse-web3/go-ds-s3-walrus/plugin@latest

RUN cd kubo && \
    echo "\nwalrusds github.com/lighthouse-web3/go-ds-s3-walrus/plugin 0" >> plugin/loader/preload_list && \
    make build && \
    go mod tidy && \
    make build && \
    cp cmd/ipfs/ipfs /usr/local/bin/ipfs
```

Notes:
- The preload `name` token (`walrusds`) is just a label; the import path is what matters.
- The first `make build` may fail until `go mod tidy` resolves deps — run `make build` again.
- Pure Go (Postgres driver `lib/pq`); no CGO required.

To build/install the `.so` locally instead: `make install` (drops
`walrusplugin.so` into `$IPFS_PATH/plugins/go-ds-s3-walrus.so`).

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
            "publisherURL": "https://publisher.walrus-mainnet.walrus.space",
            "aggregatorURL": "https://aggregator.walrus-mainnet.walrus.space",
            "postgresURL": "postgres://ipfs:CHANGE_ME@db-host:5432/walrusidx?sslmode=require",
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
{"mounts":[{"aggregatorURL":"https://aggregator.walrus-mainnet.walrus.space","publisherURL":"https://publisher.walrus-mainnet.walrus.space","table":"walrus_index","mountpoint":"/blocks"},{"mountpoint":"/","path":"datastore","type":"levelds"}],"type":"mount"}
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
RUN echo "{\"mounts\":[{\"aggregatorURL\":\"${WALRUS_AGGREGATOR_URL}\",\"publisherURL\":\"${WALRUS_PUBLISHER_URL}\",\"table\":\"walrus_index\",\"mountpoint\":\"/blocks\"},{\"mountpoint\":\"/\",\"path\":\"datastore\",\"type\":\"levelds\"}],\"type\":\"mount\"}" > $IPFS_PATH/datastore_spec
```

> **Critical:** the `datastore_spec` entry for `/blocks` must contain exactly the datastore's
> `DiskSpec` keys — `publisherURL`, `aggregatorURL`, and `table` — and **must not** include
> `postgresURL` (it carries credentials and is deliberately excluded from the fingerprint).
> If the spec doesn't match what the plugin computes, Kubo refuses to start with a
> "datastore configuration does not match what is on disk" error. Key order does not matter
> (Kubo normalizes before comparing); the set of keys does.

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
| `workers` | no | `100` | Concurrency for `Batch().Commit()`. |
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
  deletion needs a Sui key, out of scope). Unreferenced blobs simply expire.
- One IPFS block maps to one Walrus blob — correct but not cost-optimal for many tiny blocks.
  Block-packing is planned; the schema has room for it.
- `Query` does not support `Orders` or `Filters` (same as the S3 datastore).

## License

MIT
