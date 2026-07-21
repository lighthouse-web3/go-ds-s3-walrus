package walrusds

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/lib/pq"
)

// Record is the metadata we persist in the shared index for each IPFS key.
// It is everything needed to (a) locate the block's bytes within a Walrus blob,
// (b) answer Has/GetSize without touching Walrus, and (c) drive epoch renewal.
//
// Several keys can share a single Walrus blob. There are two packing schemes,
// distinguished by PatchID:
//
//   - Quilt rows (PatchID != ""): the block is a member ("patch") of a Walrus
//     quilt. BlobID is the quilt's own blob ID (used for renewal grouping) and
//     PatchID is the QuiltPatchID used to read the member back. Offset is
//     unused.
//   - Concat/plain rows (PatchID == ""): the block lives at the byte range
//     [Offset, Offset+Size) inside the blob BlobID. Unpacked blocks and all
//     legacy rows are this shape with Offset == 0 and Size == the block length.
type Record struct {
	BlobID string
	// ObjectID is the Sui object ID of the Walrus Blob object backing this row
	// (for a quilt, the quilt blob's object). It is what external renewal needs
	// to extend the blob in place via `walrus extend` instead of re-uploading.
	// It may be empty for legacy rows written before object IDs were tracked,
	// or when the publisher returned an already-certified blob (no owned object).
	ObjectID string
	PatchID  string
	Offset   int64
	Size     int64
	// EncodedSize is the Walrus *encoded* size (datacap, in bytes) of the whole
	// backing blob/quilt this row lives in — the billed figure, which includes
	// erasure-coding expansion and per-blob metadata. It is a blob-level value:
	// every row sharing a blob_id (all members of a quilt, all blocks of a
	// concat pack) records the same number, so per-file datacap is summed over
	// DISTINCT blob_id. Zero on legacy rows written before billing was tracked.
	EncodedSize int64
	// Cost is the WAL actually paid for the backing blob, in FROST (1 WAL = 1e9
	// FROST), also blob-level. Zero when the blob was deduplicated
	// (already-certified) so nothing was paid, or on legacy rows.
	Cost      uint64
	Deletable bool
	EndEpoch  uint64
	ExpiresAt sql.NullTime
}

// KeyRecord pairs a datastore key with its Record for bulk insertion.
type KeyRecord struct {
	Key string
	Rec Record
}

// ListItem is a single entry returned by a prefix listing.
type ListItem struct {
	Key  string
	Size int64
}

// RenewItem identifies a Walrus blob whose paid storage is approaching expiry.
// Renewal operates per blob (not per key) so a packed blob holding many blocks
// is re-uploaded exactly once.
type RenewItem struct {
	BlobID string
}

// StageEntry is one block buffered durably in the Postgres staging table,
// waiting to be packed into a full-size Walrus quilt. Staging exists because a
// single Batch.Commit rarely carries enough blocks to fill a pack: without it
// every commit became its own under-filled Walrus blob, each paying the fixed
// ~64 MB per-blob metadata overhead. Blocks are durable the moment they are
// staged (Postgres is the durable index store), so the datastore's commit
// contract holds while the flusher accumulates a full pack across commits.
type StageEntry struct {
	Key   string
	Value []byte
}

// StageStats summarizes the claimable (unleased) rows of the staging table so
// the flusher can decide whether a pack is worth uploading yet.
type StageStats struct {
	Count  int64
	Bytes  int64
	Oldest sql.NullTime
}

// Index is the durable key -> blob mapping. It is intentionally an interface
// so the backend (Postgres today, DynamoDB/SQLite later) can be swapped
// without touching the datastore logic.
type Index interface {
	Put(ctx context.Context, key string, rec Record) error
	PutMany(ctx context.Context, recs []KeyRecord) error
	Get(ctx context.Context, key string) (Record, error)
	Delete(ctx context.Context, key string) error
	DeleteMany(ctx context.Context, keys []string) error
	List(ctx context.Context, prefix string, limit, offset int) ([]ListItem, error)
	DueForRenewal(ctx context.Context, before time.Time, limit int) ([]RenewItem, error)
	UpdateBlobAfterRenewal(ctx context.Context, oldBlobID, newBlobID, newObjectID string, encodedSize int64, cost uint64, endEpoch uint64, expiresAt sql.NullTime) error

	// Staging buffer: durable cross-commit accumulation of blocks so the
	// flusher can build full-size packs. See StageEntry.
	StagePutMany(ctx context.Context, entries []StageEntry) error
	StageGet(ctx context.Context, key string) ([]byte, error)
	StageGetSize(ctx context.Context, key string) (int64, error)
	StageStats(ctx context.Context) (StageStats, error)
	// StageClaim leases up to maxCount unleased staged blocks whose cumulative
	// size stays within maxBytes (always at least one row when any is
	// claimable), so concurrent flushers on different nodes never build packs
	// from the same blocks. The lease expires automatically, making a crashed
	// flusher's claim recoverable; the upload is idempotent (Walrus blob IDs
	// are content-derived), so a re-claim after a crash is safe.
	StageClaim(ctx context.Context, maxCount int, maxBytes int64, lease time.Duration) ([]StageEntry, error)
	// StageRelease returns claimed rows to the claimable pool after a failed
	// upload so the next flush retries them.
	StageRelease(ctx context.Context, keys []string) error
	// PromoteStaged atomically moves blocks from staging to the index: rows are
	// inserted only for keys still present in staging (a concurrent Delete
	// wins), and the staged bytes are removed, in one transaction.
	PromoteStaged(ctx context.Context, recs []KeyRecord) error

	Close() error
}

// postgresIndex implements Index on top of a Postgres database accessed via
// database/sql. A single table holds the mapping; it is safe for many nodes
// (uploaders and retrievers) to share concurrently.
type postgresIndex struct {
	db    *sql.DB
	table string
}

// stagingTable is the name of the staging buffer table paired with the index
// table (e.g. walrus_index -> walrus_index_staging).
func (p *postgresIndex) stagingTable() string {
	return p.table + "_staging"
}

// putManyChunk bounds how many rows go into a single multi-row INSERT so we
// stay well under Postgres' 65535 bound parameter limit (8 params per row).
const putManyChunk = 500

// putManyParams is the number of bound parameters per row in PutMany's
// multi-row INSERT (created_at/updated_at use now() and bind nothing).
const putManyParams = 11

// newPostgresIndex opens the Postgres connection, verifies connectivity and
// ensures the backing table and indexes exist. maxOpenConns bounds the
// connection pool; a value <= 0 applies defaultMaxOpenConns.
func newPostgresIndex(ctx context.Context, dsn, table string, maxOpenConns int) (*postgresIndex, error) {
	if table == "" {
		table = "walrus_index"
	}
	if maxOpenConns <= 0 {
		maxOpenConns = defaultMaxOpenConns
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("walrusds: opening postgres: %w", err)
	}
	// Bound the pool so a Commit fan-out (or many concurrent nodes) cannot
	// exhaust Postgres' max_connections. Idle conns are capped lower so we do
	// not hold the whole pool open between bursts.
	db.SetMaxOpenConns(maxOpenConns)
	idle := maxOpenConns
	if idle > 16 {
		idle = 16
	}
	db.SetMaxIdleConns(idle)
	db.SetConnMaxIdleTime(5 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("walrusds: connecting to postgres: %w", err)
	}

	idx := &postgresIndex{db: db, table: table}
	if err := idx.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return idx, nil
}

func (p *postgresIndex) migrate(ctx context.Context) error {
	stmts := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			key         TEXT PRIMARY KEY,
			blob_id     TEXT NOT NULL,
			object_id   TEXT NOT NULL DEFAULT '',
			patch_id    TEXT NOT NULL DEFAULT '',
			blob_offset BIGINT NOT NULL DEFAULT 0,
			size        BIGINT NOT NULL,
			encoded_size BIGINT NOT NULL DEFAULT 0,
			cost        BIGINT NOT NULL DEFAULT 0,
			deletable   BOOLEAN NOT NULL DEFAULT FALSE,
			end_epoch   BIGINT NOT NULL DEFAULT 0,
			expires_at  TIMESTAMPTZ,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)`, p.table),
		// Backward compatibility: repos created before packing lack blob_offset.
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS blob_offset BIGINT NOT NULL DEFAULT 0`, p.table),
		// Backward compatibility: repos created before quilt support lack patch_id.
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS patch_id TEXT NOT NULL DEFAULT ''`, p.table),
		// Backward compatibility: repos created before in-place extend lack object_id.
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS object_id TEXT NOT NULL DEFAULT ''`, p.table),
		// Backward compatibility: repos created before per-blob billing lack the
		// encoded-size (datacap) and WAL-cost columns.
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS encoded_size BIGINT NOT NULL DEFAULT 0`, p.table),
		fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS cost BIGINT NOT NULL DEFAULT 0`, p.table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_key_prefix_idx ON %s (key text_pattern_ops)`, p.table, p.table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_expires_at_idx ON %s (expires_at)`, p.table, p.table),
		// Renewal groups by blob_id; packed blobs are touched once per blob.
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_blob_id_idx ON %s (blob_id)`, p.table, p.table),
		// Staging buffer: blocks land here durably at Commit time and are moved
		// to the index (and Walrus) by the pack flusher. leased_until implements
		// the claim lease; created_at drives FIFO packing and age-based flushes.
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			key          TEXT PRIMARY KEY,
			value        BYTEA NOT NULL,
			size         BIGINT NOT NULL,
			created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
			leased_until TIMESTAMPTZ
		)`, p.stagingTable()),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_created_idx ON %s (created_at)`, p.stagingTable(), p.stagingTable()),
	}
	for _, s := range stmts {
		if _, err := p.db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("walrusds: migrating index: %w", err)
		}
	}
	return nil
}

func (p *postgresIndex) Put(ctx context.Context, key string, rec Record) error {
	q := fmt.Sprintf(`INSERT INTO %s (key, blob_id, object_id, patch_id, blob_offset, size, encoded_size, cost, deletable, end_epoch, expires_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now(), now())
		ON CONFLICT (key) DO UPDATE SET
			blob_id      = EXCLUDED.blob_id,
			object_id    = EXCLUDED.object_id,
			patch_id     = EXCLUDED.patch_id,
			blob_offset  = EXCLUDED.blob_offset,
			size         = EXCLUDED.size,
			encoded_size = EXCLUDED.encoded_size,
			cost         = EXCLUDED.cost,
			deletable    = EXCLUDED.deletable,
			end_epoch    = EXCLUDED.end_epoch,
			expires_at   = EXCLUDED.expires_at,
			updated_at   = now()`, p.table)
	_, err := p.db.ExecContext(ctx, q, key, rec.BlobID, rec.ObjectID, rec.PatchID, rec.Offset, rec.Size, rec.EncodedSize, int64(rec.Cost), rec.Deletable, int64(rec.EndEpoch), rec.ExpiresAt)
	if err != nil {
		return fmt.Errorf("walrusds: index put %q: %w", key, err)
	}
	return nil
}

// PutMany inserts/updates many rows in one transaction, chunked into multi-row
// INSERT statements. It is used when committing a packed blob: every block in
// the pack is written atomically after the blob upload succeeds.
func (p *postgresIndex) PutMany(ctx context.Context, recs []KeyRecord) error {
	if len(recs) == 0 {
		return nil
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("walrusds: index put-many begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit

	if err := p.putManyTx(ctx, tx, recs); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("walrusds: index put-many commit: %w", err)
	}
	return nil
}

// putManyTx performs PutMany's chunked multi-row upsert inside an existing
// transaction. It is shared by PutMany and PromoteStaged.
func (p *postgresIndex) putManyTx(ctx context.Context, tx *sql.Tx, recs []KeyRecord) error {
	for start := 0; start < len(recs); start += putManyChunk {
		end := start + putManyChunk
		if end > len(recs) {
			end = len(recs)
		}
		chunk := recs[start:end]

		var (
			sb   strings.Builder
			args = make([]interface{}, 0, len(chunk)*putManyParams)
		)
		sb.WriteString(fmt.Sprintf(`INSERT INTO %s (key, blob_id, object_id, patch_id, blob_offset, size, encoded_size, cost, deletable, end_epoch, expires_at, created_at, updated_at) VALUES `, p.table))
		for i, kr := range chunk {
			if i > 0 {
				sb.WriteByte(',')
			}
			b := i * putManyParams
			sb.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,now(),now())",
				b+1, b+2, b+3, b+4, b+5, b+6, b+7, b+8, b+9, b+10, b+11))
			args = append(args, kr.Key, kr.Rec.BlobID, kr.Rec.ObjectID, kr.Rec.PatchID, kr.Rec.Offset, kr.Rec.Size,
				kr.Rec.EncodedSize, int64(kr.Rec.Cost), kr.Rec.Deletable, int64(kr.Rec.EndEpoch), kr.Rec.ExpiresAt)
		}
		sb.WriteString(` ON CONFLICT (key) DO UPDATE SET
			blob_id      = EXCLUDED.blob_id,
			object_id    = EXCLUDED.object_id,
			patch_id     = EXCLUDED.patch_id,
			blob_offset  = EXCLUDED.blob_offset,
			size         = EXCLUDED.size,
			encoded_size = EXCLUDED.encoded_size,
			cost         = EXCLUDED.cost,
			deletable    = EXCLUDED.deletable,
			end_epoch    = EXCLUDED.end_epoch,
			expires_at   = EXCLUDED.expires_at,
			updated_at   = now()`)

		if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
			return fmt.Errorf("walrusds: index put-many: %w", err)
		}
	}
	return nil
}

func (p *postgresIndex) Get(ctx context.Context, key string) (Record, error) {
	q := fmt.Sprintf(`SELECT blob_id, object_id, patch_id, blob_offset, size, encoded_size, cost, deletable, end_epoch, expires_at FROM %s WHERE key = $1`, p.table)
	var (
		rec      Record
		cost     int64
		endEpoch int64
	)
	err := p.db.QueryRowContext(ctx, q, key).Scan(&rec.BlobID, &rec.ObjectID, &rec.PatchID, &rec.Offset, &rec.Size, &rec.EncodedSize, &cost, &rec.Deletable, &endEpoch, &rec.ExpiresAt)
	switch {
	case err == sql.ErrNoRows:
		return Record{}, ds.ErrNotFound
	case err != nil:
		return Record{}, fmt.Errorf("walrusds: index get %q: %w", key, err)
	}
	rec.Cost = uint64(cost)
	rec.EndEpoch = uint64(endEpoch)
	return rec, nil
}

// Delete removes a key from both the index and the staging buffer in one
// transaction. Deleting the staged row is what makes a concurrent flusher's
// PromoteStaged a no-op for this key (promotion only inserts rows for keys
// still staged), so a deleted block cannot be resurrected by an in-flight pack
// upload.
func (p *postgresIndex) Delete(ctx context.Context, key string) error {
	return p.DeleteMany(ctx, []string{key})
}

func (p *postgresIndex) DeleteMany(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("walrusds: index delete-many begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	q := fmt.Sprintf(`DELETE FROM %s WHERE key = ANY($1)`, p.table)
	if _, err := tx.ExecContext(ctx, q, pq.Array(keys)); err != nil {
		return fmt.Errorf("walrusds: index delete-many: %w", err)
	}
	q = fmt.Sprintf(`DELETE FROM %s WHERE key = ANY($1)`, p.stagingTable())
	if _, err := tx.ExecContext(ctx, q, pq.Array(keys)); err != nil {
		return fmt.Errorf("walrusds: staging delete-many: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("walrusds: index delete-many commit: %w", err)
	}
	return nil
}

// List enumerates keys from the index and the staging buffer together, so
// blocks that are durably staged but not yet packed onto Walrus are visible to
// Query (and therefore to GC, refs local, etc.). The UNION dedupes a key that
// is momentarily in both tables during promotion.
func (p *postgresIndex) List(ctx context.Context, prefix string, limit, offset int) ([]ListItem, error) {
	var (
		q    string
		args []interface{}
	)
	if prefix == "" || prefix == "/" {
		q = fmt.Sprintf(`SELECT key, size FROM %s UNION SELECT key, size FROM %s ORDER BY key`,
			p.table, p.stagingTable())
	} else {
		q = fmt.Sprintf(`SELECT key, size FROM %s WHERE key = $1 OR key LIKE $2
			UNION SELECT key, size FROM %s WHERE key = $1 OR key LIKE $2 ORDER BY key`,
			p.table, p.stagingTable())
		args = append(args, prefix, prefix+"/%")
	}
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	if offset > 0 {
		q += fmt.Sprintf(" OFFSET %d", offset)
	}

	rows, err := p.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("walrusds: index list %q: %w", prefix, err)
	}
	defer rows.Close()

	var items []ListItem
	for rows.Next() {
		var it ListItem
		if err := rows.Scan(&it.Key, &it.Size); err != nil {
			return nil, fmt.Errorf("walrusds: scanning list row: %w", err)
		}
		items = append(items, it)
	}
	return items, rows.Err()
}

// DueForRenewal returns the distinct blob IDs whose paid storage expires on or
// before `before`. Grouping by blob_id ensures a packed blob holding many
// blocks yields a single renewal job.
func (p *postgresIndex) DueForRenewal(ctx context.Context, before time.Time, limit int) ([]RenewItem, error) {
	q := fmt.Sprintf(`SELECT blob_id FROM %s
		WHERE expires_at IS NOT NULL AND expires_at <= $1
		GROUP BY blob_id
		ORDER BY MIN(expires_at)
		LIMIT $2`, p.table)
	rows, err := p.db.QueryContext(ctx, q, before, limit)
	if err != nil {
		return nil, fmt.Errorf("walrusds: querying renewals: %w", err)
	}
	defer rows.Close()

	var items []RenewItem
	for rows.Next() {
		var it RenewItem
		if err := rows.Scan(&it.BlobID); err != nil {
			return nil, fmt.Errorf("walrusds: scanning renewal row: %w", err)
		}
		items = append(items, it)
	}
	return items, rows.Err()
}

// UpdateBlobAfterRenewal points every block that lived in oldBlobID at the
// freshly re-uploaded blob and records its new epoch window. Because Walrus is
// content-addressed, re-uploading identical bytes typically yields the same
// blob ID; we still write newBlobID so the index is correct either way. A
// re-upload creates a new Sui Blob object, so newObjectID is recorded too (it
// is what future in-place extends need). Byte offsets are unchanged (the
// packfile bytes are identical).
func (p *postgresIndex) UpdateBlobAfterRenewal(ctx context.Context, oldBlobID, newBlobID, newObjectID string, encodedSize int64, cost uint64, endEpoch uint64, expiresAt sql.NullTime) error {
	q := fmt.Sprintf(`UPDATE %s SET blob_id = $2, object_id = $3, encoded_size = $4, cost = $5, end_epoch = $6, expires_at = $7, updated_at = now() WHERE blob_id = $1`, p.table)
	if _, err := p.db.ExecContext(ctx, q, oldBlobID, newBlobID, newObjectID, encodedSize, int64(cost), int64(endEpoch), expiresAt); err != nil {
		return fmt.Errorf("walrusds: updating renewed blob %q: %w", oldBlobID, err)
	}
	return nil
}

// stagePutChunk bounds rows per multi-row staging INSERT (3 bound params per
// row; the BYTEA values dominate the statement size, not the param count).
const stagePutChunk = 200

// StagePutMany durably buffers blocks in the staging table in one transaction.
// Once this commits the datastore's durability contract is satisfied: the
// bytes live in Postgres until the flusher packs them onto Walrus.
func (p *postgresIndex) StagePutMany(ctx context.Context, entries []StageEntry) error {
	if len(entries) == 0 {
		return nil
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("walrusds: stage put-many begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	for start := 0; start < len(entries); start += stagePutChunk {
		end := start + stagePutChunk
		if end > len(entries) {
			end = len(entries)
		}
		chunk := entries[start:end]

		var (
			sb   strings.Builder
			args = make([]interface{}, 0, len(chunk)*3)
		)
		sb.WriteString(fmt.Sprintf(`INSERT INTO %s (key, value, size, created_at, leased_until) VALUES `, p.stagingTable()))
		for i, e := range chunk {
			if i > 0 {
				sb.WriteByte(',')
			}
			b := i * 3
			sb.WriteString(fmt.Sprintf("($%d,$%d,$%d,now(),NULL)", b+1, b+2, b+3))
			args = append(args, e.Key, e.Value, int64(len(e.Value)))
		}
		// A re-put of a staged key refreshes its bytes and clears any lease.
		// Keys are content-addressed (same key => same bytes), so if the row was
		// leased by an in-flight pack upload the subsequent promote simply lands
		// the identical bytes.
		sb.WriteString(` ON CONFLICT (key) DO UPDATE SET
			value        = EXCLUDED.value,
			size         = EXCLUDED.size,
			created_at   = now(),
			leased_until = NULL`)

		if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
			return fmt.Errorf("walrusds: stage put-many: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("walrusds: stage put-many commit: %w", err)
	}
	return nil
}

func (p *postgresIndex) StageGet(ctx context.Context, key string) ([]byte, error) {
	q := fmt.Sprintf(`SELECT value FROM %s WHERE key = $1`, p.stagingTable())
	var val []byte
	err := p.db.QueryRowContext(ctx, q, key).Scan(&val)
	switch {
	case err == sql.ErrNoRows:
		return nil, ds.ErrNotFound
	case err != nil:
		return nil, fmt.Errorf("walrusds: stage get %q: %w", key, err)
	}
	return val, nil
}

func (p *postgresIndex) StageGetSize(ctx context.Context, key string) (int64, error) {
	q := fmt.Sprintf(`SELECT size FROM %s WHERE key = $1`, p.stagingTable())
	var size int64
	err := p.db.QueryRowContext(ctx, q, key).Scan(&size)
	switch {
	case err == sql.ErrNoRows:
		return -1, ds.ErrNotFound
	case err != nil:
		return -1, fmt.Errorf("walrusds: stage get-size %q: %w", key, err)
	}
	return size, nil
}

// StageStats reports the claimable (unleased or lease-expired) staging rows.
func (p *postgresIndex) StageStats(ctx context.Context) (StageStats, error) {
	q := fmt.Sprintf(`SELECT count(*), COALESCE(sum(size), 0), min(created_at) FROM %s
		WHERE leased_until IS NULL OR leased_until < now()`, p.stagingTable())
	var st StageStats
	if err := p.db.QueryRowContext(ctx, q).Scan(&st.Count, &st.Bytes, &st.Oldest); err != nil {
		return StageStats{}, fmt.Errorf("walrusds: stage stats: %w", err)
	}
	return st, nil
}

// StageClaim selects the oldest claimable rows up to maxCount / maxBytes and
// leases them. Candidate selection and the leasing UPDATE are separate
// statements, so a row grabbed by a concurrent flusher in between simply drops
// out of the RETURNING set — both packs stay disjoint.
func (p *postgresIndex) StageClaim(ctx context.Context, maxCount int, maxBytes int64, lease time.Duration) ([]StageEntry, error) {
	if maxCount <= 0 || maxBytes <= 0 {
		return nil, nil
	}

	q := fmt.Sprintf(`SELECT key, size FROM %s
		WHERE leased_until IS NULL OR leased_until < now()
		ORDER BY created_at, key
		LIMIT $1`, p.stagingTable())
	rows, err := p.db.QueryContext(ctx, q, maxCount)
	if err != nil {
		return nil, fmt.Errorf("walrusds: stage claim select: %w", err)
	}
	var (
		keys  []string
		total int64
	)
	for rows.Next() {
		var (
			key  string
			size int64
		)
		if err := rows.Scan(&key, &size); err != nil {
			rows.Close()
			return nil, fmt.Errorf("walrusds: stage claim scan: %w", err)
		}
		// Always take at least one row so a single oversized block still flushes.
		if len(keys) > 0 && total+size > maxBytes {
			break
		}
		keys = append(keys, key)
		total += size
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("walrusds: stage claim rows: %w", err)
	}
	if len(keys) == 0 {
		return nil, nil
	}

	uq := fmt.Sprintf(`UPDATE %s SET leased_until = now() + $2
		WHERE key = ANY($1) AND (leased_until IS NULL OR leased_until < now())
		RETURNING key, value`, p.stagingTable())
	urows, err := p.db.QueryContext(ctx, uq, pq.Array(keys), lease)
	if err != nil {
		return nil, fmt.Errorf("walrusds: stage claim lease: %w", err)
	}
	defer urows.Close()

	var entries []StageEntry
	for urows.Next() {
		var e StageEntry
		if err := urows.Scan(&e.Key, &e.Value); err != nil {
			return nil, fmt.Errorf("walrusds: stage claim lease scan: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, urows.Err()
}

func (p *postgresIndex) StageRelease(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	q := fmt.Sprintf(`UPDATE %s SET leased_until = NULL WHERE key = ANY($1)`, p.stagingTable())
	if _, err := p.db.ExecContext(ctx, q, pq.Array(keys)); err != nil {
		return fmt.Errorf("walrusds: stage release: %w", err)
	}
	return nil
}

// PromoteStaged atomically moves a flushed pack from staging into the index:
// the staged rows are deleted (RETURNING which keys were actually still
// present) and index rows are inserted only for those survivors. A key deleted
// concurrently — its staging row already gone — is thereby skipped, so a
// Delete can never be undone by an in-flight pack upload.
func (p *postgresIndex) PromoteStaged(ctx context.Context, recs []KeyRecord) error {
	if len(recs) == 0 {
		return nil
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("walrusds: promote begin: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	keys := make([]string, len(recs))
	for i, kr := range recs {
		keys[i] = kr.Key
	}
	dq := fmt.Sprintf(`DELETE FROM %s WHERE key = ANY($1) RETURNING key`, p.stagingTable())
	rows, err := tx.QueryContext(ctx, dq, pq.Array(keys))
	if err != nil {
		return fmt.Errorf("walrusds: promote delete: %w", err)
	}
	survivors := make(map[string]struct{}, len(recs))
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			rows.Close()
			return fmt.Errorf("walrusds: promote scan: %w", err)
		}
		survivors[k] = struct{}{}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("walrusds: promote rows: %w", err)
	}

	live := make([]KeyRecord, 0, len(recs))
	for _, kr := range recs {
		if _, ok := survivors[kr.Key]; ok {
			live = append(live, kr)
		}
	}
	if len(live) > 0 {
		if err := p.putManyTx(ctx, tx, live); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("walrusds: promote commit: %w", err)
	}
	return nil
}

func (p *postgresIndex) Close() error {
	return p.db.Close()
}
