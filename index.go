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
	ObjectID  string
	PatchID   string
	Offset    int64
	Size      int64
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
	UpdateBlobAfterRenewal(ctx context.Context, oldBlobID, newBlobID, newObjectID string, endEpoch uint64, expiresAt sql.NullTime) error
	Close() error
}

// postgresIndex implements Index on top of a Postgres database accessed via
// database/sql. A single table holds the mapping; it is safe for many nodes
// (uploaders and retrievers) to share concurrently.
type postgresIndex struct {
	db    *sql.DB
	table string
}

// putManyChunk bounds how many rows go into a single multi-row INSERT so we
// stay well under Postgres' 65535 bound parameter limit (8 params per row).
const putManyChunk = 500

// putManyParams is the number of bound parameters per row in PutMany's
// multi-row INSERT (created_at/updated_at use now() and bind nothing).
const putManyParams = 9

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
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_key_prefix_idx ON %s (key text_pattern_ops)`, p.table, p.table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_expires_at_idx ON %s (expires_at)`, p.table, p.table),
		// Renewal groups by blob_id; packed blobs are touched once per blob.
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_blob_id_idx ON %s (blob_id)`, p.table, p.table),
	}
	for _, s := range stmts {
		if _, err := p.db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("walrusds: migrating index: %w", err)
		}
	}
	return nil
}

func (p *postgresIndex) Put(ctx context.Context, key string, rec Record) error {
	q := fmt.Sprintf(`INSERT INTO %s (key, blob_id, object_id, patch_id, blob_offset, size, deletable, end_epoch, expires_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), now())
		ON CONFLICT (key) DO UPDATE SET
			blob_id     = EXCLUDED.blob_id,
			object_id   = EXCLUDED.object_id,
			patch_id    = EXCLUDED.patch_id,
			blob_offset = EXCLUDED.blob_offset,
			size        = EXCLUDED.size,
			deletable   = EXCLUDED.deletable,
			end_epoch   = EXCLUDED.end_epoch,
			expires_at  = EXCLUDED.expires_at,
			updated_at  = now()`, p.table)
	_, err := p.db.ExecContext(ctx, q, key, rec.BlobID, rec.ObjectID, rec.PatchID, rec.Offset, rec.Size, rec.Deletable, int64(rec.EndEpoch), rec.ExpiresAt)
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
		sb.WriteString(fmt.Sprintf(`INSERT INTO %s (key, blob_id, object_id, patch_id, blob_offset, size, deletable, end_epoch, expires_at, created_at, updated_at) VALUES `, p.table))
		for i, kr := range chunk {
			if i > 0 {
				sb.WriteByte(',')
			}
			b := i * putManyParams
			sb.WriteString(fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,now(),now())",
				b+1, b+2, b+3, b+4, b+5, b+6, b+7, b+8, b+9))
			args = append(args, kr.Key, kr.Rec.BlobID, kr.Rec.ObjectID, kr.Rec.PatchID, kr.Rec.Offset, kr.Rec.Size,
				kr.Rec.Deletable, int64(kr.Rec.EndEpoch), kr.Rec.ExpiresAt)
		}
		sb.WriteString(` ON CONFLICT (key) DO UPDATE SET
			blob_id     = EXCLUDED.blob_id,
			object_id   = EXCLUDED.object_id,
			patch_id    = EXCLUDED.patch_id,
			blob_offset = EXCLUDED.blob_offset,
			size        = EXCLUDED.size,
			deletable   = EXCLUDED.deletable,
			end_epoch   = EXCLUDED.end_epoch,
			expires_at  = EXCLUDED.expires_at,
			updated_at  = now()`)

		if _, err := tx.ExecContext(ctx, sb.String(), args...); err != nil {
			return fmt.Errorf("walrusds: index put-many: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("walrusds: index put-many commit: %w", err)
	}
	return nil
}

func (p *postgresIndex) Get(ctx context.Context, key string) (Record, error) {
	q := fmt.Sprintf(`SELECT blob_id, object_id, patch_id, blob_offset, size, deletable, end_epoch, expires_at FROM %s WHERE key = $1`, p.table)
	var (
		rec      Record
		endEpoch int64
	)
	err := p.db.QueryRowContext(ctx, q, key).Scan(&rec.BlobID, &rec.ObjectID, &rec.PatchID, &rec.Offset, &rec.Size, &rec.Deletable, &endEpoch, &rec.ExpiresAt)
	switch {
	case err == sql.ErrNoRows:
		return Record{}, ds.ErrNotFound
	case err != nil:
		return Record{}, fmt.Errorf("walrusds: index get %q: %w", key, err)
	}
	rec.EndEpoch = uint64(endEpoch)
	return rec, nil
}

func (p *postgresIndex) Delete(ctx context.Context, key string) error {
	q := fmt.Sprintf(`DELETE FROM %s WHERE key = $1`, p.table)
	if _, err := p.db.ExecContext(ctx, q, key); err != nil {
		return fmt.Errorf("walrusds: index delete %q: %w", key, err)
	}
	return nil
}

func (p *postgresIndex) DeleteMany(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	q := fmt.Sprintf(`DELETE FROM %s WHERE key = ANY($1)`, p.table)
	if _, err := p.db.ExecContext(ctx, q, pq.Array(keys)); err != nil {
		return fmt.Errorf("walrusds: index delete-many: %w", err)
	}
	return nil
}

func (p *postgresIndex) List(ctx context.Context, prefix string, limit, offset int) ([]ListItem, error) {
	var (
		q    string
		args []interface{}
	)
	if prefix == "" || prefix == "/" {
		q = fmt.Sprintf(`SELECT key, size FROM %s ORDER BY key`, p.table)
	} else {
		q = fmt.Sprintf(`SELECT key, size FROM %s WHERE key = $1 OR key LIKE $2 ORDER BY key`, p.table)
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
func (p *postgresIndex) UpdateBlobAfterRenewal(ctx context.Context, oldBlobID, newBlobID, newObjectID string, endEpoch uint64, expiresAt sql.NullTime) error {
	q := fmt.Sprintf(`UPDATE %s SET blob_id = $2, object_id = $3, end_epoch = $4, expires_at = $5, updated_at = now() WHERE blob_id = $1`, p.table)
	if _, err := p.db.ExecContext(ctx, q, oldBlobID, newBlobID, newObjectID, int64(endEpoch), expiresAt); err != nil {
		return fmt.Errorf("walrusds: updating renewed blob %q: %w", oldBlobID, err)
	}
	return nil
}

func (p *postgresIndex) Close() error {
	return p.db.Close()
}
