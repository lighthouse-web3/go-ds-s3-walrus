package walrusds

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	ds "github.com/ipfs/go-datastore"
	_ "github.com/lib/pq"
)

// Record is the metadata we persist in the shared index for each IPFS key.
// It is everything needed to (a) fetch the bytes from Walrus, (b) answer
// Has/GetSize without touching Walrus, and (c) drive epoch renewal.
type Record struct {
	BlobID    string
	Size      int64
	Deletable bool
	EndEpoch  uint64
	ExpiresAt sql.NullTime
}

// ListItem is a single entry returned by a prefix listing.
type ListItem struct {
	Key  string
	Size int64
}

// RenewItem identifies a blob whose paid storage is approaching expiry.
type RenewItem struct {
	Key    string
	BlobID string
}

// Index is the durable key -> blob mapping. It is intentionally an interface
// so the backend (Postgres today, DynamoDB/SQLite later) can be swapped
// without touching the datastore logic.
type Index interface {
	Put(ctx context.Context, key string, rec Record) error
	Get(ctx context.Context, key string) (Record, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string, limit, offset int) ([]ListItem, error)
	DueForRenewal(ctx context.Context, before time.Time, limit int) ([]RenewItem, error)
	UpdateAfterRenewal(ctx context.Context, key, blobID string, endEpoch uint64, expiresAt sql.NullTime) error
	Close() error
}

// postgresIndex implements Index on top of a Postgres database accessed via
// database/sql. A single table holds the mapping; it is safe for many nodes
// (uploaders and retrievers) to share concurrently.
type postgresIndex struct {
	db    *sql.DB
	table string
}

// newPostgresIndex opens the Postgres connection, verifies connectivity and
// ensures the backing table and indexes exist.
func newPostgresIndex(ctx context.Context, dsn, table string) (*postgresIndex, error) {
	if table == "" {
		table = "walrus_index"
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("walrusds: opening postgres: %w", err)
	}
	db.SetMaxOpenConns(0) // unlimited; pooling handled by driver defaults
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
			size        BIGINT NOT NULL,
			deletable   BOOLEAN NOT NULL DEFAULT FALSE,
			end_epoch   BIGINT NOT NULL DEFAULT 0,
			expires_at  TIMESTAMPTZ,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)`, p.table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_key_prefix_idx ON %s (key text_pattern_ops)`, p.table, p.table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_expires_at_idx ON %s (expires_at)`, p.table, p.table),
	}
	for _, s := range stmts {
		if _, err := p.db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("walrusds: migrating index: %w", err)
		}
	}
	return nil
}

func (p *postgresIndex) Put(ctx context.Context, key string, rec Record) error {
	q := fmt.Sprintf(`INSERT INTO %s (key, blob_id, size, deletable, end_epoch, expires_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, now(), now())
		ON CONFLICT (key) DO UPDATE SET
			blob_id   = EXCLUDED.blob_id,
			size      = EXCLUDED.size,
			deletable = EXCLUDED.deletable,
			end_epoch = EXCLUDED.end_epoch,
			expires_at = EXCLUDED.expires_at,
			updated_at = now()`, p.table)
	_, err := p.db.ExecContext(ctx, q, key, rec.BlobID, rec.Size, rec.Deletable, int64(rec.EndEpoch), rec.ExpiresAt)
	if err != nil {
		return fmt.Errorf("walrusds: index put %q: %w", key, err)
	}
	return nil
}

func (p *postgresIndex) Get(ctx context.Context, key string) (Record, error) {
	q := fmt.Sprintf(`SELECT blob_id, size, deletable, end_epoch, expires_at FROM %s WHERE key = $1`, p.table)
	var (
		rec      Record
		endEpoch int64
	)
	err := p.db.QueryRowContext(ctx, q, key).Scan(&rec.BlobID, &rec.Size, &rec.Deletable, &endEpoch, &rec.ExpiresAt)
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

func (p *postgresIndex) DueForRenewal(ctx context.Context, before time.Time, limit int) ([]RenewItem, error) {
	q := fmt.Sprintf(`SELECT key, blob_id FROM %s
		WHERE expires_at IS NOT NULL AND expires_at <= $1
		ORDER BY expires_at LIMIT $2`, p.table)
	rows, err := p.db.QueryContext(ctx, q, before, limit)
	if err != nil {
		return nil, fmt.Errorf("walrusds: querying renewals: %w", err)
	}
	defer rows.Close()

	var items []RenewItem
	for rows.Next() {
		var it RenewItem
		if err := rows.Scan(&it.Key, &it.BlobID); err != nil {
			return nil, fmt.Errorf("walrusds: scanning renewal row: %w", err)
		}
		items = append(items, it)
	}
	return items, rows.Err()
}

func (p *postgresIndex) UpdateAfterRenewal(ctx context.Context, key, blobID string, endEpoch uint64, expiresAt sql.NullTime) error {
	q := fmt.Sprintf(`UPDATE %s SET blob_id = $2, end_epoch = $3, expires_at = $4, updated_at = now() WHERE key = $1`, p.table)
	if _, err := p.db.ExecContext(ctx, q, key, blobID, int64(endEpoch), expiresAt); err != nil {
		return fmt.Errorf("walrusds: updating renewed blob %q: %w", key, err)
	}
	return nil
}

func (p *postgresIndex) Close() error {
	return p.db.Close()
}
