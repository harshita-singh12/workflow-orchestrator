// Package postgres is the Postgres/pgx implementation of store.Store.
package postgres

import (
	"context"
	"embed"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aryanraj/workflow-orchestrator/internal/store"
)

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

// dbtx is the subset of pgx's query surface shared by *pgxpool.Pool and pgx.Tx, which lets
// the `queries` struct below work unmodified whether it's wrapping the pool (autocommit) or
// a transaction (see Store.WithTx).
type dbtx interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Store embeds *queries so it directly satisfies store.Queries (autocommit: every call is
// its own implicit transaction) in addition to store.Store (via WithTx below).
type Store struct {
	*queries
	pool *pgxpool.Pool
}

// New connects to Postgres and runs any pending migrations before returning.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	if err := runMigrations(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: migrate: %w", err)
	}
	s := &Store{pool: pool, queries: &queries{db: pool}}
	return s, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// WithTx implements store.Store.
func (s *Store) WithTx(ctx context.Context, fn func(ctx context.Context, q store.Queries) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op if already committed

	txq := &queries{db: tx}
	if err := fn(ctx, txq); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit tx: %w", err)
	}
	return nil
}

// runMigrations applies every embedded *.sql file in filename order that hasn't been
// recorded in schema_migrations yet, each in its own transaction. Simple and dependency-free
// on purpose: for a project this size a full migration framework is overkill, and this is
// enough to make `docker compose up` and local dev idempotently converge on the same schema.
func runMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		filename TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return err
	}

	entries, err := embeddedMigrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		var already bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE filename=$1)`, name).Scan(&already); err != nil {
			return err
		}
		if already {
			continue
		}
		sqlBytes, err := embeddedMigrations.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (filename) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}
