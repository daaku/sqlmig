// Package sqlmig supports running embedded database/sql migrations.
package sqlmig

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"slices"
)

// Source defines a FS and correlated Glob to provide a source of migrations.
type Source struct {
	FS   fs.FS
	Glob string
}

// DB must be satisfied for executing migrations.
type DB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// Migrate runs the migrations on the target DB.
func (s Source) Migrate(ctx context.Context, db DB) error {
	files, err := fs.Glob(s.FS, s.Glob)
	if err != nil {
		return fmt.Errorf("sqlmig: error globbing: %q: %w", s.Glob, err)
	}
	slices.Sort(files)
	const migrationSchemaSQL = `
	create table if not exists db_migrations (
		name text primary key
	)`
	if _, err := db.ExecContext(ctx, migrationSchemaSQL); err != nil {
		return fmt.Errorf("sqlmig: error creating db_migrations table: %w", err)
	}
	for _, filename := range files {
		data, err := fs.ReadFile(s.FS, filename)
		if err != nil {
			return fmt.Errorf("sqlmig: error reading migration: %q: %w", filename, err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("sqlmig: error starting tx: %w", err)
		}
		defer tx.Rollback()

		const alreadyDoneSQL = `select count(*) from db_migrations where name = ?`
		var alreadyDone int
		if err := tx.QueryRowContext(ctx, alreadyDoneSQL, filename).Scan(&alreadyDone); err != nil {
			return fmt.Errorf("sqlmig: error checking migration status: %q: %w", filename, err)
		}
		if alreadyDone == 1 {
			tx.Rollback()
			continue
		}
		if _, err := tx.ExecContext(ctx, `insert into db_migrations values (?)`, filename); err != nil {
			return fmt.Errorf("sqlmig: error updating migration status: %q: %w", filename, err)
		}
		if _, err := tx.ExecContext(ctx, string(data)); err != nil {
			return fmt.Errorf("sqlmig: error executing migration: %q: %w", filename, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("sqlmig: error commiting migration: %q: %w", filename, err)
		}
	}
	return nil
}
