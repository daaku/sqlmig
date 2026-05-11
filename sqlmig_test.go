package sqlmig

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"testing/fstest"

	_ "modernc.org/sqlite"
)

var _ DB = (*sql.DB)(nil)

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("opening test db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestFNVEmpty(t *testing.T) {
	_ = fnv1a(nil)
	_ = fnv1a([]byte{})
}

func TestFNVDeterministic(t *testing.T) {
	data := []byte("hello world")
	h1 := fnv1a(data)
	h2 := fnv1a(data)
	if h1 != h2 {
		t.Fatalf("expected same hash, got %d and %d", h1, h2)
	}
}

func TestFNVDifferent(t *testing.T) {
	h1 := fnv1a([]byte("a"))
	h2 := fnv1a([]byte("b"))
	if h1 == h2 {
		t.Fatalf("expected different hashes, got %d and %d", h1, h2)
	}
}

func TestMigrateEmpty(t *testing.T) {
	db := testDB(t)
	source := Source{FS: fstest.MapFS{}, Glob: "*.sql"}
	if err := source.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	var name string
	err := db.QueryRow("select name from sqlite_master where type = 'table' and name = 'db_migrations'").Scan(&name)
	if err != nil {
		t.Fatalf("db_migrations table not found: %v", err)
	}
}

func TestMigrateFirstRun(t *testing.T) {
	db := testDB(t)
	mfs := fstest.MapFS{
		"migrations/001.sql": &fstest.MapFile{Data: []byte("create table foo (id integer primary key);")},
	}
	source := Source{FS: mfs, Glob: "migrations/*.sql"}
	if err := source.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	var name string
	err := db.QueryRow("select name from sqlite_master where type = 'table' and name = 'foo'").Scan(&name)
	if err != nil {
		t.Fatalf("foo table not found: %v", err)
	}
	var count int
	if err := db.QueryRow("select count(*) from db_migrations").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1 migration record, got %d", count)
	}
}

func TestMigrateOrder(t *testing.T) {
	db := testDB(t)
	mfs := fstest.MapFS{
		"migrations/002_insert_b.sql": &fstest.MapFile{Data: []byte("insert into order_test (name) values ('b');")},
		"migrations/001_schema.sql":   &fstest.MapFile{Data: []byte("create table order_test (name text);")},
		"migrations/003_insert_a.sql": &fstest.MapFile{Data: []byte("insert into order_test (name) values ('a');")},
	}
	source := Source{FS: mfs, Glob: "migrations/*.sql"}
	if err := source.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("select name from order_test order by rowid")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
	}
	if len(names) != 2 || names[0] != "b" || names[1] != "a" {
		t.Fatalf("unexpected order: %v", names)
	}
}

func TestMigrateReRun(t *testing.T) {
	db := testDB(t)
	mfs := fstest.MapFS{
		"migrations/001.sql": &fstest.MapFile{Data: []byte("create table foo (id integer primary key);")},
	}
	source := Source{FS: mfs, Glob: "migrations/*.sql"}
	ctx := context.Background()
	if err := source.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := source.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow("select count(*) from db_migrations").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1 migration record, got %d", count)
	}
}

func TestMigrateModified(t *testing.T) {
	db := testDB(t)
	mfs := fstest.MapFS{
		"migrations/001.sql": &fstest.MapFile{Data: []byte("create table foo (id integer primary key);")},
	}
	source := Source{FS: mfs, Glob: "migrations/*.sql"}
	ctx := context.Background()
	if err := source.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	mfs["migrations/001.sql"] = &fstest.MapFile{Data: []byte("create table bar (id integer primary key);")}
	err := source.Migrate(ctx, db)
	if err == nil {
		t.Fatal("expected error for modified migration")
	}
	if !strings.Contains(err.Error(), "migration was modified") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMigrateInvalidSQL(t *testing.T) {
	db := testDB(t)
	mfs := fstest.MapFS{
		"migrations/001.sql": &fstest.MapFile{Data: []byte("this is not valid sql")},
	}
	source := Source{FS: mfs, Glob: "migrations/*.sql"}
	err := source.Migrate(context.Background(), db)
	if err == nil {
		t.Fatal("expected error for invalid sql")
	}
	if !strings.Contains(err.Error(), "error executing migration") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMigrateGlobError(t *testing.T) {
	db := testDB(t)
	source := Source{FS: fstest.MapFS{}, Glob: "[bad"}
	err := source.Migrate(context.Background(), db)
	if err == nil {
		t.Fatal("expected error for bad glob")
	}
	if !strings.Contains(err.Error(), "error globbing") {
		t.Fatalf("unexpected error: %v", err)
	}
}
