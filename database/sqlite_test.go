package database

import (
	"path/filepath"
	"testing"

	"db-ferry/config"
)

func TestSQLiteBasicFlow(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sqlite.db")
	s, err := NewSQLiteDB(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteDB() error = %v", err)
	}
	defer s.Close()

	cols := []ColumnMetadata{
		{Name: "id", DatabaseType: "INTEGER"},
		{Name: "name", DatabaseType: "VARCHAR"},
	}
	if err := s.CreateTable("users", cols); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}
	if err := s.InsertData("users", cols, [][]any{{1, "a"}, {2, "b"}}); err != nil {
		t.Fatalf("InsertData() error = %v", err)
	}

	rows, err := s.Query(`SELECT id, name FROM "users" ORDER BY id`)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	rows.Close()

	cnt, err := s.GetRowCount(`SELECT id FROM "users"`)
	if err != nil {
		t.Fatalf("GetRowCount() error = %v", err)
	}
	if cnt != 2 {
		t.Fatalf("GetRowCount() = %d, want 2", cnt)
	}

	tableCnt, err := s.GetTableRowCount("users")
	if err != nil {
		t.Fatalf("GetTableRowCount() error = %v", err)
	}
	if tableCnt != 2 {
		t.Fatalf("GetTableRowCount() = %d, want 2", tableCnt)
	}
}

func TestSQLiteEnsureTableAndUpsert(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sqlite.db")
	s, err := NewSQLiteDB(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteDB() error = %v", err)
	}
	defer s.Close()

	cols := []ColumnMetadata{
		{Name: "id", DatabaseType: "INTEGER"},
		{Name: "name", DatabaseType: "VARCHAR"},
	}

	if err := s.EnsureTable("users", cols); err != nil {
		t.Fatalf("EnsureTable() error = %v", err)
	}
	if _, err := s.db.Exec(`CREATE UNIQUE INDEX idx_users_id ON "users"("id")`); err != nil {
		t.Fatalf("create unique index error = %v", err)
	}
	if err := s.InsertData("users", cols, [][]any{{1, "old"}}); err != nil {
		t.Fatalf("InsertData() error = %v", err)
	}
	if err := s.UpsertData("users", cols, [][]any{{1, "new"}, {2, "b"}}, []string{"id"}); err != nil {
		t.Fatalf("UpsertData() error = %v", err)
	}

	cnt, err := s.GetTableRowCount("users")
	if err != nil {
		t.Fatalf("GetTableRowCount() error = %v", err)
	}
	if cnt != 2 {
		t.Fatalf("GetTableRowCount() = %d, want 2", cnt)
	}
}

func TestSQLiteCreateIndexesAndBuildIndexSQL(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sqlite.db")
	s, err := NewSQLiteDB(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteDB() error = %v", err)
	}
	defer s.Close()

	cols := []ColumnMetadata{{Name: "id", DatabaseType: "INTEGER"}, {Name: "name", DatabaseType: "TEXT"}}
	if err := s.CreateTable("users", cols); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	index := config.IndexConfig{
		Name:    "idx_users_name",
		Columns: []string{"name:DESC"},
		Where:   "name IS NOT NULL",
	}
	if err := s.CreateIndexes("users", []config.IndexConfig{index}); err != nil {
		t.Fatalf("CreateIndexes() error = %v", err)
	}

	parsed := config.IndexConfig{
		Name:          "idx_users_id",
		ParsedColumns: []config.IndexColumn{{Name: "id", Order: "ASC"}},
		Unique:        true,
	}
	sqlText, err := s.buildIndexSQL("users", parsed)
	if err != nil {
		t.Fatalf("buildIndexSQL() error = %v", err)
	}
	if sqlText != `CREATE UNIQUE INDEX IF NOT EXISTS "idx_users_id" ON "users" ("id" ASC)` {
		t.Fatalf("unexpected index sql: %s", sqlText)
	}
}

func TestSQLiteQueryAndCountErrors(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sqlite.db")
	s, err := NewSQLiteDB(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteDB() error = %v", err)
	}
	defer s.Close()

	_, err = s.Query("SELECT * FROM nonexistent")
	if err == nil {
		t.Fatalf("expected query error for missing table")
	}

	_, err = s.GetRowCount("SELECT * FROM nonexistent")
	if err == nil {
		t.Fatalf("expected GetRowCount error for missing table")
	}
}

func TestSQLitePingAndExec(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sqlite.db")
	s, err := NewSQLiteDB(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteDB() error = %v", err)
	}
	defer s.Close()

	if err := s.Ping(); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}

	if err := s.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("Exec() error = %v", err)
	}
}

func TestSQLiteGetTablesError(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sqlite.db")
	s, err := NewSQLiteDB(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteDB() error = %v", err)
	}
	defer s.Close()

	// Close the underlying DB to force GetTables to fail.
	_ = s.db.Close()
	_, err = s.GetTables()
	if err == nil {
		t.Fatalf("expected GetTables error after closing db")
	}
}

func TestSQLiteGetTables(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sqlite.db")
	s, err := NewSQLiteDB(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteDB() error = %v", err)
	}
	defer s.Close()

	if _, err := s.db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("create table error = %v", err)
	}
	if _, err := s.db.Exec(`CREATE VIEW active_users AS SELECT * FROM users WHERE name IS NOT NULL`); err != nil {
		t.Fatalf("create view error = %v", err)
	}

	tables, err := s.GetTables()
	if err != nil {
		t.Fatalf("GetTables() error = %v", err)
	}
	want := []string{"active_users", "users"}
	if len(tables) != len(want) {
		t.Fatalf("GetTables() = %v, want %v", tables, want)
	}
	for i := range want {
		if tables[i] != want[i] {
			t.Fatalf("GetTables()[%d] = %s, want %s", i, tables[i], want[i])
		}
	}
}

func TestSQLiteNewDBError(t *testing.T) {
	// Use a directory path where SQLite cannot create a database file.
	dir := t.TempDir()
	_, err := NewSQLiteDB(dir)
	if err == nil {
		t.Fatalf("expected error when opening directory as sqlite db")
	}
}

func TestSQLiteCloseNilDB(t *testing.T) {
	s := &SQLiteDB{db: nil}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() with nil db should return nil, got %v", err)
	}
}

func TestSQLiteEdgeCases(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sqlite.db")
	s, err := NewSQLiteDB(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteDB() error = %v", err)
	}
	defer s.Close()

	if err := s.CreateTable("bad", nil); err == nil {
		t.Fatalf("expected create table error when columns are empty")
	}
	if err := s.InsertData("missing", nil, nil); err != nil {
		t.Fatalf("InsertData() with empty values should be nil, got %v", err)
	}
	if err := s.UpsertData("missing", nil, [][]any{{1}}, nil); err == nil {
		t.Fatalf("expected merge_keys required error")
	}

	cases := []struct {
		meta ColumnMetadata
		want string
	}{
		{ColumnMetadata{DatabaseType: "VARCHAR"}, "TEXT"},
		{ColumnMetadata{DatabaseType: "REAL"}, "REAL"},
		{ColumnMetadata{DatabaseType: "INT"}, "INTEGER"},
		{ColumnMetadata{DatabaseType: "DATE"}, "TEXT"},
		{ColumnMetadata{DatabaseType: "BLOB"}, "BLOB"},
		{ColumnMetadata{DatabaseType: "UNKNOWN"}, "TEXT"},
	}
	for _, tc := range cases {
		if got := s.mapToSQLiteType(tc.meta); got != tc.want {
			t.Fatalf("mapToSQLiteType(%+v) = %s, want %s", tc.meta, got, tc.want)
		}
	}
}
