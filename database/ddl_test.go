package database

import (
	"strings"
	"testing"

	"db-ferry/config"
)

func TestQuoteIdentifier(t *testing.T) {
	cases := []struct {
		dbType string
		name   string
		want   string
	}{
		{config.DatabaseTypeSQLite, "users", `"users"`},
		{config.DatabaseTypeMySQL, "users", "`users`"},
		{config.DatabaseTypePostgreSQL, "users", `"users"`},
		{config.DatabaseTypeOracle, "users", `"USERS"`},
		{config.DatabaseTypeSQLServer, "users", `[users]`},
		{config.DatabaseTypeDuckDB, "users", `"users"`},
		{config.DatabaseTypeMySQL, "user`name", "`user``name`"},
		{config.DatabaseTypeOracle, `user"name`, `"USER""NAME"`},
		{config.DatabaseTypeSQLServer, "user]name", `[user]]name]`},
	}
	for _, tc := range cases {
		got := QuoteIdentifier(tc.dbType, tc.name)
		if got != tc.want {
			t.Fatalf("QuoteIdentifier(%q, %q) = %q, want %q", tc.dbType, tc.name, got, tc.want)
		}
	}
}

func TestMapType(t *testing.T) {
	col := ColumnMetadata{DatabaseType: "VARCHAR", LengthValid: true, Length: 255}
	if got := MapType(config.DatabaseTypeMySQL, col); got != "VARCHAR(255)" {
		t.Fatalf("MapType(mysql, VARCHAR) = %q, want VARCHAR(255)", got)
	}
	if got := MapType(config.DatabaseTypePostgreSQL, col); got != "VARCHAR(255)" {
		t.Fatalf("MapType(postgresql, VARCHAR) = %q, want VARCHAR(255)", got)
	}
	if got := MapType(config.DatabaseTypeSQLite, col); got != "TEXT" {
		t.Fatalf("MapType(sqlite, VARCHAR) = %q, want TEXT", got)
	}
	if got := MapType(config.DatabaseTypeOracle, col); got != "VARCHAR2(255)" {
		t.Fatalf("MapType(oracle, VARCHAR) = %q, want VARCHAR2(255)", got)
	}
	if got := MapType(config.DatabaseTypeSQLServer, col); got != "NVARCHAR(255)" {
		t.Fatalf("MapType(sqlserver, VARCHAR) = %q, want NVARCHAR(255)", got)
	}
	if got := MapType(config.DatabaseTypeDuckDB, col); got != "VARCHAR(255)" {
		t.Fatalf("MapType(duckdb, VARCHAR) = %q, want VARCHAR(255)", got)
	}
}

func TestBuildDropTableSQL(t *testing.T) {
	sql := BuildDropTableSQL(config.DatabaseTypeSQLite, "users")
	if !strings.Contains(sql, `DROP TABLE IF EXISTS "users"`) {
		t.Fatalf("unexpected SQLite drop SQL: %s", sql)
	}

	sql = BuildDropTableSQL(config.DatabaseTypeOracle, "users")
	if !strings.Contains(sql, "BEGIN EXECUTE IMMEDIATE") {
		t.Fatalf("unexpected Oracle drop SQL: %s", sql)
	}

	sql = BuildDropTableSQL(config.DatabaseTypeSQLServer, "users")
	if !strings.Contains(sql, "IF OBJECT_ID") {
		t.Fatalf("unexpected SQL Server drop SQL: %s", sql)
	}
}

func TestBuildCreateTableSQL(t *testing.T) {
	cols := []ColumnMetadata{
		{Name: "id", DatabaseType: "INTEGER"},
		{Name: "name", DatabaseType: "VARCHAR", LengthValid: true, Length: 100},
	}

	stmts := BuildCreateTableSQL(config.DatabaseTypeSQLite, "users", cols, true)
	if len(stmts) != 2 {
		t.Fatalf("expected 2 statements, got %d", len(stmts))
	}
	if !strings.Contains(stmts[0], "DROP TABLE IF EXISTS") {
		t.Fatalf("expected drop statement, got %s", stmts[0])
	}
	if !strings.Contains(stmts[1], `CREATE TABLE "users"`) {
		t.Fatalf("expected create statement, got %s", stmts[1])
	}

	stmts = BuildCreateTableSQL(config.DatabaseTypeSQLite, "users", cols, false)
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	if !strings.Contains(stmts[0], "CREATE TABLE IF NOT EXISTS") {
		t.Fatalf("expected IF NOT EXISTS, got %s", stmts[0])
	}
}

func TestBuildCreateIndexSQL(t *testing.T) {
	idx := config.IndexConfig{Name: "idx_name", Columns: []string{"name:DESC"}, Unique: true}
	if err := idx.ParseColumns(); err != nil {
		t.Fatalf("ParseColumns() error = %v", err)
	}

	sql, err := BuildCreateIndexSQL(config.DatabaseTypeSQLite, "users", idx)
	if err != nil {
		t.Fatalf("BuildCreateIndexSQL() error = %v", err)
	}
	wantParts := []string{
		"CREATE UNIQUE INDEX",
		`"idx_name"`,
		`ON "users"`,
		`"name" DESC`,
	}
	for _, part := range wantParts {
		if !strings.Contains(sql, part) {
			t.Fatalf("expected %q in %q", part, sql)
		}
	}
}

func TestGeneratePlanDDL(t *testing.T) {
	cols := []ColumnMetadata{
		{Name: "id", DatabaseType: "INTEGER"},
	}
	indexes := []config.IndexConfig{
		{Name: "idx_id", Columns: []string{"id"}},
	}

	ddl, err := GeneratePlanDDL(config.DatabaseTypeSQLite, "users", cols, config.TaskModeReplace, false, indexes)
	if err != nil {
		t.Fatalf("GeneratePlanDDL() error = %v", err)
	}
	if len(ddl) != 3 {
		t.Fatalf("expected 3 DDL statements for replace mode, got %d", len(ddl))
	}

	ddl, err = GeneratePlanDDL(config.DatabaseTypeSQLite, "users", cols, config.TaskModeAppend, false, nil)
	if err != nil {
		t.Fatalf("GeneratePlanDDL() error = %v", err)
	}
	if len(ddl) != 1 {
		t.Fatalf("expected 1 DDL statement for append mode, got %d", len(ddl))
	}
	if !strings.Contains(ddl[0], "CREATE TABLE IF NOT EXISTS") {
		t.Fatalf("expected IF NOT EXISTS for append mode, got %s", ddl[0])
	}

	ddl, err = GeneratePlanDDL(config.DatabaseTypeSQLite, "users", cols, config.TaskModeReplace, true, nil)
	if err != nil {
		t.Fatalf("GeneratePlanDDL() error = %v", err)
	}
	if len(ddl) != 0 {
		t.Fatalf("expected no DDL when skip_create is true, got %d", len(ddl))
	}
}

func TestGeneratePlanDDLEmptyColumns(t *testing.T) {
	ddl, err := GeneratePlanDDL(config.DatabaseTypeSQLite, "users", nil, config.TaskModeReplace, false, nil)
	if err != nil {
		t.Fatalf("GeneratePlanDDL() error = %v", err)
	}
	if len(ddl) != 0 {
		t.Fatalf("expected no DDL for empty columns, got %d", len(ddl))
	}
}
