package main

import (
	"strings"
	"testing"

	"db-ferry/config"
)

func TestGenerateTOML(t *testing.T) {
	state := &wizardState{
		SourceDB: config.DatabaseConfig{
			Name:     "src",
			Type:     "mysql",
			Host:     "localhost",
			Port:     "3306",
			Database: "src_db",
			User:     "root",
			Password: "secret",
		},
		TargetDB: config.DatabaseConfig{
			Name: "dst",
			Type: "sqlite",
			Path: "./data/output.db",
		},
		SelectedTables: []string{"users", "orders"},
		Mode:           "append",
		BatchSize:      2000,
		MaxRetries:     3,
		Validate:       "row_count",
		StateFile:      "./state/db-ferry.json",
		ResumeKey:      "run-001",
	}

	out, err := generateTOML(state)
	if err != nil {
		t.Fatalf("generateTOML() error = %v", err)
	}

	if !strings.Contains(out, `name = "src"`) {
		t.Fatalf("missing source database name")
	}
	if !strings.Contains(out, `name = "dst"`) {
		t.Fatalf("missing target database name")
	}
	if !strings.Contains(out, `type = "mysql"`) {
		t.Fatalf("missing source database type")
	}
	if !strings.Contains(out, `type = "sqlite"`) {
		t.Fatalf("missing target database type")
	}
	if !strings.Contains(out, `path = "./data/output.db"`) {
		t.Fatalf("missing target path")
	}
	if !strings.Contains(out, "sql = \"SELECT * FROM `users`\"") {
		t.Fatalf("missing users task sql")
	}
	if !strings.Contains(out, "sql = \"SELECT * FROM `orders`\"") {
		t.Fatalf("missing orders task sql")
	}
	if !strings.Contains(out, `mode = "append"`) {
		t.Fatalf("missing mode")
	}
	if !strings.Contains(out, `batch_size = 2000`) {
		t.Fatalf("missing batch_size")
	}
	if !strings.Contains(out, `validate = "row_count"`) {
		t.Fatalf("missing validate")
	}
	if !strings.Contains(out, `state_file = "./state/db-ferry.json"`) {
		t.Fatalf("missing state_file")
	}
	if !strings.Contains(out, `resume_key = "run-001"`) {
		t.Fatalf("missing resume_key")
	}

	// Ensure exactly two tasks are generated
	count := strings.Count(out, "[[tasks]]")
	if count != 2 {
		t.Fatalf("expected 2 tasks, got %d", count)
	}
}

func TestGenerateTOMLOracleService(t *testing.T) {
	state := &wizardState{
		SourceDB: config.DatabaseConfig{
			Name:     "ora_src",
			Type:     "oracle",
			Host:     "dbhost",
			Port:     "1521",
			Service:  "ORCLPDB1",
			User:     "hr",
			Password: "secret",
		},
		TargetDB: config.DatabaseConfig{
			Name:     "pg_dst",
			Type:     "postgresql",
			Host:     "localhost",
			Port:     "5432",
			Database: "warehouse",
			User:     "postgres",
			Password: "secret",
		},
		SelectedTables: []string{"employees"},
		Mode:           "replace",
		BatchSize:      1000,
		MaxRetries:     2,
		Validate:       "none",
	}

	out, err := generateTOML(state)
	if err != nil {
		t.Fatalf("generateTOML() error = %v", err)
	}

	if !strings.Contains(out, `service = "ORCLPDB1"`) {
		t.Fatalf("missing oracle service")
	}
	if !strings.Contains(out, `database = "warehouse"`) {
		t.Fatalf("missing postgres database")
	}
	if !strings.Contains(out, `sql = "SELECT * FROM \"employees\""`) {
		t.Fatalf("missing employees task sql")
	}
	if strings.Contains(out, `validate =`) {
		t.Fatalf("validate should not appear when set to none")
	}
	if strings.Contains(out, `state_file =`) {
		t.Fatalf("state_file should not appear when empty")
	}
}

func TestNeedsHostPortAndDefaultPort(t *testing.T) {
	tests := []struct {
		typ      string
		expected bool
	}{
		{"oracle", true},
		{"mysql", true},
		{"postgresql", true},
		{"sqlserver", true},
		{"sqlite", false},
		{"duckdb", false},
		{"unknown", false},
	}
	for _, tc := range tests {
		if got := needsHostPort(tc.typ); got != tc.expected {
			t.Fatalf("needsHostPort(%q) = %v, want %v", tc.typ, got, tc.expected)
		}
	}

	cases := []struct {
		typ  string
		want string
	}{
		{"oracle", "1521"},
		{"mysql", "3306"},
		{"postgresql", "5432"},
		{"sqlserver", "1433"},
		{"sqlite", ""},
	}
	for _, tc := range cases {
		if got := defaultPort(tc.typ); got != tc.want {
			t.Fatalf("defaultPort(%q) = %q, want %q", tc.typ, got, tc.want)
		}
	}
}

func TestParseInt(t *testing.T) {
	if got := parseInt("42", 10); got != 42 {
		t.Fatalf("parseInt(\"42\", 10) = %d, want 42", got)
	}
	if got := parseInt("  42  ", 10); got != 42 {
		t.Fatalf("parseInt(\"  42  \", 10) = %d, want 42", got)
	}
	if got := parseInt("abc", 7); got != 7 {
		t.Fatalf("parseInt(\"abc\", 7) = %d, want 7", got)
	}
	if got := parseInt("-1", 5); got != 5 {
		t.Fatalf("parseInt(\"-1\", 5) = %d, want 5", got)
	}
}

func TestNonEmptyValidator(t *testing.T) {
	v := nonEmpty("field is required")
	if err := v("hello"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := v("  hello  "); err != nil {
		t.Fatalf("unexpected error for trimmed value: %v", err)
	}
	if err := v("   "); err == nil {
		t.Fatalf("expected error for empty string")
	}
	if err := v(""); err == nil {
		t.Fatalf("expected error for empty string")
	}
}

func TestParseStringList(t *testing.T) {
	if got := parseStringList("a, b, c"); len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("unexpected parseStringList result: %v", got)
	}
	if got := parseStringList("  a  , , b "); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("unexpected parseStringList result: %v", got)
	}
}

func TestTomlStringArray(t *testing.T) {
	if got := tomlStringArray([]string{"a", "b"}); got != `["a", "b"]` {
		t.Fatalf("tomlStringArray() = %s", got)
	}
	if got := tomlStringArray(nil); got != "[]" {
		t.Fatalf("tomlStringArray(nil) = %s", got)
	}
}

func TestQuoteSQLIdentifier(t *testing.T) {
	cases := []struct {
		dbType string
		name   string
		want   string
	}{
		{config.DatabaseTypeMySQL, "users", "`users`"},
		{config.DatabaseTypeMySQL, "u`s", "`u``s`"},
		{config.DatabaseTypePostgreSQL, "users", `"users"`},
		{config.DatabaseTypePostgreSQL, `u"s`, `"u""s"`},
		{config.DatabaseTypeSQLServer, "users", "[users]"},
		{config.DatabaseTypeSQLServer, "u]s", "[u]]s]"},
		{config.DatabaseTypeOracle, "users", `"users"`},
		{config.DatabaseTypeSQLite, "users", `"users"`},
		{config.DatabaseTypeDuckDB, "users", `"users"`},
	}
	for _, tc := range cases {
		if got := quoteSQLIdentifier(tc.dbType, tc.name); got != tc.want {
			t.Fatalf("quoteSQLIdentifier(%q, %q) = %q, want %q", tc.dbType, tc.name, got, tc.want)
		}
	}
}
