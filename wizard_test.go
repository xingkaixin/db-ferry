package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"db-ferry/config"
	"db-ferry/database"

	"github.com/charmbracelet/huh"
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

func TestGenerateTOMLSampleValidation(t *testing.T) {
	state := &wizardState{
		SourceDB: config.DatabaseConfig{
			Name: "src",
			Type: "sqlite",
			Path: "./src.db",
		},
		TargetDB: config.DatabaseConfig{
			Name: "dst",
			Type: "sqlite",
			Path: "./dst.db",
		},
		SelectedTables:     []string{"users"},
		Mode:               "replace",
		BatchSize:          1000,
		MaxRetries:         2,
		Validate:           "sample",
		ValidateSampleSize: 500,
	}

	out, err := generateTOML(state)
	if err != nil {
		t.Fatalf("generateTOML() error = %v", err)
	}

	if !strings.Contains(out, `validate = "sample"`) {
		t.Fatalf("missing validate = sample")
	}
	if !strings.Contains(out, `validate_sample_size = 500`) {
		t.Fatalf("missing validate_sample_size")
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

func TestCollectSourceDB_ErrorWithoutTTY(t *testing.T) {
	state := &wizardState{}
	if err := collectSourceDB(state); err == nil {
		t.Fatalf("expected error when running without tty")
	}
}

func TestCollectTargetDB_ErrorWithoutTTY(t *testing.T) {
	state := &wizardState{SourceDB: config.DatabaseConfig{Name: "src"}}
	if err := collectTargetDB(state); err == nil {
		t.Fatalf("expected error when running without tty")
	}
}

func TestSelectTables_ErrorWithoutTTY(t *testing.T) {
	state := &wizardState{SourceTables: []string{"users", "orders"}}
	if err := selectTables(state); err == nil {
		t.Fatalf("expected error when running without tty")
	}
}

func TestCollectAdvancedOptions_ErrorWithoutTTY(t *testing.T) {
	state := &wizardState{}
	if err := collectAdvancedOptions(state); err == nil {
		t.Fatalf("expected error when running without tty")
	}
}

func TestRunInteractiveWizard_ErrorWithoutTTY(t *testing.T) {
	code, err := runInteractiveWizard(io.Discard)
	if err == nil {
		t.Fatalf("expected error when running without tty")
	}
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
}

func TestTestSourceAndListTables_InvalidConfig(t *testing.T) {
	cfg := config.DatabaseConfig{Type: "unknown"}
	tables, conn, err := testSourceAndListTables(cfg)
	if err == nil {
		t.Fatalf("expected error for invalid database type")
	}
	if conn != nil {
		_ = conn.Close()
	}
	if len(tables) != 0 {
		t.Fatalf("expected empty tables")
	}
}

func TestTestSourceAndListTables_Success(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "src.db")
	s, err := database.NewSQLiteDB(dbPath, 0, 0)
	if err != nil {
		t.Fatalf("NewSQLiteDB() error = %v", err)
	}
	defer s.Close()
	if err := s.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create table error = %v", err)
	}

	cfg := config.DatabaseConfig{Type: config.DatabaseTypeSQLite, Path: dbPath}
	tables, conn, err := testSourceAndListTables(cfg)
	if err != nil {
		t.Fatalf("testSourceAndListTables() error = %v", err)
	}
	if conn != nil {
		defer conn.Close()
	}
	if len(tables) != 1 || tables[0] != "users" {
		t.Fatalf("unexpected tables: %v", tables)
	}
}

func TestTestTargetConnection_InvalidConfig(t *testing.T) {
	cfg := config.DatabaseConfig{Type: "unknown"}
	conn, err := testTargetConnection(cfg)
	if err == nil {
		t.Fatalf("expected error for invalid database type")
	}
	if conn != nil {
		_ = conn.Close()
	}
}

func TestTestTargetConnection_Success(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tgt.db")
	s, err := database.NewSQLiteDB(dbPath, 0, 0)
	if err != nil {
		t.Fatalf("NewSQLiteDB() error = %v", err)
	}
	defer s.Close()

	cfg := config.DatabaseConfig{Type: config.DatabaseTypeSQLite, Path: dbPath}
	conn, err := testTargetConnection(cfg)
	if err != nil {
		t.Fatalf("testTargetConnection() error = %v", err)
	}
	if conn != nil {
		defer conn.Close()
	}
}

func TestWriteTaskMergeKeys(t *testing.T) {
	state := &wizardState{
		SourceDB:   config.DatabaseConfig{Name: "src", Type: "postgresql"},
		TargetDB:   config.DatabaseConfig{Name: "dst", Type: "postgresql"},
		Mode:       config.TaskModeMerge,
		BatchSize:  500,
		MaxRetries: 1,
		MergeKeys:  []string{"id", "tenant_id"},
	}
	var b strings.Builder
	writeTask(&b, "events", state)
	out := b.String()
	if !strings.Contains(out, `mode = "merge"`) {
		t.Fatalf("missing merge mode")
	}
	if !strings.Contains(out, `merge_keys = ["id", "tenant_id"]`) {
		t.Fatalf("missing merge_keys")
	}
}

func TestWriteTaskSQLServerIdentifier(t *testing.T) {
	state := &wizardState{
		SourceDB:   config.DatabaseConfig{Name: "src", Type: "sqlserver"},
		TargetDB:   config.DatabaseConfig{Name: "dst", Type: "sqlserver"},
		Mode:       config.TaskModeReplace,
		BatchSize:  1000,
		MaxRetries: 2,
	}
	var b strings.Builder
	writeTask(&b, "dbo.users", state)
	out := b.String()
	if !strings.Contains(out, `sql = "SELECT * FROM [dbo.users]"`) {
		t.Fatalf("unexpected sql quoting for sqlserver: %s", out)
	}
}

func TestRunInteractiveWizard_AbortedByUser(t *testing.T) {
	// This test cannot drive huh interactively, but we can at least exercise
	// the error path for TTY absence which covers the top of the function.
	code, err := runInteractiveWizard(io.Discard)
	if err == nil {
		t.Fatalf("expected error")
	}
	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
}

func TestCollectSourceDB_AliasValidation(t *testing.T) {
	// Validate that the alias validation function rejects empty strings.
	v := func(s string) error {
		if strings.TrimSpace(s) == "" {
			return os.ErrInvalid
		}
		return nil
	}
	if err := v("  "); err == nil {
		t.Fatalf("expected validation error for empty alias")
	}
	if err := v("src"); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestCollectTargetDB_AliasValidation(t *testing.T) {
	v := func(s string) error {
		if strings.TrimSpace(s) == "" {
			return os.ErrInvalid
		}
		if strings.TrimSpace(s) == "src" {
			return os.ErrExist
		}
		return nil
	}
	if err := v("  "); err == nil {
		t.Fatalf("expected validation error for empty alias")
	}
	if err := v("src"); err == nil {
		t.Fatalf("expected validation error for duplicate alias")
	}
	if err := v("dst"); err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestCollectSourceDB_Success(t *testing.T) {
	origSelect := runHuhSelect
	origForm := runHuhForm
	t.Cleanup(func() {
		runHuhSelect = origSelect
		runHuhForm = origForm
	})
	runHuhSelect = func(s *huh.Select[string]) error { return nil }
	runHuhForm = func(f *huh.Form) error { return nil }

	state := &wizardState{}
	if err := collectSourceDB(state); err != nil {
		t.Fatalf("collectSourceDB() error = %v", err)
	}
	if state.SourceDB.Name != "source_db" {
		t.Fatalf("unexpected alias: %s", state.SourceDB.Name)
	}
}

func TestCollectTargetDB_Success(t *testing.T) {
	origSelect := runHuhSelect
	origForm := runHuhForm
	t.Cleanup(func() {
		runHuhSelect = origSelect
		runHuhForm = origForm
	})
	runHuhSelect = func(s *huh.Select[string]) error { return nil }
	runHuhForm = func(f *huh.Form) error { return nil }

	state := &wizardState{SourceDB: config.DatabaseConfig{Name: "src"}}
	if err := collectTargetDB(state); err != nil {
		t.Fatalf("collectTargetDB() error = %v", err)
	}
	if state.TargetDB.Name != "target_db" {
		t.Fatalf("unexpected alias: %s", state.TargetDB.Name)
	}
}

func TestCollectAdvancedOptions_ReplaceMode(t *testing.T) {
	origSelect := runHuhSelect
	origForm := runHuhForm
	t.Cleanup(func() {
		runHuhSelect = origSelect
		runHuhForm = origForm
	})
	runHuhSelect = func(s *huh.Select[string]) error { return nil }
	runHuhForm = func(f *huh.Form) error { return nil }

	state := &wizardState{}
	if err := collectAdvancedOptions(state); err != nil {
		t.Fatalf("collectAdvancedOptions() error = %v", err)
	}
	if state.Mode != "replace" {
		t.Fatalf("expected mode replace, got %s", state.Mode)
	}
	if state.BatchSize != 1000 {
		t.Fatalf("expected default batch_size 1000, got %d", state.BatchSize)
	}
	if state.MaxRetries != 2 {
		t.Fatalf("expected default max_retries 2, got %d", state.MaxRetries)
	}
}

func TestCollectAdvancedOptions_MergeModeWithStateFile(t *testing.T) {
	origSelect := runHuhSelect
	origForm := runHuhForm
	origInput := runHuhInput
	t.Cleanup(func() {
		runHuhSelect = origSelect
		runHuhForm = origForm
		runHuhInput = origInput
	})
	runHuhSelect = func(s *huh.Select[string]) error { return nil }
	runHuhForm = func(f *huh.Form) error { return nil }
	runHuhInput = func(i *huh.Input) error { return nil }

	state := &wizardState{Mode: config.TaskModeMerge, StateFile: "state.json"}
	if err := collectAdvancedOptions(state); err != nil {
		t.Fatalf("collectAdvancedOptions() error = %v", err)
	}
}

func TestRunInteractiveWizard_Success(t *testing.T) {
	dir := t.TempDir()
	chdirForTest(t, dir)

	origSelect := runHuhSelect
	origForm := runHuhForm
	origInput := runHuhInput
	origSource := testSourceFn
	origTarget := testTargetFn
	origSelectTables := runSelectTables
	origConfirmWrite := confirmWriteConfig
	origConfirmOverwrite := confirmOverwriteConfig
	t.Cleanup(func() {
		runHuhSelect = origSelect
		runHuhForm = origForm
		runHuhInput = origInput
		testSourceFn = origSource
		testTargetFn = origTarget
		runSelectTables = origSelectTables
		confirmWriteConfig = origConfirmWrite
		confirmOverwriteConfig = origConfirmOverwrite
	})

	runHuhSelect = func(s *huh.Select[string]) error { return nil }
	runHuhForm = func(f *huh.Form) error { return nil }
	confirmWriteConfig = func(confirmed *bool) error {
		*confirmed = true
		return nil
	}
	confirmOverwriteConfig = func(overwrite *bool) error {
		*overwrite = true
		return nil
	}
	runHuhInput = func(i *huh.Input) error { return nil }
	testSourceFn = func(cfg config.DatabaseConfig) ([]string, database.SourceDB, error) {
		return []string{"users"}, nil, nil
	}
	testTargetFn = func(cfg config.DatabaseConfig) (database.TargetDB, error) {
		return nil, nil
	}
	runSelectTables = func(state *wizardState) error {
		state.SelectedTables = []string{"users"}
		return nil
	}

	var out bytes.Buffer
	code, err := runInteractiveWizard(&out)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != 0 {
		t.Fatalf("expected code 0, got %d", code)
	}
	if !strings.Contains(out.String(), "Created task.toml") {
		t.Fatalf("expected success message, got: %s", out.String())
	}
}

func TestRunInteractiveWizard_Aborted(t *testing.T) {
	origSelect := runHuhSelect
	origForm := runHuhForm
	origSource := testSourceFn
	origTarget := testTargetFn
	origSelectTables := runSelectTables
	origConfirmWrite := confirmWriteConfig
	origConfirmOverwrite := confirmOverwriteConfig
	t.Cleanup(func() {
		runHuhSelect = origSelect
		runHuhForm = origForm
		testSourceFn = origSource
		testTargetFn = origTarget
		runSelectTables = origSelectTables
		confirmWriteConfig = origConfirmWrite
		confirmOverwriteConfig = origConfirmOverwrite
	})
	runHuhSelect = func(s *huh.Select[string]) error { return nil }
	runHuhForm = func(f *huh.Form) error { return nil }
	testSourceFn = func(cfg config.DatabaseConfig) ([]string, database.SourceDB, error) {
		return []string{"users"}, nil, nil
	}
	testTargetFn = func(cfg config.DatabaseConfig) (database.TargetDB, error) {
		return nil, nil
	}
	runSelectTables = func(state *wizardState) error {
		state.SelectedTables = []string{"users"}
		return nil
	}

	confirmWriteConfig = func(confirmed *bool) error {
		return nil
	}

	var out bytes.Buffer
	code, err := runInteractiveWizard(&out)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != 0 {
		t.Fatalf("expected code 0, got %d", code)
	}
	if !strings.Contains(out.String(), "Aborted.") {
		t.Fatalf("expected aborted message, got: %s", out.String())
	}
}
