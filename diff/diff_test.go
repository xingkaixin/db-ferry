package diff

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"db-ferry/config"
	"db-ferry/database"

	_ "github.com/mattn/go-sqlite3"
)

func TestResolveKeys(t *testing.T) {
	columns := []database.ColumnMetadata{
		{Name: "id"},
		{Name: "name"},
		{Name: "email"},
	}

	keys, err := resolveKeys(columns, []string{"id"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(keys) != 1 || keys[0] != "id" {
		t.Fatalf("expected [id], got %v", keys)
	}

	keys, err = resolveKeys(columns, nil, []string{"name", "email"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(keys) != 2 || keys[0] != "name" || keys[1] != "email" {
		t.Fatalf("expected [name email], got %v", keys)
	}

	// CLI overrides task keys.
	keys, err = resolveKeys(columns, []string{"id"}, []string{"email"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(keys) != 1 || keys[0] != "email" {
		t.Fatalf("expected [email], got %v", keys)
	}

	// Missing keys.
	_, err = resolveKeys(columns, nil, nil)
	if err == nil {
		t.Fatal("expected error for missing keys")
	}

	// Unknown key.
	_, err = resolveKeys(columns, []string{"unknown"}, nil)
	if err == nil {
		t.Fatal("expected error for unknown key")
	}
}

func TestBuildLimitedSQL(t *testing.T) {
	cases := []struct {
		dbType string
		want   string
	}{
		{config.DatabaseTypeMySQL, "LIMIT 100"},
		{config.DatabaseTypePostgreSQL, "LIMIT 100"},
		{config.DatabaseTypeSQLite, "LIMIT 100"},
		{config.DatabaseTypeDuckDB, "LIMIT 100"},
		{config.DatabaseTypeSQLServer, "SELECT TOP 100 *"},
		{config.DatabaseTypeOracle, "FETCH FIRST 100 ROWS ONLY"},
	}
	for _, tc := range cases {
		sqlText := buildLimitedSQL("SELECT 1", tc.dbType, "x > 1", 100)
		if !strings.Contains(sqlText, tc.want) {
			t.Errorf("buildLimitedSQL(%s) = %s, want substring %s", tc.dbType, sqlText, tc.want)
		}
		if !strings.Contains(sqlText, "WHERE x > 1") {
			t.Errorf("buildLimitedSQL(%s) missing WHERE clause: %s", tc.dbType, sqlText)
		}
	}
}

func TestBuildTargetSQL(t *testing.T) {
	sqlText := buildTargetSQL("users", config.DatabaseTypeMySQL, "id = 1", 10)
	want := "SELECT * FROM `users` WHERE id = 1 LIMIT 10"
	if sqlText != want {
		t.Errorf("buildTargetSQL() = %s, want %s", sqlText, want)
	}
}

func TestDiffEndToEnd(t *testing.T) {
	testWithFileDB(t)
}

func testWithFileDB(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	srcPath := dir + "/source.db"
	dstPath := dir + "/target.db"

	src, err := sql.Open("sqlite3", srcPath)
	if err != nil {
		t.Fatalf("open source db error = %v", err)
	}
	defer src.Close()

	dst, err := sql.Open("sqlite3", dstPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer dst.Close()

	if _, err := src.Exec(`CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)`); err != nil {
		t.Fatalf("create source table error = %v", err)
	}
	if _, err := dst.Exec(`CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, status TEXT)`); err != nil {
		t.Fatalf("create target table error = %v", err)
	}

	if _, err := src.Exec(`INSERT INTO orders VALUES (1, 100, 'pending'), (2, 200, 'pending'), (3, 300, 'done')`); err != nil {
		t.Fatalf("insert source error = %v", err)
	}
	if _, err := dst.Exec(`INSERT INTO orders VALUES (2, 200, 'shipped'), (3, 300, 'done'), (4, 400, 'pending')`); err != nil {
		t.Fatalf("insert target error = %v", err)
	}

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: "sqlite", Path: srcPath},
			{Name: "dst", Type: "sqlite", Path: dstPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName: "orders",
				SQL:       "SELECT id, amount, status FROM orders",
				SourceDB:  "src",
				TargetDB:  "dst",
				MergeKeys: []string{"id"},
				Mode:      "merge",
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config error = %v", err)
	}

	var buf bytes.Buffer
	opts := Options{
		TaskName: "orders",
		Format:   "json",
	}
	if err := Run(cfg, opts, &buf); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	var result Result
	if err := decodeJSON(bytes.NewReader(buf.Bytes()), &result); err != nil {
		t.Fatalf("decode json error = %v", err)
	}

	if result.Summary.SourceTotal != 3 {
		t.Errorf("source total = %d, want 3", result.Summary.SourceTotal)
	}
	if result.Summary.TargetTotal != 3 {
		t.Errorf("target total = %d, want 3", result.Summary.TargetTotal)
	}
	if len(result.SourceOnly) != 1 {
		t.Errorf("source only = %d, want 1", len(result.SourceOnly))
	}
	if len(result.TargetOnly) != 1 {
		t.Errorf("target only = %d, want 1", len(result.TargetOnly))
	}
	if len(result.Mismatch) != 1 {
		t.Errorf("mismatch = %d, want 1", len(result.Mismatch))
	}

	// Verify mismatch details.
	m := result.Mismatch[0]
	if fmt.Sprintf("%v", m.Key["id"]) != "2" {
		t.Errorf("mismatch key id = %v, want 2", m.Key["id"])
	}
	if !containsString(m.DiffCols, "status") {
		t.Errorf("expected status in diff cols, got %v", m.DiffCols)
	}

	// Verify source_only row.
	if fmt.Sprintf("%v", result.SourceOnly[0]["id"]) != "1" {
		t.Errorf("source only id = %v, want 1", result.SourceOnly[0]["id"])
	}

	// Verify target_only row.
	if fmt.Sprintf("%v", result.TargetOnly[0]["id"]) != "4" {
		t.Errorf("target only id = %v, want 4", result.TargetOnly[0]["id"])
	}
}

func TestDiffWithWhereAndLimit(t *testing.T) {
	dir := t.TempDir()
	srcPath := dir + "/source.db"
	dstPath := dir + "/target.db"

	src, err := sql.Open("sqlite3", srcPath)
	if err != nil {
		t.Fatalf("open source db error = %v", err)
	}
	defer src.Close()

	dst, err := sql.Open("sqlite3", dstPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer dst.Close()

	if _, err := src.Exec(`CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER)`); err != nil {
		t.Fatalf("create source table error = %v", err)
	}
	if _, err := dst.Exec(`CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER)`); err != nil {
		t.Fatalf("create target table error = %v", err)
	}

	if _, err := src.Exec(`INSERT INTO orders VALUES (1, 100), (2, 200), (3, 300)`); err != nil {
		t.Fatalf("insert source error = %v", err)
	}
	if _, err := dst.Exec(`INSERT INTO orders VALUES (2, 200), (3, 999), (4, 400)`); err != nil {
		t.Fatalf("insert target error = %v", err)
	}

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: "sqlite", Path: srcPath},
			{Name: "dst", Type: "sqlite", Path: dstPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName: "orders",
				SQL:       "SELECT id, amount FROM orders",
				SourceDB:  "src",
				TargetDB:  "dst",
				MergeKeys: []string{"id"},
				Mode:      "merge",
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config error = %v", err)
	}

	var buf bytes.Buffer
	opts := Options{
		TaskName: "orders",
		Format:   "json",
		Where:    "id > 1",
		Limit:    2,
	}
	if err := Run(cfg, opts, &buf); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	var result Result
	if err := decodeJSON(bytes.NewReader(buf.Bytes()), &result); err != nil {
		t.Fatalf("decode json error = %v", err)
	}

	// With limit 2 and where id > 1, source gets rows 2,3 and target gets rows 2,3.
	if result.Summary.SourceTotal != 2 {
		t.Errorf("source total = %d, want 2", result.Summary.SourceTotal)
	}
	if result.Summary.TargetTotal != 2 {
		t.Errorf("target total = %d, want 2", result.Summary.TargetTotal)
	}
	if len(result.Mismatch) != 1 {
		t.Errorf("mismatch = %d, want 1", len(result.Mismatch))
	}
	if len(result.SourceOnly) != 0 {
		t.Errorf("source only = %d, want 0", len(result.SourceOnly))
	}
	if len(result.TargetOnly) != 0 {
		t.Errorf("target only = %d, want 0", len(result.TargetOnly))
	}
}

func TestDiffNoKeysUsesCLI(t *testing.T) {
	dir := t.TempDir()
	srcPath := dir + "/source.db"
	dstPath := dir + "/target.db"

	src, err := sql.Open("sqlite3", srcPath)
	if err != nil {
		t.Fatalf("open source db error = %v", err)
	}
	defer src.Close()

	dst, err := sql.Open("sqlite3", dstPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer dst.Close()

	if _, err := src.Exec(`CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER)`); err != nil {
		t.Fatalf("create source table error = %v", err)
	}
	if _, err := dst.Exec(`CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER)`); err != nil {
		t.Fatalf("create target table error = %v", err)
	}

	if _, err := src.Exec(`INSERT INTO orders VALUES (1, 100)`); err != nil {
		t.Fatalf("insert source error = %v", err)
	}
	if _, err := dst.Exec(`INSERT INTO orders VALUES (1, 100)`); err != nil {
		t.Fatalf("insert target error = %v", err)
	}

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: "sqlite", Path: srcPath},
			{Name: "dst", Type: "sqlite", Path: dstPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName: "orders",
				SQL:       "SELECT id, amount FROM orders",
				SourceDB:  "src",
				TargetDB:  "dst",
				Mode:      "replace",
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config error = %v", err)
	}

	var buf bytes.Buffer
	opts := Options{
		TaskName: "orders",
		Format:   "json",
		Keys:     []string{"id"},
	}
	if err := Run(cfg, opts, &buf); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	var result Result
	if err := decodeJSON(bytes.NewReader(buf.Bytes()), &result); err != nil {
		t.Fatalf("decode json error = %v", err)
	}
	if result.Summary.Mismatch != 0 {
		t.Errorf("mismatch = %d, want 0", result.Summary.Mismatch)
	}
}

func TestWriteReportFormats(t *testing.T) {
	result := &Result{
		SourceOnly: []Row{{"id": int64(1), "name": "alice"}},
		TargetOnly: []Row{{"id": int64(2), "name": "bob"}},
		Mismatch: []MismatchRow{
			{
				Key:      map[string]any{"id": int64(3)},
				Source:   Row{"id": int64(3), "name": "charlie"},
				Target:   Row{"id": int64(3), "name": "chuck"},
				DiffCols: []string{"name"},
			},
		},
		Summary: Summary{SourceTotal: 2, TargetTotal: 2, SourceOnly: 1, TargetOnly: 1, Mismatch: 1},
	}
	columns := []database.ColumnMetadata{{Name: "id"}, {Name: "name"}}

	// JSON
	var jsonBuf bytes.Buffer
	if err := writeReport(result, columns, "json", "", &jsonBuf); err != nil {
		t.Fatalf("writeReport json error = %v", err)
	}
	if !strings.Contains(jsonBuf.String(), `"source_only"`) {
		t.Errorf("json report missing source_only")
	}

	// CSV
	var csvBuf bytes.Buffer
	if err := writeReport(result, columns, "csv", "", &csvBuf); err != nil {
		t.Fatalf("writeReport csv error = %v", err)
	}
	if !strings.Contains(csvBuf.String(), "source_only") {
		t.Errorf("csv report missing source_only header")
	}
	if !strings.Contains(csvBuf.String(), "target_only") {
		t.Errorf("csv report missing target_only header")
	}
	if !strings.Contains(csvBuf.String(), "mismatch") {
		t.Errorf("csv report missing mismatch header")
	}

	// HTML
	var htmlBuf bytes.Buffer
	if err := writeReport(result, columns, "html", "", &htmlBuf); err != nil {
		t.Fatalf("writeReport html error = %v", err)
	}
	if !strings.Contains(htmlBuf.String(), "<!DOCTYPE html>") {
		t.Errorf("html report missing doctype")
	}
	if !strings.Contains(htmlBuf.String(), "alice") {
		t.Errorf("html report missing data")
	}
}

func TestWriteReportToFile(t *testing.T) {
	result := &Result{
		SourceOnly: []Row{},
		TargetOnly: []Row{},
		Mismatch:   []MismatchRow{},
		Summary:    Summary{},
	}
	columns := []database.ColumnMetadata{{Name: "id"}}

	f := t.TempDir() + "/report.json"
	if err := writeReport(result, columns, "json", f, nil); err != nil {
		t.Fatalf("writeReport to file error = %v", err)
	}
	data, err := os.ReadFile(f)
	if err != nil {
		t.Fatalf("read file error = %v", err)
	}
	if !strings.Contains(string(data), `"summary"`) {
		t.Errorf("file content missing summary")
	}
}

func decodeJSON(r *bytes.Reader, v *Result) error {
	dec := json.NewDecoder(r)
	return dec.Decode(v)
}
