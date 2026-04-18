package mcp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"db-ferry/database"

	"github.com/mark3labs/mcp-go/mcp"
)

func createTestSQLiteDB(t *testing.T, path string) database.SourceDB {
	t.Helper()
	src, err := database.NewSQLiteDB(path, 0, 0, "")
	if err != nil {
		t.Fatalf("NewSQLiteDB() error = %v", err)
	}
	if err := src.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("create table error = %v", err)
	}
	if err := src.Exec(`INSERT INTO users(id, name) VALUES (1, 'alice'), (2, 'bob')`); err != nil {
		t.Fatalf("insert rows error = %v", err)
	}
	return src
}

func TestHandleListTables(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db := createTestSQLiteDB(t, dbPath)
	defer db.Close()

	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"database": map[string]any{
					"type": "sqlite",
					"path": dbPath,
				},
			},
		},
	}

	res, err := handleListTables(context.Background(), req)
	if err != nil {
		t.Fatalf("handleListTables() error = %v", err)
	}
	if res.IsError {
		t.Fatalf("handleListTables() returned error result: %v", res.Content)
	}
}

func TestHandleListTablesInvalidDB(t *testing.T) {
	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"database": map[string]any{
					"type": "sqlite",
					"path": "/nonexistent/path/db.sqlite",
				},
			},
		},
	}

	res, err := handleListTables(context.Background(), req)
	if err != nil {
		t.Fatalf("handleListTables() error = %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected error result for invalid database connection")
	}
}

func TestHandleListTablesMissingDatabase(t *testing.T) {
	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{},
		},
	}

	res, err := handleListTables(context.Background(), req)
	if err != nil {
		t.Fatalf("handleListTables() error = %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected error result for missing database")
	}
}

func TestHandleGetSchema(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db := createTestSQLiteDB(t, dbPath)
	defer db.Close()

	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"database": map[string]any{
					"type": "sqlite",
					"path": dbPath,
				},
				"table_name": "users",
			},
		},
	}

	res, err := handleGetSchema(context.Background(), req)
	if err != nil {
		t.Fatalf("handleGetSchema() error = %v", err)
	}
	if res.IsError {
		t.Fatalf("handleGetSchema() returned error result: %v", res.Content)
	}
}

func TestHandleGetSchemaMissingDatabase(t *testing.T) {
	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"table_name": "users",
			},
		},
	}

	res, err := handleGetSchema(context.Background(), req)
	if err != nil {
		t.Fatalf("handleGetSchema() error = %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected error result for missing database")
	}
}

func TestHandleGetSchemaMissingTableName(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db := createTestSQLiteDB(t, dbPath)
	defer db.Close()

	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"database": map[string]any{
					"type": "sqlite",
					"path": dbPath,
				},
			},
		},
	}

	res, err := handleGetSchema(context.Background(), req)
	if err != nil {
		t.Fatalf("handleGetSchema() error = %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected error result for missing table_name")
	}
}

func TestHandleGetSchemaInvalidDB(t *testing.T) {
	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"database": map[string]any{
					"type": "sqlite",
					"path": "/nonexistent/path/db.sqlite",
				},
				"table_name": "users",
			},
		},
	}

	res, err := handleGetSchema(context.Background(), req)
	if err != nil {
		t.Fatalf("handleGetSchema() error = %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected error result for invalid database connection")
	}
}

func TestHandleGetSchemaMissingTable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db := createTestSQLiteDB(t, dbPath)
	defer db.Close()

	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"database": map[string]any{
					"type": "sqlite",
					"path": dbPath,
				},
				"table_name": "nonexistent",
			},
		},
	}

	res, err := handleGetSchema(context.Background(), req)
	if err != nil {
		t.Fatalf("handleGetSchema() error = %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected error result for missing table")
	}
}

func TestHandleGenerateTask(t *testing.T) {
	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"source_db": map[string]any{
					"type":     "mysql",
					"host":     "localhost",
					"port":     "3306",
					"database": "src",
					"user":     "root",
					"password": "secret",
				},
				"target_db": map[string]any{
					"type": "duckdb",
					"path": "/tmp/target.db",
				},
				"table_name": "orders",
				"mode":       "replace",
			},
		},
	}

	res, err := handleGenerateTask(context.Background(), req)
	if err != nil {
		t.Fatalf("handleGenerateTask() error = %v", err)
	}
	if res.IsError {
		t.Fatalf("handleGenerateTask() returned error result: %v", res.Content)
	}

	text := getTextFromResult(t, res)
	if !strings.Contains(text, "orders") {
		t.Fatalf("expected task to contain table name, got: %s", text)
	}
	if !strings.Contains(text, `mode = "replace"`) {
		t.Fatalf("expected task to contain replace mode, got: %s", text)
	}
}

func TestHandleGenerateTaskMergeMode(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db := createTestSQLiteDB(t, dbPath)
	defer db.Close()

	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"source_db": map[string]any{
					"type": "sqlite",
					"path": dbPath,
				},
				"target_db": map[string]any{
					"type": "sqlite",
					"path": dbPath,
				},
				"table_name": "users",
				"mode":       "merge",
			},
		},
	}

	res, err := handleGenerateTask(context.Background(), req)
	if err != nil {
		t.Fatalf("handleGenerateTask() error = %v", err)
	}
	if res.IsError {
		t.Fatalf("handleGenerateTask() returned error result: %v", res.Content)
	}

	text := getTextFromResult(t, res)
	if !strings.Contains(text, `mode = "merge"`) {
		t.Fatalf("expected task to contain merge mode, got: %s", text)
	}
	if !strings.Contains(text, "merge_keys") {
		t.Fatalf("expected task to contain merge_keys, got: %s", text)
	}
}

func TestHandleGenerateTaskMissingSource(t *testing.T) {
	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"target_db": map[string]any{
					"type": "sqlite",
					"path": "/tmp/db.sqlite",
				},
				"table_name": "users",
			},
		},
	}

	res, err := handleGenerateTask(context.Background(), req)
	if err != nil {
		t.Fatalf("handleGenerateTask() error = %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected error result for missing source_db")
	}
}

func TestHandleGenerateTaskMissingTarget(t *testing.T) {
	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"source_db": map[string]any{
					"type": "sqlite",
					"path": "/tmp/db.sqlite",
				},
				"table_name": "users",
			},
		},
	}

	res, err := handleGenerateTask(context.Background(), req)
	if err != nil {
		t.Fatalf("handleGenerateTask() error = %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected error result for missing target_db")
	}
}

func TestHandleGenerateTaskMissingTable(t *testing.T) {
	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"source_db": map[string]any{
					"type": "sqlite",
					"path": "/tmp/db.sqlite",
				},
				"target_db": map[string]any{
					"type": "sqlite",
					"path": "/tmp/db.sqlite",
				},
			},
		},
	}

	res, err := handleGenerateTask(context.Background(), req)
	if err != nil {
		t.Fatalf("handleGenerateTask() error = %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected error result for missing table_name")
	}
}

func TestHandleValidateConfigValid(t *testing.T) {
	content := `
[[databases]]
name = "src"
type = "sqlite"
path = "` + filepath.Join(t.TempDir(), "src.db") + `"

[[tasks]]
table_name = "users"
sql = "SELECT 1"
source_db = "src"
target_db = "src"
allow_same_table = true
mode = "replace"
`

	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"config_content": content,
			},
		},
	}

	res, err := handleValidateConfig(context.Background(), req)
	if err != nil {
		t.Fatalf("handleValidateConfig() error = %v", err)
	}
	if res.IsError {
		t.Fatalf("handleValidateConfig() returned error result: %v", res.Content)
	}
}

func TestHandleGenerateTaskWithNetworkTarget(t *testing.T) {
	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"source_db": map[string]any{
					"type":     "oracle",
					"host":     "localhost",
					"port":     "1521",
					"database": "src",
					"user":     "root",
					"password": "secret",
					"service":  "ORCL",
				},
				"target_db": map[string]any{
					"type":     "postgresql",
					"host":     "localhost",
					"port":     "5432",
					"database": "dst",
					"user":     "postgres",
					"password": "secret",
				},
				"table_name": "orders",
				"mode":       "replace",
			},
		},
	}

	res, err := handleGenerateTask(context.Background(), req)
	if err != nil {
		t.Fatalf("handleGenerateTask() error = %v", err)
	}
	if res.IsError {
		t.Fatalf("handleGenerateTask() returned error result: %v", res.Content)
	}

	text := getTextFromResult(t, res)
	if !strings.Contains(text, "service") {
		t.Fatalf("expected task to contain service, got: %s", text)
	}
	if !strings.Contains(text, "host = \"localhost\"") {
		t.Fatalf("expected task to contain host, got: %s", text)
	}
}

func TestHandleValidateConfigWithPath(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "task.toml")
	content := `
[[databases]]
name = "src"
type = "sqlite"
path = "` + filepath.Join(dir, "src.db") + `"

[[tasks]]
table_name = "users"
sql = "SELECT 1"
source_db = "src"
target_db = "src"
allow_same_table = true
mode = "replace"
`
	if err := os.WriteFile(cfgPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write config error = %v", err)
	}

	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"config_path": cfgPath,
			},
		},
	}

	res, err := handleValidateConfig(context.Background(), req)
	if err != nil {
		t.Fatalf("handleValidateConfig() error = %v", err)
	}
	if res.IsError {
		t.Fatalf("handleValidateConfig() returned error result: %v", res.Content)
	}
}

func TestHandleValidateConfigConnectionFail(t *testing.T) {
	content := `
[[databases]]
name = "src"
type = "sqlite"
path = "/nonexistent/path/db.sqlite"

[[tasks]]
table_name = "users"
sql = "SELECT 1"
source_db = "src"
target_db = "src"
mode = "replace"
`

	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"config_content": content,
			},
		},
	}

	res, err := handleValidateConfig(context.Background(), req)
	if err != nil {
		t.Fatalf("handleValidateConfig() error = %v", err)
	}
	if res.IsError {
		t.Fatalf("expected JSON result for connection failure, not error")
	}
	text := getTextFromResult(t, res)
	if !strings.Contains(text, "valid") {
		t.Fatalf("expected validation result in output, got: %s", text)
	}
}

func TestHandleValidateConfigInvalid(t *testing.T) {
	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"config_content": "invalid toml content {{{",
			},
		},
	}

	res, err := handleValidateConfig(context.Background(), req)
	if err != nil {
		t.Fatalf("handleValidateConfig() error = %v", err)
	}
	if res.IsError {
		t.Fatalf("expected JSON result for invalid config, not error")
	}
	text := getTextFromResult(t, res)
	if !strings.Contains(text, "valid") {
		t.Fatalf("expected validation result in output, got: %s", text)
	}
}

func TestHandleValidateConfigMissingArgs(t *testing.T) {
	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{},
		},
	}

	res, err := handleValidateConfig(context.Background(), req)
	if err != nil {
		t.Fatalf("handleValidateConfig() error = %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected error result for missing args")
	}
}

func TestHandleEstimateMigration(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db := createTestSQLiteDB(t, dbPath)
	defer db.Close()

	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"source_db": map[string]any{
					"type": "sqlite",
					"path": dbPath,
				},
				"sql": "SELECT * FROM users",
			},
		},
	}

	res, err := handleEstimateMigration(context.Background(), req)
	if err != nil {
		t.Fatalf("handleEstimateMigration() error = %v", err)
	}
	if res.IsError {
		t.Fatalf("handleEstimateMigration() returned error result: %v", res.Content)
	}

	text := getTextFromResult(t, res)
	if !strings.Contains(text, "row_count") {
		t.Fatalf("expected row_count in result, got: %s", text)
	}
}

func TestHandleEstimateMigrationWithTargetDB(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db := createTestSQLiteDB(t, dbPath)
	defer db.Close()

	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"source_db": map[string]any{
					"type": "sqlite",
					"path": dbPath,
				},
				"sql": "SELECT * FROM users",
				"target_db": map[string]any{
					"type": "postgresql",
				},
			},
		},
	}

	res, err := handleEstimateMigration(context.Background(), req)
	if err != nil {
		t.Fatalf("handleEstimateMigration() error = %v", err)
	}
	if res.IsError {
		t.Fatalf("handleEstimateMigration() returned error result: %v", res.Content)
	}

	text := getTextFromResult(t, res)
	if !strings.Contains(text, "row_count") {
		t.Fatalf("expected row_count in result, got: %s", text)
	}
}

func TestHandleEstimateMigrationMissingSource(t *testing.T) {
	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"sql": "SELECT * FROM users",
			},
		},
	}

	res, err := handleEstimateMigration(context.Background(), req)
	if err != nil {
		t.Fatalf("handleEstimateMigration() error = %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected error result for missing source_db")
	}
}

func TestHandleEstimateMigrationEmptySQL(t *testing.T) {
	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"source_db": map[string]any{
					"type": "sqlite",
					"path": "/tmp/db.sqlite",
				},
			},
		},
	}

	res, err := handleEstimateMigration(context.Background(), req)
	if err != nil {
		t.Fatalf("handleEstimateMigration() error = %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected error result for empty sql")
	}
}

func TestHandleEstimateMigrationInvalidSQL(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db := createTestSQLiteDB(t, dbPath)
	defer db.Close()

	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"source_db": map[string]any{
					"type": "sqlite",
					"path": dbPath,
				},
				"sql": "SELECT * FROM nonexistent_table",
			},
		},
	}

	res, err := handleEstimateMigration(context.Background(), req)
	if err != nil {
		t.Fatalf("handleEstimateMigration() error = %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected error result for invalid sql")
	}
}

func TestHandleEstimateMigrationInvalidDB(t *testing.T) {
	req := mcp.CallToolRequest{
		Params: mcp.CallToolParams{
			Arguments: map[string]any{
				"source_db": map[string]any{
					"type": "sqlite",
					"path": "/nonexistent/path/to/db.sqlite",
				},
				"sql": "SELECT * FROM users",
			},
		},
	}

	res, err := handleEstimateMigration(context.Background(), req)
	if err != nil {
		t.Fatalf("handleEstimateMigration() error = %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected error result for invalid database")
	}
}

func getTextFromResult(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	for _, c := range res.Content {
		if text, ok := mcp.AsTextContent(c); ok {
			return text.Text
		}
	}
	t.Fatalf("no text content in result")
	return ""
}
