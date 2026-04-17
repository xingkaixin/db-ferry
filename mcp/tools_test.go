package mcp

import (
	"context"
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
