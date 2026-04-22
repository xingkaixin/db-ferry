package mcp

import (
	"testing"

	mcptypes "github.com/mark3labs/mcp-go/mcp"
)

func TestNewServer(t *testing.T) {
	srv := NewServer("test-version")
	//nolint:staticcheck
	if srv == nil {
		t.Fatal("NewServer() returned nil")
	}
	if srv.mcpServer == nil {
		t.Fatal("mcpServer not initialized")
	}
}

func TestExtractDatabaseConfigInvalidType(t *testing.T) {
	req := mcptypes.CallToolRequest{
		Params: mcptypes.CallToolParams{
			Arguments: map[string]any{
				"database": "not-a-map",
			},
		},
	}

	_, err := extractDatabaseConfig(req, "database")
	if err == nil {
		t.Fatal("expected error for invalid database config type")
	}
}
