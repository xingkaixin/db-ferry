package mcp

import (
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// Server wraps the MCP server for db-ferry.
type Server struct {
	mcpServer *server.MCPServer
}

// NewServer creates a new db-ferry MCP server.
func NewServer(version string) *Server {
	s := &Server{
		mcpServer: server.NewMCPServer(
			"db-ferry",
			version,
		),
	}
	s.registerTools()
	return s
}

// ServeStdio starts the MCP server over stdio.
func (s *Server) ServeStdio() error {
	return server.ServeStdio(s.mcpServer)
}

func (s *Server) registerTools() {
	s.mcpServer.AddTool(mcp.NewTool(
		"db_ferry_list_tables",
		mcp.WithDescription("List all tables and views in a database"),
		mcp.WithObject("database",
			mcp.Required(),
			mcp.Description("Database connection configuration"),
		),
		mcp.WithString("database.type",
			mcp.Required(),
			mcp.Description("Database type: mysql, postgresql, sqlite, duckdb, sqlserver, oracle"),
		),
		mcp.WithString("database.host",
			mcp.Description("Database host (for network databases)"),
		),
		mcp.WithString("database.port",
			mcp.Description("Database port (for network databases)"),
		),
		mcp.WithString("database.database",
			mcp.Description("Database name (for network databases)"),
		),
		mcp.WithString("database.user",
			mcp.Description("Database user (for network databases)"),
		),
		mcp.WithString("database.password",
			mcp.Description("Database password (for network databases)"),
		),
		mcp.WithString("database.path",
			mcp.Description("Database file path (for sqlite, duckdb)"),
		),
		mcp.WithString("database.service",
			mcp.Description("Oracle service name"),
		),
	), handleListTables)

	s.mcpServer.AddTool(mcp.NewTool(
		"db_ferry_get_schema",
		mcp.WithDescription("Get the schema (columns, primary key, indexes) of a table"),
		mcp.WithObject("database",
			mcp.Required(),
			mcp.Description("Database connection configuration"),
		),
		mcp.WithString("database.type",
			mcp.Required(),
			mcp.Description("Database type: mysql, postgresql, sqlite, duckdb, sqlserver, oracle"),
		),
		mcp.WithString("database.host",
			mcp.Description("Database host (for network databases)"),
		),
		mcp.WithString("database.port",
			mcp.Description("Database port (for network databases)"),
		),
		mcp.WithString("database.database",
			mcp.Description("Database name (for network databases)"),
		),
		mcp.WithString("database.user",
			mcp.Description("Database user (for network databases)"),
		),
		mcp.WithString("database.password",
			mcp.Description("Database password (for network databases)"),
		),
		mcp.WithString("database.path",
			mcp.Description("Database file path (for sqlite, duckdb)"),
		),
		mcp.WithString("database.service",
			mcp.Description("Oracle service name"),
		),
		mcp.WithString("table_name",
			mcp.Required(),
			mcp.Description("Name of the table to introspect"),
		),
	), handleGetSchema)

	s.mcpServer.AddTool(mcp.NewTool(
		"db_ferry_generate_task",
		mcp.WithDescription("Generate a recommended task.toml snippet for migrating a table"),
		mcp.WithObject("source_db",
			mcp.Required(),
			mcp.Description("Source database connection configuration"),
		),
		mcp.WithString("source_db.type",
			mcp.Required(),
			mcp.Description("Database type: mysql, postgresql, sqlite, duckdb, sqlserver, oracle"),
		),
		mcp.WithString("source_db.host",
			mcp.Description("Database host (for network databases)"),
		),
		mcp.WithString("source_db.port",
			mcp.Description("Database port (for network databases)"),
		),
		mcp.WithString("source_db.database",
			mcp.Description("Database name (for network databases)"),
		),
		mcp.WithString("source_db.user",
			mcp.Description("Database user (for network databases)"),
		),
		mcp.WithString("source_db.password",
			mcp.Description("Database password (for network databases)"),
		),
		mcp.WithString("source_db.path",
			mcp.Description("Database file path (for sqlite, duckdb)"),
		),
		mcp.WithString("source_db.service",
			mcp.Description("Oracle service name"),
		),
		mcp.WithObject("target_db",
			mcp.Required(),
			mcp.Description("Target database connection configuration"),
		),
		mcp.WithString("target_db.type",
			mcp.Required(),
			mcp.Description("Database type: mysql, postgresql, sqlite, duckdb, sqlserver, oracle"),
		),
		mcp.WithString("target_db.host",
			mcp.Description("Database host (for network databases)"),
		),
		mcp.WithString("target_db.port",
			mcp.Description("Database port (for network databases)"),
		),
		mcp.WithString("target_db.database",
			mcp.Description("Database name (for network databases)"),
		),
		mcp.WithString("target_db.user",
			mcp.Description("Database user (for network databases)"),
		),
		mcp.WithString("target_db.password",
			mcp.Description("Database password (for network databases)"),
		),
		mcp.WithString("target_db.path",
			mcp.Description("Database file path (for sqlite, duckdb)"),
		),
		mcp.WithString("target_db.service",
			mcp.Description("Oracle service name"),
		),
		mcp.WithString("table_name",
			mcp.Required(),
			mcp.Description("Name of the table to migrate"),
		),
		mcp.WithString("sql",
			mcp.Description("Custom SQL query (default: SELECT * FROM table_name)"),
		),
		mcp.WithString("mode",
			mcp.Description("Migration mode: replace, append, merge (default: replace)"),
		),
	), handleGenerateTask)

	s.mcpServer.AddTool(mcp.NewTool(
		"db_ferry_validate_config",
		mcp.WithDescription("Validate a task.toml configuration file or content"),
		mcp.WithString("config_path",
			mcp.Description("Path to the task.toml file"),
		),
		mcp.WithString("config_content",
			mcp.Description("Raw TOML configuration content"),
		),
	), handleValidateConfig)

	s.mcpServer.AddTool(mcp.NewTool(
		"db_ferry_estimate_migration",
		mcp.WithDescription("Estimate migration time based on row count and table size"),
		mcp.WithObject("source_db",
			mcp.Required(),
			mcp.Description("Source database connection configuration"),
		),
		mcp.WithString("source_db.type",
			mcp.Required(),
			mcp.Description("Database type: mysql, postgresql, sqlite, duckdb, sqlserver, oracle"),
		),
		mcp.WithString("source_db.host",
			mcp.Description("Database host (for network databases)"),
		),
		mcp.WithString("source_db.port",
			mcp.Description("Database port (for network databases)"),
		),
		mcp.WithString("source_db.database",
			mcp.Description("Database name (for network databases)"),
		),
		mcp.WithString("source_db.user",
			mcp.Description("Database user (for network databases)"),
		),
		mcp.WithString("source_db.password",
			mcp.Description("Database password (for network databases)"),
		),
		mcp.WithString("source_db.path",
			mcp.Description("Database file path (for sqlite, duckdb)"),
		),
		mcp.WithString("source_db.service",
			mcp.Description("Oracle service name"),
		),
		mcp.WithString("sql",
			mcp.Required(),
			mcp.Description("SQL query to estimate"),
		),
		mcp.WithObject("target_db",
			mcp.Description("Target database connection configuration (optional, used for throughput hints)"),
		),
		mcp.WithString("target_db.type",
			mcp.Description("Database type: mysql, postgresql, sqlite, duckdb, sqlserver, oracle"),
		),
		mcp.WithString("target_db.host",
			mcp.Description("Database host (for network databases)"),
		),
		mcp.WithString("target_db.port",
			mcp.Description("Database port (for network databases)"),
		),
		mcp.WithString("target_db.database",
			mcp.Description("Database name (for network databases)"),
		),
		mcp.WithString("target_db.user",
			mcp.Description("Database user (for network databases)"),
		),
		mcp.WithString("target_db.password",
			mcp.Description("Database password (for network databases)"),
		),
		mcp.WithString("target_db.path",
			mcp.Description("Database file path (for sqlite, duckdb)"),
		),
		mcp.WithString("target_db.service",
			mcp.Description("Oracle service name"),
		),
	), handleEstimateMigration)
}

func extractDatabaseConfig(req mcp.CallToolRequest, prefix string) (map[string]any, error) {
	args := req.GetArguments()
	val, ok := args[prefix]
	if !ok {
		return nil, fmt.Errorf("missing %s configuration", prefix)
	}
	m, ok := val.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid %s configuration", prefix)
	}
	return m, nil
}

func getString(m map[string]any, key string) string {
	v, ok := m[key].(string)
	if !ok {
		return ""
	}
	return v
}
