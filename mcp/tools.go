package mcp

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"db-ferry/config"
	"db-ferry/database"

	"github.com/BurntSushi/toml"
	"github.com/mark3labs/mcp-go/mcp"
)

func parseDatabaseConfigFromMap(m map[string]any) config.DatabaseConfig {
	return config.DatabaseConfig{
		Type:     getString(m, "type"),
		Host:     getString(m, "host"),
		Port:     getString(m, "port"),
		Database: getString(m, "database"),
		User:     getString(m, "user"),
		Password: getString(m, "password"),
		Path:     getString(m, "path"),
		Service:  getString(m, "service"),
	}
}

func handleListTables(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	dbMap, err := extractDatabaseConfig(req, "database")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	dbCfg := parseDatabaseConfigFromMap(dbMap)
	src, err := database.OpenSource(dbCfg)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to connect to database: %v", err)), nil
	}
	defer src.Close()

	tables, err := src.GetTables()
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to list tables: %v", err)), nil
	}

	return mcp.NewToolResultJSON(map[string]any{
		"tables": tables,
	})
}

func handleGetSchema(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	dbMap, err := extractDatabaseConfig(req, "database")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	tableName := mcp.ParseString(req, "table_name", "")
	if tableName == "" {
		return mcp.NewToolResultError("table_name is required"), nil
	}

	dbCfg := parseDatabaseConfigFromMap(dbMap)
	src, err := database.OpenSource(dbCfg)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to connect to database: %v", err)), nil
	}
	defer src.Close()

	columns, err := database.GetTableSchema(src, tableName)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get table schema: %v", err)), nil
	}

	pk, _ := database.GetTablePrimaryKey(src, dbCfg.Type, tableName)
	indexes, _ := database.GetTableIndexes(src, dbCfg.Type, tableName)

	return mcp.NewToolResultJSON(map[string]any{
		"columns":     columns,
		"primary_key": pk,
		"indexes":     indexes,
	})
}

func handleGenerateTask(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	sourceMap, err := extractDatabaseConfig(req, "source_db")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	targetMap, err := extractDatabaseConfig(req, "target_db")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	tableName := mcp.ParseString(req, "table_name", "")
	if tableName == "" {
		return mcp.NewToolResultError("table_name is required"), nil
	}

	sqlText := mcp.ParseString(req, "sql", "")
	mode := mcp.ParseString(req, "mode", "replace")

	sourceCfg := parseDatabaseConfigFromMap(sourceMap)
	targetCfg := parseDatabaseConfigFromMap(targetMap)

	if sqlText == "" {
		sqlText = fmt.Sprintf("SELECT * FROM %s", tableName)
	}

	var mergeKeys []string
	if mode == "merge" || mode == "upsert" {
		src, err := database.OpenSource(sourceCfg)
		if err == nil {
			defer src.Close()
			pk, _ := database.GetTablePrimaryKey(src, sourceCfg.Type, tableName)
			if len(pk) > 0 {
				mergeKeys = pk
			}
		}
	}

	task := config.TaskConfig{
		TableName: tableName,
		SQL:       sqlText,
		SourceDB:  sourceCfg.Name,
		TargetDB:  targetCfg.Name,
		Mode:      mode,
		BatchSize: 1000,
		MergeKeys: mergeKeys,
	}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "[[databases]]\n")
	fmt.Fprintf(&buf, "name = \"source\"\n")
	fmt.Fprintf(&buf, "type = \"%s\"\n", sourceCfg.Type)
	if sourceCfg.Host != "" {
		fmt.Fprintf(&buf, "host = \"%s\"\n", sourceCfg.Host)
	}
	if sourceCfg.Port != "" {
		fmt.Fprintf(&buf, "port = \"%s\"\n", sourceCfg.Port)
	}
	if sourceCfg.Database != "" {
		fmt.Fprintf(&buf, "database = \"%s\"\n", sourceCfg.Database)
	}
	if sourceCfg.User != "" {
		fmt.Fprintf(&buf, "user = \"%s\"\n", sourceCfg.User)
	}
	if sourceCfg.Password != "" {
		fmt.Fprintf(&buf, "password = \"%s\"\n", sourceCfg.Password)
	}
	if sourceCfg.Path != "" {
		fmt.Fprintf(&buf, "path = \"%s\"\n", sourceCfg.Path)
	}
	if sourceCfg.Service != "" {
		fmt.Fprintf(&buf, "service = \"%s\"\n", sourceCfg.Service)
	}
	fmt.Fprintln(&buf)

	fmt.Fprintf(&buf, "[[databases]]\n")
	fmt.Fprintf(&buf, "name = \"target\"\n")
	fmt.Fprintf(&buf, "type = \"%s\"\n", targetCfg.Type)
	if targetCfg.Host != "" {
		fmt.Fprintf(&buf, "host = \"%s\"\n", targetCfg.Host)
	}
	if targetCfg.Port != "" {
		fmt.Fprintf(&buf, "port = \"%s\"\n", targetCfg.Port)
	}
	if targetCfg.Database != "" {
		fmt.Fprintf(&buf, "database = \"%s\"\n", targetCfg.Database)
	}
	if targetCfg.User != "" {
		fmt.Fprintf(&buf, "user = \"%s\"\n", targetCfg.User)
	}
	if targetCfg.Password != "" {
		fmt.Fprintf(&buf, "password = \"%s\"\n", targetCfg.Password)
	}
	if targetCfg.Path != "" {
		fmt.Fprintf(&buf, "path = \"%s\"\n", targetCfg.Path)
	}
	if targetCfg.Service != "" {
		fmt.Fprintf(&buf, "service = \"%s\"\n", targetCfg.Service)
	}
	fmt.Fprintln(&buf)

	fmt.Fprintf(&buf, "[[tasks]]\n")
	enc := toml.NewEncoder(&buf)
	if err := enc.Encode(task); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to encode task: %v", err)), nil
	}

	return mcp.NewToolResultText(buf.String()), nil
}

func handleValidateConfig(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	configPath := mcp.ParseString(req, "config_path", "")
	configContent := mcp.ParseString(req, "config_content", "")

	if configPath == "" && configContent == "" {
		return mcp.NewToolResultError("either config_path or config_content is required"), nil
	}

	var cfg *config.Config
	var err error

	if configContent != "" {
		cfg = &config.Config{}
		_, err = toml.Decode(configContent, cfg)
	} else {
		cfg, err = config.LoadConfig(configPath)
	}

	if err != nil {
		return mcp.NewToolResultJSON(map[string]any{
			"valid":  false,
			"error":  err.Error(),
			"detail": "configuration parsing/validation failed",
		})
	}

	// Connection check
	manager := database.NewConnectionManager(cfg)
	defer manager.CloseAll()

	connResults := make(map[string]string)
	for _, dbCfg := range cfg.Databases {
		_, err := manager.GetSource(dbCfg.Name)
		if err != nil {
			connResults[dbCfg.Name] = fmt.Sprintf("failed: %v", err)
		} else {
			connResults[dbCfg.Name] = "ok"
		}
	}

	return mcp.NewToolResultJSON(map[string]any{
		"valid":      true,
		"databases":  connResults,
		"task_count": len(cfg.Tasks),
	})
}

func handleEstimateMigration(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	sourceMap, err := extractDatabaseConfig(req, "source_db")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	sqlText := mcp.ParseString(req, "sql", "")
	if sqlText == "" {
		return mcp.NewToolResultError("sql is required"), nil
	}

	sourceCfg := parseDatabaseConfigFromMap(sourceMap)
	src, err := database.OpenSource(sourceCfg)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to connect to source database: %v", err)), nil
	}
	defer src.Close()

	rowCount, err := src.GetRowCount(sqlText)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get row count: %v", err)), nil
	}

	// Rough heuristic: base 5s + 0.05s per 1000 rows for file targets, 0.02s for network targets.
	var secondsPerK float64 = 0.05
	if targetMap, ok := req.GetArguments()["target_db"].(map[string]any); ok {
		targetType := strings.ToLower(getString(targetMap, "type"))
		if targetType == config.DatabaseTypePostgreSQL || targetType == config.DatabaseTypeMySQL ||
			targetType == config.DatabaseTypeSQLServer || targetType == config.DatabaseTypeOracle {
			secondsPerK = 0.02
		}
	}

	estimatedSeconds := 5.0 + float64(rowCount)/1000.0*secondsPerK

	return mcp.NewToolResultJSON(map[string]any{
		"row_count":         rowCount,
		"estimated_seconds": int(estimatedSeconds),
		"estimated_minutes": fmt.Sprintf("%.1f", estimatedSeconds/60.0),
	})
}
