package database

import (
	"fmt"
	"strings"

	"db-ferry/config"
)

// QuoteIdentifier returns the properly quoted identifier for a given database type.
func QuoteIdentifier(dbType, name string) string {
	switch strings.ToLower(dbType) {
	case config.DatabaseTypeMySQL:
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	case config.DatabaseTypeOracle:
		upper := strings.ToUpper(name)
		return `"` + strings.ReplaceAll(upper, `"`, `""`) + `"`
	case config.DatabaseTypeSQLServer:
		return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
	case config.DatabaseTypeSQLite, config.DatabaseTypePostgreSQL, config.DatabaseTypeDuckDB:
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	default:
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
}

// MapType maps column metadata to a target database column type.
func MapType(dbType string, column ColumnMetadata) string {
	switch strings.ToLower(dbType) {
	case config.DatabaseTypeSQLite:
		return MapToSQLiteType(column)
	case config.DatabaseTypeMySQL:
		return MapToMySQLType(column)
	case config.DatabaseTypePostgreSQL:
		return MapToPostgresType(column)
	case config.DatabaseTypeOracle:
		return MapToOracleType(column)
	case config.DatabaseTypeSQLServer:
		return MapToSQLServerType(column)
	case config.DatabaseTypeDuckDB:
		return MapToDuckDBType(column)
	default:
		return "TEXT"
	}
}

// BuildDropTableSQL returns the drop table SQL for the given database type.
func BuildDropTableSQL(dbType, tableName string) string {
	q := QuoteIdentifier(dbType, tableName)
	switch strings.ToLower(dbType) {
	case config.DatabaseTypeOracle:
		return fmt.Sprintf("BEGIN EXECUTE IMMEDIATE 'DROP TABLE %s'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;", q)
	case config.DatabaseTypeSQLServer:
		literal := strings.ReplaceAll(q, "'", "''")
		return fmt.Sprintf("IF OBJECT_ID(N'%s', 'U') IS NOT NULL DROP TABLE %s", literal, q)
	default:
		return "DROP TABLE IF EXISTS " + q
	}
}

// BuildCreateTableSQL returns the CREATE TABLE SQL statements for the given database type.
// If dropExisting is true, a DROP TABLE statement is included first.
func BuildCreateTableSQL(dbType, tableName string, columns []ColumnMetadata, dropExisting bool) []string {
	if len(columns) == 0 {
		return nil
	}

	var stmts []string
	if dropExisting {
		stmts = append(stmts, BuildDropTableSQL(dbType, tableName))
	}

	columnDefs := make([]string, len(columns))
	for i, col := range columns {
		columnDefs[i] = fmt.Sprintf("%s %s", QuoteIdentifier(dbType, col.Name), MapType(dbType, col))
	}

	qTable := QuoteIdentifier(dbType, tableName)
	var createSQL string
	switch strings.ToLower(dbType) {
	case config.DatabaseTypeOracle:
		createSQL = fmt.Sprintf("CREATE TABLE %s (%s)", qTable, strings.Join(columnDefs, ", "))
	case config.DatabaseTypeSQLServer:
		createSQL = fmt.Sprintf("CREATE TABLE %s (%s)", qTable, strings.Join(columnDefs, ", "))
		if !dropExisting {
			literal := strings.ReplaceAll(qTable, "'", "''")
			createSQL = fmt.Sprintf("IF OBJECT_ID(N'%s', 'U') IS NULL %s", literal, createSQL)
		}
	default:
		createStmt := "CREATE TABLE"
		if !dropExisting {
			createStmt = "CREATE TABLE IF NOT EXISTS"
		}
		createSQL = fmt.Sprintf("%s %s (%s)", createStmt, qTable, strings.Join(columnDefs, ", "))
	}

	stmts = append(stmts, createSQL)
	return stmts
}

// BuildCreateIndexSQL returns the CREATE INDEX SQL for the given database type.
func BuildCreateIndexSQL(dbType, tableName string, index config.IndexConfig) (string, error) {
	if len(index.ParsedColumns) == 0 {
		if err := index.ParseColumns(); err != nil {
			return "", err
		}
	}

	columns := make([]string, len(index.ParsedColumns))
	for i, col := range index.ParsedColumns {
		columns[i] = fmt.Sprintf("%s %s", QuoteIdentifier(dbType, col.Name), col.Order)
	}

	uniqueStr := ""
	if index.Unique {
		uniqueStr = "UNIQUE "
	}

	qIndex := QuoteIdentifier(dbType, index.Name)
	qTable := QuoteIdentifier(dbType, tableName)

	var sql string
	switch strings.ToLower(dbType) {
	case config.DatabaseTypeSQLite:
		sql = fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)", uniqueStr, qIndex, qTable, strings.Join(columns, ", "))
		if index.Where != "" {
			sql += " WHERE " + index.Where
		}
	default:
		sql = fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)", uniqueStr, qIndex, qTable, strings.Join(columns, ", "))
	}

	return sql, nil
}

// GeneratePlanDDL generates a list of DDL statements for a task in dry-run mode.
func GeneratePlanDDL(dbType, tableName string, columns []ColumnMetadata, mode string, skipCreate bool, indexes []config.IndexConfig) ([]string, error) {
	var stmts []string

	if !skipCreate {
		switch mode {
		case config.TaskModeAppend, config.TaskModeMerge:
			stmts = BuildCreateTableSQL(dbType, tableName, columns, false)
		default:
			stmts = BuildCreateTableSQL(dbType, tableName, columns, true)
		}
	}

	for _, idx := range indexes {
		sql, err := BuildCreateIndexSQL(dbType, tableName, idx)
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, sql)
	}

	return stmts, nil
}
