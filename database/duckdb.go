//go:build !windows

package database

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"db-ferry/config"

	_ "github.com/duckdb/duckdb-go/v2"
)

type DuckDB struct {
	db *sql.DB
}

var (
	_ SourceDB = (*DuckDB)(nil)
	_ TargetDB = (*DuckDB)(nil)
)

func NewDuckDB(path string) (*DuckDB, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb database: %w", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping duckdb database: %w", err)
	}

	log.Printf("Successfully connected to DuckDB database at %s", path)
	return &DuckDB{db: db}, nil
}

func (d *DuckDB) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

func (d *DuckDB) Query(sql string) (*sql.Rows, error) {
	log.Printf("Executing DuckDB query: %s", sql)
	rows, err := d.db.Query(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute duckdb query: %w", err)
	}
	return rows, nil
}

func (d *DuckDB) GetRowCount(sql string) (int, error) {
	var count int
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS count_query", sql)
	if err := d.db.QueryRow(countSQL).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err)
	}
	return count, nil
}

func (d *DuckDB) CreateTable(tableName string, columns []ColumnMetadata) error {
	if len(columns) == 0 {
		return fmt.Errorf("no columns provided for table creation")
	}

	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", d.quoteIdentifier(tableName))
	if _, err := d.db.Exec(dropSQL); err != nil {
		return fmt.Errorf("failed to drop table %s: %w", tableName, err)
	}

	columnDefs := make([]string, len(columns))
	for i, col := range columns {
		columnDefs[i] = fmt.Sprintf("%s %s", d.quoteIdentifier(col.Name), d.mapToDuckDBType(col))
	}

	createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", d.quoteIdentifier(tableName), strings.Join(columnDefs, ", "))
	if _, err := d.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	return nil
}

func (d *DuckDB) InsertData(tableName string, columns []ColumnMetadata, values [][]any) error {
	if len(values) == 0 {
		return nil
	}

	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	placeholders := make([]string, len(columns))
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		placeholders[i] = "?"
		columnNames[i] = d.quoteIdentifier(col.Name)
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		d.quoteIdentifier(tableName),
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "))

	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	for _, row := range values {
		if _, err := stmt.Exec(row...); err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (d *DuckDB) CreateIndexes(tableName string, indexes []config.IndexConfig) error {
	if len(indexes) == 0 {
		return nil
	}

	for _, idx := range indexes {
		index := idx
		if len(index.ParsedColumns) == 0 {
			if err := index.ParseColumns(); err != nil {
				return fmt.Errorf("failed to parse index columns for '%s': %w", index.Name, err)
			}
		}

		if err := d.createIndex(tableName, index); err != nil {
			return fmt.Errorf("failed to create index '%s' on table '%s': %w", index.Name, tableName, err)
		}
	}

	return nil
}

func (d *DuckDB) createIndex(tableName string, index config.IndexConfig) error {
	if _, err := d.db.Exec(fmt.Sprintf("DROP INDEX IF EXISTS %s", d.quoteIdentifier(index.Name))); err != nil {
		log.Printf("Warning: failed to drop existing index '%s': %v", index.Name, err)
	}

	columns := make([]string, len(index.ParsedColumns))
	for i, col := range index.ParsedColumns {
		columns[i] = fmt.Sprintf("%s %s", d.quoteIdentifier(col.Name), col.Order)
	}

	uniqueStr := ""
	if index.Unique {
		uniqueStr = "UNIQUE "
	}

	createSQL := fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
		uniqueStr,
		d.quoteIdentifier(index.Name),
		d.quoteIdentifier(tableName),
		strings.Join(columns, ", "))

	if _, err := d.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create index '%s': %w", index.Name, err)
	}

	return nil
}

func (d *DuckDB) mapToDuckDBType(column ColumnMetadata) string {
	typeName := strings.ToUpper(column.DatabaseType)
	if typeName == "" {
		typeName = strings.ToUpper(column.GoType)
	}

	length := int64(0)
	if column.LengthValid {
		length = column.Length
	}

	precision := int64(0)
	scale := int64(0)
	if column.PrecisionScaleValid {
		precision = column.Precision
		scale = column.Scale
	}

	switch {
	case strings.Contains(typeName, "INT"), strings.Contains(typeName, "NUMBER") && !column.PrecisionScaleValid:
		return "BIGINT"
	case strings.Contains(typeName, "DEC"), strings.Contains(typeName, "NUMERIC"), strings.Contains(typeName, "NUMBER"):
		if precision > 0 {
			if scale < 0 {
				scale = 0
			}
			return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
		}
		return "DECIMAL(38,0)"
	case strings.Contains(typeName, "DOUBLE"), strings.Contains(typeName, "FLOAT"), strings.Contains(typeName, "REAL"):
		return "DOUBLE"
	case strings.Contains(typeName, "CHAR"), strings.Contains(typeName, "TEXT"), strings.Contains(typeName, "CLOB"), strings.Contains(typeName, "STRING"):
		if length > 0 && length <= 1048576 {
			return fmt.Sprintf("VARCHAR(%d)", length)
		}
		return "VARCHAR"
	case strings.Contains(typeName, "DATE"), strings.Contains(typeName, "TIME"):
		return "TIMESTAMP"
	case strings.Contains(typeName, "BLOB"), strings.Contains(typeName, "BINARY"), strings.Contains(typeName, "RAW"):
		return "BLOB"
	case strings.Contains(typeName, "BOOL"):
		return "BOOLEAN"
	default:
		if column.PrecisionScaleValid && column.Scale > 0 {
			return "DOUBLE"
		}
		return "VARCHAR"
	}
}

func (d *DuckDB) quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
