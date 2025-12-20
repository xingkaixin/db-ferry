//go:build windows

package database

import (
	"database/sql"
	"fmt"

	"db-ferry/config"
)

type DuckDB struct {
	db *sql.DB
}

var (
	_ SourceDB = (*DuckDB)(nil)
	_ TargetDB = (*DuckDB)(nil)
)

func NewDuckDB(path string) (*DuckDB, error) {
	return nil, fmt.Errorf("duckdb is not supported on windows builds")
}

func (d *DuckDB) Close() error {
	return fmt.Errorf("duckdb is not supported on windows builds")
}

func (d *DuckDB) Query(sql string) (*sql.Rows, error) {
	return nil, fmt.Errorf("duckdb is not supported on windows builds")
}

func (d *DuckDB) GetRowCount(sql string) (int, error) {
	return 0, fmt.Errorf("duckdb is not supported on windows builds")
}

func (d *DuckDB) CreateTable(tableName string, columns []ColumnMetadata) error {
	return fmt.Errorf("duckdb is not supported on windows builds")
}

func (d *DuckDB) EnsureTable(tableName string, columns []ColumnMetadata) error {
	return fmt.Errorf("duckdb is not supported on windows builds")
}

func (d *DuckDB) InsertData(tableName string, columns []ColumnMetadata, values [][]any) error {
	return fmt.Errorf("duckdb is not supported on windows builds")
}

func (d *DuckDB) UpsertData(tableName string, columns []ColumnMetadata, values [][]any, mergeKeys []string) error {
	return fmt.Errorf("duckdb is not supported on windows builds")
}

func (d *DuckDB) GetTableRowCount(tableName string) (int, error) {
	return 0, fmt.Errorf("duckdb is not supported on windows builds")
}

func (d *DuckDB) CreateIndexes(tableName string, indexes []config.IndexConfig) error {
	return fmt.Errorf("duckdb is not supported on windows builds")
}
