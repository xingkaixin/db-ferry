package database

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"cbd_data_go/config"
	_ "github.com/mattn/go-sqlite3"
)

type SQLiteDB struct {
	db *sql.DB
}

func NewSQLiteDB(dbPath string) (*SQLiteDB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping sqlite database: %w", err)
	}

	log.Printf("Successfully connected to SQLite database at %s", dbPath)
	return &SQLiteDB{db: db}, nil
}

func (s *SQLiteDB) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *SQLiteDB) CreateTable(tableName string, columns []string, columnTypes []string) error {
	if len(columns) != len(columnTypes) {
		return fmt.Errorf("columns and columnTypes length mismatch")
	}

	// Drop existing table if it exists
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS \"%s\"", tableName)
	log.Printf("Dropping existing table: %s", dropSQL)
	if _, err := s.db.Exec(dropSQL); err != nil {
		return fmt.Errorf("failed to drop table %s: %w", tableName, err)
	}

	// Create new table
	var columnDefs []string
	for i, col := range columns {
		sqlType := s.mapOracleTypeToSQLite(columnTypes[i])
		columnDefs = append(columnDefs, fmt.Sprintf(`"%s" %s`, col, sqlType))
	}

	createSQL := fmt.Sprintf("CREATE TABLE \"%s\" (%s)",
		tableName, strings.Join(columnDefs, ", "))

	log.Printf("Creating new SQLite table: %s", createSQL)
	_, err := s.db.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	return nil
}

func (s *SQLiteDB) InsertData(tableName string, columns []string, values [][]any) error {
	if len(values) == 0 {
		return nil
	}

	// Start transaction
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Prepare insert statement
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	insertSQL := fmt.Sprintf("INSERT INTO \"%s\" (\"%s\") VALUES (%s)",
		tableName,
		strings.Join(columns, "\", \""),
		strings.Join(placeholders, ", "))

	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	// Insert data in batches
	for _, row := range values {
		_, err := stmt.Exec(row...)
		if err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}


// CreateIndexes 为指定表创建所有索引
func (s *SQLiteDB) CreateIndexes(tableName string, indexes []config.IndexConfig) error {
	if len(indexes) == 0 {
		return nil
	}

	for _, index := range indexes {
		if err := s.CreateIndex(tableName, index); err != nil {
			return fmt.Errorf("failed to create index '%s' on table '%s': %w", index.Name, tableName, err)
		}
	}

	return nil
}

// CreateIndex 创建单个索引
func (s *SQLiteDB) CreateIndex(tableName string, index config.IndexConfig) error {
	sql, err := s.CreateIndexSQL(tableName, index)
	if err != nil {
		return err
	}

	log.Printf("Creating index: %s", sql)
	_, err = s.db.Exec(sql)
	if err != nil {
		return fmt.Errorf("failed to execute index creation SQL: %w", err)
	}

	log.Printf("Successfully created index '%s' on table '%s'", index.Name, tableName)
	return nil
}

// CreateIndexSQL 生成创建索引的 SQL 语句
func (s *SQLiteDB) CreateIndexSQL(tableName string, index config.IndexConfig) (string, error) {
	if err := index.ParseColumns(); err != nil {
		return "", fmt.Errorf("failed to parse index columns: %w", err)
	}

	var columnDefs []string
	for _, col := range index.ParsedColumns {
		columnDefs = append(columnDefs, fmt.Sprintf(`"%s" %s`, col.Name, col.Order))
	}

	uniqueStr := ""
	if index.Unique {
		uniqueStr = "UNIQUE "
	}

	sql := fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS \"%s\" ON \"%s\" (%s)",
		uniqueStr,
		index.Name,
		tableName,
		strings.Join(columnDefs, ", "))

	if index.Where != "" {
		sql += " WHERE " + index.Where
	}

	return sql, nil
}

func (s *SQLiteDB) mapOracleTypeToSQLite(oracleType string) string {
	oracleType = strings.ToUpper(oracleType)

	switch {
	case strings.Contains(oracleType, "VARCHAR2"), strings.Contains(oracleType, "CHAR"), strings.Contains(oracleType, "CLOB"):
		return "TEXT"
	case strings.Contains(oracleType, "NUMBER"), strings.Contains(oracleType, "INTEGER"), strings.Contains(oracleType, "DECIMAL"):
		if strings.Contains(oracleType, "(") && strings.Contains(oracleType, ",") {
			return "REAL" // For numbers with precision/scale
		}
		return "INTEGER"
	case strings.Contains(oracleType, "DATE"), strings.Contains(oracleType, "TIMESTAMP"):
		return "TEXT" // Store dates as TEXT in ISO format
	case strings.Contains(oracleType, "BLOB"):
		return "BLOB"
	default:
		return "TEXT"
	}
}