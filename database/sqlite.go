package database

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"db-ferry/config"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteDB struct {
	db *sql.DB
}

var (
	_ SourceDB = (*SQLiteDB)(nil)
	_ TargetDB = (*SQLiteDB)(nil)
)

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

func (s *SQLiteDB) Query(sql string) (*sql.Rows, error) {
	log.Printf("Executing SQLite query: %s", sql)
	rows, err := s.db.Query(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sqlite query: %w", err)
	}
	return rows, nil
}

func (s *SQLiteDB) GetRowCount(sql string) (int, error) {
	var count int
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", sql)
	if err := s.db.QueryRow(countSQL).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err)
	}
	return count, nil
}

func (s *SQLiteDB) CreateTable(tableName string, columns []ColumnMetadata) error {
	return s.createTable(tableName, columns, true)
}

func (s *SQLiteDB) EnsureTable(tableName string, columns []ColumnMetadata) error {
	return s.createTable(tableName, columns, false)
}

func (s *SQLiteDB) createTable(tableName string, columns []ColumnMetadata, dropExisting bool) error {
	if len(columns) == 0 {
		return fmt.Errorf("no columns provided for table creation")
	}

	if dropExisting {
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS \"%s\"", tableName)
		log.Printf("Dropping existing table: %s", dropSQL)
		if _, err := s.db.Exec(dropSQL); err != nil {
			return fmt.Errorf("failed to drop table %s: %w", tableName, err)
		}
	}

	var columnDefs []string
	for _, col := range columns {
		sqlType := s.mapToSQLiteType(col)
		columnDefs = append(columnDefs, fmt.Sprintf(`"%s" %s`, col.Name, sqlType))
	}

	createStmt := "CREATE TABLE"
	if !dropExisting {
		createStmt = "CREATE TABLE IF NOT EXISTS"
	}
	createSQL := fmt.Sprintf("%s \"%s\" (%s)",
		createStmt, tableName, strings.Join(columnDefs, ", "))

	log.Printf("Creating new SQLite table: %s", createSQL)
	_, err := s.db.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	return nil
}

func (s *SQLiteDB) InsertData(tableName string, columns []ColumnMetadata, values [][]any) error {
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
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		placeholders[i] = "?"
		columnNames[i] = col.Name
	}

	insertSQL := fmt.Sprintf("INSERT INTO \"%s\" (\"%s\") VALUES (%s)",
		tableName,
		strings.Join(columnNames, "\", \""),
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

func (s *SQLiteDB) UpsertData(tableName string, columns []ColumnMetadata, values [][]any, mergeKeys []string) error {
	if len(values) == 0 {
		return nil
	}
	if len(mergeKeys) == 0 {
		return fmt.Errorf("merge_keys is required for upsert")
	}

	keySet := make(map[string]struct{}, len(mergeKeys))
	for _, key := range mergeKeys {
		keySet[strings.ToLower(key)] = struct{}{}
	}

	placeholders := make([]string, len(columns))
	columnNames := make([]string, len(columns))
	updateAssignments := make([]string, 0, len(columns))
	for i, col := range columns {
		placeholders[i] = "?"
		columnNames[i] = col.Name
		if _, isKey := keySet[strings.ToLower(col.Name)]; !isKey {
			updateAssignments = append(updateAssignments, fmt.Sprintf(`"%s"=excluded."%s"`, col.Name, col.Name))
		}
	}

	conflictCols := make([]string, len(mergeKeys))
	for i, key := range mergeKeys {
		conflictCols[i] = fmt.Sprintf(`"%s"`, key)
	}

	action := "DO NOTHING"
	if len(updateAssignments) > 0 {
		action = fmt.Sprintf("DO UPDATE SET %s", strings.Join(updateAssignments, ", "))
	}

	insertSQL := fmt.Sprintf("INSERT INTO \"%s\" (\"%s\") VALUES (%s) ON CONFLICT(%s) %s",
		tableName,
		strings.Join(columnNames, "\", \""),
		strings.Join(placeholders, ", "),
		strings.Join(conflictCols, ", "),
		action,
	)

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare upsert statement: %w", err)
	}
	defer stmt.Close()

	for _, row := range values {
		if _, err := stmt.Exec(row...); err != nil {
			return fmt.Errorf("failed to upsert row: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (s *SQLiteDB) GetTableRowCount(tableName string) (int, error) {
	var count int
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM \"%s\"", tableName)
	if err := s.db.QueryRow(countSQL).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to get row count for table %s: %w", tableName, err)
	}
	return count, nil
}

// CreateIndexes 为指定表创建所有索引
func (s *SQLiteDB) CreateIndexes(tableName string, indexes []config.IndexConfig) error {
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

		if err := s.createIndex(tableName, index); err != nil {
			return fmt.Errorf("failed to create index '%s' on table '%s': %w", index.Name, tableName, err)
		}
	}

	return nil
}

func (s *SQLiteDB) createIndex(tableName string, index config.IndexConfig) error {
	sql, err := s.buildIndexSQL(tableName, index)
	if err != nil {
		return err
	}

	log.Printf("Creating index: %s", sql)
	if _, err := s.db.Exec(sql); err != nil {
		return fmt.Errorf("failed to execute index creation SQL: %w", err)
	}

	log.Printf("Successfully created index '%s' on table '%s'", index.Name, tableName)
	return nil
}

func (s *SQLiteDB) buildIndexSQL(tableName string, index config.IndexConfig) (string, error) {
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

func (s *SQLiteDB) mapToSQLiteType(column ColumnMetadata) string {
	typeName := strings.ToUpper(column.DatabaseType)
	if typeName == "" {
		typeName = strings.ToUpper(column.GoType)
	}

	switch {
	case strings.Contains(typeName, "CHAR"), strings.Contains(typeName, "TEXT"), strings.Contains(typeName, "CLOB"), strings.Contains(typeName, "STRING"):
		return "TEXT"
	case strings.Contains(typeName, "NUMBER"), strings.Contains(typeName, "INT"), strings.Contains(typeName, "DEC"), strings.Contains(typeName, "NUMERIC"), strings.Contains(typeName, "REAL"), strings.Contains(typeName, "DOUBLE"), strings.Contains(typeName, "FLOAT"), strings.Contains(typeName, "BIT"), strings.Contains(typeName, "BOOL"):
		if strings.Contains(typeName, "REAL") || strings.Contains(typeName, "DOUBLE") || strings.Contains(typeName, "FLOAT") || (column.PrecisionScaleValid && column.Scale > 0) {
			return "REAL"
		}
		return "INTEGER"
	case strings.Contains(typeName, "DATE"), strings.Contains(typeName, "TIME"):
		return "TEXT"
	case strings.Contains(typeName, "BLOB"), strings.Contains(typeName, "BINARY"), strings.Contains(typeName, "RAW"):
		return "BLOB"
	default:
		return "TEXT"
	}
}
