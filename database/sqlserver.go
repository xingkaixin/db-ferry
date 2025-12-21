package database

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"db-ferry/config"

	_ "github.com/denisenkom/go-mssqldb"
)

type SQLServerDB struct {
	db *sql.DB
}

var (
	_ SourceDB = (*SQLServerDB)(nil)
	_ TargetDB = (*SQLServerDB)(nil)
)

func NewSQLServerDB(connectionString string) (*SQLServerDB, error) {
	db, err := sql.Open("sqlserver", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlserver connection: %w", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping sqlserver database: %w", err)
	}

	log.Println("Successfully connected to SQL Server database")
	return &SQLServerDB{db: db}, nil
}

func (s *SQLServerDB) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *SQLServerDB) Query(sql string) (*sql.Rows, error) {
	log.Printf("Executing SQL Server query: %s", sql)
	rows, err := s.db.Query(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sqlserver query: %w", err)
	}
	return rows, nil
}

func (s *SQLServerDB) GetRowCount(sql string) (int, error) {
	var count int
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS count_query", sql)
	if err := s.db.QueryRow(countSQL).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err)
	}
	return count, nil
}

func (s *SQLServerDB) CreateTable(tableName string, columns []ColumnMetadata) error {
	return s.createTable(tableName, columns, true)
}

func (s *SQLServerDB) EnsureTable(tableName string, columns []ColumnMetadata) error {
	return s.createTable(tableName, columns, false)
}

func (s *SQLServerDB) createTable(tableName string, columns []ColumnMetadata, dropExisting bool) error {
	if len(columns) == 0 {
		return fmt.Errorf("no columns provided for table creation")
	}

	if dropExisting {
		dropSQL := fmt.Sprintf("IF OBJECT_ID(N'%s', 'U') IS NOT NULL DROP TABLE %s",
			s.objectNameLiteral(tableName),
			s.quoteIdentifier(tableName),
		)
		log.Printf("Dropping existing SQL Server table: %s", dropSQL)
		if _, err := s.db.Exec(dropSQL); err != nil {
			return fmt.Errorf("failed to drop table %s: %w", tableName, err)
		}
	}

	columnDefs := make([]string, len(columns))
	for i, col := range columns {
		typeDef := s.mapToSQLServerType(col)
		columnDefs[i] = fmt.Sprintf("%s %s", s.quoteIdentifier(col.Name), typeDef)
	}

	createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", s.quoteIdentifier(tableName), strings.Join(columnDefs, ", "))
	if !dropExisting {
		createSQL = fmt.Sprintf("IF OBJECT_ID(N'%s', 'U') IS NULL %s", s.objectNameLiteral(tableName), createSQL)
	}

	log.Printf("Creating new SQL Server table: %s", createSQL)
	if _, err := s.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	return nil
}

func (s *SQLServerDB) InsertData(tableName string, columns []ColumnMetadata, values [][]any) error {
	if len(values) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	placeholders := buildSQLServerPlaceholders(len(columns))
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = s.quoteIdentifier(col.Name)
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		s.quoteIdentifier(tableName),
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

func (s *SQLServerDB) UpsertData(tableName string, columns []ColumnMetadata, values [][]any, mergeKeys []string) error {
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

	placeholders := buildSQLServerPlaceholders(len(columns))
	columnNames := make([]string, len(columns))
	sourceColumnNames := make([]string, len(columns))
	updateAssignments := make([]string, 0, len(columns))
	sourceRefs := make([]string, len(columns))
	for i, col := range columns {
		quoted := s.quoteIdentifier(col.Name)
		columnNames[i] = quoted
		sourceColumnNames[i] = quoted
		sourceRefs[i] = fmt.Sprintf("source.%s", quoted)
		if _, isKey := keySet[strings.ToLower(col.Name)]; !isKey {
			updateAssignments = append(updateAssignments, fmt.Sprintf("target.%s=source.%s", quoted, quoted))
		}
	}

	onClauses := make([]string, len(mergeKeys))
	for i, key := range mergeKeys {
		quoted := s.quoteIdentifier(key)
		onClauses[i] = fmt.Sprintf("target.%s=source.%s", quoted, quoted)
	}

	mergeSQL := fmt.Sprintf("MERGE INTO %s AS target USING (VALUES (%s)) AS source (%s) ON %s",
		s.quoteIdentifier(tableName),
		strings.Join(placeholders, ", "),
		strings.Join(sourceColumnNames, ", "),
		strings.Join(onClauses, " AND "),
	)

	if len(updateAssignments) > 0 {
		mergeSQL += " WHEN MATCHED THEN UPDATE SET " + strings.Join(updateAssignments, ", ")
	}

	mergeSQL += fmt.Sprintf(" WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
		strings.Join(columnNames, ", "),
		strings.Join(sourceRefs, ", "),
	)

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(mergeSQL)
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

func (s *SQLServerDB) GetTableRowCount(tableName string) (int, error) {
	var count int
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", s.quoteIdentifier(tableName))
	if err := s.db.QueryRow(countSQL).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to get row count for table %s: %w", tableName, err)
	}
	return count, nil
}

func (s *SQLServerDB) CreateIndexes(tableName string, indexes []config.IndexConfig) error {
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

func (s *SQLServerDB) createIndex(tableName string, index config.IndexConfig) error {
	dropSQL := fmt.Sprintf("DROP INDEX IF EXISTS %s ON %s",
		s.quoteIdentifier(index.Name),
		s.quoteIdentifier(tableName),
	)
	if _, err := s.db.Exec(dropSQL); err != nil {
		log.Printf("Warning: failed to drop existing index '%s': %v", index.Name, err)
	}

	columns := make([]string, len(index.ParsedColumns))
	for i, col := range index.ParsedColumns {
		columns[i] = fmt.Sprintf("%s %s", s.quoteIdentifier(col.Name), col.Order)
	}

	uniqueStr := ""
	if index.Unique {
		uniqueStr = "UNIQUE "
	}

	createSQL := fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
		uniqueStr,
		s.quoteIdentifier(index.Name),
		s.quoteIdentifier(tableName),
		strings.Join(columns, ", "))

	log.Printf("Creating SQL Server index: %s", createSQL)
	if _, err := s.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create index '%s': %w", index.Name, err)
	}

	return nil
}

func (s *SQLServerDB) mapToSQLServerType(column ColumnMetadata) string {
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
	case strings.Contains(typeName, "INT"):
		return "BIGINT"
	case strings.Contains(typeName, "DOUBLE"), strings.Contains(typeName, "FLOAT"), strings.Contains(typeName, "REAL"):
		return "FLOAT"
	case strings.Contains(typeName, "DEC"), strings.Contains(typeName, "NUMERIC"), strings.Contains(typeName, "NUMBER"):
		if precision > 0 {
			if scale < 0 {
				scale = 0
			}
			return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
		}
		return "DECIMAL(38,0)"
	case strings.Contains(typeName, "CHAR"), strings.Contains(typeName, "TEXT"), strings.Contains(typeName, "CLOB"), strings.Contains(typeName, "STRING"):
		if length > 0 && length <= 4000 {
			return fmt.Sprintf("NVARCHAR(%d)", length)
		}
		return "NVARCHAR(MAX)"
	case strings.Contains(typeName, "DATE"), strings.Contains(typeName, "TIME"):
		return "DATETIME2"
	case strings.Contains(typeName, "BLOB"), strings.Contains(typeName, "BINARY"), strings.Contains(typeName, "RAW"):
		return "VARBINARY(MAX)"
	case strings.Contains(typeName, "BOOL"):
		return "BIT"
	default:
		return "NVARCHAR(MAX)"
	}
}

func (s *SQLServerDB) quoteIdentifier(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

func (s *SQLServerDB) objectNameLiteral(name string) string {
	quoted := s.quoteIdentifier(name)
	return strings.ReplaceAll(quoted, "'", "''")
}

func buildSQLServerPlaceholders(count int) []string {
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = fmt.Sprintf("@p%d", i+1)
	}
	return placeholders
}
