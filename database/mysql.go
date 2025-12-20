package database

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"db-ferry/config"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLDB struct {
	db *sql.DB
}

var (
	_ SourceDB = (*MySQLDB)(nil)
	_ TargetDB = (*MySQLDB)(nil)
)

func NewMySQLDB(connectionString string) (*MySQLDB, error) {
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open mysql connection: %w", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping mysql database: %w", err)
	}

	log.Println("Successfully connected to MySQL database")
	return &MySQLDB{db: db}, nil
}

func (m *MySQLDB) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

func (m *MySQLDB) Query(sql string) (*sql.Rows, error) {
	log.Printf("Executing MySQL query: %s", sql)
	rows, err := m.db.Query(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute mysql query: %w", err)
	}
	return rows, nil
}

func (m *MySQLDB) GetRowCount(sql string) (int, error) {
	var count int
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS count_query", sql)
	if err := m.db.QueryRow(countSQL).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err)
	}
	return count, nil
}

func (m *MySQLDB) CreateTable(tableName string, columns []ColumnMetadata) error {
	return m.createTable(tableName, columns, true)
}

func (m *MySQLDB) EnsureTable(tableName string, columns []ColumnMetadata) error {
	return m.createTable(tableName, columns, false)
}

func (m *MySQLDB) createTable(tableName string, columns []ColumnMetadata, dropExisting bool) error {
	if len(columns) == 0 {
		return fmt.Errorf("no columns provided for table creation")
	}

	if dropExisting {
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", m.quoteIdentifier(tableName))
		log.Printf("Dropping existing MySQL table: %s", dropSQL)
		if _, err := m.db.Exec(dropSQL); err != nil {
			return fmt.Errorf("failed to drop table %s: %w", tableName, err)
		}
	}

	columnDefs := make([]string, len(columns))
	for i, col := range columns {
		typeDef := m.mapToMySQLType(col)
		columnDefs[i] = fmt.Sprintf("%s %s", m.quoteIdentifier(col.Name), typeDef)
	}

	createStmt := "CREATE TABLE"
	if !dropExisting {
		createStmt = "CREATE TABLE IF NOT EXISTS"
	}
	createSQL := fmt.Sprintf("%s %s (%s)", createStmt, m.quoteIdentifier(tableName), strings.Join(columnDefs, ", "))
	log.Printf("Creating new MySQL table: %s", createSQL)
	if _, err := m.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	return nil
}

func (m *MySQLDB) InsertData(tableName string, columns []ColumnMetadata, values [][]any) error {
	if len(values) == 0 {
		return nil
	}

	tx, err := m.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	placeholders := make([]string, len(columns))
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		placeholders[i] = "?"
		columnNames[i] = m.quoteIdentifier(col.Name)
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		m.quoteIdentifier(tableName),
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

func (m *MySQLDB) UpsertData(tableName string, columns []ColumnMetadata, values [][]any, mergeKeys []string) error {
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
		columnNames[i] = m.quoteIdentifier(col.Name)
		if _, isKey := keySet[strings.ToLower(col.Name)]; !isKey {
			updateAssignments = append(updateAssignments, fmt.Sprintf("%s=VALUES(%s)", m.quoteIdentifier(col.Name), m.quoteIdentifier(col.Name)))
		}
	}

	if len(updateAssignments) == 0 {
		keyName := m.quoteIdentifier(mergeKeys[0])
		updateAssignments = append(updateAssignments, fmt.Sprintf("%s=%s", keyName, keyName))
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		m.quoteIdentifier(tableName),
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(updateAssignments, ", "),
	)

	tx, err := m.db.Begin()
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

func (m *MySQLDB) GetTableRowCount(tableName string) (int, error) {
	var count int
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", m.quoteIdentifier(tableName))
	if err := m.db.QueryRow(countSQL).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to get row count for table %s: %w", tableName, err)
	}
	return count, nil
}

func (m *MySQLDB) CreateIndexes(tableName string, indexes []config.IndexConfig) error {
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

		if err := m.createIndex(tableName, index); err != nil {
			return fmt.Errorf("failed to create index '%s' on table '%s': %w", index.Name, tableName, err)
		}
	}

	return nil
}

func (m *MySQLDB) createIndex(tableName string, index config.IndexConfig) error {
	dropSQL := fmt.Sprintf("DROP INDEX IF EXISTS %s ON %s", m.quoteIdentifier(index.Name), m.quoteIdentifier(tableName))
	if _, err := m.db.Exec(dropSQL); err != nil {
		log.Printf("Warning: failed to drop existing index '%s': %v", index.Name, err)
	}

	columns := make([]string, len(index.ParsedColumns))
	for i, col := range index.ParsedColumns {
		columns[i] = fmt.Sprintf("%s %s", m.quoteIdentifier(col.Name), col.Order)
	}

	uniqueStr := ""
	if index.Unique {
		uniqueStr = "UNIQUE "
	}

	createSQL := fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
		uniqueStr,
		m.quoteIdentifier(index.Name),
		m.quoteIdentifier(tableName),
		strings.Join(columns, ", "))

	log.Printf("Creating MySQL index: %s", createSQL)
	if _, err := m.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create index '%s': %w", index.Name, err)
	}

	return nil
}

func (m *MySQLDB) mapToMySQLType(column ColumnMetadata) string {
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
		return "DOUBLE"
	case strings.Contains(typeName, "DEC"), strings.Contains(typeName, "NUMERIC"), strings.Contains(typeName, "NUMBER"):
		if precision > 0 {
			if scale < 0 {
				scale = 0
			}
			return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
		}
		return "DECIMAL(38,0)"
	case strings.Contains(typeName, "CHAR"), strings.Contains(typeName, "TEXT"), strings.Contains(typeName, "CLOB"), strings.Contains(typeName, "STRING"):
		if length > 0 && length <= 65535 {
			return fmt.Sprintf("VARCHAR(%d)", length)
		}
		return "TEXT"
	case strings.Contains(typeName, "DATE"), strings.Contains(typeName, "TIME"):
		return "DATETIME"
	case strings.Contains(typeName, "BLOB"), strings.Contains(typeName, "BINARY"), strings.Contains(typeName, "RAW"):
		return "LONGBLOB"
	case strings.Contains(typeName, "BOOL"):
		return "TINYINT(1)"
	default:
		return "TEXT"
	}
}

func (m *MySQLDB) quoteIdentifier(name string) string {
	escaped := strings.ReplaceAll(name, "`", "``")
	return "`" + escaped + "`"
}
