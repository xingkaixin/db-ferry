package database

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"db-ferry/config"

	_ "github.com/lib/pq"
)

type PostgresDB struct {
	db *sql.DB
}

var (
	_ SourceDB = (*PostgresDB)(nil)
	_ TargetDB = (*PostgresDB)(nil)
)

func NewPostgresDB(connectionString string) (*PostgresDB, error) {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgresql connection: %w", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping postgresql database: %w", err)
	}

	log.Println("Successfully connected to PostgreSQL database")
	return &PostgresDB{db: db}, nil
}

func (p *PostgresDB) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

func (p *PostgresDB) Query(sql string) (*sql.Rows, error) {
	log.Printf("Executing PostgreSQL query: %s", sql)
	rows, err := p.db.Query(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute postgresql query: %w", err)
	}
	return rows, nil
}

func (p *PostgresDB) GetRowCount(sql string) (int, error) {
	var count int
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS count_query", sql)
	if err := p.db.QueryRow(countSQL).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err)
	}
	return count, nil
}

func (p *PostgresDB) CreateTable(tableName string, columns []ColumnMetadata) error {
	return p.createTable(tableName, columns, true)
}

func (p *PostgresDB) EnsureTable(tableName string, columns []ColumnMetadata) error {
	return p.createTable(tableName, columns, false)
}

func (p *PostgresDB) createTable(tableName string, columns []ColumnMetadata, dropExisting bool) error {
	if len(columns) == 0 {
		return fmt.Errorf("no columns provided for table creation")
	}

	if dropExisting {
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", p.quoteIdentifier(tableName))
		log.Printf("Dropping existing PostgreSQL table: %s", dropSQL)
		if _, err := p.db.Exec(dropSQL); err != nil {
			return fmt.Errorf("failed to drop table %s: %w", tableName, err)
		}
	}

	columnDefs := make([]string, len(columns))
	for i, col := range columns {
		typeDef := p.mapToPostgresType(col)
		columnDefs[i] = fmt.Sprintf("%s %s", p.quoteIdentifier(col.Name), typeDef)
	}

	createStmt := "CREATE TABLE"
	if !dropExisting {
		createStmt = "CREATE TABLE IF NOT EXISTS"
	}
	createSQL := fmt.Sprintf("%s %s (%s)", createStmt, p.quoteIdentifier(tableName), strings.Join(columnDefs, ", "))
	log.Printf("Creating new PostgreSQL table: %s", createSQL)
	if _, err := p.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	return nil
}

func (p *PostgresDB) InsertData(tableName string, columns []ColumnMetadata, values [][]any) error {
	if len(values) == 0 {
		return nil
	}

	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	placeholders := buildPostgresPlaceholders(len(columns))
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = p.quoteIdentifier(col.Name)
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		p.quoteIdentifier(tableName),
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

func (p *PostgresDB) UpsertData(tableName string, columns []ColumnMetadata, values [][]any, mergeKeys []string) error {
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

	placeholders := buildPostgresPlaceholders(len(columns))
	columnNames := make([]string, len(columns))
	updateAssignments := make([]string, 0, len(columns))
	for i, col := range columns {
		columnNames[i] = p.quoteIdentifier(col.Name)
		if _, isKey := keySet[strings.ToLower(col.Name)]; !isKey {
			quoted := p.quoteIdentifier(col.Name)
			updateAssignments = append(updateAssignments, fmt.Sprintf("%s=EXCLUDED.%s", quoted, quoted))
		}
	}

	conflictCols := make([]string, len(mergeKeys))
	for i, key := range mergeKeys {
		conflictCols[i] = p.quoteIdentifier(key)
	}

	action := "DO NOTHING"
	if len(updateAssignments) > 0 {
		action = fmt.Sprintf("DO UPDATE SET %s", strings.Join(updateAssignments, ", "))
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT(%s) %s",
		p.quoteIdentifier(tableName),
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(conflictCols, ", "),
		action,
	)

	tx, err := p.db.Begin()
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

func (p *PostgresDB) GetTableRowCount(tableName string) (int, error) {
	var count int
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", p.quoteIdentifier(tableName))
	if err := p.db.QueryRow(countSQL).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to get row count for table %s: %w", tableName, err)
	}
	return count, nil
}

func (p *PostgresDB) CreateIndexes(tableName string, indexes []config.IndexConfig) error {
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

		if err := p.createIndex(tableName, index); err != nil {
			return fmt.Errorf("failed to create index '%s' on table '%s': %w", index.Name, tableName, err)
		}
	}

	return nil
}

func (p *PostgresDB) createIndex(tableName string, index config.IndexConfig) error {
	if _, err := p.db.Exec(fmt.Sprintf("DROP INDEX IF EXISTS %s", p.quoteIdentifier(index.Name))); err != nil {
		log.Printf("Warning: failed to drop existing index '%s': %v", index.Name, err)
	}

	columns := make([]string, len(index.ParsedColumns))
	for i, col := range index.ParsedColumns {
		columns[i] = fmt.Sprintf("%s %s", p.quoteIdentifier(col.Name), col.Order)
	}

	uniqueStr := ""
	if index.Unique {
		uniqueStr = "UNIQUE "
	}

	createSQL := fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
		uniqueStr,
		p.quoteIdentifier(index.Name),
		p.quoteIdentifier(tableName),
		strings.Join(columns, ", "))

	log.Printf("Creating PostgreSQL index: %s", createSQL)
	if _, err := p.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create index '%s': %w", index.Name, err)
	}

	return nil
}

func (p *PostgresDB) mapToPostgresType(column ColumnMetadata) string {
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
		return "DOUBLE PRECISION"
	case strings.Contains(typeName, "DEC"), strings.Contains(typeName, "NUMERIC"), strings.Contains(typeName, "NUMBER"):
		if precision > 0 {
			if scale < 0 {
				scale = 0
			}
			return fmt.Sprintf("NUMERIC(%d,%d)", precision, scale)
		}
		return "NUMERIC(38,0)"
	case strings.Contains(typeName, "CHAR"), strings.Contains(typeName, "TEXT"), strings.Contains(typeName, "CLOB"), strings.Contains(typeName, "STRING"):
		if length > 0 {
			return fmt.Sprintf("VARCHAR(%d)", length)
		}
		return "TEXT"
	case strings.Contains(typeName, "DATE"), strings.Contains(typeName, "TIME"):
		return "TIMESTAMP"
	case strings.Contains(typeName, "BLOB"), strings.Contains(typeName, "BINARY"), strings.Contains(typeName, "RAW"):
		return "BYTEA"
	case strings.Contains(typeName, "BOOL"):
		return "BOOLEAN"
	default:
		return "TEXT"
	}
}

func (p *PostgresDB) quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func buildPostgresPlaceholders(count int) []string {
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	return placeholders
}
