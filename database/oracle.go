package database

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"db-ferry/config"

	_ "github.com/sijms/go-ora/v2"
)

type OracleDB struct {
	db *sql.DB
}

var (
	_ SourceDB = (*OracleDB)(nil)
	_ TargetDB = (*OracleDB)(nil)
)

func NewOracleDB(connectionString string) (*OracleDB, error) {
	db, err := sql.Open("oracle", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open oracle connection: %w", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping oracle database: %w", err)
	}

	log.Println("Successfully connected to Oracle database")
	return &OracleDB{db: db}, nil
}

func (o *OracleDB) Close() error {
	if o.db != nil {
		return o.db.Close()
	}
	return nil
}

func (o *OracleDB) Query(sql string) (*sql.Rows, error) {
	log.Printf("Executing Oracle query: %s", sql)
	rows, err := o.db.Query(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute oracle query: %w", err)
	}
	return rows, nil
}

func (o *OracleDB) GetRowCount(sql string) (int, error) {
	var count int
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", sql)
	if err := o.db.QueryRow(countSQL).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err)
	}
	return count, nil
}

func (o *OracleDB) CreateTable(tableName string, columns []ColumnMetadata) error {
	if len(columns) == 0 {
		return fmt.Errorf("no columns provided for table creation")
	}

	dropSQL := fmt.Sprintf("BEGIN EXECUTE IMMEDIATE 'DROP TABLE %s'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;", o.ident(tableName))
	log.Printf("Dropping existing Oracle table (if exists): %s", dropSQL)
	if _, err := o.db.Exec(dropSQL); err != nil {
		return fmt.Errorf("failed to drop table %s: %w", tableName, err)
	}

	columnDefs := make([]string, len(columns))
	for i, col := range columns {
		typeDef := o.mapToOracleType(col)
		columnDefs[i] = fmt.Sprintf("%s %s", o.ident(col.Name), typeDef)
	}

	createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", o.ident(tableName), strings.Join(columnDefs, ", "))
	log.Printf("Creating new Oracle table: %s", createSQL)
	if _, err := o.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	return nil
}

func (o *OracleDB) InsertData(tableName string, columns []ColumnMetadata, values [][]any) error {
	if len(values) == 0 {
		return nil
	}

	tx, err := o.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	placeholders := make([]string, len(columns))
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		placeholders[i] = fmt.Sprintf(":%d", i+1)
		columnNames[i] = o.ident(col.Name)
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		o.ident(tableName),
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

func (o *OracleDB) CreateIndexes(tableName string, indexes []config.IndexConfig) error {
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

		if err := o.createIndex(tableName, index); err != nil {
			return fmt.Errorf("failed to create index '%s' on table '%s': %w", index.Name, tableName, err)
		}
	}

	return nil
}

func (o *OracleDB) createIndex(tableName string, index config.IndexConfig) error {
	dropSQL := fmt.Sprintf("BEGIN EXECUTE IMMEDIATE 'DROP INDEX %s'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -1418 AND SQLCODE != -942 THEN RAISE; END IF; END;", o.ident(index.Name))
	if _, err := o.db.Exec(dropSQL); err != nil {
		log.Printf("Warning: failed to drop existing index '%s': %v", index.Name, err)
	}

	columns := make([]string, len(index.ParsedColumns))
	for i, col := range index.ParsedColumns {
		columns[i] = fmt.Sprintf("%s %s", o.ident(col.Name), col.Order)
	}

	uniqueStr := ""
	if index.Unique {
		uniqueStr = "UNIQUE "
	}

	createSQL := fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
		uniqueStr,
		o.ident(index.Name),
		o.ident(tableName),
		strings.Join(columns, ", "))

	log.Printf("Creating Oracle index: %s", createSQL)
	if _, err := o.db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create index '%s': %w", index.Name, err)
	}

	return nil
}

func (o *OracleDB) mapToOracleType(column ColumnMetadata) string {
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
	case strings.Contains(typeName, "CHAR"), strings.Contains(typeName, "CLOB"), strings.Contains(typeName, "TEXT"), strings.Contains(typeName, "STRING"):
		if length > 0 && length <= 4000 {
			return fmt.Sprintf("VARCHAR2(%d)", length)
		}
		return "CLOB"
	case strings.Contains(typeName, "DATE"), strings.Contains(typeName, "TIME"):
		return "TIMESTAMP"
	case strings.Contains(typeName, "BLOB"), strings.Contains(typeName, "BINARY"), strings.Contains(typeName, "RAW"):
		return "BLOB"
	case strings.Contains(typeName, "DEC"), strings.Contains(typeName, "NUMERIC"), strings.Contains(typeName, "NUMBER"):
		if precision > 0 {
			if scale < 0 {
				scale = 0
			}
			return fmt.Sprintf("NUMBER(%d,%d)", precision, scale)
		}
		return "NUMBER"
	case strings.Contains(typeName, "FLOAT"), strings.Contains(typeName, "DOUBLE"), strings.Contains(typeName, "REAL"):
		return "BINARY_DOUBLE"
	case strings.Contains(typeName, "INT"), strings.Contains(typeName, "BIT"), strings.Contains(typeName, "BOOL"):
		return "NUMBER(19,0)"
	default:
		return "CLOB"
	}
}

func (o *OracleDB) ident(name string) string {
	return strings.ToUpper(name)
}
