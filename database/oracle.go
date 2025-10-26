package database

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/sijms/go-ora/v2"
)

type OracleDB struct {
	db *sql.DB
}

// 确保 OracleDB 实现了 SourceDB 接口
var _ SourceDB = (*OracleDB)(nil)

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
	err := o.db.QueryRow(countSQL).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err)
	}
	return count, nil
}

func (o *OracleDB) GetColumnTypes(sql string) ([]string, error) {
	rows, err := o.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	var columnNames []string
	for _, ct := range columnTypes {
		columnNames = append(columnNames, ct.Name())
	}

	return columnNames, nil
}