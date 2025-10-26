package database

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLDB struct {
	db *sql.DB
}

// 确保 MySQLDB 实现了 SourceDB 接口
var _ SourceDB = (*MySQLDB)(nil)

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
	err := m.db.QueryRow(countSQL).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err)
	}
	return count, nil
}

func (m *MySQLDB) GetColumnTypes(sql string) ([]string, error) {
	rows, err := m.Query(sql)
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