package database

import "database/sql"

// SourceDB 定义源数据库的通用接口
// Oracle和MySQL都需要实现这个接口
type SourceDB interface {
	// Close 关闭数据库连接
	Close() error

	// Query 执行查询并返回结果集
	Query(sql string) (*sql.Rows, error)

	// GetRowCount 获取查询结果的行数
	GetRowCount(sql string) (int, error)

	// GetColumnTypes 获取查询结果的列名
	GetColumnTypes(sql string) ([]string, error)
}