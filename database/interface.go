package database

import (
	"database/sql"

	"db-ferry/config"
)

// ColumnMetadata captures column information extracted from a query result set.
type ColumnMetadata struct {
	Name                string
	DatabaseType        string
	Length              int64
	LengthValid         bool
	Precision           int64
	Scale               int64
	PrecisionScaleValid bool
	Nullable            bool
	NullableValid       bool
	GoType              string
}

// SourceDB 定义源数据库的通用接口
type SourceDB interface {
	// Close 关闭数据库连接
	Close() error

	// Query 执行查询并返回结果集
	Query(sql string) (*sql.Rows, error)

	// GetRowCount 获取查询结果的行数
	GetRowCount(sql string) (int, error)
}

// TargetDB 定义目标数据库的通用接口
type TargetDB interface {
	// Close 关闭数据库连接
	Close() error

	// CreateTable 根据列元数据创建目标表
	CreateTable(tableName string, columns []ColumnMetadata) error

	// EnsureTable 创建目标表(如果不存在)
	EnsureTable(tableName string, columns []ColumnMetadata) error

	// InsertData 批量插入数据
	InsertData(tableName string, columns []ColumnMetadata, values [][]any) error

	// GetTableRowCount 获取目标表的行数
	GetTableRowCount(tableName string) (int, error)

	// CreateIndexes 创建索引
	CreateIndexes(tableName string, indexes []config.IndexConfig) error
}
