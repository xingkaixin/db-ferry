package database

import (
	"fmt"
	"log"
	"strings"

	"db-ferry/config"
)

// BuildAlterTableAddColumnSQL returns the ALTER TABLE ADD COLUMN SQL for a single column.
func BuildAlterTableAddColumnSQL(dbType, tableName string, column ColumnMetadata) string {
	qTable := QuoteIdentifier(dbType, tableName)
	qCol := QuoteIdentifier(dbType, column.Name)
	colType := MapType(dbType, column)

	switch strings.ToLower(dbType) {
	case config.DatabaseTypeOracle, config.DatabaseTypeSQLServer:
		return fmt.Sprintf("ALTER TABLE %s ADD %s %s", qTable, qCol, colType)
	default:
		return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", qTable, qCol, colType)
	}
}

// SyncSchema compares desired columns with existing table columns and adds missing ones.
func SyncSchema(db TargetDB, dbType, tableName string, desiredColumns []ColumnMetadata) error {
	if len(desiredColumns) == 0 {
		return nil
	}

	existingColumns, err := db.GetTableColumns(tableName)
	if err != nil {
		return fmt.Errorf("failed to inspect table columns: %w", err)
	}

	existingMap := make(map[string]struct{}, len(existingColumns))
	for _, col := range existingColumns {
		existingMap[strings.ToLower(col.Name)] = struct{}{}
	}

	for _, col := range desiredColumns {
		if _, ok := existingMap[strings.ToLower(col.Name)]; ok {
			continue
		}

		sql := BuildAlterTableAddColumnSQL(dbType, tableName, col)
		log.Printf("Schema evolution: adding column %s to %s", col.Name, tableName)
		if err := db.Exec(sql); err != nil {
			return fmt.Errorf("failed to add column %s to %s: %w", col.Name, tableName, err)
		}
	}

	return nil
}
