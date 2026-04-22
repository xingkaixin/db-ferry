package database

import (
	"database/sql"
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

// TableSchema represents the schema information for a table.
type TableSchema struct {
	Columns    []ColumnMetadata `json:"columns"`
	PrimaryKey []string         `json:"primary_key"`
	Indexes    []IndexInfo      `json:"indexes"`
}

// IndexInfo represents a database index.
type IndexInfo struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
}

// GetTableSchema extracts column metadata by executing a dummy query.
func GetTableSchema(source SourceDB, tableName string) ([]ColumnMetadata, error) {
	sqlText := fmt.Sprintf("SELECT * FROM %s WHERE 1=0", tableName)
	rows, err := source.Query(sqlText)
	if err != nil {
		return nil, fmt.Errorf("failed to query table schema: %w", err)
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	metadata := make([]ColumnMetadata, len(columnTypes))
	for i, ct := range columnTypes {
		scanType := ct.ScanType()
		goType := ""
		if scanType != nil {
			goType = scanType.String()
		}
		meta := ColumnMetadata{
			Name:         ct.Name(),
			DatabaseType: ct.DatabaseTypeName(),
			GoType:       goType,
		}

		if length, ok := ct.Length(); ok {
			meta.Length = length
			meta.LengthValid = true
		}

		if precision, scale, ok := ct.DecimalSize(); ok {
			meta.Precision = precision
			meta.Scale = scale
			meta.PrecisionScaleValid = true
		}

		if nullable, ok := ct.Nullable(); ok {
			meta.Nullable = nullable
			meta.NullableValid = true
		}
		metadata[i] = meta
	}
	return metadata, nil
}

// GetTablePrimaryKey attempts to retrieve the primary key columns for a table.
// This is best-effort and may return an empty slice for unsupported database types.
func GetTablePrimaryKey(source SourceDB, dbType, tableName string) ([]string, error) {
	var query string
	switch strings.ToLower(dbType) {
	case config.DatabaseTypeMySQL:
		query = fmt.Sprintf(`
			SELECT k.column_name
			FROM information_schema.table_constraints t
			JOIN information_schema.key_column_usage k
			  ON t.constraint_name = k.constraint_name
			 AND t.table_schema = k.table_schema
			WHERE t.constraint_type = 'PRIMARY KEY'
			  AND t.table_schema = DATABASE()
			  AND t.table_name = '%s'
			ORDER BY k.ordinal_position
		`, tableName)
	case config.DatabaseTypePostgreSQL:
		query = fmt.Sprintf(`
			SELECT kcu.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
			  ON tc.constraint_name = kcu.constraint_name
			 AND tc.table_schema = kcu.table_schema
			WHERE tc.constraint_type = 'PRIMARY KEY'
			  AND tc.table_schema = current_schema()
			  AND tc.table_name = '%s'
			ORDER BY kcu.ordinal_position
		`, tableName)
	case config.DatabaseTypeSQLite:
		query = fmt.Sprintf(`
			SELECT name FROM pragma_table_info('%s') WHERE pk > 0 ORDER BY pk
		`, tableName)
	case config.DatabaseTypeSQLServer:
		query = fmt.Sprintf(`
			SELECT kcu.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
			  ON tc.constraint_name = kcu.constraint_name
			WHERE tc.constraint_type = 'PRIMARY KEY'
			  AND tc.table_name = '%s'
			ORDER BY kcu.ordinal_position
		`, tableName)
	case config.DatabaseTypeDuckDB:
		query = fmt.Sprintf(`
			SELECT constraint_column_names
			FROM duckdb_constraints()
			WHERE constraint_type = 'PRIMARY KEY'
			  AND table_name = '%s'
		`, tableName)
	default:
		return nil, nil
	}

	rows, err := source.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary key: %w", err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			if dbType == config.DatabaseTypeDuckDB {
				// DuckDB may return list types; skip on scan error
				continue
			}
			return nil, fmt.Errorf("failed to scan primary key column: %w", err)
		}
		pks = append(pks, name)
	}
	return pks, rows.Err()
}

// GetTableIndexes attempts to retrieve index information for a table.
// This is best-effort and may return an empty slice for unsupported database types.
func GetTableIndexes(source SourceDB, dbType, tableName string) ([]IndexInfo, error) {
	var query string
	switch strings.ToLower(dbType) {
	case config.DatabaseTypeMySQL:
		query = fmt.Sprintf(`
			SELECT index_name, column_name, non_unique = 0 as is_unique
			FROM information_schema.statistics
			WHERE table_schema = DATABASE()
			  AND table_name = '%s'
			ORDER BY index_name, seq_in_index
		`, tableName)
	case config.DatabaseTypePostgreSQL:
		query = fmt.Sprintf(`
			SELECT i.relname as index_name, a.attname as column_name, ix.indisunique as is_unique
			FROM pg_index ix
			JOIN pg_class t ON t.oid = ix.indrelid
			JOIN pg_class i ON i.oid = ix.indexrelid
			JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
			WHERE t.relname = '%s'
			ORDER BY i.relname, array_position(ix.indkey, a.attnum)
		`, tableName)
	case config.DatabaseTypeSQLite:
		listQuery := fmt.Sprintf(`SELECT name, "unique" FROM pragma_index_list('%s')`, tableName)
		rows, err := source.Query(listQuery)
		if err != nil {
			return nil, fmt.Errorf("failed to query indexes: %w", err)
		}
		defer rows.Close()

		indexMap := make(map[string]*IndexInfo)
		for rows.Next() {
			var name string
			var unique int
			if err := rows.Scan(&name, &unique); err != nil {
				return nil, fmt.Errorf("failed to scan index info: %w", err)
			}
			indexMap[name] = &IndexInfo{Name: name, Unique: unique == 1}
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}

		for name, info := range indexMap {
			colQuery := fmt.Sprintf(`SELECT name FROM pragma_index_info('%s')`, name)
			colRows, err := source.Query(colQuery)
			if err != nil {
				continue
			}
			for colRows.Next() {
				var col string
				if err := colRows.Scan(&col); err == nil {
					info.Columns = append(info.Columns, col)
				}
			}
			colRows.Close()
		}

		result := make([]IndexInfo, 0, len(indexMap))
		for _, info := range indexMap {
			result = append(result, *info)
		}
		return result, nil
	case config.DatabaseTypeSQLServer:
		query = fmt.Sprintf(`
			SELECT i.name as index_name, c.name as column_name, i.is_unique as is_unique
			FROM sys.indexes i
			JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
			JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
			WHERE i.object_id = OBJECT_ID('%s')
			  AND i.type > 0
			ORDER BY i.name, ic.key_ordinal
		`, tableName)
	default:
		return nil, nil
	}

	rows, err := source.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query indexes: %w", err)
	}
	defer rows.Close()

	indexMap := make(map[string]*IndexInfo)
	for rows.Next() {
		var name, col string
		var unique sql.NullBool
		if err := rows.Scan(&name, &col, &unique); err != nil {
			return nil, fmt.Errorf("failed to scan index info: %w", err)
		}
		info, ok := indexMap[name]
		if !ok {
			info = &IndexInfo{Name: name, Unique: unique.Valid && unique.Bool}
			indexMap[name] = info
		}
		info.Columns = append(info.Columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := make([]IndexInfo, 0, len(indexMap))
	for _, info := range indexMap {
		result = append(result, *info)
	}
	return result, nil
}
