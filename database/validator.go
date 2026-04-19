package database

import (
	"crypto/md5"
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"db-ferry/config"
	"db-ferry/metrics"
)

// queryer abstracts the minimal query capability needed for validation.
type queryer interface {
	Query(sql string) (*sql.Rows, error)
}

// ValidateTask runs the configured validation strategy for a completed task.
func ValidateTask(sourceDB, targetDB queryer, sourceDBType, targetDBType string,
	task config.TaskConfig, columns []ColumnMetadata, sourceWrappedSQL string,
	processedRows int, targetCountBefore int, recorder metrics.Recorder) error {

	switch task.Validate {
	case config.TaskValidateRowCount:
		return validateRowCount(targetDB, task, processedRows, targetCountBefore, recorder)
	case config.TaskValidateChecksum:
		return validateChecksum(sourceDB, targetDB, sourceDBType, targetDBType, task, columns, sourceWrappedSQL, recorder)
	case config.TaskValidateSample:
		return validateSample(sourceDB, targetDB, sourceDBType, targetDBType, task, columns, sourceWrappedSQL, recorder)
	default:
		return nil
	}
}

func validateRowCount(targetDB queryer, task config.TaskConfig, processedRows, targetCountBefore int, recorder metrics.Recorder) error {
	targetCountAfter, err := getTableRowCount(targetDB, task.TableName, task.TargetDB)
	if err != nil {
		return fmt.Errorf("failed to get target row count after insert: %w", err)
	}
	inserted := targetCountAfter - targetCountBefore
	if inserted != processedRows {
		recorder.RecordValidationMismatch(task.TableName, task.SourceDB, task.TargetDB, task.Validate)
		return fmt.Errorf("row count validation failed for table %s: expected %d inserted rows but got %d", task.TableName, processedRows, inserted)
	}
	return nil
}

func getTableRowCount(q queryer, tableName, dbType string) (int, error) {
	var countSQL string
	switch dbType {
	case config.DatabaseTypeSQLite:
		countSQL = fmt.Sprintf("SELECT COUNT(*) FROM \"%s\"", tableName)
	case config.DatabaseTypeMySQL:
		countSQL = fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)
	case config.DatabaseTypePostgreSQL, config.DatabaseTypeDuckDB:
		countSQL = fmt.Sprintf("SELECT COUNT(*) FROM \"%s\"", tableName)
	case config.DatabaseTypeOracle:
		countSQL = fmt.Sprintf("SELECT COUNT(*) FROM \"%s\"", strings.ToUpper(tableName))
	case config.DatabaseTypeSQLServer:
		countSQL = fmt.Sprintf("SELECT COUNT(*) FROM [%s]", tableName)
	default:
		countSQL = fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	}
	var count int
	rows, err := q.Query(countSQL)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, fmt.Errorf("no row returned for count query")
	}
	if err := rows.Scan(&count); err != nil {
		return 0, err
	}
	return count, rows.Err()
}

func validateChecksum(sourceDB, targetDB queryer, sourceDBType, targetDBType string,
	task config.TaskConfig, columns []ColumnMetadata, sourceWrappedSQL string, recorder metrics.Recorder) error {

	sourceChecksum, err := computeChecksum(sourceDB, sourceDBType, columns, sourceWrappedSQL)
	if err != nil {
		return fmt.Errorf("checksum validation failed computing source checksum: %w", err)
	}

	targetWrappedSQL := fmt.Sprintf("SELECT * FROM %s", QuoteTableName(task.TableName, targetDBType))
	targetChecksum, err := computeChecksum(targetDB, targetDBType, columns, targetWrappedSQL)
	if err != nil {
		return fmt.Errorf("checksum validation failed computing target checksum: %w", err)
	}

	if sourceChecksum != targetChecksum {
		recorder.RecordValidationMismatch(task.TableName, task.SourceDB, task.TargetDB, task.Validate)
		return fmt.Errorf("checksum validation failed for table %s: source checksum %q != target checksum %q", task.TableName, sourceChecksum, targetChecksum)
	}
	log.Printf("Checksum validation passed for table %s", task.TableName)
	return nil
}

func computeChecksum(q queryer, dbType string, columns []ColumnMetadata, wrappedSQL string) (string, error) {
	if dbType == config.DatabaseTypeSQLite {
		return computeSQLiteChecksum(q, columns, wrappedSQL)
	}

	sqlText := buildChecksumSQL(dbType, columns, wrappedSQL)
	rows, err := q.Query(sqlText)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var hashes []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return "", err
		}
		hashes = append(hashes, h)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	sort.Strings(hashes)
	return aggregateHashes(hashes), nil
}

func buildChecksumSQL(dbType string, columns []ColumnMetadata, wrappedSQL string) string {
	colNames := make([]string, len(columns))
	for i, col := range columns {
		colNames[i] = QuoteIdentifier(dbType, col.Name)
	}

	var hashExpr string
	switch dbType {
	case config.DatabaseTypeMySQL, config.DatabaseTypePostgreSQL, config.DatabaseTypeDuckDB:
		hashExpr = fmt.Sprintf("MD5(CONCAT_WS('|', %s))", strings.Join(colNames, ", "))
	case config.DatabaseTypeSQLServer:
		concatExpr := strings.Join(colNames, " + '|' + ")
		hashExpr = fmt.Sprintf("CONVERT(VARCHAR(32), HASHBYTES('MD5', %s), 2)", concatExpr)
	case config.DatabaseTypeOracle:
		concatExpr := strings.Join(colNames, "||'|'||")
		hashExpr = fmt.Sprintf("STANDARD_HASH(%s, 'MD5')", concatExpr)
	default:
		// Fallback to a simple concat
		hashExpr = fmt.Sprintf("MD5(CONCAT_WS('|', %s))", strings.Join(colNames, ", "))
	}

	var fromClause string
	switch dbType {
	case config.DatabaseTypeOracle:
		fromClause = fmt.Sprintf("(%s) t", wrappedSQL)
	default:
		fromClause = fmt.Sprintf("(%s) AS t", wrappedSQL)
	}

	return fmt.Sprintf("SELECT %s FROM %s ORDER BY 1", hashExpr, fromClause)
}

func computeSQLiteChecksum(q queryer, columns []ColumnMetadata, wrappedSQL string) (string, error) {
	colNames := make([]string, len(columns))
	for i, col := range columns {
		colNames[i] = fmt.Sprintf("\"%s\"", col.Name)
	}
	sqlText := fmt.Sprintf("SELECT %s FROM (%s)", strings.Join(colNames, ", "), wrappedSQL)
	rows, err := q.Query(sqlText)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var hashes []string
	for rows.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return "", err
		}
		hashes = append(hashes, rowHash(columns, values))
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	sort.Strings(hashes)
	return aggregateHashes(hashes), nil
}

func rowHash(columns []ColumnMetadata, values []any) string {
	parts := make([]string, len(values))
	for i, v := range values {
		if v == nil {
			parts[i] = "NULL"
			continue
		}
		if b, ok := v.([]byte); ok {
			parts[i] = fmt.Sprintf("%x", b)
		} else {
			parts[i] = fmt.Sprintf("%v", v)
		}
	}
	return fmt.Sprintf("%x", md5.Sum([]byte(strings.Join(parts, "|"))))
}

func aggregateHashes(hashes []string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(strings.Join(hashes, "\n"))))
}

func validateSample(sourceDB, targetDB queryer, sourceDBType, targetDBType string,
	task config.TaskConfig, columns []ColumnMetadata, sourceWrappedSQL string, recorder metrics.Recorder) error {

	sampleSize := task.ValidateSampleSize
	if sampleSize <= 0 {
		sampleSize = 1000
	}

	sqlText := buildSampleSQL(sourceDBType, sourceWrappedSQL, sampleSize)
	rows, err := sourceDB.Query(sqlText)
	if err != nil {
		return fmt.Errorf("sample validation failed querying source: %w", err)
	}
	defer rows.Close()

	var diffs []string
	rowNum := 0
	for rows.Next() {
		rowNum++
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("sample validation failed scanning source row: %w", err)
		}
		// Normalize []byte to string for textual columns to match processor behavior
		for i, v := range values {
			if b, ok := v.([]byte); ok && IsTextualColumn(columns[i]) {
				values[i] = string(b)
			}
		}

		targetRow, found, err := findTargetRow(targetDB, targetDBType, task.TableName, columns, values)
		if err != nil {
			return fmt.Errorf("sample validation failed querying target for row %d: %w", rowNum, err)
		}
		if !found {
			diffs = append(diffs, fmt.Sprintf("row %d: source=%v target=NOT_FOUND", rowNum, formatRowPreview(values, columns)))
			if len(diffs) >= 5 {
				break
			}
			continue
		}

		var diffCols []string
		for i := range columns {
			if !CompareValues(values[i], targetRow[i]) {
				diffCols = append(diffCols, columns[i].Name)
			}
		}
		if len(diffCols) > 0 {
			diffs = append(diffs, fmt.Sprintf("row %d: source=%v target=%v diff=[%s]",
				rowNum, formatRowPreview(values, columns), formatRowPreview(targetRow, columns), strings.Join(diffCols, ", ")))
			if len(diffs) >= 5 {
				break
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("sample validation error during source iteration: %w", err)
	}

	if len(diffs) > 0 {
		recorder.RecordValidationMismatch(task.TableName, task.SourceDB, task.TargetDB, task.Validate)
		return fmt.Errorf("sample validation failed for table %s (%d/%d sample rows differ):\n  %s",
			task.TableName, len(diffs), rowNum, strings.Join(diffs, "\n  "))
	}
	log.Printf("Sample validation passed for table %s (checked %d rows)", task.TableName, rowNum)
	return nil
}

func buildSampleSQL(dbType, wrappedSQL string, limit int) string {
	switch dbType {
	case config.DatabaseTypeMySQL:
		return fmt.Sprintf("SELECT * FROM (%s) AS t ORDER BY RAND() LIMIT %d", wrappedSQL, limit)
	case config.DatabaseTypePostgreSQL, config.DatabaseTypeSQLite, config.DatabaseTypeDuckDB:
		return fmt.Sprintf("SELECT * FROM (%s) AS t ORDER BY RANDOM() LIMIT %d", wrappedSQL, limit)
	case config.DatabaseTypeSQLServer:
		return fmt.Sprintf("SELECT TOP %d * FROM (%s) AS t ORDER BY NEWID()", limit, wrappedSQL)
	case config.DatabaseTypeOracle:
		return fmt.Sprintf("SELECT * FROM (%s) t ORDER BY DBMS_RANDOM.VALUE FETCH FIRST %d ROWS ONLY", wrappedSQL, limit)
	default:
		return fmt.Sprintf("SELECT * FROM (%s) AS t ORDER BY RANDOM() LIMIT %d", wrappedSQL, limit)
	}
}

func findTargetRow(targetDB queryer, dbType, tableName string, columns []ColumnMetadata, values []any) ([]any, bool, error) {
	sqlText := buildMatchSQL(dbType, tableName, columns, values)
	rows, err := targetDB.Query(sqlText)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, false, rows.Err()
	}

	row := make([]any, len(columns))
	ptrs := make([]any, len(columns))
	for i := range row {
		ptrs[i] = &row[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, false, err
	}
	// Normalize textual bytes to string
	for i, v := range row {
		if b, ok := v.([]byte); ok && IsTextualColumn(columns[i]) {
			row[i] = string(b)
		}
	}
	return row, true, rows.Err()
}

func buildMatchSQL(dbType, tableName string, columns []ColumnMetadata, values []any) string {
	quotedTable := QuoteTableName(tableName, dbType)
	var conditions []string
	for i, col := range columns {
		quotedCol := QuoteIdentifier(dbType, col.Name)
		if values[i] == nil {
			conditions = append(conditions, fmt.Sprintf("%s IS NULL", quotedCol))
		} else {
			conditions = append(conditions, fmt.Sprintf("%s = %s", quotedCol, formatSQLValue(values[i])))
		}
	}
	whereClause := "1=1"
	if len(conditions) > 0 {
		whereClause = strings.Join(conditions, " AND ")
	}

	var limitClause string
	switch dbType {
	case config.DatabaseTypeMySQL, config.DatabaseTypePostgreSQL, config.DatabaseTypeSQLite, config.DatabaseTypeDuckDB:
		limitClause = "LIMIT 1"
	case config.DatabaseTypeSQLServer:
		limitClause = "TOP 1"
		return fmt.Sprintf("SELECT %s * FROM %s WHERE %s", limitClause, quotedTable, whereClause)
	case config.DatabaseTypeOracle:
		return fmt.Sprintf("SELECT * FROM %s WHERE %s FETCH FIRST 1 ROWS ONLY", quotedTable, whereClause)
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s %s", quotedTable, whereClause, limitClause)
}

func formatSQLValue(v any) string {
	switch val := v.(type) {
	case string:
		return quoteSQLString(val)
	case []byte:
		return quoteSQLString(string(val))
	default:
		if v == nil {
			return "NULL"
		}
		return fmt.Sprintf("%v", v)
	}
}

func quoteSQLString(value string) string {
	escaped := strings.ReplaceAll(value, "'", "''")
	return "'" + escaped + "'"
}

func CompareValues(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	if ta, ok := a.(time.Time); ok {
		tb, ok := b.(time.Time)
		if !ok {
			return false
		}
		return ta.Equal(tb)
	}
	if _, ok := b.(time.Time); ok {
		return false
	}

	if ab, ok := a.([]byte); ok {
		bb, ok := b.([]byte)
		if !ok {
			return false
		}
		return string(ab) == string(bb)
	}

	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func formatRowPreview(values []any, columns []ColumnMetadata) string {
	var parts []string
	maxCols := 3
	if len(values) < maxCols {
		maxCols = len(values)
	}
	for i := 0; i < maxCols; i++ {
		v := values[i]
		if v == nil {
			parts = append(parts, fmt.Sprintf("%s:NULL", columns[i].Name))
		} else {
			preview := fmt.Sprintf("%v", v)
			if len(preview) > 20 {
				preview = preview[:17] + "..."
			}
			parts = append(parts, fmt.Sprintf("%s:%s", columns[i].Name, preview))
		}
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func QuoteTableName(tableName, dbType string) string {
	return QuoteIdentifier(dbType, tableName)
}

// IsTextualColumn mirrors processor.isTextualColumn.
func IsTextualColumn(column ColumnMetadata) bool {
	typeName := strings.ToUpper(column.DatabaseType)
	if typeName == "" {
		typeName = strings.ToUpper(column.GoType)
	}
	switch {
	case strings.Contains(typeName, "CHAR"), strings.Contains(typeName, "TEXT"), strings.Contains(typeName, "CLOB"), strings.Contains(typeName, "STRING"):
		return true
	default:
		return false
	}
}
