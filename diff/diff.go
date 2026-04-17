package diff

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"log"
	"os"
	"strings"

	"db-ferry/config"
	"db-ferry/database"
)

// Options configures the diff command.
type Options struct {
	TaskName string
	Output   string
	Format   string
	Where    string
	Limit    int
	Keys     []string
}

// Row is a single row represented as column-name -> value.
type Row map[string]any

// MismatchRow captures a row where key matches but column values differ.
type MismatchRow struct {
	Key      map[string]any `json:"key"`
	Source   map[string]any `json:"source"`
	Target   map[string]any `json:"target"`
	DiffCols []string       `json:"diff_cols"`
}

// Summary holds counts for the diff report.
type Summary struct {
	SourceTotal int `json:"source_total"`
	TargetTotal int `json:"target_total"`
	SourceOnly  int `json:"source_only_count"`
	TargetOnly  int `json:"target_only_count"`
	Mismatch    int `json:"mismatch_count"`
}

// Result is the complete diff output.
type Result struct {
	SourceOnly []Row         `json:"source_only"`
	TargetOnly []Row         `json:"target_only"`
	Mismatch   []MismatchRow `json:"mismatch"`
	Summary    Summary       `json:"summary"`
}

// Run executes the diff command for a single task.
func Run(cfg *config.Config, opts Options, stdout io.Writer) error {
	if opts.TaskName == "" {
		return fmt.Errorf("-task is required")
	}

	var task config.TaskConfig
	found := false
	for _, t := range cfg.Tasks {
		if strings.EqualFold(t.TableName, opts.TaskName) {
			task = t
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("task %q not found in configuration", opts.TaskName)
	}

	sourceDBCfg, ok := cfg.GetDatabase(task.SourceDB)
	if !ok {
		return fmt.Errorf("source_db %q not found", task.SourceDB)
	}
	targetDBCfg, ok := cfg.GetDatabase(task.TargetDB)
	if !ok {
		return fmt.Errorf("target_db %q not found", task.TargetDB)
	}

	manager := database.NewConnectionManager(cfg)
	defer func() {
		if err := manager.CloseAll(); err != nil {
			log.Printf("Warning: failed to close connections: %v", err)
		}
	}()

	sourceDB, err := manager.GetSource(task.SourceDB)
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	targetDB, err := manager.GetTarget(task.TargetDB)
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}

	// Extract column metadata from source query.
	sourceSQL := buildLimitedSQL(task.SQL, sourceDBCfg.Type, opts.Where, opts.Limit)
	rows, err := sourceDB.Query(sourceSQL)
	if err != nil {
		return fmt.Errorf("failed to query source: %w", err)
	}
	columnsMeta, err := extractColumnMetadata(rows)
	rows.Close()
	if err != nil {
		return fmt.Errorf("failed to extract column metadata: %w", err)
	}

	keys, err := resolveKeys(columnsMeta, task.MergeKeys, opts.Keys)
	if err != nil {
		return err
	}

	sourceMap, sourceTotal, err := loadSourceData(sourceDB, sourceSQL, columnsMeta, keys)
	if err != nil {
		return fmt.Errorf("failed to load source data: %w", err)
	}

	targetSQL := buildTargetSQL(task.TableName, targetDBCfg.Type, opts.Where, opts.Limit)
	result, err := compareWithTarget(targetDB, targetSQL, columnsMeta, keys, sourceMap, sourceTotal)
	if err != nil {
		return fmt.Errorf("failed to compare with target: %w", err)
	}

	if err := writeReport(result, columnsMeta, opts.Format, opts.Output, stdout); err != nil {
		return fmt.Errorf("failed to write report: %w", err)
	}

	return nil
}

func resolveKeys(columns []database.ColumnMetadata, taskKeys, cliKeys []string) ([]string, error) {
	var keys []string
	if len(cliKeys) > 0 {
		keys = cliKeys
	} else if len(taskKeys) > 0 {
		keys = taskKeys
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("no diff keys specified; provide -keys or set merge_keys in task configuration")
	}

	resolved := make([]string, len(keys))
	for i, key := range keys {
		found := ""
		for _, col := range columns {
			if strings.EqualFold(col.Name, key) {
				found = col.Name
				break
			}
		}
		if found == "" {
			return nil, fmt.Errorf("diff key %q not found in source query columns", key)
		}
		resolved[i] = found
	}
	return resolved, nil
}

func extractColumnMetadata(rows *sql.Rows) ([]database.ColumnMetadata, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	metadata := make([]database.ColumnMetadata, len(columnTypes))
	for i, ct := range columnTypes {
		scanType := ct.ScanType()
		goType := ""
		if scanType != nil {
			goType = scanType.String()
		}
		metadata[i] = database.ColumnMetadata{
			Name:         ct.Name(),
			DatabaseType: ct.DatabaseTypeName(),
			GoType:       goType,
		}
	}
	return metadata, nil
}

func loadSourceData(sourceDB database.SourceDB, sqlText string, columns []database.ColumnMetadata, keys []string) (map[string]Row, int, error) {
	rows, err := sourceDB.Query(sqlText)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	data := make(map[string]Row)
	count := 0
	for rows.Next() {
		row, err := scanRow(rows, columns)
		if err != nil {
			return nil, 0, err
		}
		count++
		data[rowKey(row, keys)] = row
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return data, count, nil
}

func scanRow(rows *sql.Rows, columns []database.ColumnMetadata) (Row, error) {
	values := make([]any, len(columns))
	ptrs := make([]any, len(columns))
	for i := range values {
		ptrs[i] = &values[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	// Normalize []byte to string for textual columns.
	for i, v := range values {
		if b, ok := v.([]byte); ok && database.IsTextualColumn(columns[i]) {
			values[i] = string(b)
		}
	}
	row := make(Row, len(columns))
	for i, col := range columns {
		row[col.Name] = values[i]
	}
	return row, nil
}

func rowKey(row Row, keys []string) string {
	parts := make([]string, len(keys))
	for i, k := range keys {
		v := row[k]
		if v == nil {
			parts[i] = "\x00NULL\x00"
		} else {
			parts[i] = fmt.Sprintf("%v", v)
		}
	}
	return strings.Join(parts, "\x00|\x00")
}

func compareWithTarget(targetDB database.TargetDB, sqlText string, columns []database.ColumnMetadata, keys []string, sourceMap map[string]Row, sourceTotal int) (*Result, error) {
	rows, err := targetDB.Query(sqlText)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := &Result{
		SourceOnly: make([]Row, 0),
		TargetOnly: make([]Row, 0),
		Mismatch:   make([]MismatchRow, 0),
	}

	targetTotal := 0
	for rows.Next() {
		targetTotal++
		row, err := scanRow(rows, columns)
		if err != nil {
			return nil, err
		}
		k := rowKey(row, keys)
		sourceRow, found := sourceMap[k]
		if !found {
			result.TargetOnly = append(result.TargetOnly, row)
			continue
		}
		delete(sourceMap, k)

		var diffCols []string
		for _, col := range columns {
			if !database.CompareValues(sourceRow[col.Name], row[col.Name]) {
				diffCols = append(diffCols, col.Name)
			}
		}
		if len(diffCols) > 0 {
			keyMap := make(map[string]any, len(keys))
			for _, key := range keys {
				keyMap[key] = row[key]
			}
			result.Mismatch = append(result.Mismatch, MismatchRow{
				Key:      keyMap,
				Source:   sourceRow,
				Target:   row,
				DiffCols: diffCols,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, row := range sourceMap {
		result.SourceOnly = append(result.SourceOnly, row)
	}

	result.Summary = Summary{
		SourceTotal: sourceTotal,
		TargetTotal: targetTotal,
		SourceOnly:  len(result.SourceOnly),
		TargetOnly:  len(result.TargetOnly),
		Mismatch:    len(result.Mismatch),
	}
	return result, nil
}

func buildLimitedSQL(baseSQL, dbType, where string, limit int) string {
	sqlText := fmt.Sprintf("SELECT * FROM (%s)", trimSQL(baseSQL))
	if dbType != config.DatabaseTypeOracle {
		sqlText += " AS t"
	} else {
		sqlText += " t"
	}
	if where != "" {
		sqlText += " WHERE " + where
	}
	if limit > 0 {
		switch dbType {
		case config.DatabaseTypeMySQL, config.DatabaseTypePostgreSQL, config.DatabaseTypeSQLite, config.DatabaseTypeDuckDB:
			sqlText += fmt.Sprintf(" LIMIT %d", limit)
		case config.DatabaseTypeSQLServer:
			// For SQL Server, TOP must go before SELECT. We rewrite the whole query.
			prefix := "TOP " + fmt.Sprintf("%d ", limit)
			// Replace the leading SELECT * with SELECT TOP N *
			// This is a best-effort approach for wrapped subqueries.
			sqlText = strings.Replace(sqlText, "SELECT *", "SELECT "+prefix+"*", 1)
		case config.DatabaseTypeOracle:
			sqlText += fmt.Sprintf(" FETCH FIRST %d ROWS ONLY", limit)
		default:
			sqlText += fmt.Sprintf(" LIMIT %d", limit)
		}
	}
	return sqlText
}

func buildTargetSQL(tableName, dbType, where string, limit int) string {
	quotedTable := database.QuoteTableName(tableName, dbType)
	sqlText := fmt.Sprintf("SELECT * FROM %s", quotedTable)
	if where != "" {
		sqlText += " WHERE " + where
	}
	if limit > 0 {
		switch dbType {
		case config.DatabaseTypeMySQL, config.DatabaseTypePostgreSQL, config.DatabaseTypeSQLite, config.DatabaseTypeDuckDB:
			sqlText += fmt.Sprintf(" LIMIT %d", limit)
		case config.DatabaseTypeSQLServer:
			sqlText = strings.Replace(sqlText, "SELECT *", fmt.Sprintf("SELECT TOP %d *", limit), 1)
		case config.DatabaseTypeOracle:
			sqlText += fmt.Sprintf(" FETCH FIRST %d ROWS ONLY", limit)
		default:
			sqlText += fmt.Sprintf(" LIMIT %d", limit)
		}
	}
	return sqlText
}

func trimSQL(sqlText string) string {
	trimmed := strings.TrimSpace(sqlText)
	for strings.HasSuffix(trimmed, ";") {
		trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
	}
	return trimmed
}

func writeReport(result *Result, columns []database.ColumnMetadata, format, output string, stdout io.Writer) error {
	w := stdout
	if output != "" {
		f, err := os.Create(output)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}

	switch strings.ToLower(format) {
	case "csv":
		return writeCSV(w, result, columns)
	case "html":
		return writeHTML(w, result, columns)
	default:
		return writeJSON(w, result)
	}
}

func writeJSON(w io.Writer, result *Result) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

func writeCSV(w io.Writer, result *Result, columns []database.ColumnMetadata) error {
	cw := csv.NewWriter(w)
	headers := make([]string, len(columns))
	for i, col := range columns {
		headers[i] = col.Name
	}

	writeSection := func(title string, rows []Row) error {
		if err := cw.Write([]string{title}); err != nil {
			return err
		}
		if err := cw.Write(headers); err != nil {
			return err
		}
		for _, row := range rows {
			record := make([]string, len(columns))
			for i, col := range columns {
				if row[col.Name] == nil {
					record[i] = ""
				} else {
					record[i] = fmt.Sprintf("%v", row[col.Name])
				}
			}
			if err := cw.Write(record); err != nil {
				return err
			}
		}
		return cw.Write([]string{})
	}

	if err := writeSection("source_only", result.SourceOnly); err != nil {
		return err
	}
	if err := writeSection("target_only", result.TargetOnly); err != nil {
		return err
	}

	// Mismatch section has extra diff_cols column.
	if err := cw.Write([]string{"mismatch"}); err != nil {
		return err
	}
	mismatchHeaders := append(headers, "diff_cols")
	if err := cw.Write(mismatchHeaders); err != nil {
		return err
	}
	for _, m := range result.Mismatch {
		record := make([]string, len(columns)+1)
		for i, col := range columns {
			if m.Source[col.Name] == nil {
				record[i] = ""
			} else {
				record[i] = fmt.Sprintf("%v", m.Source[col.Name])
			}
		}
		record[len(columns)] = strings.Join(m.DiffCols, ", ")
		if err := cw.Write(record); err != nil {
			return err
		}
	}

	cw.Flush()
	return cw.Error()
}

func writeHTML(w io.Writer, result *Result, columns []database.ColumnMetadata) error {
	head := `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>db-ferry Diff Report</title>
<style>
body { font-family: sans-serif; margin: 2em; }
h1 { border-bottom: 2px solid #333; }
h2 { margin-top: 1.5em; color: #444; }
table { border-collapse: collapse; margin: 1em 0; width: 100%; }
th, td { border: 1px solid #ccc; padding: 6px 10px; text-align: left; }
th { background: #f4f4f4; }
.section { margin-bottom: 2em; }
.summary { font-size: 1.1em; margin: 1em 0; }
.empty { color: #888; font-style: italic; }
.mismatch-src { background: #ffe6e6; }
.mismatch-dst { background: #e6f7ff; }
</style>
</head>
<body>
<h1>db-ferry Diff Report</h1>
`
	if _, err := fmt.Fprint(w, head); err != nil {
		return err
	}

	fmt.Fprintf(w, `<div class="summary">
Source rows: <strong>%d</strong> | Target rows: <strong>%d</strong><br>
Source only: <strong>%d</strong> | Target only: <strong>%d</strong> | Mismatch: <strong>%d</strong>
</div>
`, result.Summary.SourceTotal, result.Summary.TargetTotal, result.Summary.SourceOnly, result.Summary.TargetOnly, result.Summary.Mismatch)

	writeTable := func(title string, rows []Row) error {
		fmt.Fprintf(w, `<div class="section"><h2>%s (%d)</h2>`, html.EscapeString(title), len(rows))
		if len(rows) == 0 {
			fmt.Fprint(w, `<p class="empty">No differences.</p></div>`)
			return nil
		}
		fmt.Fprint(w, `<table><tr>`)
		for _, col := range columns {
			fmt.Fprintf(w, `<th>%s</th>`, html.EscapeString(col.Name))
		}
		fmt.Fprint(w, `</tr>`)
		for _, row := range rows {
			fmt.Fprint(w, `<tr>`)
			for _, col := range columns {
				v := ""
				if row[col.Name] != nil {
					v = fmt.Sprintf("%v", row[col.Name])
				}
				fmt.Fprintf(w, `<td>%s</td>`, html.EscapeString(v))
			}
			fmt.Fprint(w, `</tr>`)
		}
		fmt.Fprint(w, `</table></div>`)
		return nil
	}

	if err := writeTable("Source Only", result.SourceOnly); err != nil {
		return err
	}
	if err := writeTable("Target Only", result.TargetOnly); err != nil {
		return err
	}

	// Mismatch table: show source and target side by side per column.
	fmt.Fprintf(w, `<div class="section"><h2>Mismatch (%d)</h2>`, len(result.Mismatch))
	if len(result.Mismatch) == 0 {
		fmt.Fprint(w, `<p class="empty">No differences.</p></div>`)
	} else {
		fmt.Fprint(w, `<table><tr><th>Key</th>`)
		for _, col := range columns {
			fmt.Fprintf(w, `<th>%s</th>`, html.EscapeString(col.Name))
		}
		fmt.Fprint(w, `</tr>`)
		for _, m := range result.Mismatch {
			keyParts := make([]string, 0, len(m.Key))
			for _, k := range sortedKeys(m.Key) {
				keyParts = append(keyParts, fmt.Sprintf("%s=%v", k, m.Key[k]))
			}
			keyStr := strings.Join(keyParts, ", ")

			// Source row
			fmt.Fprint(w, `<tr><td rowspan="2">`+html.EscapeString(keyStr)+`</td>`)
			for _, col := range columns {
				v := ""
				if m.Source[col.Name] != nil {
					v = fmt.Sprintf("%v", m.Source[col.Name])
				}
				cls := ""
				if containsString(m.DiffCols, col.Name) {
					cls = ` class="mismatch-src"`
				}
				fmt.Fprintf(w, `<td%s>%s</td>`, cls, html.EscapeString(v))
			}
			fmt.Fprint(w, `</tr>`)
			// Target row
			fmt.Fprint(w, `<tr>`)
			for _, col := range columns {
				v := ""
				if m.Target[col.Name] != nil {
					v = fmt.Sprintf("%v", m.Target[col.Name])
				}
				cls := ""
				if containsString(m.DiffCols, col.Name) {
					cls = ` class="mismatch-dst"`
				}
				fmt.Fprintf(w, `<td%s>%s</td>`, cls, html.EscapeString(v))
			}
			fmt.Fprint(w, `</tr>`)
		}
		fmt.Fprint(w, `</table></div>`)
	}

	_, err := fmt.Fprint(w, `</body></html>`)
	return err
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
