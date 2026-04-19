package assertion

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"db-ferry/config"
	"db-ferry/database"
)

// queryer abstracts the minimal query capability needed for assertions.
type queryer interface {
	Query(sql string) (*sql.Rows, error)
}

// Result captures the outcome of a single assertion check.
type Result struct {
	Rule          Rule
	ViolationCount int64
	Err           error
}

// Passed returns true if the assertion passed (no violations and no error).
func (r Result) Passed() bool {
	return r.Err == nil && r.ViolationCount == 0
}

// Engine executes data quality assertions against a database.
type Engine struct {
	rules []Rule
}

// NewEngine creates an assertion engine from a list of assertion configs.
func NewEngine(assertions []config.AssertionConfig) *Engine {
	rules := make([]Rule, len(assertions))
	for i, cfg := range assertions {
		rules[i] = NewRule(cfg)
	}
	return &Engine{rules: rules}
}

// HasRules returns true if the engine has any rules to check.
func (e *Engine) HasRules() bool {
	return len(e.rules) > 0
}

// RunPreCheck runs all assertions against the source query (wrappedSQL).
// It returns a slice of results, one per rule.
func (e *Engine) RunPreCheck(db queryer, dbType, wrappedSQL string) []Result {
	fromClause := BuildFromClause(dbType, wrappedSQL)
	return e.runChecks(db, dbType, fromClause)
}

// RunPostCheck runs all assertions against the target table.
// It returns a slice of results, one per rule.
func (e *Engine) RunPostCheck(db queryer, dbType, tableName string) []Result {
	fromClause := BuildTableFromClause(dbType, tableName)
	return e.runChecks(db, dbType, fromClause)
}

func (e *Engine) runChecks(db queryer, dbType, fromClause string) []Result {
	results := make([]Result, len(e.rules))
	for i, rule := range e.rules {
		sqlText := rule.CheckSQL(dbType, fromClause)
		count, err := executeCount(db, sqlText)
		results[i] = Result{
			Rule:           rule,
			ViolationCount: count,
			Err:            err,
		}
	}
	return results
}

// HandleResults processes assertion results according to each rule's on_fail setting.
// It returns an error if any abort rule fails, or nil if all pass or only warn rules fail.
// For DLQ rules, it queries violating rows and writes them via the provided writeDLQ function.
func (e *Engine) HandleResults(results []Result, columns []database.ColumnMetadata, writeDLQ func(row []any, errMsg string) error) error {
	var abortErrs []string
	var warnMsgs []string

	for _, res := range results {
		if res.Err != nil {
			msg := fmt.Sprintf("assertion '%s' check failed: %v", res.Rule.Description(), res.Err)
			switch res.Rule.Config().OnFail {
			case config.AssertionActionAbort:
				abortErrs = append(abortErrs, msg)
			default:
				warnMsgs = append(warnMsgs, msg)
			}
			continue
		}
		if res.ViolationCount == 0 {
			continue
		}

		msg := fmt.Sprintf("assertion '%s' failed: %d violation(s)", res.Rule.Description(), res.ViolationCount)
		switch res.Rule.Config().OnFail {
		case config.AssertionActionWarn:
			warnMsgs = append(warnMsgs, msg)
		case config.AssertionActionAbort:
			abortErrs = append(abortErrs, msg)
		case config.AssertionActionDLQ:
			if writeDLQ != nil {
				warnMsgs = append(warnMsgs, msg+" (written to DLQ)")
			} else {
				warnMsgs = append(warnMsgs, msg+" (no DLQ writer available, treated as warn)")
			}
		}
	}

	for _, msg := range warnMsgs {
		log.Printf("[ASSERTION WARN] %s", msg)
	}
	if len(abortErrs) > 0 {
		return fmt.Errorf("assertion(s) failed:\n  %s", strings.Join(abortErrs, "\n  "))
	}
	return nil
}

// FetchViolations queries violating rows for a given result and writes them to DLQ.
// This should be called separately after HandleResults for DLQ rules.
func (e *Engine) FetchViolations(db queryer, dbType, fromClause string, result Result, columns []database.ColumnMetadata, writeDLQ func(row []any, errMsg string) error) error {
	if result.Passed() || result.Err != nil {
		return nil
	}
	if result.Rule.Config().OnFail != config.AssertionActionDLQ || writeDLQ == nil {
		return nil
	}

	colNames := make([]string, len(columns))
	for i, c := range columns {
		colNames[i] = c.Name
	}
	sqlText := result.Rule.FetchSQL(dbType, fromClause, colNames)
	rows, err := db.Query(sqlText)
	if err != nil {
		return fmt.Errorf("failed to query violations for '%s': %w", result.Rule.Description(), err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	scanCols := make([]any, len(colTypes))
	ptrs := make([]any, len(colTypes))

	for rows.Next() {
		for i := range scanCols {
			ptrs[i] = &scanCols[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("failed to scan violation row: %w", err)
		}

		rowCopy := make([]any, len(scanCols))
		copy(rowCopy, scanCols)
		for i, v := range rowCopy {
			if b, ok := v.([]byte); ok {
				if i < len(columns) && isTextualColumn(columns[i]) {
					rowCopy[i] = string(b)
				} else {
					copied := make([]byte, len(b))
					copy(copied, b)
					rowCopy[i] = copied
				}
			}
		}

		errMsg := fmt.Sprintf("assertion failed: %s", result.Rule.Description())
		if err := writeDLQ(rowCopy, errMsg); err != nil {
			return fmt.Errorf("failed to write violation to DLQ: %w", err)
		}
	}
	return rows.Err()
}

func executeCount(db queryer, sqlText string) (int64, error) {
	rows, err := db.Query(sqlText)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, fmt.Errorf("no row returned for count query")
	}
	var count int64
	if err := rows.Scan(&count); err != nil {
		return 0, err
	}
	return count, rows.Err()
}

func isTextualColumn(column database.ColumnMetadata) bool {
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
