package assertion

import (
	"database/sql"
	"testing"

	"db-ferry/config"
	"db-ferry/database"

	_ "github.com/mattn/go-sqlite3"
)

func TestNotNullRuleSQL(t *testing.T) {
	rule := NewRule(config.AssertionConfig{Column: "order_id", Rule: config.AssertionRuleNotNull})

	preSQL := rule.CheckSQL(config.DatabaseTypeMySQL, "(SELECT * FROM orders) AS __src")
	expected := "SELECT COUNT(*) FROM (SELECT * FROM orders) AS __src WHERE `order_id` IS NULL"
	if preSQL != expected {
		t.Errorf("not_null pre-check SQL mismatch:\n  got:      %s\n  expected: %s", preSQL, expected)
	}

	postSQL := rule.CheckSQL(config.DatabaseTypePostgreSQL, `"orders"`)
	expectedPost := "SELECT COUNT(*) FROM \"orders\" WHERE \"order_id\" IS NULL"
	if postSQL != expectedPost {
		t.Errorf("not_null post-check SQL mismatch:\n  got:      %s\n  expected: %s", postSQL, expectedPost)
	}
}

func TestRangeRuleSQL(t *testing.T) {
	minVal := 0.0
	maxVal := 1000000.0
	rule := NewRule(config.AssertionConfig{Column: "amount", Rule: config.AssertionRuleRange, Min: &minVal, Max: &maxVal})

	sqlText := rule.CheckSQL(config.DatabaseTypeMySQL, "(SELECT * FROM orders) AS __src")
	expected := "SELECT COUNT(*) FROM (SELECT * FROM orders) AS __src WHERE `amount` < 0 OR `amount` > 1e+06"
	if sqlText != expected {
		t.Errorf("range check SQL mismatch:\n  got:      %s\n  expected: %s", sqlText, expected)
	}
}

func TestInSetRuleSQL(t *testing.T) {
	rule := NewRule(config.AssertionConfig{Column: "status", Rule: config.AssertionRuleInSet, Values: []string{"pending", "paid"}})

	sqlText := rule.CheckSQL(config.DatabaseTypeMySQL, "(SELECT * FROM orders) AS __src")
	expected := "SELECT COUNT(*) FROM (SELECT * FROM orders) AS __src WHERE `status` NOT IN ('pending', 'paid')"
	if sqlText != expected {
		t.Errorf("in_set check SQL mismatch:\n  got:      %s\n  expected: %s", sqlText, expected)
	}
}

func TestUniqueRuleSQL(t *testing.T) {
	rule := NewRule(config.AssertionConfig{Columns: []string{"user_id", "order_date"}, Rule: config.AssertionRuleUnique})

	// MySQL
	sqlText := rule.CheckSQL(config.DatabaseTypeMySQL, "(SELECT * FROM orders) AS __src")
	expected := "SELECT COUNT(*) FROM (SELECT 1 FROM (SELECT * FROM orders) AS __src AS __uq GROUP BY `user_id`, `order_date` HAVING COUNT(*) > 1) AS __violations"
	if sqlText != expected {
		t.Errorf("unique check SQL mismatch (MySQL):\n  got:      %s\n  expected: %s", sqlText, expected)
	}

	// Oracle (no AS for subquery alias)
	oracleSQL := rule.CheckSQL(config.DatabaseTypeOracle, "(SELECT * FROM orders) __src")
	expectedOracle := "SELECT COUNT(*) FROM (SELECT 1 FROM (SELECT * FROM orders) __src GROUP BY \"USER_ID\", \"ORDER_DATE\" HAVING COUNT(*) > 1) AS __violations"
	if oracleSQL != expectedOracle {
		t.Errorf("unique check SQL mismatch (Oracle):\n  got:      %s\n  expected: %s", oracleSQL, expectedOracle)
	}
}

func TestRegexRuleSQL(t *testing.T) {
	rule := NewRule(config.AssertionConfig{Column: "email", Rule: config.AssertionRuleRegex, Pattern: `^[a-z]+@[a-z]+\.com$`})

	// MySQL
	mysqlSQL := rule.CheckSQL(config.DatabaseTypeMySQL, "(SELECT * FROM users) AS __src")
	expected := "SELECT COUNT(*) FROM (SELECT * FROM users) AS __src WHERE NOT (`email` REGEXP '^[a-z]+@[a-z]+\\.com$')"
	if mysqlSQL != expected {
		t.Errorf("regex check SQL mismatch (MySQL):\n  got:      %s\n  expected: %s", mysqlSQL, expected)
	}

	// PostgreSQL
	pgSQL := rule.CheckSQL(config.DatabaseTypePostgreSQL, "(SELECT * FROM users) AS __src")
	expectedPg := "SELECT COUNT(*) FROM (SELECT * FROM users) AS __src WHERE NOT (\"email\" ~ '^[a-z]+@[a-z]+\\.com$')"
	if pgSQL != expectedPg {
		t.Errorf("regex check SQL mismatch (PostgreSQL):\n  got:      %s\n  expected: %s", pgSQL, expectedPg)
	}

	// Oracle
	oraSQL := rule.CheckSQL(config.DatabaseTypeOracle, "(SELECT * FROM users) __src")
	expectedOra := "SELECT COUNT(*) FROM (SELECT * FROM users) __src WHERE NOT (REGEXP_LIKE(\"EMAIL\", '^[a-z]+@[a-z]+\\.com$'))"
	if oraSQL != expectedOra {
		t.Errorf("regex check SQL mismatch (Oracle):\n  got:      %s\n  expected: %s", oraSQL, expectedOra)
	}
}

func TestMinLengthRuleSQL(t *testing.T) {
	rule := NewRule(config.AssertionConfig{Column: "name", Rule: config.AssertionRuleMinLength, Length: 3})

	sqlText := rule.CheckSQL(config.DatabaseTypeMySQL, "(SELECT * FROM users) AS __src")
	expected := "SELECT COUNT(*) FROM (SELECT * FROM users) AS __src WHERE LENGTH(`name`) < 3"
	if sqlText != expected {
		t.Errorf("min_length check SQL mismatch:\n  got:      %s\n  expected: %s", sqlText, expected)
	}
}

func TestMaxLengthRuleSQL(t *testing.T) {
	rule := NewRule(config.AssertionConfig{Column: "code", Rule: config.AssertionRuleMaxLength, Length: 10})

	// SQL Server uses LEN instead of LENGTH
	mssqlSQL := rule.CheckSQL(config.DatabaseTypeSQLServer, "[orders]")
	expected := "SELECT COUNT(*) FROM [orders] WHERE LEN([code]) > 10"
	if mssqlSQL != expected {
		t.Errorf("max_length check SQL mismatch (SQL Server):\n  got:      %s\n  expected: %s", mssqlSQL, expected)
	}
}

func TestDescription(t *testing.T) {
	tests := []struct {
		cfg      config.AssertionConfig
		expected string
	}{
		{config.AssertionConfig{Column: "order_id", Rule: config.AssertionRuleNotNull}, "column 'order_id' must not be null"},
		{func() config.AssertionConfig {
			min := 0.0
			max := 100.0
			return config.AssertionConfig{Column: "amount", Rule: config.AssertionRuleRange, Min: &min, Max: &max}
		}(), "column 'amount' must be in range (min=0, max=100)"},
		{config.AssertionConfig{Column: "status", Rule: config.AssertionRuleInSet, Values: []string{"a", "b"}}, "column 'status' must be in set [a b]"},
		{config.AssertionConfig{Columns: []string{"a", "b"}, Rule: config.AssertionRuleUnique}, "columns [a b] must be unique"},
		{config.AssertionConfig{Column: "email", Rule: config.AssertionRuleRegex, Pattern: `.*`}, "column 'email' must match pattern '.*'"},
	}

	for _, tt := range tests {
		rule := NewRule(tt.cfg)
		if got := rule.Description(); got != tt.expected {
			t.Errorf("description mismatch for rule %s: got %q, expected %q", tt.cfg.Rule, got, tt.expected)
		}
	}
}

func TestEngineHasRules(t *testing.T) {
	if NewEngine(nil).HasRules() {
		t.Error("expected HasRules() to be false for nil assertions")
	}
	if NewEngine([]config.AssertionConfig{}).HasRules() {
		t.Error("expected HasRules() to be false for empty assertions")
	}
	if !NewEngine([]config.AssertionConfig{{Column: "a", Rule: config.AssertionRuleNotNull}}).HasRules() {
		t.Error("expected HasRules() to be true for non-empty assertions")
	}
}

func TestHandleResultsAllPass(t *testing.T) {
	engine := NewEngine([]config.AssertionConfig{
		{Column: "a", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionAbort},
	})
	results := []Result{{Rule: NewRule(config.AssertionConfig{Column: "a", Rule: config.AssertionRuleNotNull}), ViolationCount: 0}}
	if err := engine.HandleResults(results, nil, nil); err != nil {
		t.Errorf("expected no error when all pass, got: %v", err)
	}
}

func TestHandleResultsAbort(t *testing.T) {
	engine := NewEngine([]config.AssertionConfig{
		{Column: "a", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionAbort},
	})
	results := []Result{{Rule: NewRule(config.AssertionConfig{Column: "a", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionAbort}), ViolationCount: 5}}
	if err := engine.HandleResults(results, nil, nil); err == nil {
		t.Error("expected error for abort rule with violations")
	}
}

func TestHandleResultsWarn(t *testing.T) {
	engine := NewEngine([]config.AssertionConfig{
		{Column: "a", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionWarn},
	})
	results := []Result{{Rule: NewRule(config.AssertionConfig{Column: "a", Rule: config.AssertionRuleNotNull}), ViolationCount: 5}}
	if err := engine.HandleResults(results, nil, nil); err != nil {
		t.Errorf("expected no error for warn rule with violations, got: %v", err)
	}
}

func TestHandleResultsMixed(t *testing.T) {
	engine := NewEngine([]config.AssertionConfig{
		{Column: "a", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionWarn},
		{Column: "b", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionAbort},
	})
	results := []Result{
		{Rule: NewRule(config.AssertionConfig{Column: "a", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionWarn}), ViolationCount: 5},
		{Rule: NewRule(config.AssertionConfig{Column: "b", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionAbort}), ViolationCount: 3},
	}
	if err := engine.HandleResults(results, nil, nil); err == nil {
		t.Error("expected error because abort rule failed")
	}
}

func TestBuildFromClause(t *testing.T) {
	if got := BuildFromClause(config.DatabaseTypeMySQL, "SELECT * FROM t"); got != "(SELECT * FROM t) AS __src" {
		t.Errorf("unexpected from clause for MySQL: %s", got)
	}
	if got := BuildFromClause(config.DatabaseTypeOracle, "SELECT * FROM t"); got != "(SELECT * FROM t) __src" {
		t.Errorf("unexpected from clause for Oracle: %s", got)
	}
}

func TestBuildTableFromClause(t *testing.T) {
	if got := BuildTableFromClause(config.DatabaseTypeMySQL, "orders"); got != "`orders`" {
		t.Errorf("unexpected table clause for MySQL: %s", got)
	}
	if got := BuildTableFromClause(config.DatabaseTypePostgreSQL, "orders"); got != `"orders"` {
		t.Errorf("unexpected table clause for PostgreSQL: %s", got)
	}
}

func TestResultPassed(t *testing.T) {
	if !(Result{ViolationCount: 0, Err: nil}.Passed()) {
		t.Error("expected Passed()=true for zero violations and no error")
	}
	if (Result{ViolationCount: 1, Err: nil}.Passed()) {
		t.Error("expected Passed()=false for violations")
	}
	if (Result{ViolationCount: 0, Err: sql.ErrNoRows}.Passed()) {
		t.Error("expected Passed()=false for error")
	}
}

// dbQueryer wraps *sql.DB to match the queryer interface.
type dbQueryer struct{ db *sql.DB }

func (d *dbQueryer) Query(sql string) (*sql.Rows, error) {
	return d.db.Query(sql)
}

func TestEngineRunCheck(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory sqlite: %v", err)
	}
	defer db.Close()
	q := &dbQueryer{db: db}

	if _, err := db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, amount REAL)"); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	if _, err := db.Exec("INSERT INTO test (id, name, amount) VALUES (1, 'alice', 100), (2, 'bob', 200), (3, NULL, 300)"); err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	engine := NewEngine([]config.AssertionConfig{
		{Column: "name", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionWarn},
		{Column: "amount", Rule: config.AssertionRuleRange, Min: floatPtr(0), Max: floatPtr(250), OnFail: config.AssertionActionWarn},
	})

	results := engine.RunPostCheck(q, config.DatabaseTypeSQLite, "test")
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].ViolationCount != 1 {
		t.Errorf("expected 1 null violation, got %d", results[0].ViolationCount)
	}
	if results[1].ViolationCount != 1 {
		t.Errorf("expected 1 range violation, got %d", results[1].ViolationCount)
	}
}

func TestEngineRunPreCheck(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory sqlite: %v", err)
	}
	defer db.Close()
	q := &dbQueryer{db: db}

	if _, err := db.Exec("CREATE TABLE src (id INTEGER, status TEXT)"); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	if _, err := db.Exec("INSERT INTO src VALUES (1, 'active'), (2, 'inactive'), (3, 'unknown')"); err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	engine := NewEngine([]config.AssertionConfig{
		{Column: "status", Rule: config.AssertionRuleInSet, Values: []string{"active", "inactive"}, OnFail: config.AssertionActionWarn},
	})

	results := engine.RunPreCheck(q, config.DatabaseTypeSQLite, "SELECT * FROM src")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ViolationCount != 1 {
		t.Errorf("expected 1 in_set violation, got %d", results[0].ViolationCount)
	}
}

func TestFetchViolations(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory sqlite: %v", err)
	}
	defer db.Close()
	q := &dbQueryer{db: db}

	if _, err := db.Exec("CREATE TABLE test (id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	if _, err := db.Exec("INSERT INTO test VALUES (1, 'alice'), (2, NULL)"); err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	engine := NewEngine([]config.AssertionConfig{
		{Column: "name", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionDLQ},
	})

	results := engine.RunPostCheck(q, config.DatabaseTypeSQLite, "test")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	var captured [][]any
	writeFn := func(row []any, errMsg string) error {
		captured = append(captured, row)
		return nil
	}

	if err := engine.FetchViolations(q, config.DatabaseTypeSQLite, "test", results[0], []database.ColumnMetadata{{Name: "id"}, {Name: "name"}}, writeFn); err != nil {
		t.Fatalf("FetchViolations failed: %v", err)
	}
	if len(captured) != 1 {
		t.Fatalf("expected 1 violation row, got %d", len(captured))
	}
}

func TestFetchViolationsSkipsPassed(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory sqlite: %v", err)
	}
	defer db.Close()
	q := &dbQueryer{db: db}

	engine := NewEngine([]config.AssertionConfig{
		{Column: "name", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionDLQ},
	})

	// Create empty table so no violations occur
	if _, err := db.Exec("CREATE TABLE test (id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	results := engine.RunPostCheck(q, config.DatabaseTypeSQLite, "test")
	if err := engine.FetchViolations(q, config.DatabaseTypeSQLite, "test", results[0], nil, nil); err != nil {
		t.Errorf("expected no error for passed result, got: %v", err)
	}
}

func TestHandleResultsCheckError(t *testing.T) {
	engine := NewEngine([]config.AssertionConfig{
		{Column: "a", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionAbort},
	})
	results := []Result{{Rule: NewRule(config.AssertionConfig{Column: "a", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionAbort}), ViolationCount: 0, Err: sql.ErrConnDone}}
	if err := engine.HandleResults(results, nil, nil); err == nil {
		t.Error("expected error when check itself failed with abort on_fail")
	}
}

func TestExecuteCountError(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory sqlite: %v", err)
	}
	defer db.Close()
	q := &dbQueryer{db: db}

	_, err = executeCount(q, "SELECT COUNT(*) FROM nonexistent")
	if err == nil {
		t.Error("expected error for invalid table")
	}
}

func TestIsTextualColumn(t *testing.T) {
	if !isTextualColumn(database.ColumnMetadata{DatabaseType: "VARCHAR"}) {
		t.Error("expected VARCHAR to be textual")
	}
	if !isTextualColumn(database.ColumnMetadata{DatabaseType: "TEXT"}) {
		t.Error("expected TEXT to be textual")
	}
	if isTextualColumn(database.ColumnMetadata{DatabaseType: "INTEGER"}) {
		t.Error("expected INTEGER not to be textual")
	}
	if !isTextualColumn(database.ColumnMetadata{DatabaseType: "", GoType: "string"}) {
		t.Error("expected GoType string to be textual when DatabaseType is empty")
	}
}

func TestUnsupportedRule(t *testing.T) {
	rule := NewRule(config.AssertionConfig{Rule: "unknown"})
	if rule.CheckSQL("mysql", "t") != "SELECT 0" {
		t.Error("unsupported rule should return fallback SQL")
	}
	if rule.FetchSQL("mysql", "t", nil) != "SELECT 0 WHERE 1=0" {
		t.Error("unsupported rule should return fallback fetch SQL")
	}
	if rule.Description() != "unsupported rule 'unknown'" {
		t.Errorf("unexpected description: %s", rule.Description())
	}
	if rule.Config().Rule != "unknown" {
		t.Error("Config() should return underlying config")
	}
}

func TestAllRulesFetchSQL(t *testing.T) {
	tests := []struct {
		name string
		rule Rule
	}{
		{"not_null", NewRule(config.AssertionConfig{Column: "a", Rule: config.AssertionRuleNotNull})},
		{"range", NewRule(config.AssertionConfig{Column: "a", Rule: config.AssertionRuleRange, Min: floatPtr(0), Max: floatPtr(10)})},
		{"in_set", NewRule(config.AssertionConfig{Column: "a", Rule: config.AssertionRuleInSet, Values: []string{"x"}})},
		{"unique", NewRule(config.AssertionConfig{Columns: []string{"a", "b"}, Rule: config.AssertionRuleUnique})},
		{"regex", NewRule(config.AssertionConfig{Column: "a", Rule: config.AssertionRuleRegex, Pattern: `.*`})},
		{"min_length", NewRule(config.AssertionConfig{Column: "a", Rule: config.AssertionRuleMinLength, Length: 5})},
		{"max_length", NewRule(config.AssertionConfig{Column: "a", Rule: config.AssertionRuleMaxLength, Length: 10})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Verify Config() returns non-empty
			if tt.rule.Config().Rule == "" {
				t.Error("Config().Rule should not be empty")
			}
			// Verify FetchSQL returns non-empty
			fetch := tt.rule.FetchSQL(config.DatabaseTypeMySQL, "(SELECT 1) AS t", []string{"a", "b"})
			if fetch == "" {
				t.Error("FetchSQL should not be empty")
			}
			// Verify Description returns non-empty
			if tt.rule.Description() == "" {
				t.Error("Description should not be empty")
			}
		})
	}
}

func TestHandleResultsDLQ(t *testing.T) {
	engine := NewEngine([]config.AssertionConfig{
		{Column: "a", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionDLQ},
	})
	results := []Result{{Rule: NewRule(config.AssertionConfig{Column: "a", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionDLQ}), ViolationCount: 3}}
	// HandleResults logs DLQ message but does not call writeFn; FetchViolations does
	if err := engine.HandleResults(results, nil, nil); err != nil {
		t.Errorf("expected no error for DLQ rule, got: %v", err)
	}
}

func TestHandleResultsDLQNoWriter(t *testing.T) {
	engine := NewEngine([]config.AssertionConfig{
		{Column: "a", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionDLQ},
	})
	results := []Result{{Rule: NewRule(config.AssertionConfig{Column: "a", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionDLQ}), ViolationCount: 3}}
	if err := engine.HandleResults(results, nil, nil); err != nil {
		t.Errorf("expected no error for DLQ rule without writer, got: %v", err)
	}
}

func TestHandleResultsWarnWithError(t *testing.T) {
	engine := NewEngine([]config.AssertionConfig{
		{Column: "a", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionWarn},
	})
	results := []Result{{Rule: NewRule(config.AssertionConfig{Column: "a", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionWarn}), ViolationCount: 0, Err: sql.ErrConnDone}}
	if err := engine.HandleResults(results, nil, nil); err != nil {
		t.Errorf("expected no error for warn rule with check error, got: %v", err)
	}
}

func TestRegexMatchExprSQLServer(t *testing.T) {
	expr := regexMatchExpr(config.DatabaseTypeSQLServer, "[col]", "pattern")
	if expr != "[col] LIKE 'pattern'" {
		t.Errorf("unexpected SQL Server regex expr: %s", expr)
	}
}

func TestRegexMatchExprSQLite(t *testing.T) {
	expr := regexMatchExpr(config.DatabaseTypeSQLite, "col", "pattern")
	if expr != "1=1" {
		t.Errorf("unexpected SQLite regex expr: %s", expr)
	}
}

func TestLengthExprSQLServer(t *testing.T) {
	expr := lengthExpr(config.DatabaseTypeSQLServer, "[name]")
	if expr != "LEN([name])" {
		t.Errorf("unexpected SQL Server length expr: %s", expr)
	}
}

func TestSelectColumnsEmpty(t *testing.T) {
	if got := selectColumns(config.DatabaseTypeMySQL, nil); got != "*" {
		t.Errorf("expected *, got: %s", got)
	}
	if got := selectColumns(config.DatabaseTypeMySQL, []string{}); got != "*" {
		t.Errorf("expected *, got: %s", got)
	}
}

func TestFetchViolationsNoDLQConfig(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory sqlite: %v", err)
	}
	defer db.Close()
	q := &dbQueryer{db: db}

	if _, err := db.Exec("CREATE TABLE test (id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	if _, err := db.Exec("INSERT INTO test VALUES (1, 'alice'), (2, NULL)"); err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	engine := NewEngine([]config.AssertionConfig{
		{Column: "name", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionAbort},
	})

	results := engine.RunPostCheck(q, config.DatabaseTypeSQLite, "test")
	// abort rule should not trigger FetchViolations
	if err := engine.FetchViolations(q, config.DatabaseTypeSQLite, "test", results[0], nil, nil); err != nil {
		t.Errorf("expected no error when on_fail is not DLQ, got: %v", err)
	}
}

func TestFetchViolationsWithErrorResult(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory sqlite: %v", err)
	}
	defer db.Close()
	q := &dbQueryer{db: db}

	engine := NewEngine([]config.AssertionConfig{
		{Column: "name", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionDLQ},
	})

	res := Result{Rule: NewRule(config.AssertionConfig{Column: "name", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionDLQ}), ViolationCount: 0, Err: sql.ErrConnDone}
	if err := engine.FetchViolations(q, config.DatabaseTypeSQLite, "test", res, nil, nil); err != nil {
		t.Errorf("expected no error for result with Err, got: %v", err)
	}
}

func TestMinMaxLengthDescriptions(t *testing.T) {
	minRule := NewRule(config.AssertionConfig{Column: "name", Rule: config.AssertionRuleMinLength, Length: 3})
	if minRule.Description() != "column 'name' length must be >= 3" {
		t.Errorf("unexpected min_length description: %s", minRule.Description())
	}

	maxRule := NewRule(config.AssertionConfig{Column: "code", Rule: config.AssertionRuleMaxLength, Length: 10})
	if maxRule.Description() != "column 'code' length must be <= 10" {
		t.Errorf("unexpected max_length description: %s", maxRule.Description())
	}
}

func TestUniqueRuleFetchSQLOracle(t *testing.T) {
	rule := NewRule(config.AssertionConfig{Columns: []string{"a", "b"}, Rule: config.AssertionRuleUnique})
	fetchSQL := rule.FetchSQL(config.DatabaseTypeOracle, "(SELECT * FROM t) __src", []string{"a", "b"})
	expected := "SELECT \"A\", \"B\" FROM (SELECT \"A\", \"B\" FROM (SELECT * FROM t) __src GROUP BY \"A\", \"B\" HAVING COUNT(*) > 1) AS __violations"
	if fetchSQL != expected {
		t.Errorf("unique FetchSQL mismatch (Oracle):\n  got:      %s\n  expected: %s", fetchSQL, expected)
	}
}

func TestLengthExprOracle(t *testing.T) {
	expr := lengthExpr(config.DatabaseTypeOracle, "\"NAME\"")
	if expr != "LENGTH(\"NAME\")" {
		t.Errorf("unexpected Oracle length expr: %s", expr)
	}
}

func floatPtr(v float64) *float64 {
	return &v
}
