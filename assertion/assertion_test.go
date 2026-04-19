package assertion

import (
	"database/sql"
	"testing"

	"db-ferry/config"
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
		{func() config.AssertionConfig { min := 0.0; max := 100.0; return config.AssertionConfig{Column: "amount", Rule: config.AssertionRuleRange, Min: &min, Max: &max} }(), "column 'amount' must be in range (min=0, max=100)"},
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

// mockQueryer implements the queryer interface for testing.
type mockQueryer struct {
	queryFn func(sql string) (*sql.Rows, error)
}

func (m *mockQueryer) Query(sql string) (*sql.Rows, error) {
	return m.queryFn(sql)
}
