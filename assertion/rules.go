package assertion

import (
	"fmt"
	"strconv"
	"strings"

	"db-ferry/config"
	"db-ferry/database"
)

// Rule defines an assertion rule that can generate check SQL.
type Rule interface {
	Config() config.AssertionConfig
	// CheckSQL returns a SELECT COUNT(*) query for violations against a FROM clause.
	CheckSQL(dbType, fromClause string) string
	// FetchSQL returns a SELECT query to retrieve violating rows.
	FetchSQL(dbType, fromClause string, columns []string) string
	// Description returns a human-readable description of the rule.
	Description() string
}

// NewRule creates a Rule from an AssertionConfig.
func NewRule(cfg config.AssertionConfig) Rule {
	switch cfg.Rule {
	case config.AssertionRuleNotNull:
		return &notNullRule{cfg: cfg}
	case config.AssertionRuleRange:
		return &rangeRule{cfg: cfg}
	case config.AssertionRuleInSet:
		return &inSetRule{cfg: cfg}
	case config.AssertionRuleUnique:
		return &uniqueRule{cfg: cfg}
	case config.AssertionRuleRegex:
		return &regexRule{cfg: cfg}
	case config.AssertionRuleMinLength:
		return &minLengthRule{cfg: cfg}
	case config.AssertionRuleMaxLength:
		return &maxLengthRule{cfg: cfg}
	default:
		return &unsupportedRule{cfg: cfg}
	}
}

type notNullRule struct{ cfg config.AssertionConfig }

func (r *notNullRule) Config() config.AssertionConfig { return r.cfg }

func (r *notNullRule) CheckSQL(dbType, fromClause string) string {
	col := database.QuoteIdentifier(dbType, r.cfg.Column)
	return fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IS NULL", fromClause, col)
}

func (r *notNullRule) FetchSQL(dbType, fromClause string, columns []string) string {
	col := database.QuoteIdentifier(dbType, r.cfg.Column)
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NULL",
		selectColumns(dbType, columns), fromClause, col)
}

func (r *notNullRule) Description() string {
	return fmt.Sprintf("column '%s' must not be null", r.cfg.Column)
}

type rangeRule struct{ cfg config.AssertionConfig }

func (r *rangeRule) Config() config.AssertionConfig { return r.cfg }

func (r *rangeRule) CheckSQL(dbType, fromClause string) string {
	col := database.QuoteIdentifier(dbType, r.cfg.Column)
	var conditions []string
	if r.cfg.Min != nil {
		conditions = append(conditions, fmt.Sprintf("%s < %v", col, *r.cfg.Min))
	}
	if r.cfg.Max != nil {
		conditions = append(conditions, fmt.Sprintf("%s > %v", col, *r.cfg.Max))
	}
	where := "1=0"
	if len(conditions) > 0 {
		where = strings.Join(conditions, " OR ")
	}
	return fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", fromClause, where)
}

func (r *rangeRule) FetchSQL(dbType, fromClause string, columns []string) string {
	col := database.QuoteIdentifier(dbType, r.cfg.Column)
	var conditions []string
	if r.cfg.Min != nil {
		conditions = append(conditions, fmt.Sprintf("%s < %v", col, *r.cfg.Min))
	}
	if r.cfg.Max != nil {
		conditions = append(conditions, fmt.Sprintf("%s > %v", col, *r.cfg.Max))
	}
	where := "1=0"
	if len(conditions) > 0 {
		where = strings.Join(conditions, " OR ")
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		selectColumns(dbType, columns), fromClause, where)
}

func (r *rangeRule) Description() string {
	parts := []string{}
	if r.cfg.Min != nil {
		parts = append(parts, fmt.Sprintf("min=%v", *r.cfg.Min))
	}
	if r.cfg.Max != nil {
		parts = append(parts, fmt.Sprintf("max=%v", *r.cfg.Max))
	}
	return fmt.Sprintf("column '%s' must be in range (%s)", r.cfg.Column, strings.Join(parts, ", "))
}

type inSetRule struct{ cfg config.AssertionConfig }

func (r *inSetRule) Config() config.AssertionConfig { return r.cfg }

func (r *inSetRule) CheckSQL(dbType, fromClause string) string {
	col := database.QuoteIdentifier(dbType, r.cfg.Column)
	values := formatInValues(r.cfg.Values)
	return fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s NOT IN (%s)", fromClause, col, values)
}

func (r *inSetRule) FetchSQL(dbType, fromClause string, columns []string) string {
	col := database.QuoteIdentifier(dbType, r.cfg.Column)
	values := formatInValues(r.cfg.Values)
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s NOT IN (%s)",
		selectColumns(dbType, columns), fromClause, col, values)
}

func (r *inSetRule) Description() string {
	return fmt.Sprintf("column '%s' must be in set %v", r.cfg.Column, r.cfg.Values)
}

func formatInValues(values []string) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = quoteSQLString(v)
	}
	return strings.Join(quoted, ", ")
}

type uniqueRule struct{ cfg config.AssertionConfig }

func (r *uniqueRule) Config() config.AssertionConfig { return r.cfg }

func (r *uniqueRule) CheckSQL(dbType, fromClause string) string {
	cols := make([]string, len(r.cfg.Columns))
	for i, c := range r.cfg.Columns {
		cols[i] = database.QuoteIdentifier(dbType, c)
	}
	groupBy := strings.Join(cols, ", ")
	var inner string
	switch dbType {
	case config.DatabaseTypeOracle:
		inner = fmt.Sprintf("SELECT 1 FROM %s GROUP BY %s HAVING COUNT(*) > 1", fromClause, groupBy)
	default:
		inner = fmt.Sprintf("SELECT 1 FROM %s AS __uq GROUP BY %s HAVING COUNT(*) > 1", fromClause, groupBy)
	}
	return fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS __violations", inner)
}

func (r *uniqueRule) FetchSQL(dbType, fromClause string, columns []string) string {
	cols := make([]string, len(r.cfg.Columns))
	for i, c := range r.cfg.Columns {
		cols[i] = database.QuoteIdentifier(dbType, c)
	}
	groupBy := strings.Join(cols, ", ")
	var inner string
	switch dbType {
	case config.DatabaseTypeOracle:
		inner = fmt.Sprintf("SELECT %s FROM %s GROUP BY %s HAVING COUNT(*) > 1", groupBy, fromClause, groupBy)
	default:
		inner = fmt.Sprintf("SELECT %s FROM %s AS __uq GROUP BY %s HAVING COUNT(*) > 1", groupBy, fromClause, groupBy)
	}
	return fmt.Sprintf("SELECT %s FROM (%s) AS __violations", selectColumns(dbType, columns), inner)
}

func (r *uniqueRule) Description() string {
	return fmt.Sprintf("columns %v must be unique", r.cfg.Columns)
}

type regexRule struct{ cfg config.AssertionConfig }

func (r *regexRule) Config() config.AssertionConfig { return r.cfg }

func (r *regexRule) CheckSQL(dbType, fromClause string) string {
	col := database.QuoteIdentifier(dbType, r.cfg.Column)
	expr := regexMatchExpr(dbType, col, r.cfg.Pattern)
	return fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE NOT (%s)", fromClause, expr)
}

func (r *regexRule) FetchSQL(dbType, fromClause string, columns []string) string {
	col := database.QuoteIdentifier(dbType, r.cfg.Column)
	expr := regexMatchExpr(dbType, col, r.cfg.Pattern)
	return fmt.Sprintf("SELECT %s FROM %s WHERE NOT (%s)",
		selectColumns(dbType, columns), fromClause, expr)
}

func (r *regexRule) Description() string {
	return fmt.Sprintf("column '%s' must match pattern '%s'", r.cfg.Column, r.cfg.Pattern)
}

func regexMatchExpr(dbType, col, pattern string) string {
	switch dbType {
	case config.DatabaseTypeMySQL:
		return fmt.Sprintf("%s REGEXP %s", col, quoteSQLString(pattern))
	case config.DatabaseTypePostgreSQL, config.DatabaseTypeDuckDB:
		return fmt.Sprintf("%s ~ %s", col, quoteSQLString(pattern))
	case config.DatabaseTypeOracle:
		return fmt.Sprintf("REGEXP_LIKE(%s, %s)", col, quoteSQLString(pattern))
	case config.DatabaseTypeSQLServer:
		// SQL Server does not support native regex; fall back to LIKE with simple wildcard
		return fmt.Sprintf("%s LIKE %s", col, quoteSQLString(pattern))
	default:
		// SQLite: no native regex, return always-true expression
		return "1=1"
	}
}

type minLengthRule struct{ cfg config.AssertionConfig }

func (r *minLengthRule) Config() config.AssertionConfig { return r.cfg }

func (r *minLengthRule) CheckSQL(dbType, fromClause string) string {
	col := database.QuoteIdentifier(dbType, r.cfg.Column)
	lenExpr := lengthExpr(dbType, col)
	return fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s < %d", fromClause, lenExpr, r.cfg.Length)
}

func (r *minLengthRule) FetchSQL(dbType, fromClause string, columns []string) string {
	col := database.QuoteIdentifier(dbType, r.cfg.Column)
	lenExpr := lengthExpr(dbType, col)
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s < %d",
		selectColumns(dbType, columns), fromClause, lenExpr, r.cfg.Length)
}

func (r *minLengthRule) Description() string {
	return fmt.Sprintf("column '%s' length must be >= %d", r.cfg.Column, r.cfg.Length)
}

type maxLengthRule struct{ cfg config.AssertionConfig }

func (r *maxLengthRule) Config() config.AssertionConfig { return r.cfg }

func (r *maxLengthRule) CheckSQL(dbType, fromClause string) string {
	col := database.QuoteIdentifier(dbType, r.cfg.Column)
	lenExpr := lengthExpr(dbType, col)
	return fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s > %d", fromClause, lenExpr, r.cfg.Length)
}

func (r *maxLengthRule) FetchSQL(dbType, fromClause string, columns []string) string {
	col := database.QuoteIdentifier(dbType, r.cfg.Column)
	lenExpr := lengthExpr(dbType, col)
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s > %d",
		selectColumns(dbType, columns), fromClause, lenExpr, r.cfg.Length)
}

func (r *maxLengthRule) Description() string {
	return fmt.Sprintf("column '%s' length must be <= %d", r.cfg.Column, r.cfg.Length)
}

func lengthExpr(dbType, col string) string {
	switch dbType {
	case config.DatabaseTypeOracle:
		return fmt.Sprintf("LENGTH(%s)", col)
	case config.DatabaseTypeSQLServer:
		return fmt.Sprintf("LEN(%s)", col)
	default:
		return fmt.Sprintf("LENGTH(%s)", col)
	}
}

type unsupportedRule struct{ cfg config.AssertionConfig }

func (r *unsupportedRule) Config() config.AssertionConfig { return r.cfg }
func (r *unsupportedRule) CheckSQL(_, _ string) string    { return "SELECT 0" }
func (r *unsupportedRule) FetchSQL(_, _ string, _ []string) string {
	return "SELECT 0 WHERE 1=0"
}
func (r *unsupportedRule) Description() string {
	return fmt.Sprintf("unsupported rule '%s'", r.cfg.Rule)
}

func selectColumns(dbType string, columns []string) string {
	if len(columns) == 0 {
		return "*"
	}
	quoted := make([]string, len(columns))
	for i, c := range columns {
		quoted[i] = database.QuoteIdentifier(dbType, c)
	}
	return strings.Join(quoted, ", ")
}

func quoteSQLString(value string) string {
	escaped := strings.ReplaceAll(value, "'", "''")
	return "'" + escaped + "'"
}

// BuildFromClause builds a FROM clause for pre-migration checks.
func BuildFromClause(dbType, wrappedSQL string) string {
	if dbType == config.DatabaseTypeOracle {
		return fmt.Sprintf("(%s) __src", wrappedSQL)
	}
	return fmt.Sprintf("(%s) AS __src", wrappedSQL)
}

// BuildTableFromClause builds a FROM clause for post-migration checks.
func BuildTableFromClause(dbType, tableName string) string {
	return database.QuoteTableName(tableName, dbType)
}

// formatSQLValue formats a value for use in SQL.
func formatSQLValue(v any) string {
	switch val := v.(type) {
	case string:
		return quoteSQLString(val)
	case []byte:
		return quoteSQLString(string(val))
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("%v", val)
	}
}

// intPtr converts a string pointer to int, returning 0 on error.
func intPtr(s *string) int {
	if s == nil {
		return 0
	}
	v, err := strconv.Atoi(*s)
	if err != nil {
		return 0
	}
	return v
}
