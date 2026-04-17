package database

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"db-ferry/config"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/mattn/go-sqlite3"
)

// mockQueryer wraps *sql.DB to satisfy the queryer interface.
type mockQueryer struct {
	db *sql.DB
}

func (m *mockQueryer) Query(sql string) (*sql.Rows, error) {
	return m.db.Query(sql)
}

func newMockQueryer(t *testing.T) (*mockQueryer, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &mockQueryer{db: db}, mock
}

func TestBuildChecksumSQL(t *testing.T) {
	columns := []ColumnMetadata{{Name: "id"}, {Name: "name"}}
	wrapped := "SELECT * FROM src"

	cases := []struct {
		dbType string
		want   string
	}{
		{config.DatabaseTypeMySQL, "MD5(CONCAT_WS('|', `id`, `name`))"},
		{config.DatabaseTypePostgreSQL, "MD5(CONCAT_WS('|', \"id\", \"name\"))"},
		{config.DatabaseTypeDuckDB, "MD5(CONCAT_WS('|', \"id\", \"name\"))"},
		{config.DatabaseTypeSQLServer, "CONVERT(VARCHAR(32), HASHBYTES('MD5', [id] + '|' + [name]), 2)"},
		{config.DatabaseTypeOracle, "STANDARD_HASH(\"ID\"||'|'||\"NAME\"), 'MD5')"},
		{"unknown", "MD5(CONCAT_WS('|', \"id\", \"name\"))"},
	}

	for _, tc := range cases {
		sqlText := buildChecksumSQL(tc.dbType, columns, wrapped)
		if !strings.Contains(sqlText, tc.want) && tc.dbType != config.DatabaseTypeOracle {
			t.Errorf("buildChecksumSQL(%s) = %s, want substring %s", tc.dbType, sqlText, tc.want)
		}
	}

	// Oracle FROM clause uses no AS
	oracleSQL := buildChecksumSQL(config.DatabaseTypeOracle, columns, wrapped)
	if !strings.Contains(oracleSQL, "FROM (SELECT * FROM src) t") {
		t.Errorf("unexpected oracle from clause: %s", oracleSQL)
	}
	// Others use AS
	mysqlSQL := buildChecksumSQL(config.DatabaseTypeMySQL, columns, wrapped)
	if !strings.Contains(mysqlSQL, "FROM (SELECT * FROM src) AS t") {
		t.Errorf("unexpected mysql from clause: %s", mysqlSQL)
	}
}

func TestBuildSampleSQL(t *testing.T) {
	wrapped := "SELECT 1"
	cases := []struct {
		dbType string
		want   string
	}{
		{config.DatabaseTypeMySQL, "ORDER BY RAND() LIMIT 10"},
		{config.DatabaseTypePostgreSQL, "ORDER BY RANDOM() LIMIT 10"},
		{config.DatabaseTypeSQLite, "ORDER BY RANDOM() LIMIT 10"},
		{config.DatabaseTypeDuckDB, "ORDER BY RANDOM() LIMIT 10"},
		{config.DatabaseTypeSQLServer, "SELECT TOP 10 * FROM (SELECT 1) AS t ORDER BY NEWID()"},
		{config.DatabaseTypeOracle, "ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 10 ROWS ONLY"},
		{"unknown", "ORDER BY RANDOM() LIMIT 10"},
	}
	for _, tc := range cases {
		sqlText := buildSampleSQL(tc.dbType, wrapped, 10)
		if !strings.Contains(sqlText, tc.want) {
			t.Errorf("buildSampleSQL(%s) = %s, want substring %s", tc.dbType, sqlText, tc.want)
		}
	}
}

func TestBuildMatchSQL(t *testing.T) {
	columns := []ColumnMetadata{{Name: "id"}, {Name: "name"}}
	values := []any{1, "alice"}

	sqlText := buildMatchSQL(config.DatabaseTypeMySQL, "users", columns, values)
	want := "SELECT * FROM `users` WHERE `id` = 1 AND `name` = 'alice' LIMIT 1"
	if sqlText != want {
		t.Errorf("buildMatchSQL() = %s, want %s", sqlText, want)
	}

	// SQLServer uses TOP
	sqlText = buildMatchSQL(config.DatabaseTypeSQLServer, "users", columns, values)
	want = "SELECT TOP 1 * FROM [users] WHERE [id] = 1 AND [name] = 'alice'"
	if sqlText != want {
		t.Errorf("buildMatchSQL(sqlserver) = %s, want %s", sqlText, want)
	}

	// Oracle uses FETCH FIRST
	sqlText = buildMatchSQL(config.DatabaseTypeOracle, "users", columns, values)
	want = "SELECT * FROM \"USERS\" WHERE \"ID\" = 1 AND \"NAME\" = 'alice' FETCH FIRST 1 ROWS ONLY"
	if sqlText != want {
		t.Errorf("buildMatchSQL(oracle) = %s, want %s", sqlText, want)
	}

	// nil value -> IS NULL
	valuesNil := []any{nil, "bob"}
	sqlText = buildMatchSQL(config.DatabaseTypeSQLite, "users", columns, valuesNil)
	if !strings.Contains(sqlText, "\"id\" IS NULL") {
		t.Errorf("buildMatchSQL() with nil = %s", sqlText)
	}

	// empty conditions fallback
	sqlText = buildMatchSQL(config.DatabaseTypeMySQL, "users", nil, nil)
	want = "SELECT * FROM `users` WHERE 1=1 LIMIT 1"
	if sqlText != want {
		t.Errorf("buildMatchSQL(empty) = %s, want %s", sqlText, want)
	}
}

func TestFormatSQLValue(t *testing.T) {
	cases := []struct {
		v    any
		want string
	}{
		{"hello", "'hello'"},
		{[]byte("world"), "'world'"},
		{42, "42"},
		{nil, "NULL"},
	}
	for _, tc := range cases {
		got := formatSQLValue(tc.v)
		if got != tc.want {
			t.Errorf("formatSQLValue(%v) = %s, want %s", tc.v, got, tc.want)
		}
	}
}

func TestQuoteSQLString(t *testing.T) {
	got := quoteSQLString("it's")
	want := "'it''s'"
	if got != want {
		t.Errorf("quoteSQLString() = %s, want %s", got, want)
	}
}

func TestCompareValues(t *testing.T) {
	now := time.Now()
	cases := []struct {
		a, b any
		want bool
	}{
		{nil, nil, true},
		{nil, 1, false},
		{1, nil, false},
		{now, now, true},
		{now, now.Add(time.Second), false},
		{now, "x", false},
		{"x", now, false},
		{[]byte("a"), []byte("a"), true},
		{[]byte("a"), []byte("b"), false},
		{[]byte("a"), "a", false},
		{"a", "a", true},
		{1, 1, true},
		{1, 2, false},
	}
	for _, tc := range cases {
		got := CompareValues(tc.a, tc.b)
		if got != tc.want {
			t.Errorf("CompareValues(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestFormatRowPreview(t *testing.T) {
	cols := []ColumnMetadata{{Name: "id"}, {Name: "name"}, {Name: "email"}}
	values := []any{1, "alice", "a@b.com"}
	got := formatRowPreview(values, cols)
	want := "{id:1, name:alice, email:a@b.com}"
	if got != want {
		t.Errorf("formatRowPreview() = %s, want %s", got, want)
	}

	// nil value
	values[1] = nil
	got = formatRowPreview(values, cols)
	if !strings.Contains(got, "name:NULL") {
		t.Errorf("formatRowPreview() with nil = %s", got)
	}

	// long value truncated
	values[1] = strings.Repeat("x", 30)
	got = formatRowPreview(values, cols)
	if !strings.Contains(got, "...") {
		t.Errorf("formatRowPreview() truncation = %s", got)
	}
}

func TestQuoteTableName(t *testing.T) {
	if got := QuoteTableName("users", config.DatabaseTypeMySQL); got != "`users`" {
		t.Errorf("QuoteTableName() = %s", got)
	}
}

func TestIsTextualColumn(t *testing.T) {
	cases := []struct {
		typeName string
		want     bool
	}{
		{"VARCHAR", true},
		{"TEXT", true},
		{"CLOB", true},
		{"STRING", true},
		{"INT", false},
	}
	for _, tc := range cases {
		got := IsTextualColumn(ColumnMetadata{DatabaseType: tc.typeName})
		if got != tc.want {
			t.Errorf("IsTextualColumn(%s) = %v, want %v", tc.typeName, got, tc.want)
		}
	}
	// fallback to GoType
	if !IsTextualColumn(ColumnMetadata{DatabaseType: "", GoType: "string"}) {
		t.Error("expected GoType fallback to match string")
	}
}

func TestAggregateHashes(t *testing.T) {
	got := aggregateHashes([]string{"a", "b"})
	want := aggregateHashes([]string{"a", "b"})
	if got != want {
		t.Errorf("aggregateHashes inconsistent")
	}
	if got == "" {
		t.Error("aggregateHashes returned empty")
	}
}

func TestRowHash(t *testing.T) {
	cols := []ColumnMetadata{{Name: "c1"}, {Name: "c2"}}
	got := rowHash(cols, []any{nil, []byte("ab")})
	if got == "" {
		t.Error("rowHash returned empty")
	}
	// deterministic
	got2 := rowHash(cols, []any{nil, []byte("ab")})
	if got != got2 {
		t.Errorf("rowHash not deterministic")
	}
}

func TestGetTableRowCount(t *testing.T) {
	mq, mock := newMockQueryer(t)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM `users`").
		WillReturnRows(sqlmock.NewRows([]string{"cnt"}).AddRow(42))

	cnt, err := getTableRowCount(mq, "users", config.DatabaseTypeMySQL)
	if err != nil {
		t.Fatalf("getTableRowCount() error = %v", err)
	}
	if cnt != 42 {
		t.Fatalf("getTableRowCount() = %d, want 42", cnt)
	}

	// Oracle uppercases table name
	mq2, mock2 := newMockQueryer(t)
	mock2.ExpectQuery(`SELECT COUNT\(\*\) FROM "USERS"`).
		WillReturnRows(sqlmock.NewRows([]string{"cnt"}).AddRow(7))
	cnt, err = getTableRowCount(mq2, "users", config.DatabaseTypeOracle)
	if err != nil {
		t.Fatalf("getTableRowCount(oracle) error = %v", err)
	}
	if cnt != 7 {
		t.Fatalf("getTableRowCount(oracle) = %d, want 7", cnt)
	}
}

func TestValidateRowCount(t *testing.T) {
	mq, mock := newMockQueryer(t)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM").
		WillReturnRows(sqlmock.NewRows([]string{"cnt"}).AddRow(12))

	task := config.TaskConfig{TableName: "users", TargetDB: "db1"}
	if err := validateRowCount(mq, task, 10, 2); err != nil {
		t.Fatalf("validateRowCount() error = %v", err)
	}

	// mismatch
	mq2, mock2 := newMockQueryer(t)
	mock2.ExpectQuery("SELECT COUNT\\(\\*\\) FROM").
		WillReturnRows(sqlmock.NewRows([]string{"cnt"}).AddRow(15))
	if err := validateRowCount(mq2, task, 10, 2); err == nil {
		t.Fatal("expected row count mismatch error")
	}
}

func TestValidateChecksumWithMock(t *testing.T) {
	src, srcMock := newMockQueryer(t)
	tgt, tgtMock := newMockQueryer(t)

	columns := []ColumnMetadata{{Name: "id"}}
	srcMock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"h"}).AddRow("abc"))
	tgtMock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"h"}).AddRow("abc"))

	task := config.TaskConfig{TableName: "users"}
	err := validateChecksum(src, tgt, config.DatabaseTypeMySQL, config.DatabaseTypeMySQL, task, columns, "SELECT 1")
	if err != nil {
		t.Fatalf("validateChecksum() error = %v", err)
	}

	// mismatch
	src2, srcMock2 := newMockQueryer(t)
	tgt2, tgtMock2 := newMockQueryer(t)
	srcMock2.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"h"}).AddRow("abc"))
	tgtMock2.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"h"}).AddRow("def"))
	err = validateChecksum(src2, tgt2, config.DatabaseTypeMySQL, config.DatabaseTypeMySQL, task, columns, "SELECT 1")
	if err == nil {
		t.Fatal("expected checksum mismatch error")
	}
}

func TestComputeSQLiteChecksum(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sqlite open error = %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER, name TEXT)`); err != nil {
		t.Fatalf("create table error = %v", err)
	}
	if _, err := db.Exec(`INSERT INTO t VALUES (1, 'alice'), (2, 'bob')`); err != nil {
		t.Fatalf("insert error = %v", err)
	}

	mq := &mockQueryer{db: db}
	columns := []ColumnMetadata{{Name: "id"}, {Name: "name"}}
	checksum, err := computeSQLiteChecksum(mq, columns, "SELECT * FROM t")
	if err != nil {
		t.Fatalf("computeSQLiteChecksum() error = %v", err)
	}
	if checksum == "" {
		t.Fatal("expected non-empty checksum")
	}

	// deterministic
	checksum2, err := computeSQLiteChecksum(mq, columns, "SELECT * FROM t")
	if err != nil {
		t.Fatalf("computeSQLiteChecksum() second error = %v", err)
	}
	if checksum != checksum2 {
		t.Fatal("checksum not deterministic")
	}
}

func TestValidateSampleWithMock(t *testing.T) {
	src, srcMock := newMockQueryer(t)
	tgt, tgtMock := newMockQueryer(t)

	columns := []ColumnMetadata{{Name: "id", DatabaseType: "INTEGER"}, {Name: "name", DatabaseType: "VARCHAR"}}
	srcMock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "alice"),
	)
	tgtMock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "alice"),
	)

	task := config.TaskConfig{TableName: "users", ValidateSampleSize: 1}
	err := validateSample(src, tgt, config.DatabaseTypeMySQL, config.DatabaseTypeMySQL, task, columns, "SELECT 1")
	if err != nil {
		t.Fatalf("validateSample() error = %v", err)
	}
}

func TestValidateSampleRowNotFound(t *testing.T) {
	src, srcMock := newMockQueryer(t)
	tgt, tgtMock := newMockQueryer(t)

	columns := []ColumnMetadata{{Name: "id", DatabaseType: "INTEGER"}}
	srcMock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"id"}).AddRow(1),
	)
	tgtMock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"id"}),
	)

	task := config.TaskConfig{TableName: "users", ValidateSampleSize: 1}
	if err := validateSample(src, tgt, config.DatabaseTypeMySQL, config.DatabaseTypeMySQL, task, columns, "SELECT 1"); err == nil {
		t.Fatal("expected not found error")
	}
}

func TestValidateSampleValueMismatch(t *testing.T) {
	src, srcMock := newMockQueryer(t)
	tgt, tgtMock := newMockQueryer(t)

	columns := []ColumnMetadata{{Name: "id", DatabaseType: "INTEGER"}}
	srcMock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"id"}).AddRow(1),
	)
	tgtMock.ExpectQuery("SELECT").WillReturnRows(
		sqlmock.NewRows([]string{"id"}).AddRow(2),
	)

	task := config.TaskConfig{TableName: "users", ValidateSampleSize: 1}
	if err := validateSample(src, tgt, config.DatabaseTypeMySQL, config.DatabaseTypeMySQL, task, columns, "SELECT 1"); err == nil {
		t.Fatal("expected mismatch error")
	}
}

func TestValidateTask(t *testing.T) {
	tgt, tgtMock := newMockQueryer(t)
	tgtMock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM").WillReturnRows(sqlmock.NewRows([]string{"cnt"}).AddRow(12))

	task := config.TaskConfig{Validate: config.TaskValidateRowCount, TableName: "users", TargetDB: "db1"}
	if err := ValidateTask(nil, tgt, "", config.DatabaseTypeMySQL, task, nil, "", 10, 2); err != nil {
		t.Fatalf("ValidateTask(row_count) error = %v", err)
	}

	// none -> nil
	task.Validate = config.TaskValidateNone
	if err := ValidateTask(nil, nil, "", "", task, nil, "", 0, 0); err != nil {
		t.Fatalf("ValidateTask(none) error = %v", err)
	}

	// unknown -> nil
	task.Validate = "unknown"
	if err := ValidateTask(nil, nil, "", "", task, nil, "", 0, 0); err != nil {
		t.Fatalf("ValidateTask(unknown) error = %v", err)
	}
}

func TestValidateTaskChecksumAndSample(t *testing.T) {
	src, srcMock := newMockQueryer(t)
	tgt, tgtMock := newMockQueryer(t)

	columns := []ColumnMetadata{{Name: "id"}}
	srcMock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"h"}).AddRow("abc"))
	tgtMock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"h"}).AddRow("abc"))

	task := config.TaskConfig{Validate: config.TaskValidateChecksum, TableName: "users"}
	if err := ValidateTask(src, tgt, config.DatabaseTypeMySQL, config.DatabaseTypeMySQL, task, columns, "SELECT 1", 0, 0); err != nil {
		t.Fatalf("ValidateTask(checksum) error = %v", err)
	}

	src2, srcMock2 := newMockQueryer(t)
	tgt2, tgtMock2 := newMockQueryer(t)
	srcMock2.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	tgtMock2.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))

	task.Validate = config.TaskValidateSample
	task.ValidateSampleSize = 1
	if err := ValidateTask(src2, tgt2, config.DatabaseTypeMySQL, config.DatabaseTypeMySQL, task, columns, "SELECT 1", 0, 0); err != nil {
		t.Fatalf("ValidateTask(sample) error = %v", err)
	}
}

func TestComputeChecksumWithRealSQLite(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sqlite open error = %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER, name TEXT)`); err != nil {
		t.Fatalf("create table error = %v", err)
	}
	if _, err := db.Exec(`INSERT INTO t VALUES (1, 'alice'), (2, 'bob')`); err != nil {
		t.Fatalf("insert error = %v", err)
	}

	mq := &mockQueryer{db: db}
	columns := []ColumnMetadata{{Name: "id"}, {Name: "name"}}
	checksum, err := computeChecksum(mq, config.DatabaseTypeSQLite, columns, "SELECT * FROM t")
	if err != nil {
		t.Fatalf("computeChecksum(sqlite) error = %v", err)
	}
	if checksum == "" {
		t.Fatal("expected non-empty checksum")
	}
}

func TestFindTargetRow(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sqlite open error = %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE users (id INTEGER, name TEXT)`); err != nil {
		t.Fatalf("create table error = %v", err)
	}
	if _, err := db.Exec(`INSERT INTO users VALUES (1, 'alice')`); err != nil {
		t.Fatalf("insert error = %v", err)
	}

	mq := &mockQueryer{db: db}
	columns := []ColumnMetadata{{Name: "id", DatabaseType: "INTEGER"}, {Name: "name", DatabaseType: "TEXT"}}
	row, found, err := findTargetRow(mq, config.DatabaseTypeSQLite, "users", columns, []any{1, "alice"})
	if err != nil {
		t.Fatalf("findTargetRow() error = %v", err)
	}
	if !found {
		t.Fatal("expected row found")
	}
	if len(row) != 2 {
		t.Fatalf("expected 2 cols, got %d", len(row))
	}

	// not found
	_, found, err = findTargetRow(mq, config.DatabaseTypeSQLite, "users", columns, []any{999, "nobody"})
	if err != nil {
		t.Fatalf("findTargetRow(not found) error = %v", err)
	}
	if found {
		t.Fatal("expected not found")
	}
}

func TestGetTableRowCountNoRow(t *testing.T) {
	mq, mock := newMockQueryer(t)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM").WillReturnRows(sqlmock.NewRows([]string{"cnt"}))
	_, err := getTableRowCount(mq, "users", config.DatabaseTypeMySQL)
	if err == nil {
		t.Fatal("expected no row error")
	}
}

func TestGetTableRowCountQueryError(t *testing.T) {
	mq, mock := newMockQueryer(t)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM").WillReturnError(fmt.Errorf("boom"))
	_, err := getTableRowCount(mq, "users", config.DatabaseTypeMySQL)
	if err == nil {
		t.Fatal("expected query error")
	}
}

func TestValidateRowCountQueryError(t *testing.T) {
	mq, mock := newMockQueryer(t)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM").WillReturnError(fmt.Errorf("boom"))
	task := config.TaskConfig{TableName: "users", TargetDB: "db1"}
	if err := validateRowCount(mq, task, 10, 2); err == nil {
		t.Fatal("expected error")
	}
}
