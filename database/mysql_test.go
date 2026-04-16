package database

import (
	"errors"
	"regexp"
	"testing"

	"db-ferry/config"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestMySQLCreateTableAndEnsureTable(t *testing.T) {
	db, mock := newSQLMock(t)
	m := &MySQLDB{db: db}
	cols := []ColumnMetadata{
		{Name: "id", DatabaseType: "INT"},
		{Name: "name", DatabaseType: "VARCHAR", LengthValid: true, Length: 20},
	}

	mock.ExpectExec(regexp.QuoteMeta("DROP TABLE IF EXISTS `users`")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE `users` (`id` BIGINT, `name` VARCHAR(20))")).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := m.CreateTable("users", cols); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS `users` (`id` BIGINT, `name` VARCHAR(20))")).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := m.EnsureTable("users", cols); err != nil {
		t.Fatalf("EnsureTable() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestMySQLInsertUpsertAndCount(t *testing.T) {
	db, mock := newSQLMock(t)
	m := &MySQLDB{db: db}
	cols := []ColumnMetadata{
		{Name: "id", DatabaseType: "INT"},
		{Name: "name", DatabaseType: "VARCHAR"},
	}
	values := [][]any{{1, "a"}, {2, "b"}}

	insertSQL := "INSERT INTO `users` (`id`, `name`) VALUES (?, ?)"
	mock.ExpectBegin()
	insertPrep := mock.ExpectPrepare(regexp.QuoteMeta(insertSQL))
	insertPrep.ExpectExec().WithArgs(1, "a").WillReturnResult(sqlmock.NewResult(1, 1))
	insertPrep.ExpectExec().WithArgs(2, "b").WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectCommit()
	if err := m.InsertData("users", cols, values); err != nil {
		t.Fatalf("InsertData() error = %v", err)
	}

	upsertSQL := "INSERT INTO `users` (`id`, `name`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `name`=VALUES(`name`)"
	mock.ExpectBegin()
	upsertPrep := mock.ExpectPrepare(regexp.QuoteMeta(upsertSQL))
	upsertPrep.ExpectExec().WithArgs(1, "a").WillReturnResult(sqlmock.NewResult(1, 1))
	upsertPrep.ExpectExec().WithArgs(2, "b").WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectCommit()
	if err := m.UpsertData("users", cols, values, []string{"id"}); err != nil {
		t.Fatalf("UpsertData() error = %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM (SELECT * FROM users) AS count_query")).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	cnt, err := m.GetRowCount("SELECT * FROM users")
	if err != nil {
		t.Fatalf("GetRowCount() error = %v", err)
	}
	if cnt != 2 {
		t.Fatalf("GetRowCount() = %d, want 2", cnt)
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM `users`")).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	tableCnt, err := m.GetTableRowCount("users")
	if err != nil {
		t.Fatalf("GetTableRowCount() error = %v", err)
	}
	if tableCnt != 2 {
		t.Fatalf("GetTableRowCount() = %d, want 2", tableCnt)
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1")).
		WillReturnRows(sqlmock.NewRows([]string{"one"}).AddRow(1))
	rows, err := m.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	rows.Close()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestMySQLCreateIndexes(t *testing.T) {
	db, mock := newSQLMock(t)
	m := &MySQLDB{db: db}

	mock.ExpectExec(regexp.QuoteMeta("DROP INDEX IF EXISTS `idx_users_name` ON `users`")).
		WillReturnError(errors.New("ignore"))
	mock.ExpectExec(regexp.QuoteMeta("CREATE INDEX `idx_users_name` ON `users` (`name` DESC)")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := m.CreateIndexes("users", []config.IndexConfig{
		{Name: "idx_users_name", Columns: []string{"name:DESC"}},
	})
	if err != nil {
		t.Fatalf("CreateIndexes() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestMySQLPingAndExec(t *testing.T) {
	db, mock := newSQLMock(t)
	m := &MySQLDB{db: db}

	mock.ExpectPing()
	if err := m.Ping(); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE t (id INT)")).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := m.Exec("CREATE TABLE t (id INT)"); err != nil {
		t.Fatalf("Exec() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestMySQLGetTables(t *testing.T) {
	db, mock := newSQLMock(t)
	m := &MySQLDB{db: db}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_name")).
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("users").AddRow("orders"))

	tables, err := m.GetTables()
	if err != nil {
		t.Fatalf("GetTables() error = %v", err)
	}
	want := []string{"users", "orders"}
	if len(tables) != len(want) {
		t.Fatalf("GetTables() = %v, want %v", tables, want)
	}
	for i := range want {
		if tables[i] != want[i] {
			t.Fatalf("GetTables()[%d] = %s, want %s", i, tables[i], want[i])
		}
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestMySQLQueryAndCountErrors(t *testing.T) {
	db, mock := newSQLMock(t)
	m := &MySQLDB{db: db}

	mock.ExpectQuery("SELECT 1").WillReturnError(errors.New("query failed"))
	_, err := m.Query("SELECT 1")
	if err == nil {
		t.Fatalf("expected Query error")
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM (SELECT * FROM bad) AS count_query")).WillReturnError(errors.New("count failed"))
	_, err = m.GetRowCount("SELECT * FROM bad")
	if err == nil {
		t.Fatalf("expected GetRowCount error")
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM `bad`")).WillReturnError(errors.New("table count failed"))
	_, err = m.GetTableRowCount("bad")
	if err == nil {
		t.Fatalf("expected GetTableRowCount error")
	}
}

func TestMySQLGetTablesError(t *testing.T) {
	db, mock := newSQLMock(t)
	m := &MySQLDB{db: db}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_name")).WillReturnError(errors.New("db down"))
	_, err := m.GetTables()
	if err == nil {
		t.Fatalf("expected GetTables error")
	}
}

func TestMySQLEdgeCases(t *testing.T) {
	m := &MySQLDB{}
	if err := m.CreateTable("users", nil); err == nil {
		t.Fatalf("expected CreateTable() error for empty columns")
	}
	if err := m.InsertData("users", nil, nil); err != nil {
		t.Fatalf("InsertData() empty should be nil, got %v", err)
	}
	if err := m.UpsertData("users", nil, [][]any{{1}}, nil); err == nil {
		t.Fatalf("expected merge_keys required error")
	}

	if got := m.quoteIdentifier("a`b"); got != "`a``b`" {
		t.Fatalf("quoteIdentifier() = %s", got)
	}

	cases := []struct {
		meta ColumnMetadata
		want string
	}{
		{ColumnMetadata{DatabaseType: "INT"}, "BIGINT"},
		{ColumnMetadata{DatabaseType: "DOUBLE"}, "DOUBLE"},
		{ColumnMetadata{DatabaseType: "DECIMAL", PrecisionScaleValid: true, Precision: 12, Scale: 2}, "DECIMAL(12,2)"},
		{ColumnMetadata{DatabaseType: "CHAR", LengthValid: true, Length: 10}, "VARCHAR(10)"},
		{ColumnMetadata{DatabaseType: "DATE"}, "DATETIME"},
		{ColumnMetadata{DatabaseType: "BLOB"}, "LONGBLOB"},
		{ColumnMetadata{DatabaseType: "BOOL"}, "TINYINT(1)"},
		{ColumnMetadata{DatabaseType: "OTHER"}, "TEXT"},
	}
	for _, tc := range cases {
		if got := m.mapToMySQLType(tc.meta); got != tc.want {
			t.Fatalf("mapToMySQLType(%+v) = %s, want %s", tc.meta, got, tc.want)
		}
	}
}
