//go:build !windows

package database

import (
	"errors"
	"regexp"
	"testing"

	"db-ferry/config"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestDuckDBCreateTableAndEnsureTable(t *testing.T) {
	db, mock := newSQLMock(t)
	d := &DuckDB{db: db}
	cols := []ColumnMetadata{
		{Name: "id", DatabaseType: "INT"},
		{Name: "name", DatabaseType: "VARCHAR", LengthValid: true, Length: 20},
	}

	mock.ExpectExec(regexp.QuoteMeta(`DROP TABLE IF EXISTS "users"`)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`CREATE TABLE "users" ("id" BIGINT, "name" VARCHAR(20))`)).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := d.CreateTable("users", cols); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta(`CREATE TABLE IF NOT EXISTS "users" ("id" BIGINT, "name" VARCHAR(20))`)).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := d.EnsureTable("users", cols); err != nil {
		t.Fatalf("EnsureTable() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestDuckDBInsertUpsertAndCount(t *testing.T) {
	db, mock := newSQLMock(t)
	d := &DuckDB{db: db}
	cols := []ColumnMetadata{
		{Name: "id", DatabaseType: "INT"},
		{Name: "name", DatabaseType: "VARCHAR"},
	}
	values := [][]any{{1, "a"}, {2, "b"}}

	insertSQL := `INSERT INTO "users" ("id", "name") VALUES (?, ?)`
	mock.ExpectBegin()
	insertPrep := mock.ExpectPrepare(regexp.QuoteMeta(insertSQL))
	insertPrep.ExpectExec().WithArgs(1, "a").WillReturnResult(sqlmock.NewResult(1, 1))
	insertPrep.ExpectExec().WithArgs(2, "b").WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectCommit()
	if err := d.InsertData("users", cols, values); err != nil {
		t.Fatalf("InsertData() error = %v", err)
	}

	upsertSQL := `INSERT INTO "users" ("id", "name") VALUES (?, ?) ON CONFLICT("id") DO UPDATE SET "name"=excluded."name"`
	mock.ExpectBegin()
	upsertPrep := mock.ExpectPrepare(regexp.QuoteMeta(upsertSQL))
	upsertPrep.ExpectExec().WithArgs(1, "a").WillReturnResult(sqlmock.NewResult(1, 1))
	upsertPrep.ExpectExec().WithArgs(2, "b").WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectCommit()
	if err := d.UpsertData("users", cols, values, []string{"id"}); err != nil {
		t.Fatalf("UpsertData() error = %v", err)
	}

	transformCols := []ColumnMetadata{
		{Name: "id", DatabaseType: "INT", Transform: "? + 1"},
		{Name: "name", DatabaseType: "VARCHAR", Transform: "CONCAT(?, '!')"},
	}
	insertTransformSQL := `INSERT INTO "users" ("id", "name") VALUES (? + 1, CONCAT(?, '!'))`
	mock.ExpectBegin()
	transformPrep := mock.ExpectPrepare(regexp.QuoteMeta(insertTransformSQL))
	transformPrep.ExpectExec().WithArgs(1, "a").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	if err := d.InsertData("users", transformCols, [][]any{{1, "a"}}); err != nil {
		t.Fatalf("InsertData() with transform error = %v", err)
	}

	upsertTransformSQL := `INSERT INTO "users" ("id", "name") VALUES (? + 1, CONCAT(?, '!')) ON CONFLICT("id") DO UPDATE SET "name"=excluded."name"`
	mock.ExpectBegin()
	upsertTransformPrep := mock.ExpectPrepare(regexp.QuoteMeta(upsertTransformSQL))
	upsertTransformPrep.ExpectExec().WithArgs(1, "a").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	if err := d.UpsertData("users", transformCols, [][]any{{1, "a"}}, []string{"id"}); err != nil {
		t.Fatalf("UpsertData() with transform error = %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM (SELECT * FROM users) AS count_query`)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	cnt, err := d.GetRowCount("SELECT * FROM users")
	if err != nil {
		t.Fatalf("GetRowCount() error = %v", err)
	}
	if cnt != 2 {
		t.Fatalf("GetRowCount() = %d, want 2", cnt)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM "users"`)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	tableCnt, err := d.GetTableRowCount("users")
	if err != nil {
		t.Fatalf("GetTableRowCount() error = %v", err)
	}
	if tableCnt != 2 {
		t.Fatalf("GetTableRowCount() = %d, want 2", tableCnt)
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1")).
		WillReturnRows(sqlmock.NewRows([]string{"one"}).AddRow(1))
	rows, err := d.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	rows.Close()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestDuckDBCreateIndexesAndHelpers(t *testing.T) {
	db, mock := newSQLMock(t)
	d := &DuckDB{db: db}

	mock.ExpectExec(regexp.QuoteMeta(`DROP INDEX IF EXISTS "idx_users_name"`)).
		WillReturnError(errors.New("ignore"))
	mock.ExpectExec(regexp.QuoteMeta(`CREATE UNIQUE INDEX "idx_users_name" ON "users" ("name" DESC)`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := d.CreateIndexes("users", []config.IndexConfig{
		{Name: "idx_users_name", Columns: []string{"name:DESC"}, Unique: true},
	})
	if err != nil {
		t.Fatalf("CreateIndexes() error = %v", err)
	}

	if got := d.quoteIdentifier(`a"b`); got != `"a""b"` {
		t.Fatalf("quoteIdentifier() = %s", got)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestDuckDBExec(t *testing.T) {
	db, mock := newSQLMock(t)
	d := &DuckDB{db: db}

	mock.ExpectExec(regexp.QuoteMeta(`CREATE TABLE t (id INT)`)).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := d.Exec(`CREATE TABLE t (id INT)`); err != nil {
		t.Fatalf("Exec() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestDuckDBGetTables(t *testing.T) {
	db, mock := newSQLMock(t)
	d := &DuckDB{db: db}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_name")).
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("users").AddRow("orders"))

	tables, err := d.GetTables()
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

func TestDuckDBEdgeCasesAndTypeMapping(t *testing.T) {
	d := &DuckDB{}
	if err := d.CreateTable("users", nil); err == nil {
		t.Fatalf("expected CreateTable() error for empty columns")
	}
	if err := d.InsertData("users", nil, nil); err != nil {
		t.Fatalf("InsertData() empty should be nil, got %v", err)
	}
	if err := d.UpsertData("users", nil, [][]any{{1}}, nil); err == nil {
		t.Fatalf("expected merge_keys required error")
	}

	cases := []struct {
		meta ColumnMetadata
		want string
	}{
		{ColumnMetadata{DatabaseType: "INT"}, "BIGINT"},
		{ColumnMetadata{DatabaseType: "DECIMAL", PrecisionScaleValid: true, Precision: 10, Scale: 2}, "DECIMAL(10,2)"},
		{ColumnMetadata{DatabaseType: "DOUBLE"}, "DOUBLE"},
		{ColumnMetadata{DatabaseType: "CHAR", LengthValid: true, Length: 20}, "VARCHAR(20)"},
		{ColumnMetadata{DatabaseType: "DATE"}, "TIMESTAMP"},
		{ColumnMetadata{DatabaseType: "BLOB"}, "BLOB"},
		{ColumnMetadata{DatabaseType: "BOOL"}, "BOOLEAN"},
		{ColumnMetadata{DatabaseType: "X"}, "VARCHAR"},
	}
	for _, tc := range cases {
		if got := d.mapToDuckDBType(tc.meta); got != tc.want {
			t.Fatalf("mapToDuckDBType(%+v) = %s, want %s", tc.meta, got, tc.want)
		}
	}
}
