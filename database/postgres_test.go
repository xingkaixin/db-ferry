package database

import (
	"errors"
	"regexp"
	"testing"

	"db-ferry/config"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresCreateTableAndEnsureTable(t *testing.T) {
	db, mock := newSQLMock(t)
	p := &PostgresDB{db: db}
	cols := []ColumnMetadata{
		{Name: "id", DatabaseType: "INT"},
		{Name: "name", DatabaseType: "VARCHAR", LengthValid: true, Length: 20},
	}

	mock.ExpectExec(regexp.QuoteMeta(`DROP TABLE IF EXISTS "users"`)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`CREATE TABLE "users" ("id" BIGINT, "name" VARCHAR(20))`)).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := p.CreateTable("users", cols); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta(`CREATE TABLE IF NOT EXISTS "users" ("id" BIGINT, "name" VARCHAR(20))`)).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := p.EnsureTable("users", cols); err != nil {
		t.Fatalf("EnsureTable() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPostgresInsertUpsertAndCount(t *testing.T) {
	db, mock := newSQLMock(t)
	p := &PostgresDB{db: db}
	cols := []ColumnMetadata{
		{Name: "id", DatabaseType: "INT"},
		{Name: "name", DatabaseType: "VARCHAR"},
	}
	values := [][]any{{1, "a"}, {2, "b"}}

	insertSQL := `INSERT INTO "users" ("id", "name") VALUES ($1, $2)`
	mock.ExpectBegin()
	insertPrep := mock.ExpectPrepare(regexp.QuoteMeta(insertSQL))
	insertPrep.ExpectExec().WithArgs(1, "a").WillReturnResult(sqlmock.NewResult(1, 1))
	insertPrep.ExpectExec().WithArgs(2, "b").WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectCommit()
	if err := p.InsertData("users", cols, values); err != nil {
		t.Fatalf("InsertData() error = %v", err)
	}

	upsertSQL := `INSERT INTO "users" ("id", "name") VALUES ($1, $2) ON CONFLICT("id") DO UPDATE SET "name"=EXCLUDED."name"`
	mock.ExpectBegin()
	upsertPrep := mock.ExpectPrepare(regexp.QuoteMeta(upsertSQL))
	upsertPrep.ExpectExec().WithArgs(1, "a").WillReturnResult(sqlmock.NewResult(1, 1))
	upsertPrep.ExpectExec().WithArgs(2, "b").WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectCommit()
	if err := p.UpsertData("users", cols, values, []string{"id"}); err != nil {
		t.Fatalf("UpsertData() error = %v", err)
	}

	transformCols := []ColumnMetadata{
		{Name: "id", DatabaseType: "INT", Transform: "id + ?"},
		{Name: "name", DatabaseType: "VARCHAR", Transform: "CONCAT(?, '!')"},
	}
	insertTransformSQL := `INSERT INTO "users" ("id", "name") VALUES (id + $1, CONCAT($2, '!'))`
	mock.ExpectBegin()
	transformPrep := mock.ExpectPrepare(regexp.QuoteMeta(insertTransformSQL))
	transformPrep.ExpectExec().WithArgs(1, "a").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	if err := p.InsertData("users", transformCols, [][]any{{1, "a"}}); err != nil {
		t.Fatalf("InsertData() with transform error = %v", err)
	}

	upsertTransformSQL := `INSERT INTO "users" ("id", "name") VALUES (id + $1, CONCAT($2, '!')) ON CONFLICT("id") DO UPDATE SET "name"=EXCLUDED."name"`
	mock.ExpectBegin()
	upsertTransformPrep := mock.ExpectPrepare(regexp.QuoteMeta(upsertTransformSQL))
	upsertTransformPrep.ExpectExec().WithArgs(1, "a").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	if err := p.UpsertData("users", transformCols, [][]any{{1, "a"}}, []string{"id"}); err != nil {
		t.Fatalf("UpsertData() with transform error = %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM (SELECT * FROM users) AS count_query`)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	cnt, err := p.GetRowCount("SELECT * FROM users")
	if err != nil {
		t.Fatalf("GetRowCount() error = %v", err)
	}
	if cnt != 2 {
		t.Fatalf("GetRowCount() = %d, want 2", cnt)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM "users"`)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	tableCnt, err := p.GetTableRowCount("users")
	if err != nil {
		t.Fatalf("GetTableRowCount() error = %v", err)
	}
	if tableCnt != 2 {
		t.Fatalf("GetTableRowCount() = %d, want 2", tableCnt)
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1")).
		WillReturnRows(sqlmock.NewRows([]string{"one"}).AddRow(1))
	rows, err := p.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	rows.Close()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPostgresCreateIndexesAndHelpers(t *testing.T) {
	db, mock := newSQLMock(t)
	p := &PostgresDB{db: db}

	mock.ExpectExec(regexp.QuoteMeta(`DROP INDEX IF EXISTS "idx_users_name"`)).
		WillReturnError(errors.New("ignore"))
	mock.ExpectExec(regexp.QuoteMeta(`CREATE UNIQUE INDEX "idx_users_name" ON "users" ("name" DESC)`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := p.CreateIndexes("users", []config.IndexConfig{
		{Name: "idx_users_name", Columns: []string{"name:DESC"}, Unique: true},
	})
	if err != nil {
		t.Fatalf("CreateIndexes() error = %v", err)
	}

	holders := buildPostgresPlaceholders(3)
	if len(holders) != 3 || holders[2] != "$3" {
		t.Fatalf("unexpected placeholders: %#v", holders)
	}
	if got := p.quoteIdentifier(`a"b`); got != `"a""b"` {
		t.Fatalf("quoteIdentifier() = %s", got)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPostgresExec(t *testing.T) {
	db, mock := newSQLMock(t)
	p := &PostgresDB{db: db}

	mock.ExpectExec(regexp.QuoteMeta(`CREATE TABLE t (id INT)`)).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := p.Exec(`CREATE TABLE t (id INT)`); err != nil {
		t.Fatalf("Exec() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPostgresGetTables(t *testing.T) {
	db, mock := newSQLMock(t)
	p := &PostgresDB{db: db}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_name")).
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("users").AddRow("orders"))

	tables, err := p.GetTables()
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

func TestPostgresQueryAndCountErrors(t *testing.T) {
	db, mock := newSQLMock(t)
	p := &PostgresDB{db: db}

	mock.ExpectQuery("SELECT 1").WillReturnError(errors.New("query failed"))
	_, err := p.Query("SELECT 1")
	if err == nil {
		t.Fatalf("expected Query error")
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM (SELECT * FROM bad) AS count_query")).WillReturnError(errors.New("count failed"))
	_, err = p.GetRowCount("SELECT * FROM bad")
	if err == nil {
		t.Fatalf("expected GetRowCount error")
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM \"bad\"")).WillReturnError(errors.New("table count failed"))
	_, err = p.GetTableRowCount("bad")
	if err == nil {
		t.Fatalf("expected GetTableRowCount error")
	}
}

func TestPostgresGetTablesError(t *testing.T) {
	db, mock := newSQLMock(t)
	p := &PostgresDB{db: db}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type IN ('BASE TABLE', 'VIEW') ORDER BY table_name")).WillReturnError(errors.New("db down"))
	_, err := p.GetTables()
	if err == nil {
		t.Fatalf("expected GetTables error")
	}
}

func TestPostgresEdgeCasesAndTypeMapping(t *testing.T) {
	p := &PostgresDB{}
	if err := p.CreateTable("users", nil); err == nil {
		t.Fatalf("expected CreateTable() error for empty columns")
	}
	if err := p.InsertData("users", nil, nil); err != nil {
		t.Fatalf("InsertData() empty should be nil, got %v", err)
	}
	if err := p.UpsertData("users", nil, [][]any{{1}}, nil); err == nil {
		t.Fatalf("expected merge_keys required error")
	}

	cases := []struct {
		meta ColumnMetadata
		want string
	}{
		{ColumnMetadata{DatabaseType: "INT"}, "BIGINT"},
		{ColumnMetadata{DatabaseType: "DOUBLE"}, "DOUBLE PRECISION"},
		{ColumnMetadata{DatabaseType: "DECIMAL", PrecisionScaleValid: true, Precision: 10, Scale: 2}, "NUMERIC(10,2)"},
		{ColumnMetadata{DatabaseType: "CHAR", LengthValid: true, Length: 12}, "VARCHAR(12)"},
		{ColumnMetadata{DatabaseType: "DATE"}, "TIMESTAMP"},
		{ColumnMetadata{DatabaseType: "BLOB"}, "BYTEA"},
		{ColumnMetadata{DatabaseType: "BOOL"}, "BOOLEAN"},
		{ColumnMetadata{DatabaseType: "X"}, "TEXT"},
	}
	for _, tc := range cases {
		if got := p.mapToPostgresType(tc.meta); got != tc.want {
			t.Fatalf("mapToPostgresType(%+v) = %s, want %s", tc.meta, got, tc.want)
		}
	}
}
