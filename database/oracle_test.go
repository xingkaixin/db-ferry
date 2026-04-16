package database

import (
	"errors"
	"regexp"
	"testing"

	"db-ferry/config"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestOracleCreateTableAndEnsureTable(t *testing.T) {
	db, mock := newSQLMock(t)
	o := &OracleDB{db: db}
	cols := []ColumnMetadata{
		{Name: "id", DatabaseType: "INT"},
		{Name: "name", DatabaseType: "VARCHAR", LengthValid: true, Length: 20},
	}

	dropSQL := `BEGIN EXECUTE IMMEDIATE 'DROP TABLE "USERS"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;`
	createSQL := `CREATE TABLE "USERS" ("ID" NUMBER(19,0), "NAME" VARCHAR2(20))`
	mock.ExpectExec(regexp.QuoteMeta(dropSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(createSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := o.CreateTable("users", cols); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM user_tables WHERE table_name = :1")).
		WithArgs("USERS").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	if err := o.EnsureTable("users", cols); err != nil {
		t.Fatalf("EnsureTable() existing error = %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM user_tables WHERE table_name = :1")).
		WithArgs("USERS").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(regexp.QuoteMeta(createSQL)).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := o.EnsureTable("users", cols); err != nil {
		t.Fatalf("EnsureTable() create error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestOracleInsertUpsertAndCount(t *testing.T) {
	db, mock := newSQLMock(t)
	o := &OracleDB{db: db}
	cols := []ColumnMetadata{
		{Name: "id", DatabaseType: "INT"},
		{Name: "name", DatabaseType: "VARCHAR"},
	}
	values := [][]any{{1, "a"}, {2, "b"}}

	insertSQL := `INSERT INTO "USERS" ("ID", "NAME") VALUES (:1, :2)`
	mock.ExpectBegin()
	insertPrep := mock.ExpectPrepare(regexp.QuoteMeta(insertSQL))
	insertPrep.ExpectExec().WithArgs(1, "a").WillReturnResult(sqlmock.NewResult(1, 1))
	insertPrep.ExpectExec().WithArgs(2, "b").WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectCommit()
	if err := o.InsertData("users", cols, values); err != nil {
		t.Fatalf("InsertData() error = %v", err)
	}

	upsertSQL := `MERGE INTO "USERS" t USING (SELECT :1 "ID", :2 "NAME" FROM dual) s ON (t."ID" = s."ID") WHEN MATCHED THEN UPDATE SET t."NAME" = s."NAME" WHEN NOT MATCHED THEN INSERT ("ID", "NAME") VALUES (s."ID", s."NAME")`
	mock.ExpectBegin()
	upsertPrep := mock.ExpectPrepare(regexp.QuoteMeta(upsertSQL))
	upsertPrep.ExpectExec().WithArgs(1, "a").WillReturnResult(sqlmock.NewResult(1, 1))
	upsertPrep.ExpectExec().WithArgs(2, "b").WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectCommit()
	if err := o.UpsertData("users", cols, values, []string{"id"}); err != nil {
		t.Fatalf("UpsertData() error = %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM (SELECT * FROM users) count_query`)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	cnt, err := o.GetRowCount("SELECT * FROM users")
	if err != nil {
		t.Fatalf("GetRowCount() error = %v", err)
	}
	if cnt != 2 {
		t.Fatalf("GetRowCount() = %d, want 2", cnt)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM "USERS"`)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	tableCnt, err := o.GetTableRowCount("users")
	if err != nil {
		t.Fatalf("GetTableRowCount() error = %v", err)
	}
	if tableCnt != 2 {
		t.Fatalf("GetTableRowCount() = %d, want 2", tableCnt)
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1")).
		WillReturnRows(sqlmock.NewRows([]string{"one"}).AddRow(1))
	rows, err := o.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	rows.Close()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestOracleCreateIndexesAndHelpers(t *testing.T) {
	db, mock := newSQLMock(t)
	o := &OracleDB{db: db}

	mock.ExpectExec(regexp.QuoteMeta(`BEGIN EXECUTE IMMEDIATE 'DROP INDEX "IDX_USERS_NAME"'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -1418 AND SQLCODE != -942 THEN RAISE; END IF; END;`)).
		WillReturnError(errors.New("ignore"))
	mock.ExpectExec(regexp.QuoteMeta(`CREATE UNIQUE INDEX "IDX_USERS_NAME" ON "USERS" ("NAME" DESC)`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := o.CreateIndexes("users", []config.IndexConfig{
		{Name: "idx_users_name", Columns: []string{"name:DESC"}, Unique: true},
	})
	if err != nil {
		t.Fatalf("CreateIndexes() error = %v", err)
	}

	if got := o.ident(`a"b`); got != `"A""B"` {
		t.Fatalf("ident() = %s", got)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestOraclePingAndExec(t *testing.T) {
	db, mock := newSQLMock(t)
	o := &OracleDB{db: db}

	mock.ExpectPing()
	if err := o.Ping(); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta(`CREATE TABLE t (id INT)`)).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := o.Exec(`CREATE TABLE t (id INT)`); err != nil {
		t.Fatalf("Exec() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestOracleEdgeCasesAndTypeMapping(t *testing.T) {
	o := &OracleDB{}
	if err := o.CreateTable("users", nil); err == nil {
		t.Fatalf("expected CreateTable() error for empty columns")
	}
	if err := o.InsertData("users", nil, nil); err != nil {
		t.Fatalf("InsertData() empty should be nil, got %v", err)
	}
	if err := o.UpsertData("users", nil, [][]any{{1}}, nil); err == nil {
		t.Fatalf("expected merge_keys required error")
	}

	cases := []struct {
		meta ColumnMetadata
		want string
	}{
		{ColumnMetadata{DatabaseType: "CHAR", LengthValid: true, Length: 20}, "VARCHAR2(20)"},
		{ColumnMetadata{DatabaseType: "CLOB"}, "CLOB"},
		{ColumnMetadata{DatabaseType: "DATE"}, "TIMESTAMP"},
		{ColumnMetadata{DatabaseType: "BLOB"}, "BLOB"},
		{ColumnMetadata{DatabaseType: "DECIMAL", PrecisionScaleValid: true, Precision: 10, Scale: 2}, "NUMBER(10,2)"},
		{ColumnMetadata{DatabaseType: "DOUBLE"}, "BINARY_DOUBLE"},
		{ColumnMetadata{DatabaseType: "BOOL"}, "NUMBER(19,0)"},
		{ColumnMetadata{DatabaseType: "X"}, "CLOB"},
	}
	for _, tc := range cases {
		if got := o.mapToOracleType(tc.meta); got != tc.want {
			t.Fatalf("mapToOracleType(%+v) = %s, want %s", tc.meta, got, tc.want)
		}
	}
}
