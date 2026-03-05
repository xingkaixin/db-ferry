package database

import (
	"errors"
	"regexp"
	"testing"

	"db-ferry/config"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSQLServerCreateTableAndEnsureTable(t *testing.T) {
	db, mock := newSQLMock(t)
	s := &SQLServerDB{db: db}
	cols := []ColumnMetadata{
		{Name: "id", DatabaseType: "INT"},
		{Name: "name", DatabaseType: "VARCHAR", LengthValid: true, Length: 20},
	}

	mock.ExpectExec(regexp.QuoteMeta(`IF OBJECT_ID(N'[users]', 'U') IS NOT NULL DROP TABLE [users]`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(`CREATE TABLE [users] ([id] BIGINT, [name] NVARCHAR(20))`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	if err := s.CreateTable("users", cols); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta(`IF OBJECT_ID(N'[users]', 'U') IS NULL CREATE TABLE [users] ([id] BIGINT, [name] NVARCHAR(20))`)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	if err := s.EnsureTable("users", cols); err != nil {
		t.Fatalf("EnsureTable() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestSQLServerInsertUpsertAndCount(t *testing.T) {
	db, mock := newSQLMock(t)
	s := &SQLServerDB{db: db}
	cols := []ColumnMetadata{
		{Name: "id", DatabaseType: "INT"},
		{Name: "name", DatabaseType: "VARCHAR"},
	}
	values := [][]any{{1, "a"}, {2, "b"}}

	insertSQL := `INSERT INTO [users] ([id], [name]) VALUES (@p1, @p2)`
	mock.ExpectBegin()
	insertPrep := mock.ExpectPrepare(regexp.QuoteMeta(insertSQL))
	insertPrep.ExpectExec().WithArgs(1, "a").WillReturnResult(sqlmock.NewResult(1, 1))
	insertPrep.ExpectExec().WithArgs(2, "b").WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectCommit()
	if err := s.InsertData("users", cols, values); err != nil {
		t.Fatalf("InsertData() error = %v", err)
	}

	upsertSQL := `MERGE INTO [users] AS target USING (VALUES (@p1, @p2)) AS source ([id], [name]) ON target.[id]=source.[id] WHEN MATCHED THEN UPDATE SET target.[name]=source.[name] WHEN NOT MATCHED THEN INSERT ([id], [name]) VALUES (source.[id], source.[name]);`
	mock.ExpectBegin()
	upsertPrep := mock.ExpectPrepare(regexp.QuoteMeta(upsertSQL))
	upsertPrep.ExpectExec().WithArgs(1, "a").WillReturnResult(sqlmock.NewResult(1, 1))
	upsertPrep.ExpectExec().WithArgs(2, "b").WillReturnResult(sqlmock.NewResult(2, 1))
	mock.ExpectCommit()
	if err := s.UpsertData("users", cols, values, []string{"id"}); err != nil {
		t.Fatalf("UpsertData() error = %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM (SELECT * FROM users) AS count_query`)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	cnt, err := s.GetRowCount("SELECT * FROM users")
	if err != nil {
		t.Fatalf("GetRowCount() error = %v", err)
	}
	if cnt != 2 {
		t.Fatalf("GetRowCount() = %d, want 2", cnt)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT COUNT(*) FROM [users]`)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))
	tableCnt, err := s.GetTableRowCount("users")
	if err != nil {
		t.Fatalf("GetTableRowCount() error = %v", err)
	}
	if tableCnt != 2 {
		t.Fatalf("GetTableRowCount() = %d, want 2", tableCnt)
	}

	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1")).
		WillReturnRows(sqlmock.NewRows([]string{"one"}).AddRow(1))
	rows, err := s.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	rows.Close()

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestSQLServerCreateIndexesAndHelpers(t *testing.T) {
	db, mock := newSQLMock(t)
	s := &SQLServerDB{db: db}

	mock.ExpectExec(regexp.QuoteMeta(`DROP INDEX IF EXISTS [idx_users_name] ON [users]`)).
		WillReturnError(errors.New("ignore"))
	mock.ExpectExec(regexp.QuoteMeta(`CREATE INDEX [idx_users_name] ON [users] ([name] DESC)`)).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := s.CreateIndexes("users", []config.IndexConfig{
		{Name: "idx_users_name", Columns: []string{"name:DESC"}},
	})
	if err != nil {
		t.Fatalf("CreateIndexes() error = %v", err)
	}

	holders := buildSQLServerPlaceholders(2)
	if len(holders) != 2 || holders[0] != "@p1" || holders[1] != "@p2" {
		t.Fatalf("unexpected placeholders: %#v", holders)
	}
	if got := s.quoteIdentifier(`a]b`); got != "[a]]b]" {
		t.Fatalf("quoteIdentifier() = %s", got)
	}
	if got := s.objectNameLiteral("users"); got != "[users]" {
		t.Fatalf("objectNameLiteral() = %s", got)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestSQLServerEdgeCasesAndTypeMapping(t *testing.T) {
	s := &SQLServerDB{}
	if err := s.CreateTable("users", nil); err == nil {
		t.Fatalf("expected CreateTable() error for empty columns")
	}
	if err := s.InsertData("users", nil, nil); err != nil {
		t.Fatalf("InsertData() empty should be nil, got %v", err)
	}
	if err := s.UpsertData("users", nil, [][]any{{1}}, nil); err == nil {
		t.Fatalf("expected merge_keys required error")
	}

	cases := []struct {
		meta ColumnMetadata
		want string
	}{
		{ColumnMetadata{DatabaseType: "INT"}, "BIGINT"},
		{ColumnMetadata{DatabaseType: "DOUBLE"}, "FLOAT"},
		{ColumnMetadata{DatabaseType: "DECIMAL", PrecisionScaleValid: true, Precision: 10, Scale: 3}, "DECIMAL(10,3)"},
		{ColumnMetadata{DatabaseType: "CHAR", LengthValid: true, Length: 20}, "NVARCHAR(20)"},
		{ColumnMetadata{DatabaseType: "DATE"}, "DATETIME2"},
		{ColumnMetadata{DatabaseType: "BLOB"}, "VARBINARY(MAX)"},
		{ColumnMetadata{DatabaseType: "BOOL"}, "BIT"},
		{ColumnMetadata{DatabaseType: "X"}, "NVARCHAR(MAX)"},
	}
	for _, tc := range cases {
		if got := s.mapToSQLServerType(tc.meta); got != tc.want {
			t.Fatalf("mapToSQLServerType(%+v) = %s, want %s", tc.meta, got, tc.want)
		}
	}
}
