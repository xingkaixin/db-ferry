package database

import (
	"testing"

	"db-ferry/config"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestBuildAlterTableAddColumnSQL(t *testing.T) {
	col := ColumnMetadata{Name: "age", DatabaseType: "INTEGER"}

	cases := []struct {
		dbType string
		want   string
	}{
		{config.DatabaseTypeMySQL, "ALTER TABLE `users` ADD COLUMN `age` BIGINT"},
		{config.DatabaseTypePostgreSQL, `ALTER TABLE "users" ADD COLUMN "age" BIGINT`},
		{config.DatabaseTypeSQLite, `ALTER TABLE "users" ADD COLUMN "age" INTEGER`},
		{config.DatabaseTypeOracle, `ALTER TABLE "USERS" ADD "AGE" NUMBER(19,0)`},
		{config.DatabaseTypeSQLServer, `[users] ADD [age] BIGINT`},
		{config.DatabaseTypeDuckDB, `ALTER TABLE "users" ADD COLUMN "age" BIGINT`},
	}

	for _, tc := range cases {
		got := BuildAlterTableAddColumnSQL(tc.dbType, "users", col)
		// SQL Server 前缀与其他不同，用 Contains 判断
		if tc.dbType == config.DatabaseTypeSQLServer {
			if got != "ALTER TABLE [users] ADD [age] BIGINT" {
				t.Fatalf("BuildAlterTableAddColumnSQL(%s) = %q, want ALTER TABLE [users] ADD [age] BIGINT", tc.dbType, got)
			}
			continue
		}
		if got != tc.want {
			t.Fatalf("BuildAlterTableAddColumnSQL(%s) = %q, want %q", tc.dbType, got, tc.want)
		}
	}
}

func TestSyncSchemaAddsMissingColumns(t *testing.T) {
	db, mock := newSQLMock(t)

	// Use PostgresDB as a concrete TargetDB implementation.
	pg := &PostgresDB{db: db}

	// First query: GetTableColumns returns existing columns.
	mock.ExpectQuery("SELECT column_name, data_type FROM information_schema.columns").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "data_type"}).
			AddRow("id", "bigint").
			AddRow("name", "character varying"))

	// Second query: ALTER TABLE for the missing 'age' column.
	mock.ExpectExec(`ALTER TABLE "users" ADD COLUMN "age" BIGINT`).
		WillReturnResult(sqlmock.NewResult(0, 0))

	desired := []ColumnMetadata{
		{Name: "id", DatabaseType: "INTEGER"},
		{Name: "name", DatabaseType: "VARCHAR"},
		{Name: "age", DatabaseType: "INTEGER"},
	}

	if err := SyncSchema(pg, config.DatabaseTypePostgreSQL, "users", desired); err != nil {
		t.Fatalf("SyncSchema() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled expectations: %v", err)
	}
}

func TestSyncSchemaNoMissingColumns(t *testing.T) {
	db, mock := newSQLMock(t)
	pg := &PostgresDB{db: db}

	// All columns already exist (case-insensitive match).
	mock.ExpectQuery("SELECT column_name, data_type FROM information_schema.columns").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "data_type"}).
			AddRow("ID", "bigint").
			AddRow("NAME", "character varying"))

	desired := []ColumnMetadata{
		{Name: "id", DatabaseType: "INTEGER"},
		{Name: "name", DatabaseType: "VARCHAR"},
	}

	if err := SyncSchema(pg, config.DatabaseTypePostgreSQL, "users", desired); err != nil {
		t.Fatalf("SyncSchema() error = %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled expectations: %v", err)
	}
}

func TestSyncSchemaHandlesGetColumnsError(t *testing.T) {
	db, mock := newSQLMock(t)
	pg := &PostgresDB{db: db}

	mock.ExpectQuery("SELECT column_name, data_type FROM information_schema.columns").
		WillReturnError(sqlmock.ErrCancelled)

	if err := SyncSchema(pg, config.DatabaseTypePostgreSQL, "users", []ColumnMetadata{{Name: "id"}}); err == nil {
		t.Fatalf("expected error when GetTableColumns fails")
	}
}

func TestSyncSchemaHandlesAlterError(t *testing.T) {
	db, mock := newSQLMock(t)
	pg := &PostgresDB{db: db}

	mock.ExpectQuery("SELECT column_name, data_type FROM information_schema.columns").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "data_type"}))

	mock.ExpectExec(`ALTER TABLE "users" ADD COLUMN "id" BIGINT`).
		WillReturnError(sqlmock.ErrCancelled)

	if err := SyncSchema(pg, config.DatabaseTypePostgreSQL, "users", []ColumnMetadata{{Name: "id", DatabaseType: "INTEGER"}}); err == nil {
		t.Fatalf("expected error when ALTER TABLE fails")
	}
}
