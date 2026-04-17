package database

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"db-ferry/config"
)

func newTestSQLiteTarget(t *testing.T) TargetDB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "history.db")
	s, err := NewSQLiteDB(dbPath, 0, 0, "")
	if err != nil {
		t.Fatalf("NewSQLiteDB() error = %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestHistoryRecorder_EnsureTable(t *testing.T) {
	db := newTestSQLiteTarget(t)
	recorder := NewHistoryRecorder(config.DatabaseTypeSQLite, "test_migrations")

	if err := recorder.EnsureTable(db); err != nil {
		t.Fatalf("EnsureTable failed: %v", err)
	}

	rec := &MigrationRecord{
		ID:         "1",
		ConfigHash: "abc",
		TaskName:   "test_task",
		SourceDB:   "src",
		TargetDB:   "tgt",
		Mode:       "append",
		Version:    "dev",
	}
	_, err := recorder.Start(db, rec)
	if err != nil {
		t.Fatalf("Start after EnsureTable failed: %v", err)
	}
}

func TestHistoryRecorder_StartAndFinish(t *testing.T) {
	db := newTestSQLiteTarget(t)
	recorder := NewHistoryRecorder(config.DatabaseTypeSQLite, "test_migrations")
	_ = recorder.EnsureTable(db)

	rec := &MigrationRecord{
		ConfigHash: "hash123",
		TaskName:   "employees",
		SourceDB:   "oracle_hr",
		TargetDB:   "sqlite_local",
		Mode:       "replace",
		Version:    "1.0.0",
	}

	id, err := recorder.Start(db, rec)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty history ID")
	}
	if rec.StartedAt.IsZero() {
		t.Fatal("expected StartedAt to be set")
	}

	err = recorder.Finish(db, id, 100, 2, "success", "")
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	records, err := recorder.List(db, 10)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	r := records[0]
	if r.TaskName != "employees" {
		t.Errorf("expected task_name employees, got %s", r.TaskName)
	}
	if r.RowsProcessed != 100 {
		t.Errorf("expected rows_processed 100, got %d", r.RowsProcessed)
	}
	if r.RowsFailed != 2 {
		t.Errorf("expected rows_failed 2, got %d", r.RowsFailed)
	}
	if r.ValidationResult != "success" {
		t.Errorf("expected validation_result success, got %s", r.ValidationResult)
	}
	if r.ErrorMessage != "" {
		t.Errorf("expected empty error_message, got %s", r.ErrorMessage)
	}
	if r.FinishedAt == nil || r.FinishedAt.IsZero() {
		t.Error("expected FinishedAt to be set")
	}
}

func TestHistoryRecorder_ListOrderingAndLimit(t *testing.T) {
	db := newTestSQLiteTarget(t)
	recorder := NewHistoryRecorder(config.DatabaseTypeSQLite, "test_migrations")
	_ = recorder.EnsureTable(db)

	for i := 0; i < 5; i++ {
		rec := &MigrationRecord{
			ConfigHash: "hash",
			TaskName:   "task_",
			SourceDB:   "src",
			TargetDB:   "tgt",
			Mode:       "append",
			Version:    "dev",
		}
		_, err := recorder.Start(db, rec)
		if err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		time.Sleep(2 * time.Millisecond)
	}

	records, err := recorder.List(db, 3)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	for i := 1; i < len(records); i++ {
		if !records[i-1].StartedAt.After(records[i].StartedAt) && !records[i-1].StartedAt.Equal(records[i].StartedAt) {
			t.Errorf("records not in descending order at index %d", i)
		}
	}
}

func TestHistoryRecorder_buildCreateTableSQL_CrossDatabase(t *testing.T) {
	tests := []struct {
		dbType string
		want   string
	}{
		{config.DatabaseTypePostgreSQL, "CREATE TABLE IF NOT EXISTS"},
		{config.DatabaseTypeMySQL, "CREATE TABLE IF NOT EXISTS"},
		{config.DatabaseTypeSQLite, "CREATE TABLE IF NOT EXISTS"},
		{config.DatabaseTypeDuckDB, "CREATE TABLE IF NOT EXISTS"},
		{config.DatabaseTypeOracle, "EXECUTE IMMEDIATE"},
		{config.DatabaseTypeSQLServer, "IF OBJECT_ID"},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			r := NewHistoryRecorder(tt.dbType, "history")
			sql := r.buildCreateTableSQL()
			if !strings.Contains(sql, tt.want) {
				t.Errorf("expected SQL to contain %q, got:\n%s", tt.want, sql)
			}
			if !strings.Contains(strings.ToUpper(sql), "HISTORY") {
				t.Errorf("expected SQL to contain table name, got:\n%s", sql)
			}
		})
	}
}

func TestHistoryRecorder_buildListSQL_CrossDatabase(t *testing.T) {
	tests := []struct {
		dbType string
		want   string
	}{
		{config.DatabaseTypeSQLServer, "TOP 10"},
		{config.DatabaseTypeOracle, "ROWNUM <= 10"},
		{config.DatabaseTypePostgreSQL, "LIMIT 10"},
		{config.DatabaseTypeMySQL, "LIMIT 10"},
		{config.DatabaseTypeSQLite, "LIMIT 10"},
		{config.DatabaseTypeDuckDB, "LIMIT 10"},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			r := NewHistoryRecorder(tt.dbType, "history")
			sql := r.buildListSQL(10)
			if !strings.Contains(sql, tt.want) {
				t.Errorf("expected SQL to contain %q, got:\n%s", tt.want, sql)
			}
		})
	}
}

func TestQuoteStringLiteral(t *testing.T) {
	if got := quoteStringLiteral("hello"); got != "'hello'" {
		t.Errorf("quoteStringLiteral(hello) = %s; want 'hello'", got)
	}
	if got := quoteStringLiteral("it's"); got != "'it''s'" {
		t.Errorf("quoteStringLiteral(it's) = %s; want 'it''s'", got)
	}
	if got := quoteStringLiteral("a'b'c"); got != "'a''b''c'" {
		t.Errorf("quoteStringLiteral(a'b'c) = %s; want 'a''b''c'", got)
	}
}
