package processor

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"db-ferry/config"
	"db-ferry/database"

	_ "github.com/mattn/go-sqlite3"
)

type retryTarget struct {
	insertErrs []error
	upsertErrs []error
	insertCall int
	upsertCall int
}

func (m *retryTarget) Close() error { return nil }

func (m *retryTarget) CreateTable(string, []database.ColumnMetadata) error { return nil }

func (m *retryTarget) EnsureTable(string, []database.ColumnMetadata) error { return nil }

func (m *retryTarget) InsertData(string, []database.ColumnMetadata, [][]any) error {
	if m.insertCall < len(m.insertErrs) {
		err := m.insertErrs[m.insertCall]
		m.insertCall++
		return err
	}
	m.insertCall++
	return nil
}

func (m *retryTarget) UpsertData(string, []database.ColumnMetadata, [][]any, []string) error {
	if m.upsertCall < len(m.upsertErrs) {
		err := m.upsertErrs[m.upsertCall]
		m.upsertCall++
		return err
	}
	m.upsertCall++
	return nil
}

func (m *retryTarget) GetTableRowCount(string) (int, error) { return 0, nil }

func (m *retryTarget) Query(string) (*sql.Rows, error) { return nil, nil }

func (m *retryTarget) CreateIndexes(string, []config.IndexConfig) error { return nil }

func (m *retryTarget) Exec(string) error { return nil }

func (m *retryTarget) GetTableColumns(string) ([]database.ColumnMetadata, error) { return nil, nil }

func TestProcessorHelpers(t *testing.T) {
	if got := trimSQL(" SELECT 1;; "); got != "SELECT 1" {
		t.Fatalf("trimSQL() = %q, want %q", got, "SELECT 1")
	}

	dataSQL, countSQL := buildTaskSQL("SELECT * FROM t;", "id", "10")
	if !strings.Contains(dataSQL, "WHERE id > 10") || !strings.Contains(dataSQL, "ORDER BY id") {
		t.Fatalf("unexpected data SQL: %s", dataSQL)
	}
	if strings.Contains(countSQL, "ORDER BY") {
		t.Fatalf("count SQL should not contain ORDER BY: %s", countSQL)
	}

	if got := quoteSQLString("o'h"); got != "'o''h'" {
		t.Fatalf("quoteSQLString() = %s", got)
	}

	lit, err := formatResumeLiteral(true)
	if err != nil || lit != "1" {
		t.Fatalf("formatResumeLiteral(true) = %q, %v", lit, err)
	}
	lit, err = formatResumeLiteral("abc")
	if err != nil || lit != "'abc'" {
		t.Fatalf("formatResumeLiteral(string) = %q, %v", lit, err)
	}
	lit, err = formatResumeLiteral([]byte("bin"))
	if err != nil || lit != "'bin'" {
		t.Fatalf("formatResumeLiteral([]byte) = %q, %v", lit, err)
	}
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	lit, err = formatResumeLiteral(ts)
	if err != nil || lit != "'2026-01-02 03:04:05'" {
		t.Fatalf("formatResumeLiteral(time) = %q, %v", lit, err)
	}

	cols := []database.ColumnMetadata{{Name: "ID"}, {Name: "name"}}
	if idx := findColumnIndex(cols, "id"); idx != 0 {
		t.Fatalf("findColumnIndex() = %d, want 0", idx)
	}
	if _, err := resolveMergeKeys(cols, []string{"missing"}); err == nil {
		t.Fatalf("expected resolveMergeKeys() error for missing key")
	}
	keys, err := resolveMergeKeys(cols, []string{"id", "NAME"})
	if err != nil || len(keys) != 2 || keys[0] != "ID" || keys[1] != "name" {
		t.Fatalf("resolveMergeKeys() = %#v, %v", keys, err)
	}

	if !isTextualColumn(database.ColumnMetadata{DatabaseType: "VARCHAR"}) {
		t.Fatalf("expected textual column for VARCHAR")
	}
	if isTextualColumn(database.ColumnMetadata{DatabaseType: "BLOB"}) {
		t.Fatalf("did not expect textual column for BLOB")
	}
}

func TestFormatResumeLiteralNumericCases(t *testing.T) {
	cases := []struct {
		value any
		want  string
	}{
		{int8(1), "1"},
		{int16(2), "2"},
		{int32(3), "3"},
		{int64(4), "4"},
		{uint(5), "5"},
		{uint8(6), "6"},
		{uint16(7), "7"},
		{uint32(8), "8"},
		{uint64(9), "9"},
		{float32(1.5), "1.5"},
		{float64(2.5), "2.5"},
		{false, "0"},
		{struct{ A int }{A: 1}, "'{1}'"},
	}

	for _, tc := range cases {
		got, err := formatResumeLiteral(tc.value)
		if err != nil {
			t.Fatalf("formatResumeLiteral(%v) error = %v", tc.value, err)
		}
		if got != tc.want {
			t.Fatalf("formatResumeLiteral(%v) = %s, want %s", tc.value, got, tc.want)
		}
	}
}

func TestStateFileLoadSaveAndCorruption(t *testing.T) {
	p := &Processor{stateFiles: make(map[string]*stateFile)}
	path := filepath.Join(t.TempDir(), "state", "resume.json")

	state := &stateFile{Tasks: map[string]string{"k": "1"}}
	if err := p.saveStateFile(path, state); err != nil {
		t.Fatalf("saveStateFile() error = %v", err)
	}

	loaded, err := p.loadStateFile(path)
	if err != nil {
		t.Fatalf("loadStateFile() error = %v", err)
	}
	if loaded.Tasks["k"] != "1" {
		t.Fatalf("unexpected loaded state: %#v", loaded.Tasks)
	}

	loadedAgain, err := p.loadStateFile(path)
	if err != nil {
		t.Fatalf("loadStateFile() 2nd error = %v", err)
	}
	if loadedAgain != loaded {
		t.Fatalf("expected cached state instance")
	}

	badPath := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(badPath, []byte("{"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := p.loadStateFile(badPath); err == nil {
		t.Fatalf("expected parse error for bad state file")
	}

	taskKey := p.taskKey(config.TaskConfig{SourceDB: "s", TargetDB: "t", TableName: "x"})
	if taskKey != "s:t:x" {
		t.Fatalf("unexpected task key: %s", taskKey)
	}
}

func TestResolveResumeLiteralAndUpdateResumeState(t *testing.T) {
	p := &Processor{stateFiles: make(map[string]*stateFile)}

	literal, err := p.resolveResumeLiteral(config.TaskConfig{})
	if err != nil || literal != "" {
		t.Fatalf("resolveResumeLiteral() without resume key = %q, %v", literal, err)
	}

	task := config.TaskConfig{
		TableName:  "users",
		SourceDB:   "src",
		TargetDB:   "dst",
		ResumeKey:  "id",
		ResumeFrom: "100",
	}
	literal, err = p.resolveResumeLiteral(task)
	if err != nil || literal != "100" {
		t.Fatalf("resolveResumeLiteral() fallback = %q, %v", literal, err)
	}

	task.StateFile = filepath.Join(t.TempDir(), "resume.json")
	if err := p.updateResumeState(task, int64(123)); err != nil {
		t.Fatalf("updateResumeState() error = %v", err)
	}
	literal, err = p.resolveResumeLiteral(task)
	if err != nil || literal != "123" {
		t.Fatalf("resolveResumeLiteral() state value = %q, %v", literal, err)
	}

	if err := p.updateResumeState(task, nil); err == nil {
		t.Fatalf("expected nil resume value error")
	}
}

func TestExtractColumnMetadataAndScanRow(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "scan.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE src (id INTEGER, name TEXT, payload BLOB)`); err != nil {
		t.Fatalf("create table error = %v", err)
	}
	if _, err := db.Exec(`INSERT INTO src(id, name, payload) VALUES (1, 'alice', x'616263')`); err != nil {
		t.Fatalf("insert row error = %v", err)
	}

	rows, err := db.Query(`SELECT id, name, payload FROM src`)
	if err != nil {
		t.Fatalf("query error = %v", err)
	}
	defer rows.Close()

	p := &Processor{}
	cols, err := p.extractColumnMetadata(rows)
	if err != nil {
		t.Fatalf("extractColumnMetadata() error = %v", err)
	}
	if len(cols) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(cols))
	}

	if !rows.Next() {
		t.Fatalf("expected at least one row")
	}
	values, err := p.scanRow(rows, cols)
	if err != nil {
		t.Fatalf("scanRow() error = %v", err)
	}
	if values[1] != "alice" {
		t.Fatalf("expected text column conversion, got %#v", values[1])
	}
	payload, ok := values[2].([]byte)
	if !ok || string(payload) != "abc" {
		t.Fatalf("expected blob bytes, got %#v", values[2])
	}
}

func TestInsertBatchWithRetry(t *testing.T) {
	origSleep := sleepFn
	sleepCalls := 0
	sleepFn = func(time.Duration) { sleepCalls++ }
	t.Cleanup(func() { sleepFn = origSleep })

	p := &Processor{}
	target := &retryTarget{
		insertErrs: []error{errors.New("first"), nil},
	}

	dlqCount, err := p.insertBatchWithRetry(
		target,
		config.TaskConfig{Mode: config.TaskModeReplace, MaxRetries: 2, TableName: "t"},
		[]database.ColumnMetadata{{Name: "id"}},
		[][]any{{1}},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("insertBatchWithRetry() error = %v", err)
	}
	if dlqCount != 0 {
		t.Fatalf("expected 0 DLQ rows, got %d", dlqCount)
	}
	if target.insertCall != 2 {
		t.Fatalf("expected 2 insert attempts, got %d", target.insertCall)
	}
	if sleepCalls != 1 {
		t.Fatalf("expected 1 sleep call, got %d", sleepCalls)
	}

	mergeTarget := &retryTarget{
		upsertErrs: []error{errors.New("fail"), errors.New("fail")},
	}
	dlqCount, err = p.insertBatchWithRetry(
		mergeTarget,
		config.TaskConfig{Mode: config.TaskModeMerge, MaxRetries: 1, TableName: "t"},
		[]database.ColumnMetadata{{Name: "id"}},
		[][]any{{1}},
		[]string{"id"},
		nil,
	)
	if err == nil {
		t.Fatalf("expected retry exhaustion error")
	}
	if dlqCount != 0 {
		t.Fatalf("expected 0 DLQ rows without DLQ path, got %d", dlqCount)
	}
	if mergeTarget.upsertCall != 2 {
		t.Fatalf("expected 2 upsert attempts, got %d", mergeTarget.upsertCall)
	}
}

type selectiveTarget struct {
	insertCall int
	upsertCall int
	failBatch  bool
	failKeys   map[any]struct{}
}

func (m *selectiveTarget) Close() error                                        { return nil }
func (m *selectiveTarget) CreateTable(string, []database.ColumnMetadata) error { return nil }
func (m *selectiveTarget) EnsureTable(string, []database.ColumnMetadata) error { return nil }
func (m *selectiveTarget) GetTableColumns(string) ([]database.ColumnMetadata, error) { return nil, nil }
func (m *selectiveTarget) GetTableRowCount(string) (int, error)                { return 0, nil }
func (m *selectiveTarget) CreateIndexes(string, []config.IndexConfig) error    { return nil }
func (m *selectiveTarget) Query(string) (*sql.Rows, error)                     { return nil, nil }

func (m *selectiveTarget) InsertData(string, []database.ColumnMetadata, [][]any) error {
	m.insertCall++
	return nil
}

func (m *selectiveTarget) UpsertData(string, []database.ColumnMetadata, [][]any, []string) error {
	m.upsertCall++
	return nil
}

type failingBatchTarget struct {
	selectiveTarget
}

func (m *failingBatchTarget) InsertData(_ string, _ []database.ColumnMetadata, values [][]any) error {
	m.insertCall++
	if len(values) > 1 || m.failBatch {
		m.failBatch = false
		return errors.New("batch fail")
	}
	if len(values) == 1 && len(values[0]) > 0 {
		if _, ok := m.failKeys[values[0][0]]; ok {
			return errors.New("row fail")
		}
	}
	return nil
}

func (m *failingBatchTarget) Exec(string) error { return nil }

func TestInsertBatchWithRetryDLQ(t *testing.T) {
	origSleep := sleepFn
	sleepFn = func(time.Duration) {}
	t.Cleanup(func() { sleepFn = origSleep })

	dir := t.TempDir()
	dlqPath := filepath.Join(dir, "dlq.jsonl")

	dlqw, err := newDLQWriter(dlqPath, config.DLQFormatJSONL, []database.ColumnMetadata{{Name: "id"}, {Name: "name"}})
	if err != nil {
		t.Fatalf("newDLQWriter() error = %v", err)
	}
	defer dlqw.close()

	p := &Processor{}
	target := &failingBatchTarget{}
	target.failBatch = true
	target.failKeys = map[any]struct{}{2: {}}

	dlqCount, err := p.insertBatchWithRetry(
		target,
		config.TaskConfig{Mode: config.TaskModeReplace, MaxRetries: 1, TableName: "t"},
		[]database.ColumnMetadata{{Name: "id"}, {Name: "name"}},
		[][]any{{1, "a"}, {2, "b"}, {3, "c"}},
		nil,
		dlqw,
	)
	if err != nil {
		t.Fatalf("insertBatchWithRetry() error = %v", err)
	}
	if dlqCount != 1 {
		t.Fatalf("expected 1 DLQ row, got %d", dlqCount)
	}

	data, err := os.ReadFile(dlqPath)
	if err != nil {
		t.Fatalf("ReadFile(DLQ) error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 DLQ line, got %d", len(lines))
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("unmarshal DLQ line error = %v", err)
	}
	if entry["error"] != "row fail" {
		t.Fatalf("unexpected DLQ error field: %v", entry["error"])
	}
	if entry["table_name"] != "t" {
		t.Fatalf("unexpected DLQ table_name: %v", entry["table_name"])
	}
}

func TestInsertBatchWithRetryDLQCSV(t *testing.T) {
	origSleep := sleepFn
	sleepFn = func(time.Duration) {}
	t.Cleanup(func() { sleepFn = origSleep })

	dir := t.TempDir()
	dlqPath := filepath.Join(dir, "dlq.csv")

	dlqw, err := newDLQWriter(dlqPath, config.DLQFormatCSV, []database.ColumnMetadata{{Name: "id"}, {Name: "name"}})
	if err != nil {
		t.Fatalf("newDLQWriter() error = %v", err)
	}
	defer dlqw.close()

	p := &Processor{}
	target := &failingBatchTarget{}
	target.failBatch = true
	target.failKeys = map[any]struct{}{"bad": {}}

	dlqCount, err := p.insertBatchWithRetry(
		target,
		config.TaskConfig{Mode: config.TaskModeReplace, MaxRetries: 0, TableName: "users"},
		[]database.ColumnMetadata{{Name: "id"}, {Name: "name"}},
		[][]any{{"ok", "alice"}, {"bad", "bob"}},
		nil,
		dlqw,
	)
	if err != nil {
		t.Fatalf("insertBatchWithRetry() error = %v", err)
	}
	if dlqCount != 1 {
		t.Fatalf("expected 1 DLQ row, got %d", dlqCount)
	}

	data, err := os.ReadFile(dlqPath)
	if err != nil {
		t.Fatalf("ReadFile(DLQ) error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 CSV lines (header + 1 row), got %d", len(lines))
	}
	if !strings.Contains(lines[0], "id") || !strings.Contains(lines[0], "_dlq_error") {
		t.Fatalf("unexpected CSV header: %s", lines[0])
	}
	if !strings.Contains(lines[1], "bad") || !strings.Contains(lines[1], "bob") {
		t.Fatalf("unexpected CSV data line: %s", lines[1])
	}
}

func TestProcessTaskWithDLQ(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")
	dlqPath := filepath.Join(dir, "dlq", "failed.jsonl")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_users (id INTEGER, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_users(id, name) VALUES (1, 'a'), (1, 'b_duplicate'), (2, 'c')`)

	setupSQLiteSource(t, targetPath, `CREATE TABLE dst_users (id INTEGER PRIMARY KEY, name TEXT)`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName:       "dst_users",
				SQL:             "SELECT id, name FROM src_users",
				SourceDB:        "src",
				TargetDB:        "dst",
				Mode:            config.TaskModeAppend,
				BatchSize:       2,
				SkipCreateTable: true,
				DLQPath:         dlqPath,
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	if err := p.processTask(cfg.Tasks[0]); err != nil {
		t.Fatalf("processTask() error = %v", err)
	}

	targetDB, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer targetDB.Close()

	var count int
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM "dst_users"`).Scan(&count); err != nil {
		t.Fatalf("query target count error = %v", err)
	}
	if count != 2 {
		t.Fatalf("target row count = %d, want 2", count)
	}

	data, err := os.ReadFile(dlqPath)
	if err != nil {
		t.Fatalf("ReadFile(DLQ) error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 DLQ line, got %d", len(lines))
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("unmarshal DLQ line error = %v", err)
	}
	row, ok := entry["row"].([]any)
	if !ok || len(row) != 2 {
		t.Fatalf("unexpected DLQ row format: %v", entry["row"])
	}
	if fmt.Sprint(row[1]) != "b_duplicate" {
		t.Fatalf("expected DLQ row name 'b_duplicate', got %v", row[1])
	}
}

func TestProcessTaskReplaceAndStateFile(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")
	statePath := filepath.Join(dir, "state", "resume.json")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_users (id INTEGER PRIMARY KEY, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_users(id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName: "dst_users",
				SQL:       "SELECT id, name FROM src_users",
				SourceDB:  "src",
				TargetDB:  "dst",
				Mode:      config.TaskModeReplace,
				BatchSize: 2,
				ResumeKey: "id",
				StateFile: statePath,
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	if err := p.processTask(cfg.Tasks[0]); err != nil {
		t.Fatalf("processTask() error = %v", err)
	}

	targetDB, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer targetDB.Close()

	var count int
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM "dst_users"`).Scan(&count); err != nil {
		t.Fatalf("query target count error = %v", err)
	}
	if count != 3 {
		t.Fatalf("target row count = %d, want 3", count)
	}

	data, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("ReadFile(state) error = %v", err)
	}
	var state stateFile
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("unmarshal state error = %v", err)
	}
	if got := state.Tasks["src:dst:dst_users"]; got != "3" {
		t.Fatalf("unexpected resume value: %q", got)
	}
}

func TestProcessTaskMergeRowCountValidationFails(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_users (id INTEGER, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_users(id, name) VALUES (1, 'new'), (2, 'b')`)

	setupSQLiteSource(t, targetPath, `CREATE TABLE dst_users (id INTEGER PRIMARY KEY, name TEXT)`)
	setupSQLiteExec(t, targetPath, `INSERT INTO dst_users(id, name) VALUES (1, 'old')`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName:       "dst_users",
				SQL:             "SELECT id, name FROM src_users",
				SourceDB:        "src",
				TargetDB:        "dst",
				Mode:            config.TaskModeMerge,
				MergeKeys:       []string{"id"},
				Validate:        config.TaskValidateRowCount,
				SkipCreateTable: true,
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	// Merge processed 2 rows but only 1 net new row was inserted.
	err := p.processTask(cfg.Tasks[0])
	if err == nil || !strings.Contains(err.Error(), "row count validation failed") {
		t.Fatalf("expected row count validation failure, got %v", err)
	}

	targetDB, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer targetDB.Close()

	var count int
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM "dst_users"`).Scan(&count); err != nil {
		t.Fatalf("query row count error = %v", err)
	}
	if count != 2 {
		t.Fatalf("merge row count = %d, want 2", count)
	}
}

func TestProcessTaskAppendWithValidationAndIndexes(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_users (id INTEGER, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_users(id, name) VALUES (2, 'b'), (3, 'c')`)

	setupSQLiteSource(t, targetPath, `CREATE TABLE dst_users (id INTEGER, name TEXT)`)
	setupSQLiteExec(t, targetPath, `INSERT INTO dst_users(id, name) VALUES (1, 'a')`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName: "dst_users",
				SQL:       "SELECT id, name FROM src_users",
				SourceDB:  "src",
				TargetDB:  "dst",
				Mode:      config.TaskModeAppend,
				Validate:  config.TaskValidateRowCount,
				Indexes: []config.IndexConfig{
					{Name: "idx_dst_users_name", Columns: []string{"name:ASC"}},
				},
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	if err := p.processTask(cfg.Tasks[0]); err != nil {
		t.Fatalf("processTask() append error = %v", err)
	}

	targetDB, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer targetDB.Close()

	var count int
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM "dst_users"`).Scan(&count); err != nil {
		t.Fatalf("query row count error = %v", err)
	}
	if count != 3 {
		t.Fatalf("append row count = %d, want 3", count)
	}

	var indexExists int
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_dst_users_name'`).Scan(&indexExists); err != nil {
		t.Fatalf("query index existence error = %v", err)
	}
	if indexExists != 1 {
		t.Fatalf("expected index idx_dst_users_name to exist")
	}
}

func TestProcessTaskChecksumValidation(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_users (id INTEGER, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_users(id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName: "dst_users",
				SQL:       "SELECT id, name FROM src_users",
				SourceDB:  "src",
				TargetDB:  "dst",
				Mode:      config.TaskModeReplace,
				Validate:  config.TaskValidateChecksum,
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	if err := p.processTask(cfg.Tasks[0]); err != nil {
		t.Fatalf("processTask() checksum error = %v", err)
	}

	targetDB, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer targetDB.Close()

	var count int
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM "dst_users"`).Scan(&count); err != nil {
		t.Fatalf("query target count error = %v", err)
	}
	if count != 3 {
		t.Fatalf("target row count = %d, want 3", count)
	}
}

func TestProcessTaskSampleValidation(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_users (id INTEGER, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_users(id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName:          "dst_users",
				SQL:                "SELECT id, name FROM src_users",
				SourceDB:           "src",
				TargetDB:           "dst",
				Mode:               config.TaskModeReplace,
				Validate:           config.TaskValidateSample,
				ValidateSampleSize: 2,
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	if err := p.processTask(cfg.Tasks[0]); err != nil {
		t.Fatalf("processTask() sample error = %v", err)
	}

	targetDB, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer targetDB.Close()

	var count int
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM "dst_users"`).Scan(&count); err != nil {
		t.Fatalf("query target count error = %v", err)
	}
	if count != 3 {
		t.Fatalf("target row count = %d, want 3", count)
	}
}

func TestProcessTaskMissingResumeColumnFailsAtQuery(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_users (id INTEGER, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_users(id, name) VALUES (1, 'a')`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName:  "dst_users",
				SQL:        "SELECT name FROM src_users",
				SourceDB:   "src",
				TargetDB:   "dst",
				ResumeKey:  "id",
				ResumeFrom: "0",
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	err := p.processTask(cfg.Tasks[0])
	if err == nil || !strings.Contains(err.Error(), "no such column: id") {
		t.Fatalf("expected query error for missing resume column, got %v", err)
	}
}

func TestProcessAllTasksSkipsIgnored(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_users (id INTEGER, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_users(id, name) VALUES (1, 'a')`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName: "run_table",
				SQL:       "SELECT id, name FROM src_users",
				SourceDB:  "src",
				TargetDB:  "dst",
			},
			{
				TableName: "ignored_table",
				SQL:       "SELECT id, name FROM src_users",
				SourceDB:  "src",
				TargetDB:  "dst",
				Ignore:    true,
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	if err := p.ProcessAllTasks(); err != nil {
		t.Fatalf("ProcessAllTasks() error = %v", err)
	}

	targetDB, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer targetDB.Close()

	var count int
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM "run_table"`).Scan(&count); err != nil {
		t.Fatalf("query run_table count error = %v", err)
	}
	if count != 1 {
		t.Fatalf("run_table row count = %d, want 1", count)
	}

	var ignoredExists int
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='ignored_table'`).Scan(&ignoredExists); err != nil {
		t.Fatalf("query ignored table existence error = %v", err)
	}
	if ignoredExists != 0 {
		t.Fatalf("ignored table should not be created")
	}
}

func setupSQLiteSource(t *testing.T, dbPath, createSQL string) {
	t.Helper()
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite error = %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(createSQL); err != nil {
		t.Fatalf("create table error = %v", err)
	}
}

func setupSQLiteExec(t *testing.T, dbPath, sqlText string) {
	t.Helper()
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite error = %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(sqlText); err != nil {
		t.Fatalf("exec error = %v", err)
	}
}

func TestPlanAllTasks(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_users (id INTEGER PRIMARY KEY, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_users(id, name) VALUES (1, 'alice'), (2, 'bob')`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeMySQL, Host: "localhost", User: "u", Password: "p", Database: "db"},
		},
		Tasks: []config.TaskConfig{
			{
				TableName: "dst_users",
				SQL:       "SELECT id, name FROM src_users",
				SourceDB:  "src",
				TargetDB:  "dst",
				Mode:      config.TaskModeReplace,
				BatchSize: 500,
				Indexes: []config.IndexConfig{
					{Name: "idx_name", Columns: []string{"name"}},
				},
			},
			{
				TableName: "ignored_table",
				SQL:       "SELECT id FROM src_users",
				SourceDB:  "src",
				TargetDB:  "dst",
				Ignore:    true,
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	var buf bytes.Buffer
	if err := p.PlanAllTasks(&buf); err != nil {
		t.Fatalf("PlanAllTasks() error = %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "[PLAN] Task 1/1: dst_users") {
		t.Fatalf("expected plan header, got:\n%s", out)
	}
	if !strings.Contains(out, "Source:  src  →  Target:  dst") {
		t.Fatalf("expected source/target line, got:\n%s", out)
	}
	if !strings.Contains(out, "Mode:    replace") {
		t.Fatalf("expected mode line, got:\n%s", out)
	}
	if !strings.Contains(out, "DROP TABLE IF EXISTS") {
		t.Fatalf("expected DROP TABLE in DDL, got:\n%s", out)
	}
	if !strings.Contains(out, "CREATE TABLE") {
		t.Fatalf("expected CREATE TABLE in DDL, got:\n%s", out)
	}
	if !strings.Contains(out, "CREATE INDEX") {
		t.Fatalf("expected CREATE INDEX in DDL, got:\n%s", out)
	}
	if !strings.Contains(out, "Rows:    ~2") {
		t.Fatalf("expected row count, got:\n%s", out)
	}
	if !strings.Contains(out, "Batch:   500") {
		t.Fatalf("expected batch size, got:\n%s", out)
	}
	if strings.Contains(out, "ignored_table") {
		t.Fatalf("ignored task should not appear in plan")
	}
}

func TestPlanAllTasksAppendMode(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_users (id INTEGER PRIMARY KEY)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_users(id) VALUES (1)`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypePostgreSQL, Host: "localhost", User: "u", Password: "p", Database: "db"},
		},
		Tasks: []config.TaskConfig{
			{
				TableName:       "dst_users",
				SQL:             "SELECT id FROM src_users",
				SourceDB:        "src",
				TargetDB:        "dst",
				Mode:            config.TaskModeAppend,
				SkipCreateTable: true,
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	var buf bytes.Buffer
	if err := p.PlanAllTasks(&buf); err != nil {
		t.Fatalf("PlanAllTasks() error = %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "Mode:    append") {
		t.Fatalf("expected append mode, got:\n%s", out)
	}
	if !strings.Contains(out, "DDL:     (none)") {
		t.Fatalf("expected no DDL when skip_create_table=true, got:\n%s", out)
	}
	if !strings.Contains(out, "Rows:    ~1") {
		t.Fatalf("expected row count, got:\n%s", out)
	}
}

func TestPlanAllTasksResumeKey(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	statePath := filepath.Join(dir, "state", "resume.json")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_users (id INTEGER PRIMARY KEY, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_users(id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: filepath.Join(dir, "target.db")},
		},
		Tasks: []config.TaskConfig{
			{
				TableName:  "dst_users",
				SQL:        "SELECT id, name FROM src_users",
				SourceDB:   "src",
				TargetDB:   "dst",
				Mode:       config.TaskModeReplace,
				ResumeKey:  "id",
				ResumeFrom: "1",
				StateFile:  statePath,
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	var buf bytes.Buffer
	if err := p.PlanAllTasks(&buf); err != nil {
		t.Fatalf("PlanAllTasks() error = %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "Rows:    ~2") {
		t.Fatalf("expected 2 rows with resume from 1, got:\n%s", out)
	}
}

func TestPlanAllTasksMissingResumeColumn(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_users (name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_users(name) VALUES ('a')`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: filepath.Join(dir, "target.db")},
		},
		Tasks: []config.TaskConfig{
			{
				TableName:  "dst_users",
				SQL:        "SELECT name FROM src_users",
				SourceDB:   "src",
				TargetDB:   "dst",
				ResumeKey:  "id",
				ResumeFrom: "0",
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	var buf bytes.Buffer
	err := p.PlanAllTasks(&buf)
	if err == nil || !strings.Contains(err.Error(), "no such column: id") {
		t.Fatalf("expected query error for missing resume column, got %v", err)
	}
}

func TestFormatNumber(t *testing.T) {
	cases := []struct {
		n    int
		want string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1,000"},
		{1234567, "1,234,567"},
	}
	for _, tc := range cases {
		got := formatNumber(tc.n)
		if got != tc.want {
			t.Fatalf("formatNumber(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

func TestProcessTaskPreSQLAndPostSQL(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_users (id INTEGER, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_users(id, name) VALUES (1, 'a'), (2, 'b')`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName: "dst_users",
				SQL:       "SELECT id, name FROM src_users",
				SourceDB:  "src",
				TargetDB:  "dst",
				Mode:      config.TaskModeReplace,
				PreSQL: []string{
					"CREATE TABLE pre_log (msg TEXT)",
					"INSERT INTO pre_log(msg) VALUES ('pre_hook_ran')",
				},
				PostSQL: []string{
					"CREATE TABLE post_log (msg TEXT)",
					"INSERT INTO post_log(msg) VALUES ('post_hook_ran')",
				},
				Indexes: []config.IndexConfig{
					{Name: "idx_dst_users_name", Columns: []string{"name"}},
				},
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	if err := p.processTask(cfg.Tasks[0]); err != nil {
		t.Fatalf("processTask() error = %v", err)
	}

	targetDB, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer targetDB.Close()

	var count int
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM "dst_users"`).Scan(&count); err != nil {
		t.Fatalf("query dst_users count error = %v", err)
	}
	if count != 2 {
		t.Fatalf("dst_users row count = %d, want 2", count)
	}

	var preMsg string
	if err := targetDB.QueryRow(`SELECT msg FROM pre_log`).Scan(&preMsg); err != nil {
		t.Fatalf("query pre_log error = %v", err)
	}
	if preMsg != "pre_hook_ran" {
		t.Fatalf("pre_log msg = %q, want 'pre_hook_ran'", preMsg)
	}

	var postMsg string
	if err := targetDB.QueryRow(`SELECT msg FROM post_log`).Scan(&postMsg); err != nil {
		t.Fatalf("query post_log error = %v", err)
	}
	if postMsg != "post_hook_ran" {
		t.Fatalf("post_log msg = %q, want 'post_hook_ran'", postMsg)
	}

	var indexExists int
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_dst_users_name'`).Scan(&indexExists); err != nil {
		t.Fatalf("query index existence error = %v", err)
	}
	if indexExists != 1 {
		t.Fatalf("expected index idx_dst_users_name to exist")
	}
}

func TestProcessTaskPreSQLFailure(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_users (id INTEGER, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_users(id, name) VALUES (1, 'a')`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName: "dst_users",
				SQL:       "SELECT id, name FROM src_users",
				SourceDB:  "src",
				TargetDB:  "dst",
				Mode:      config.TaskModeReplace,
				PreSQL:    []string{"INVALID SQL STATEMENT"},
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	err := p.processTask(cfg.Tasks[0])
	if err == nil || !strings.Contains(err.Error(), "pre_sql hook failed") {
		t.Fatalf("expected pre_sql hook failure, got %v", err)
	}

	targetDB, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer targetDB.Close()

	var count int
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM "dst_users"`).Scan(&count); err != nil {
		t.Fatalf("query dst_users count error = %v", err)
	}
	if count != 0 {
		t.Fatalf("dst_users should be empty after pre_sql failure, got %d", count)
	}
}

func TestProcessTaskPostSQLFailure(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_users (id INTEGER, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_users(id, name) VALUES (1, 'a')`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName: "dst_users",
				SQL:       "SELECT id, name FROM src_users",
				SourceDB:  "src",
				TargetDB:  "dst",
				Mode:      config.TaskModeReplace,
				PostSQL:   []string{"INVALID SQL STATEMENT"},
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	err := p.processTask(cfg.Tasks[0])
	if err == nil || !strings.Contains(err.Error(), "post_sql hook failed") {
		t.Fatalf("expected post_sql hook failure, got %v", err)
	}

	targetDB, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer targetDB.Close()

	var count int
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM "dst_users"`).Scan(&count); err != nil {
		t.Fatalf("query dst_users count error = %v", err)
	}
	if count != 1 {
		t.Fatalf("dst_users should contain inserted data despite post_sql failure, got %d", count)
	}
}

func TestProcessAllTasksConcurrent(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_a (id INTEGER, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_a(id, name) VALUES (1, 'a1'), (2, 'a2')`)
	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_b (id INTEGER, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_b(id, name) VALUES (10, 'b1'), (20, 'b2')`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName: "dst_a",
				SQL:       "SELECT id, name FROM src_a",
				SourceDB:  "src",
				TargetDB:  "dst",
			},
			{
				TableName: "dst_b",
				SQL:       "SELECT id, name FROM src_b",
				SourceDB:  "src",
				TargetDB:  "dst",
			},
		},
		MaxConcurrentTasks: 2,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	if err := p.ProcessAllTasks(); err != nil {
		t.Fatalf("ProcessAllTasks() error = %v", err)
	}

	targetDB, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer targetDB.Close()

	var countA int
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM "dst_a"`).Scan(&countA); err != nil {
		t.Fatalf("query dst_a error = %v", err)
	}
	if countA != 2 {
		t.Fatalf("dst_a row count = %d, want 2", countA)
	}

	var countB int
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM "dst_b"`).Scan(&countB); err != nil {
		t.Fatalf("query dst_b error = %v", err)
	}
	if countB != 2 {
		t.Fatalf("dst_b row count = %d, want 2", countB)
	}
}

func TestProcessAllTasksDependencyChain(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_a (id INTEGER)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_a(id) VALUES (1)`)
	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_b (id INTEGER)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_b(id) VALUES (2)`)
	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_c (id INTEGER)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_c(id) VALUES (3)`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName: "dst_a",
				SQL:       "SELECT id FROM src_a",
				SourceDB:  "src",
				TargetDB:  "dst",
			},
			{
				TableName: "dst_b",
				SQL:       "SELECT id FROM src_b",
				SourceDB:  "src",
				TargetDB:  "dst",
				DependsOn: []string{"dst_a"},
			},
			{
				TableName: "dst_c",
				SQL:       "SELECT id FROM src_c",
				SourceDB:  "src",
				TargetDB:  "dst",
				DependsOn: []string{"dst_b"},
			},
		},
		MaxConcurrentTasks: 2,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	if err := p.ProcessAllTasks(); err != nil {
		t.Fatalf("ProcessAllTasks() error = %v", err)
	}

	targetDB, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer targetDB.Close()

	for _, table := range []string{"dst_a", "dst_b", "dst_c"} {
		var count int
		if err := targetDB.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, table)).Scan(&count); err != nil {
			t.Fatalf("query %s error = %v", table, err)
		}
		if count != 1 {
			t.Fatalf("%s row count = %d, want 1", table, count)
		}
	}
}

func TestProcessAllTasksUpstreamFailure(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_a (id INTEGER)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_a(id) VALUES (1)`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName: "dst_a",
				SQL:       "SELECT bad_column FROM src_a", // will fail
				SourceDB:  "src",
				TargetDB:  "dst",
			},
			{
				TableName: "dst_b",
				SQL:       "SELECT id FROM src_a",
				SourceDB:  "src",
				TargetDB:  "dst",
				DependsOn: []string{"dst_a"},
			},
		},
		MaxConcurrentTasks: 2,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	err := p.ProcessAllTasks()
	if err == nil {
		t.Fatalf("expected ProcessAllTasks error")
	}
	if !strings.Contains(err.Error(), "bad_column") {
		t.Fatalf("expected bad_column error, got %v", err)
	}

	targetDB, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer targetDB.Close()

	var exists int
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='dst_b'`).Scan(&exists); err != nil {
		t.Fatalf("query existence error = %v", err)
	}
	if exists != 0 {
		t.Fatalf("dst_b should not be created because upstream failed")
	}
}

func TestStateFileConcurrency(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")
	statePath := filepath.Join(dir, "state", "resume.json")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_a (id INTEGER PRIMARY KEY, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_a(id, name) VALUES (1, 'a'), (2, 'b')`)
	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_b (id INTEGER PRIMARY KEY, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_b(id, name) VALUES (10, 'x'), (20, 'y')`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName: "dst_a",
				SQL:       "SELECT id, name FROM src_a",
				SourceDB:  "src",
				TargetDB:  "dst",
				BatchSize: 1,
				ResumeKey: "id",
				StateFile: statePath,
			},
			{
				TableName: "dst_b",
				SQL:       "SELECT id, name FROM src_b",
				SourceDB:  "src",
				TargetDB:  "dst",
				BatchSize: 1,
				ResumeKey: "id",
				StateFile: statePath,
			},
		},
		MaxConcurrentTasks: 2,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	if err := p.ProcessAllTasks(); err != nil {
		t.Fatalf("ProcessAllTasks() error = %v", err)
	}

	data, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("ReadFile(state) error = %v", err)
	}
	var state stateFile
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("unmarshal state error = %v", err)
	}

	if got := state.Tasks["src:dst:dst_a"]; got != "2" {
		t.Fatalf("unexpected resume value for dst_a: %q", got)
	}
	if got := state.Tasks["src:dst:dst_b"]; got != "20" {
		t.Fatalf("unexpected resume value for dst_b: %q", got)
	}
}
