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

func (m *selectiveTarget) Close() error                                              { return nil }
func (m *selectiveTarget) CreateTable(string, []database.ColumnMetadata) error       { return nil }
func (m *selectiveTarget) EnsureTable(string, []database.ColumnMetadata) error       { return nil }
func (m *selectiveTarget) GetTableColumns(string) ([]database.ColumnMetadata, error) { return nil, nil }
func (m *selectiveTarget) GetTableRowCount(string) (int, error)                      { return 0, nil }
func (m *selectiveTarget) CreateIndexes(string, []config.IndexConfig) error          { return nil }
func (m *selectiveTarget) Query(string) (*sql.Rows, error)                           { return nil, nil }

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

func TestProcessTaskWithAssertions(t *testing.T) {
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
				TableName:       "dst_users",
				SQL:             "SELECT id, name FROM src_users",
				SourceDB:        "src",
				TargetDB:        "dst",
				Mode:            config.TaskModeReplace,
				BatchSize:       2,
				SkipCreateTable: false,
				Assertions: []config.AssertionConfig{
					{Column: "name", Rule: config.AssertionRuleNotNull, OnFail: config.AssertionActionWarn},
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
		t.Fatalf("query target count error = %v", err)
	}
	if count != 2 {
		t.Fatalf("target row count = %d, want 2", count)
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

func TestProcessTaskWithAdaptiveBatch(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_users (id INTEGER PRIMARY KEY, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_users(id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h'), (9, 'i'), (10, 'j')`)

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
				BatchSize: 1,
				AdaptiveBatch: config.AdaptiveBatchConfig{
					Enabled:         true,
					MinSize:         2,
					MaxSize:         8,
					TargetLatencyMs: 1000,
					MemoryLimitMB:   100,
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
		t.Fatalf("query target count error = %v", err)
	}
	if count != 10 {
		t.Fatalf("target row count = %d, want 10", count)
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

func TestApplyColumnMapping(t *testing.T) {
	sourceCols := []database.ColumnMetadata{
		{Name: "id", DatabaseType: "INT"},
		{Name: "name", DatabaseType: "TEXT"},
		{Name: "email", DatabaseType: "TEXT"},
	}

	t.Run("empty mapping returns source columns and sequential indices", func(t *testing.T) {
		cols, indices, err := applyColumnMapping(sourceCols, nil)
		if err != nil {
			t.Fatalf("applyColumnMapping() error = %v", err)
		}
		if len(cols) != 3 || len(indices) != 3 {
			t.Fatalf("expected 3 cols/indices, got %d/%d", len(cols), len(indices))
		}
		if indices[0] != 0 || indices[1] != 1 || indices[2] != 2 {
			t.Fatalf("unexpected indices: %v", indices)
		}
	})

	t.Run("mapping remaps names and order", func(t *testing.T) {
		mappings := []config.ColumnMapping{
			{Source: "email", Target: "mail"},
			{Source: "id", Target: "user_id"},
		}
		cols, indices, err := applyColumnMapping(sourceCols, mappings)
		if err != nil {
			t.Fatalf("applyColumnMapping() error = %v", err)
		}
		if len(cols) != 2 {
			t.Fatalf("expected 2 cols, got %d", len(cols))
		}
		if cols[0].Name != "mail" || cols[1].Name != "user_id" {
			t.Fatalf("unexpected target names: %v", []string{cols[0].Name, cols[1].Name})
		}
		if indices[0] != 2 || indices[1] != 0 {
			t.Fatalf("unexpected indices: %v", indices)
		}
	})

	t.Run("transform is copied to metadata", func(t *testing.T) {
		mappings := []config.ColumnMapping{
			{Source: "name", Target: "user_name", Transform: "UPPER(?)"},
		}
		cols, _, err := applyColumnMapping(sourceCols, mappings)
		if err != nil {
			t.Fatalf("applyColumnMapping() error = %v", err)
		}
		if cols[0].Transform != "UPPER(?)" {
			t.Fatalf("expected transform UPPER(?), got %q", cols[0].Transform)
		}
	})

	t.Run("missing source column errors", func(t *testing.T) {
		mappings := []config.ColumnMapping{{Source: "missing", Target: "x"}}
		_, _, err := applyColumnMapping(sourceCols, mappings)
		if err == nil || !strings.Contains(err.Error(), "not found in query result") {
			t.Fatalf("expected missing source error, got %v", err)
		}
	})
}

func TestRemapRow(t *testing.T) {
	t.Run("sequential indices return same slice", func(t *testing.T) {
		row := []any{1, "a", true}
		got := remapRow(row, []int{0, 1, 2})
		if len(got) != 3 || got[0] != 1 || got[1] != "a" || got[2] != true {
			t.Fatalf("unexpected remapped row: %v", got)
		}
	})

	t.Run("reordered indices remap correctly", func(t *testing.T) {
		row := []any{1, "a", true}
		got := remapRow(row, []int{2, 0})
		if len(got) != 2 || got[0] != true || got[1] != 1 {
			t.Fatalf("unexpected remapped row: %v", got)
		}
	})
}

func TestProcessTaskWithColumnMappingAndTransform(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_users (id INTEGER, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_users(id, name) VALUES (1, 'alice'), (2, 'bob')`)

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
				Columns: []config.ColumnMapping{
					{Source: "name", Target: "user_name"},
					{Source: "id", Target: "user_id"},
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
		t.Fatalf("query target count error = %v", err)
	}
	if count != 2 {
		t.Fatalf("target row count = %d, want 2", count)
	}

	var name string
	var uid int
	if err := targetDB.QueryRow(`SELECT user_name, user_id FROM "dst_users" WHERE user_id = 1`).Scan(&name, &uid); err != nil {
		t.Fatalf("query mapped columns error = %v", err)
	}
	if name != "alice" || uid != 1 {
		t.Fatalf("unexpected mapped data: name=%q, id=%d", name, uid)
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

func TestProcessTaskWithMasking(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_users (id INTEGER, phone TEXT, email TEXT, balance REAL)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_users(id, phone, email, balance) VALUES (1, '13800138000', 'alice@example.com', 9999.99)`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName: "dst_users",
				SQL:       "SELECT id, phone, email, balance FROM src_users",
				SourceDB:  "src",
				TargetDB:  "dst",
				Mode:      config.TaskModeReplace,
				Masking: []config.MaskingConfig{
					{Column: "phone", Rule: config.MaskRulePhoneCN},
					{Column: "email", Rule: config.MaskRuleEmail},
					{Column: "balance", Rule: config.MaskRuleRandomNumeric, Range: []float64{0, 100}},
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

	var id int
	var phone, email string
	var balance float64
	if err := targetDB.QueryRow(`SELECT id, phone, email, balance FROM "dst_users"`).Scan(&id, &phone, &email, &balance); err != nil {
		t.Fatalf("query target row error = %v", err)
	}
	if id != 1 {
		t.Fatalf("expected id 1, got %d", id)
	}
	if phone != "138****8000" {
		t.Fatalf("expected masked phone, got %q", phone)
	}
	if email != "a***@example.com" {
		t.Fatalf("expected masked email, got %q", email)
	}
	if balance < 0 || balance > 100 {
		t.Fatalf("expected balance in [0,100], got %f", balance)
	}
}
func TestSplitRange(t *testing.T) {
	t.Run("int64 even split", func(t *testing.T) {
		ranges, err := splitRange(int64(0), int64(99), 4)
		if err != nil {
			t.Fatalf("splitRange() error = %v", err)
		}
		if len(ranges) != 4 {
			t.Fatalf("expected 4 ranges, got %d", len(ranges))
		}
		if ranges[0][0] != int64(0) || ranges[0][1] != int64(24) {
			t.Fatalf("unexpected first range: %v", ranges[0])
		}
		if ranges[3][0] != int64(72) || ranges[3][1] != int64(99) {
			t.Fatalf("unexpected last range: %v", ranges[3])
		}
	})

	t.Run("int64 min equals max", func(t *testing.T) {
		ranges, err := splitRange(int64(5), int64(5), 4)
		if err != nil {
			t.Fatalf("splitRange() error = %v", err)
		}
		if len(ranges) != 1 {
			t.Fatalf("expected 1 range, got %d", len(ranges))
		}
		if ranges[0][0] != int64(5) || ranges[0][1] != int64(5) {
			t.Fatalf("unexpected range: %v", ranges[0])
		}
	})

	t.Run("float64 split", func(t *testing.T) {
		ranges, err := splitRange(0.0, 10.0, 2)
		if err != nil {
			t.Fatalf("splitRange() error = %v", err)
		}
		if len(ranges) != 2 {
			t.Fatalf("expected 2 ranges, got %d", len(ranges))
		}
		if ranges[1][0] != 5.0 || ranges[1][1] != 10.0 {
			t.Fatalf("unexpected last range: %v", ranges[1])
		}
	})

	t.Run("time split", func(t *testing.T) {
		minT := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		maxT := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
		ranges, err := splitRange(minT, maxT, 2)
		if err != nil {
			t.Fatalf("splitRange() error = %v", err)
		}
		if len(ranges) != 2 {
			t.Fatalf("expected 2 ranges, got %d", len(ranges))
		}
		if !ranges[0][0].(time.Time).Equal(minT) {
			t.Fatalf("unexpected first range start: %v", ranges[0][0])
		}
		if !ranges[1][1].(time.Time).Equal(maxT) {
			t.Fatalf("unexpected last range end: %v", ranges[1][1])
		}
	})

	t.Run("string rejected", func(t *testing.T) {
		_, err := splitRange("a", "z", 4)
		if err == nil || !strings.Contains(err.Error(), "unsupported resume_key type") {
			t.Fatalf("expected unsupported type error, got %v", err)
		}
	})
}

func TestBuildShardTaskSQL(t *testing.T) {
	t.Run("without resume literal", func(t *testing.T) {
		dataSQL, countSQL := buildShardTaskSQL("SELECT * FROM t", "id", "", int64(0), int64(10), false)
		if !strings.Contains(dataSQL, "WHERE id >= 0 AND id < 10") {
			t.Fatalf("unexpected dataSQL: %s", dataSQL)
		}
		if strings.Contains(countSQL, "ORDER BY") {
			t.Fatalf("countSQL should not contain ORDER BY: %s", countSQL)
		}
	})

	t.Run("with resume literal last shard", func(t *testing.T) {
		dataSQL, countSQL := buildShardTaskSQL("SELECT * FROM t", "id", "5", int64(10), int64(20), true)
		if !strings.Contains(dataSQL, "WHERE id > 5 AND id >= 10 AND id <= 20") {
			t.Fatalf("unexpected dataSQL: %s", dataSQL)
		}
		if !strings.Contains(dataSQL, "ORDER BY id") {
			t.Fatalf("expected ORDER BY in dataSQL: %s", dataSQL)
		}
		if strings.Contains(countSQL, "ORDER BY") {
			t.Fatalf("countSQL should not contain ORDER BY: %s", countSQL)
		}
	})
}

func TestProcessShardedTask(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_events (id INTEGER PRIMARY KEY, name TEXT)`)
	for i := 1; i <= 100; i++ {
		setupSQLiteExec(t, sourcePath, fmt.Sprintf(`INSERT INTO src_events(id, name) VALUES (%d, 'event_%d')`, i, i))
	}

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName:  "dst_events",
				SQL:        "SELECT id, name FROM src_events",
				SourceDB:   "src",
				TargetDB:   "dst",
				Mode:       config.TaskModeAppend,
				Validate:   config.TaskValidateRowCount,
				ResumeKey:  "id",
				ResumeFrom: "0",
				Shard:      config.ShardConfig{Enabled: true, Shards: 4},
			},
		},
		MaxConcurrentTasks: 4,
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
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM "dst_events"`).Scan(&count); err != nil {
		t.Fatalf("query target count error = %v", err)
	}
	if count != 100 {
		t.Fatalf("target row count = %d, want 100", count)
	}

	var minID, maxID int
	if err := targetDB.QueryRow(`SELECT MIN(id), MAX(id) FROM "dst_events"`).Scan(&minID, &maxID); err != nil {
		t.Fatalf("query min/max error = %v", err)
	}
	if minID != 1 || maxID != 100 {
		t.Fatalf("unexpected min/max: %d, %d", minID, maxID)
	}
}

func TestProcessShardedTaskWithDryRun(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_events (id INTEGER PRIMARY KEY, name TEXT)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src_events(id, name) VALUES (1, 'a'), (2, 'b')`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: filepath.Join(dir, "target.db")},
		},
		Tasks: []config.TaskConfig{
			{
				TableName:  "dst_events",
				SQL:        "SELECT id, name FROM src_events",
				SourceDB:   "src",
				TargetDB:   "dst",
				Mode:       config.TaskModeAppend,
				ResumeKey:  "id",
				ResumeFrom: "0",
				Shard:      config.ShardConfig{Enabled: true, Shards: 2},
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
	if !strings.Contains(out, "Shards:  2") {
		t.Fatalf("expected shard count in plan output, got:\n%s", out)
	}
	if !strings.Contains(out, "Rows:    ~2") {
		t.Fatalf("expected row count in plan output, got:\n%s", out)
	}
}
func TestSplitRangeEdgeCases(t *testing.T) {
	t.Run("shards lte 1 error", func(t *testing.T) {
		_, err := splitRange(int64(0), int64(10), 1)
		if err == nil || !strings.Contains(err.Error(), "shards must be > 1") {
			t.Fatalf("expected shards error, got %v", err)
		}
	})

	t.Run("int64 step zero totalRange lt shards", func(t *testing.T) {
		ranges, err := splitRange(int64(0), int64(3), 10)
		if err != nil {
			t.Fatalf("splitRange() error = %v", err)
		}
		if len(ranges) == 0 {
			t.Fatalf("expected some ranges, got none")
		}
		if ranges[len(ranges)-1][1] != int64(3) {
			t.Fatalf("expected last upper to be 3, got %v", ranges[len(ranges)-1][1])
		}
	})

	t.Run("float64 min equals max", func(t *testing.T) {
		ranges, err := splitRange(5.0, 5.0, 4)
		if err != nil {
			t.Fatalf("splitRange() error = %v", err)
		}
		if len(ranges) != 1 {
			t.Fatalf("expected 1 range, got %d", len(ranges))
		}
		if ranges[0][0] != 5.0 || ranges[0][1] != 5.0 {
			t.Fatalf("unexpected range: %v", ranges[0])
		}
	})

	t.Run("time min equals max", func(t *testing.T) {
		minT := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		ranges, err := splitRange(minT, minT, 4)
		if err != nil {
			t.Fatalf("splitRange() error = %v", err)
		}
		if len(ranges) != 1 {
			t.Fatalf("expected 1 range, got %d", len(ranges))
		}
		if !ranges[0][0].(time.Time).Equal(minT) {
			t.Fatalf("unexpected range: %v", ranges[0])
		}
	})

	t.Run("int64 type mismatch", func(t *testing.T) {
		_, err := splitRange(int64(0), 10.0, 4)
		if err == nil || !strings.Contains(err.Error(), "does not match") {
			t.Fatalf("expected type mismatch error, got %v", err)
		}
	})

	t.Run("float64 type mismatch", func(t *testing.T) {
		_, err := splitRange(0.0, int64(10), 4)
		if err == nil || !strings.Contains(err.Error(), "does not match") {
			t.Fatalf("expected type mismatch error, got %v", err)
		}
	})

	t.Run("time type mismatch", func(t *testing.T) {
		_, err := splitRange(time.Now(), int64(10), 4)
		if err == nil || !strings.Contains(err.Error(), "does not match") {
			t.Fatalf("expected type mismatch error, got %v", err)
		}
	})
}

func TestGetHistoryRecorder(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src (id INTEGER PRIMARY KEY)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src(id) VALUES (1)`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName: "dst",
				SQL:       "SELECT id FROM src",
				SourceDB:  "src",
				TargetDB:  "dst",
				Mode:      config.TaskModeAppend,
			},
		},
		History: config.HistoryConfig{Enabled: true, TableName: "test_history"},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	t.Cleanup(func() { _ = p.Close() })

	// First call creates the recorder.
	r1 := p.getHistoryRecorder("dst")
	if r1 == nil {
		t.Fatal("expected recorder, got nil")
	}

	// Second call returns cached recorder.
	r2 := p.getHistoryRecorder("dst")
	if r2 != r1 {
		t.Fatal("expected same recorder instance")
	}
}

func TestProcessTaskWithHistory(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src (id INTEGER PRIMARY KEY)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src(id) VALUES (1), (2), (3)`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName: "dst",
				SQL:       "SELECT id FROM src",
				SourceDB:  "src",
				TargetDB:  "dst",
				Mode:      config.TaskModeAppend,
				Validate:  config.TaskValidateRowCount,
			},
		},
		History: config.HistoryConfig{Enabled: true, TableName: "test_history"},
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

	db, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "dst"`).Scan(&count); err != nil {
		t.Fatalf("query target count error = %v", err)
	}
	if count != 3 {
		t.Fatalf("target row count = %d, want 3", count)
	}
}

func TestSaveStateFileEmptyPath(t *testing.T) {
	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: filepath.Join(t.TempDir(), "src.db")},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: filepath.Join(t.TempDir(), "dst.db")},
		},
		Tasks: []config.TaskConfig{
			{TableName: "t", SQL: "SELECT 1", SourceDB: "src", TargetDB: "dst"},
		},
	}
	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	defer p.Close()

	state := &stateFile{Tasks: map[string]string{"k": "v"}}
	if err := p.saveStateFile("", state); err != nil {
		t.Fatalf("saveStateFile(\"\") error = %v", err)
	}
	if err := p.saveStateFile("/tmp/state.json", nil); err != nil {
		t.Fatalf("saveStateFile(..., nil) error = %v", err)
	}
}

func TestDLQWriterCloseNil(t *testing.T) {
	var w *dlqWriter
	if err := w.close(); err != nil {
		t.Fatalf("close on nil dlqWriter should return nil, got %v", err)
	}
}

func TestGetHistoryRecorderMissingConfig(t *testing.T) {
	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: filepath.Join(t.TempDir(), "src.db")},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: filepath.Join(t.TempDir(), "dst.db")},
		},
		Tasks: []config.TaskConfig{
			{TableName: "t", SQL: "SELECT 1", SourceDB: "src", TargetDB: "dst"},
		},
		History: config.HistoryConfig{Enabled: true},
	}
	manager := database.NewConnectionManager(cfg)
	p := NewProcessor(manager, cfg)
	defer p.Close()

	recorder := p.getHistoryRecorder("nonexistent_db")
	if recorder == nil {
		t.Fatalf("expected fallback recorder, got nil")
	}
}

func TestProcessTaskWithSkipCreateTable(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src (id INTEGER PRIMARY KEY)`)
	setupSQLiteExec(t, sourcePath, `INSERT INTO src(id) VALUES (1)`)

	// Pre-create target table.
	setupSQLiteSource(t, targetPath, `CREATE TABLE dst (id INTEGER PRIMARY KEY)`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName:       "dst",
				SQL:             "SELECT id FROM src",
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

	if err := p.processTask(cfg.Tasks[0]); err != nil {
		t.Fatalf("processTask() error = %v", err)
	}

	db, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "dst"`).Scan(&count); err != nil {
		t.Fatalf("query target count error = %v", err)
	}
	if count != 1 {
		t.Fatalf("target row count = %d, want 1", count)
	}
}

func TestProcessShardedTaskEmptyTable(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_empty (id INTEGER PRIMARY KEY, name TEXT)`)

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName:  "dst_empty",
				SQL:        "SELECT id, name FROM src_empty",
				SourceDB:   "src",
				TargetDB:   "dst",
				Mode:       config.TaskModeAppend,
				ResumeKey:  "id",
				ResumeFrom: "0",
				Shard:      config.ShardConfig{Enabled: true, Shards: 4},
			},
		},
		MaxConcurrentTasks: 4,
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

	db, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "dst_empty"`).Scan(&count); err != nil {
		t.Fatalf("query target count error = %v", err)
	}
	if count != 0 {
		t.Fatalf("target row count = %d, want 0", count)
	}
}

func TestProcessShardedTaskWithResumeFrom(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")

	setupSQLiteSource(t, sourcePath, `CREATE TABLE src_events (id INTEGER PRIMARY KEY, name TEXT)`)
	for i := 1; i <= 100; i++ {
		setupSQLiteExec(t, sourcePath, fmt.Sprintf(`INSERT INTO src_events(id, name) VALUES (%d, 'event_%d')`, i, i))
	}

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "src", Type: config.DatabaseTypeSQLite, Path: sourcePath},
			{Name: "dst", Type: config.DatabaseTypeSQLite, Path: targetPath},
		},
		Tasks: []config.TaskConfig{
			{
				TableName:  "dst_events",
				SQL:        "SELECT id, name FROM src_events",
				SourceDB:   "src",
				TargetDB:   "dst",
				Mode:       config.TaskModeAppend,
				Validate:   config.TaskValidateRowCount,
				ResumeKey:  "id",
				ResumeFrom: "25",
				Shard:      config.ShardConfig{Enabled: true, Shards: 4},
			},
		},
		MaxConcurrentTasks: 4,
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

	db, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM "dst_events"`).Scan(&count); err != nil {
		t.Fatalf("query target count error = %v", err)
	}
	if count != 75 {
		t.Fatalf("target row count = %d, want 75", count)
	}
}

func TestResolvePostAssertions(t *testing.T) {
	t.Run("no mappings returns copy", func(t *testing.T) {
		assertions := []config.AssertionConfig{
			{Column: "id", Rule: config.AssertionRuleNotNull},
			{Columns: []string{"a", "b"}, Rule: config.AssertionRuleUnique},
		}
		result := resolvePostAssertions(assertions, nil)
		if len(result) != 2 {
			t.Fatalf("expected 2 assertions, got %d", len(result))
		}
		if result[0].Column != "id" {
			t.Errorf("expected column id, got %s", result[0].Column)
		}
	})

	t.Run("maps single column", func(t *testing.T) {
		assertions := []config.AssertionConfig{
			{Column: "src_id", Rule: config.AssertionRuleNotNull},
		}
		mappings := []config.ColumnMapping{{Source: "src_id", Target: "dst_id"}}
		result := resolvePostAssertions(assertions, mappings)
		if result[0].Column != "dst_id" {
			t.Errorf("expected mapped column dst_id, got %s", result[0].Column)
		}
	})

	t.Run("maps unique columns", func(t *testing.T) {
		assertions := []config.AssertionConfig{
			{Columns: []string{"src_a", "src_b"}, Rule: config.AssertionRuleUnique},
		}
		mappings := []config.ColumnMapping{
			{Source: "src_a", Target: "dst_a"},
			{Source: "src_b", Target: "dst_b"},
		}
		result := resolvePostAssertions(assertions, mappings)
		if result[0].Columns[0] != "dst_a" || result[0].Columns[1] != "dst_b" {
			t.Errorf("expected mapped columns, got %v", result[0].Columns)
		}
	})

	t.Run("partial mapping leaves unmapped columns unchanged", func(t *testing.T) {
		assertions := []config.AssertionConfig{
			{Columns: []string{"src_a", "unmapped"}, Rule: config.AssertionRuleUnique},
		}
		mappings := []config.ColumnMapping{{Source: "src_a", Target: "dst_a"}}
		result := resolvePostAssertions(assertions, mappings)
		if result[0].Columns[0] != "dst_a" || result[0].Columns[1] != "unmapped" {
			t.Errorf("expected [dst_a, unmapped], got %v", result[0].Columns)
		}
	})

	t.Run("case insensitive mapping", func(t *testing.T) {
		assertions := []config.AssertionConfig{
			{Column: "SRC_ID", Rule: config.AssertionRuleNotNull},
		}
		mappings := []config.ColumnMapping{{Source: "src_id", Target: "dst_id"}}
		result := resolvePostAssertions(assertions, mappings)
		if result[0].Column != "dst_id" {
			t.Errorf("expected case-insensitive mapped column dst_id, got %s", result[0].Column)
		}
	})
}

func TestDLQWriteFn(t *testing.T) {
	t.Run("nil writer returns nil", func(t *testing.T) {
		fn := dlqwWriteFn(nil)
		if fn != nil {
			t.Error("expected nil function for nil dlqw")
		}
	})

	t.Run("non-nil writer delegates to dlqw", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "dlq.jsonl")
		dlqw, err := newDLQWriter(path, config.DLQFormatJSONL, []database.ColumnMetadata{{Name: "id"}})
		if err != nil {
			t.Fatalf("failed to create DLQ writer: %v", err)
		}
		defer dlqw.close()

		fn := dlqwWriteFn(dlqw)
		if fn == nil {
			t.Fatal("expected non-nil function for non-nil dlqw")
		}

		if err := fn([]any{1}, "test error"); err != nil {
			t.Fatalf("write failed: %v", err)
		}

		// Verify the file was written
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("failed to read DLQ file: %v", err)
		}
		if len(content) == 0 {
			t.Error("expected DLQ file to have content")
		}
	})
}
