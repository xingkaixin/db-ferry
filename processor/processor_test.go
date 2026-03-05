package processor

import (
	"database/sql"
	"encoding/json"
	"errors"
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

func (m *retryTarget) CreateIndexes(string, []config.IndexConfig) error { return nil }

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

	err := p.insertBatchWithRetry(
		target,
		config.TaskConfig{Mode: config.TaskModeReplace, MaxRetries: 2, TableName: "t"},
		[]database.ColumnMetadata{{Name: "id"}},
		[][]any{{1}},
		nil,
	)
	if err != nil {
		t.Fatalf("insertBatchWithRetry() error = %v", err)
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
	err = p.insertBatchWithRetry(
		mergeTarget,
		config.TaskConfig{Mode: config.TaskModeMerge, MaxRetries: 1, TableName: "t"},
		[]database.ColumnMetadata{{Name: "id"}},
		[][]any{{1}},
		[]string{"id"},
	)
	if err == nil {
		t.Fatalf("expected retry exhaustion error")
	}
	if mergeTarget.upsertCall != 2 {
		t.Fatalf("expected 2 upsert attempts, got %d", mergeTarget.upsertCall)
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

func TestProcessTaskMergeSkipsRowCountValidation(t *testing.T) {
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

	if err := p.processTask(cfg.Tasks[0]); err != nil {
		t.Fatalf("processTask() merge error = %v", err)
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
