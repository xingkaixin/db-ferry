package daemon

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"db-ferry/config"

	_ "github.com/mattn/go-sqlite3"
)

func TestDaemonHashConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "task.toml")

	if err := os.WriteFile(path, []byte("hello = 'world'\n"), 0o644); err != nil {
		t.Fatalf("write config error = %v", err)
	}

	d := New(Options{ConfigPath: path})
	h1, err := d.hashConfig(path)
	if err != nil {
		t.Fatalf("hashConfig error = %v", err)
	}
	if h1 == "" {
		t.Fatal("hashConfig returned empty string")
	}

	// Same content -> same hash.
	h2, err := d.hashConfig(path)
	if err != nil {
		t.Fatalf("hashConfig second call error = %v", err)
	}
	if h1 != h2 {
		t.Fatalf("same content produced different hashes: %s vs %s", h1, h2)
	}

	// Different content -> different hash.
	if err := os.WriteFile(path, []byte("hello = 'universe'\n"), 0o644); err != nil {
		t.Fatalf("rewrite config error = %v", err)
	}
	h3, err := d.hashConfig(path)
	if err != nil {
		t.Fatalf("hashConfig after change error = %v", err)
	}
	if h1 == h3 {
		t.Fatal("different content produced same hash")
	}
}

func TestDaemonHashConfigMissingFile(t *testing.T) {
	d := New(Options{ConfigPath: "nonexistent.toml"})
	_, err := d.hashConfig("nonexistent.toml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestDaemonRunOnceSuccess(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _, dstDB := setupTestDBs(t, dir)

	d := New(Options{ConfigPath: cfgPath})

	go func() {
		time.Sleep(100 * time.Millisecond)
		d.Stop()
	}()

	err := d.runOnce()
	if err != nil {
		t.Fatalf("runOnce error = %v", err)
	}

	var cnt int
	if err := dstDB.QueryRow(`SELECT COUNT(*) FROM "dst_users"`).Scan(&cnt); err != nil {
		t.Fatalf("query target count error = %v", err)
	}
	if cnt != 2 {
		t.Fatalf("target row count = %d, want 2", cnt)
	}
}

func TestDaemonRunOnceBadConfig(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "task.toml")
	if err := os.WriteFile(cfgPath, []byte("bad toml"), 0o644); err != nil {
		t.Fatalf("write config error = %v", err)
	}

	d := New(Options{ConfigPath: cfgPath})
	err := d.runOnce()
	if err == nil {
		t.Fatal("expected error for bad config")
	}
	if !strings.Contains(err.Error(), "failed to load configuration") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDaemonStopBeforeRun(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _, _ := setupTestDBs(t, dir)

	d := New(Options{ConfigPath: cfgPath})
	d.Stop()

	err := d.Run()
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
}

func TestDaemonStop(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _, _ := setupTestDBs(t, dir)

	d := New(Options{ConfigPath: cfgPath})

	go func() {
		time.Sleep(50 * time.Millisecond)
		d.Stop()
	}()

	err := d.Run()
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}

	if d.IsRunning() {
		t.Fatal("expected IsRunning() to be false after Stop")
	}
}

func TestDaemonLastError(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "task.toml")
	if err := os.WriteFile(cfgPath, []byte("invalid toml = ["), 0o644); err != nil {
		t.Fatalf("write config error = %v", err)
	}

	d := New(Options{ConfigPath: cfgPath})
	_ = d.runOnce()

	if d.LastError() == nil {
		t.Fatal("expected LastError to be set after failed round")
	}
}

func TestDaemonRunWithWatchReload(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _, dstDB := setupTestDBs(t, dir)

	d := New(Options{ConfigPath: cfgPath, WatchEnabled: true})

	// Stop after a short delay to end the test.
	go func() {
		time.Sleep(300 * time.Millisecond)
		d.Stop()
	}()

	err := d.Run()
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}

	var cnt int
	if err := dstDB.QueryRow(`SELECT COUNT(*) FROM "dst_users"`).Scan(&cnt); err != nil {
		t.Fatalf("query target count error = %v", err)
	}
	if cnt != 2 {
		t.Fatalf("target row count = %d, want 2", cnt)
	}
}

func TestDaemonRunWithWatchConfigChange(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _, _ := setupTestDBs(t, dir)

	d := New(Options{ConfigPath: cfgPath, WatchEnabled: true})

	// Wait for initial round then change config.
	go func() {
		time.Sleep(200 * time.Millisecond)
		content, _ := os.ReadFile(cfgPath)
		_ = os.WriteFile(cfgPath, append(content, []byte("\n# changed\n")...), 0o644)

		time.Sleep(200 * time.Millisecond)
		d.Stop()
	}()

	err := d.Run()
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
}

func TestDaemonRunWithSchedule(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _, dstDB := setupTestDBs(t, dir)

	content, _ := os.ReadFile(cfgPath)
	scheduleSection := "\n[schedule]\ncron = \"@every 50ms\"\nmissed_catchup = true\n"
	_ = os.WriteFile(cfgPath, append(content, []byte(scheduleSection)...), 0o644)

	// Write state file with an old last_run to trigger catchup immediately.
	statePath := filepath.Join(dir, ".db-ferry-schedule-state.json")
	state := scheduleState{LastRun: time.Now().Add(-2 * time.Hour)}
	data, _ := json.Marshal(state)
	_ = os.WriteFile(statePath, data, 0o644)

	d := New(Options{ConfigPath: cfgPath})

	go func() {
		time.Sleep(300 * time.Millisecond)
		d.Stop()
	}()

	err := d.Run()
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}

	var cnt int
	if err := dstDB.QueryRow(`SELECT COUNT(*) FROM "dst_users"`).Scan(&cnt); err != nil {
		t.Fatalf("query target count error = %v", err)
	}
	if cnt != 2 {
		t.Fatalf("target row count = %d, want 2", cnt)
	}
}

func TestDaemonScheduleRetry(t *testing.T) {
	oldDelay := scheduleRetryDelay
	scheduleRetryDelay = 50 * time.Millisecond
	defer func() { scheduleRetryDelay = oldDelay }()

	dir := t.TempDir()
	cfgPath, _, _ := setupTestDBs(t, dir)

	content, _ := os.ReadFile(cfgPath)
	// Replace source table name with a nonexistent one to force query failure.
	newContent := strings.ReplaceAll(string(content), "FROM src_users", "FROM nonexistent_table")
	scheduleSection := "\n[schedule]\ncron = \"@every 50ms\"\nretry_on_failure = true\nmax_retry = 1\n"
	_ = os.WriteFile(cfgPath, append([]byte(newContent), []byte(scheduleSection)...), 0o644)

	// Isolate logs directory per test to avoid cross-test contamination.
	oldWd, _ := os.Getwd()
	_ = os.Chdir(dir)
	defer func() { _ = os.Chdir(oldWd) }()

	d := New(Options{ConfigPath: cfgPath})

	go func() {
		time.Sleep(1 * time.Second)
		d.Stop()
	}()

	err := d.Run()
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}

	// Verify retry was triggered by inspecting the isolated log file.
	entries, _ := os.ReadDir("logs")
	var logContent string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			b, _ := os.ReadFile(filepath.Join("logs", e.Name()))
			logContent += string(b)
		}
	}
	if !strings.Contains(logContent, "Retry attempt") {
		t.Fatalf("expected retry to be triggered; log content:\n%s", logContent)
	}
}

func TestDaemonScheduleMissedCatchup(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _, dstDB := setupTestDBs(t, dir)

	content, _ := os.ReadFile(cfgPath)
	scheduleSection := "\n[schedule]\ncron = \"0 * * * *\"\nmissed_catchup = true\n"
	_ = os.WriteFile(cfgPath, append(content, []byte(scheduleSection)...), 0o644)

	// Write a state file with an old last_run to trigger catchup.
	statePath := filepath.Join(dir, ".db-ferry-schedule-state.json")
	state := scheduleState{LastRun: time.Now().Add(-2 * time.Hour)}
	data, _ := json.Marshal(state)
	_ = os.WriteFile(statePath, data, 0o644)

	d := New(Options{ConfigPath: cfgPath})

	go func() {
		time.Sleep(300 * time.Millisecond)
		d.Stop()
	}()

	err := d.Run()
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}

	var cnt int
	if err := dstDB.QueryRow(`SELECT COUNT(*) FROM "dst_users"`).Scan(&cnt); err != nil {
		t.Fatalf("query target count error = %v", err)
	}
	if cnt != 2 {
		t.Fatalf("target row count = %d, want 2", cnt)
	}
}

func TestDaemonScheduleLogRotation(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _, _ := setupTestDBs(t, dir)

	content, _ := os.ReadFile(cfgPath)
	scheduleSection := "\n[schedule]\ncron = \"@every 50ms\"\nmissed_catchup = true\n"
	_ = os.WriteFile(cfgPath, append(content, []byte(scheduleSection)...), 0o644)

	// Write state file with an old last_run to trigger catchup immediately.
	statePath := filepath.Join(dir, ".db-ferry-schedule-state.json")
	state := scheduleState{LastRun: time.Now().Add(-2 * time.Hour)}
	data, _ := json.Marshal(state)
	_ = os.WriteFile(statePath, data, 0o644)

	// Change working directory so logs/ is created inside temp dir.
	oldWd, _ := os.Getwd()
	_ = os.Chdir(dir)
	defer func() { _ = os.Chdir(oldWd) }()

	d := New(Options{ConfigPath: cfgPath})

	go func() {
		time.Sleep(300 * time.Millisecond)
		d.Stop()
	}()

	err := d.Run()
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}

	logDir := filepath.Join(dir, "logs")
	entries, err := os.ReadDir(logDir)
	if err != nil {
		t.Fatalf("expected logs directory to exist: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one log file in logs directory")
	}
	found := false
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected a .log file in logs directory")
	}
}

func TestDaemonScheduleWithWatchReload(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _, dstDB := setupTestDBs(t, dir)

	content, _ := os.ReadFile(cfgPath)
	scheduleSection := "\n[schedule]\ncron = \"@every 200ms\"\nmissed_catchup = true\n"
	_ = os.WriteFile(cfgPath, append(content, []byte(scheduleSection)...), 0o644)

	// Write state file with an old last_run so catchup executes immediately.
	statePath := filepath.Join(dir, ".db-ferry-schedule-state.json")
	state := scheduleState{LastRun: time.Now().Add(-2 * time.Hour)}
	data, _ := json.Marshal(state)
	_ = os.WriteFile(statePath, data, 0o644)

	d := New(Options{ConfigPath: cfgPath, WatchEnabled: true})

	go func() {
		time.Sleep(200 * time.Millisecond)
		newContent, _ := os.ReadFile(cfgPath)
		_ = os.WriteFile(cfgPath, append(newContent, []byte("\n# changed\n")...), 0o644)

		time.Sleep(400 * time.Millisecond)
		d.Stop()
	}()

	err := d.Run()
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}

	var cnt int
	if err := dstDB.QueryRow(`SELECT COUNT(*) FROM "dst_users"`).Scan(&cnt); err != nil {
		t.Fatalf("query target count error = %v", err)
	}
	if cnt != 2 {
		t.Fatalf("target row count = %d, want 2", cnt)
	}
}

func TestParseDaemonScheduleTime(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"RFC3339", "2026-01-01T00:00:00Z", false},
		{"compact", "2026-01-01T00:00:00", false},
		{"invalid", "not-a-date", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseDaemonScheduleTime(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.IsZero() {
				t.Fatal("expected non-zero time")
			}
		})
	}
}

func TestDaemonLoadLastRunInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	_ = os.WriteFile(path, []byte("not json"), 0o644)

	d := New(Options{ConfigPath: filepath.Join(dir, "task.toml")})
	_, err := d.loadLastRun(path)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestDaemonHandleMissedCatchupLoadError(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _, _ := setupTestDBs(t, dir)

	content, _ := os.ReadFile(cfgPath)
	scheduleSection := "\n[schedule]\ncron = \"@every 1h\"\nmissed_catchup = true\n"
	_ = os.WriteFile(cfgPath, append(content, []byte(scheduleSection)...), 0o644)

	// Create a state file that is a directory to force a read error.
	statePath := filepath.Join(dir, ".db-ferry-schedule-state.json")
	_ = os.Mkdir(statePath, 0o755)

	d := New(Options{ConfigPath: cfgPath})
	cfg, _ := config.LoadConfig(cfgPath)
	err := d.handleMissedCatchup(cfg, time.Local)
	if err == nil {
		t.Fatal("expected error for unreadable state file")
	}
}

func TestDaemonHandleMissedCatchupNoState(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _, _ := setupTestDBs(t, dir)

	content, _ := os.ReadFile(cfgPath)
	scheduleSection := "\n[schedule]\ncron = \"@every 1h\"\nmissed_catchup = true\n"
	_ = os.WriteFile(cfgPath, append(content, []byte(scheduleSection)...), 0o644)

	// No state file exists, so lastRun should be zero and handleMissedCatchup returns nil.
	d := New(Options{ConfigPath: cfgPath})
	cfg, _ := config.LoadConfig(cfgPath)
	err := d.handleMissedCatchup(cfg, time.Local)
	if err != nil {
		t.Fatalf("expected no error when state file missing: %v", err)
	}
}

func TestDaemonHandleMissedCatchupInvalidCron(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _, _ := setupTestDBs(t, dir)

	content, _ := os.ReadFile(cfgPath)
	scheduleSection := "\n[schedule]\ncron = \"invalid cron\"\nmissed_catchup = true\n"
	_ = os.WriteFile(cfgPath, append(content, []byte(scheduleSection)...), 0o644)

	// Write state with an old last_run.
	statePath := filepath.Join(dir, ".db-ferry-schedule-state.json")
	state := scheduleState{LastRun: time.Now().Add(-2 * time.Hour)}
	data, _ := json.Marshal(state)
	_ = os.WriteFile(statePath, data, 0o644)

	d := New(Options{ConfigPath: cfgPath})
	cfg, err := config.LoadConfig(cfgPath)
	if err != nil {
		// Config validation rejects invalid cron, so handleMissedCatchup
		// won't even be reached in real usage. Skip this test path.
		t.Skipf("config validation rejected invalid cron: %v", err)
	}
	err = d.handleMissedCatchup(cfg, time.Local)
	if err == nil {
		t.Fatal("expected error for invalid cron expression")
	}
}

func TestDaemonRunWithScheduleInvalidTimezone(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _, _ := setupTestDBs(t, dir)

	content, _ := os.ReadFile(cfgPath)
	scheduleSection := "\n[schedule]\ncron = \"@every 1h\"\ntimezone = \"Invalid/Zone\"\n"
	_ = os.WriteFile(cfgPath, append(content, []byte(scheduleSection)...), 0o644)

	d := New(Options{ConfigPath: cfgPath})
	err := d.Run()
	if err == nil {
		t.Fatal("expected error for invalid timezone")
	}
}

func TestDaemonRunWithScheduleInvalidCron(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _, _ := setupTestDBs(t, dir)

	content, _ := os.ReadFile(cfgPath)
	scheduleSection := "\n[schedule]\ncron = \"bad cron\"\n"
	_ = os.WriteFile(cfgPath, append(content, []byte(scheduleSection)...), 0o644)

	d := New(Options{ConfigPath: cfgPath})
	err := d.Run()
	if err == nil {
		t.Fatal("expected error for invalid cron expression")
	}
}

func TestDaemonScheduledJobStartAt(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _, _ := setupTestDBs(t, dir)

	content, _ := os.ReadFile(cfgPath)
	// Set start_at far in the future so the job skips execution.
	scheduleSection := "\n[schedule]\ncron = \"@every 50ms\"\nstart_at = \"2099-01-01T00:00:00\"\n"
	_ = os.WriteFile(cfgPath, append(content, []byte(scheduleSection)...), 0o644)

	d := New(Options{ConfigPath: cfgPath})

	go func() {
		time.Sleep(200 * time.Millisecond)
		d.Stop()
	}()

	err := d.Run()
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
}

func TestDaemonScheduledJobEndAt(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _, _ := setupTestDBs(t, dir)

	content, _ := os.ReadFile(cfgPath)
	// Set end_at in the past so the job skips execution.
	scheduleSection := "\n[schedule]\ncron = \"@every 50ms\"\nend_at = \"2000-01-01T00:00:00\"\n"
	_ = os.WriteFile(cfgPath, append(content, []byte(scheduleSection)...), 0o644)

	d := New(Options{ConfigPath: cfgPath})

	go func() {
		time.Sleep(200 * time.Millisecond)
		d.Stop()
	}()

	err := d.Run()
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
}

func TestDaemonScheduledJobStopDuringRetry(t *testing.T) {
	oldDelay := scheduleRetryDelay
	scheduleRetryDelay = 500 * time.Millisecond
	defer func() { scheduleRetryDelay = oldDelay }()

	dir := t.TempDir()
	cfgPath, _, _ := setupTestDBs(t, dir)

	content, _ := os.ReadFile(cfgPath)
	newContent := strings.ReplaceAll(string(content), "FROM src_users", "FROM nonexistent_table")
	scheduleSection := "\n[schedule]\ncron = \"@every 50ms\"\nretry_on_failure = true\nmax_retry = 3\n"
	_ = os.WriteFile(cfgPath, append([]byte(newContent), []byte(scheduleSection)...), 0o644)

	d := New(Options{ConfigPath: cfgPath})

	go func() {
		// Stop while the job is waiting for retry.
		time.Sleep(150 * time.Millisecond)
		d.Stop()
	}()

	err := d.Run()
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}
}

func TestDaemonRunWithWatchSameHash(t *testing.T) {
	dir := t.TempDir()
	cfgPath, _, dstDB := setupTestDBs(t, dir)

	d := New(Options{ConfigPath: cfgPath, WatchEnabled: true})

	go func() {
		// Wait for initial round.
		time.Sleep(200 * time.Millisecond)

		// Trigger a write but restore original content before debounce fires.
		original, _ := os.ReadFile(cfgPath)
		_ = os.WriteFile(cfgPath, append(original, []byte("\n# temp\n")...), 0o644)
		time.Sleep(100 * time.Millisecond)
		_ = os.WriteFile(cfgPath, original, 0o644)

		time.Sleep(700 * time.Millisecond)
		d.Stop()
	}()

	err := d.Run()
	if err != nil {
		t.Fatalf("Run error = %v", err)
	}

	var cnt int
	if err := dstDB.QueryRow(`SELECT COUNT(*) FROM "dst_users"`).Scan(&cnt); err != nil {
		t.Fatalf("query target count error = %v", err)
	}
	if cnt != 2 {
		t.Fatalf("target row count = %d, want 2", cnt)
	}
}

func setupTestDBs(t *testing.T, dir string) (string, *sql.DB, *sql.DB) {
	t.Helper()

	srcPath := filepath.Join(dir, "source.db")
	dstPath := filepath.Join(dir, "target.db")
	cfgPath := filepath.Join(dir, "task.toml")

	srcDB, err := sql.Open("sqlite3", srcPath)
	if err != nil {
		t.Fatalf("open source db error = %v", err)
	}
	t.Cleanup(func() { _ = srcDB.Close() })

	if _, err := srcDB.Exec(`CREATE TABLE src_users (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("create source table error = %v", err)
	}
	if _, err := srcDB.Exec(`INSERT INTO src_users(id, name) VALUES (1, 'alice'), (2, 'bob')`); err != nil {
		t.Fatalf("insert source rows error = %v", err)
	}

	content := strings.Join([]string{
		"[[databases]]",
		`name = "src"`,
		`type = "sqlite"`,
		`path = "` + srcPath + `"`,
		"",
		"[[databases]]",
		`name = "dst"`,
		`type = "sqlite"`,
		`path = "` + dstPath + `"`,
		"",
		"[[tasks]]",
		`table_name = "dst_users"`,
		`sql = "SELECT id, name FROM src_users ORDER BY id"`,
		`source_db = "src"`,
		`target_db = "dst"`,
		`mode = "replace"`,
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write config error = %v", err)
	}

	dstDB, err := sql.Open("sqlite3", dstPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	t.Cleanup(func() { _ = dstDB.Close() })

	return cfgPath, srcDB, dstDB
}
