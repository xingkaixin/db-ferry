package daemon

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
