package main

import (
	"bytes"
	"database/sql"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestMainUsesExitCode(t *testing.T) {
	oldArgs := os.Args
	oldExit := exitFn
	defer func() {
		os.Args = oldArgs
		exitFn = oldExit
	}()

	exitCode := -1
	exitFn = func(code int) {
		exitCode = code
	}
	os.Args = []string{"db-ferry", "-version"}

	main()
	if exitCode != 0 {
		t.Fatalf("main() exit code = %d, want 0", exitCode)
	}
}

func TestMainUsesExitCodeOnFailure(t *testing.T) {
	oldArgs := os.Args
	oldExit := exitFn
	defer func() {
		os.Args = oldArgs
		exitFn = oldExit
	}()

	exitCode := -1
	exitFn = func(code int) {
		exitCode = code
	}
	os.Args = []string{"db-ferry", "-config", "missing.toml"}

	main()
	if exitCode != 1 {
		t.Fatalf("main() exit code = %d, want 1", exitCode)
	}
}

func TestRunVersion(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code, err := run([]string{"-version"}, &out, &errOut)
	if err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}
	if got := out.String(); !strings.Contains(got, "Multi-Source to SQLite Migration Tool") {
		t.Fatalf("unexpected version output: %q", got)
	}
}

func TestRunConfigError(t *testing.T) {
	oldWriter := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(oldWriter)

	var out bytes.Buffer
	var errOut bytes.Buffer
	code, err := run([]string{"-config", "missing.toml"}, &out, &errOut)
	if err == nil {
		t.Fatalf("expected error for missing config")
	}
	if code != 1 {
		t.Fatalf("run() code = %d, want 1", code)
	}
	if !strings.Contains(err.Error(), "failed to load configuration") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunHappyPath(t *testing.T) {
	oldWriter := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(oldWriter)

	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")
	cfgPath := filepath.Join(dir, "task.toml")

	sourceDB, err := sql.Open("sqlite3", sourcePath)
	if err != nil {
		t.Fatalf("open source db error = %v", err)
	}
	t.Cleanup(func() { _ = sourceDB.Close() })

	if _, err := sourceDB.Exec(`CREATE TABLE src_users (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("create source table error = %v", err)
	}
	if _, err := sourceDB.Exec(`INSERT INTO src_users(id, name) VALUES (1, 'alice'), (2, 'bob')`); err != nil {
		t.Fatalf("insert source rows error = %v", err)
	}

	content := strings.Join([]string{
		"[[databases]]",
		`name = "src"`,
		`type = "sqlite"`,
		`path = "` + sourcePath + `"`,
		"",
		"[[databases]]",
		`name = "dst"`,
		`type = "sqlite"`,
		`path = "` + targetPath + `"`,
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

	var out bytes.Buffer
	var errOut bytes.Buffer
	code, runErr := run([]string{"-config", cfgPath}, &out, &errOut)
	if runErr != nil {
		t.Fatalf("run() error = %v", runErr)
	}
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	targetDB, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	t.Cleanup(func() { _ = targetDB.Close() })

	var cnt int
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM "dst_users"`).Scan(&cnt); err != nil {
		t.Fatalf("query target count error = %v", err)
	}
	if cnt != 2 {
		t.Fatalf("target row count = %d, want 2", cnt)
	}
}
