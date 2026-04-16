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
	if got := out.String(); !strings.Contains(got, "db-ferry dev") {
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

func TestRunConfigInitCreatesTaskToml(t *testing.T) {
	dir := t.TempDir()
	chdirForTest(t, dir)

	var out bytes.Buffer
	var errOut bytes.Buffer
	code, runErr := run([]string{"config", "init"}, &out, &errOut)
	if runErr != nil {
		t.Fatalf("run() error = %v", runErr)
	}
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}
	if got := out.String(); !strings.Contains(got, "Created task.toml") {
		t.Fatalf("unexpected output: %q", got)
	}

	got, err := os.ReadFile(filepath.Join(dir, "task.toml"))
	if err != nil {
		t.Fatalf("read generated config error = %v", err)
	}
	if string(got) != defaultTaskTemplate {
		t.Fatalf("generated config does not match sample")
	}
}

func TestRunConfigInitFailsWhenTaskTomlExists(t *testing.T) {
	dir := t.TempDir()
	chdirForTest(t, dir)

	original := []byte("existing = true\n")
	if err := os.WriteFile(filepath.Join(dir, "task.toml"), original, 0o644); err != nil {
		t.Fatalf("write existing config error = %v", err)
	}

	var out bytes.Buffer
	var errOut bytes.Buffer
	code, err := run([]string{"config", "init"}, &out, &errOut)
	if err == nil {
		t.Fatalf("expected error when task.toml exists")
	}
	if code != 1 {
		t.Fatalf("run() code = %d, want 1", code)
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("unexpected error: %v", err)
	}

	got, readErr := os.ReadFile(filepath.Join(dir, "task.toml"))
	if readErr != nil {
		t.Fatalf("read existing config error = %v", readErr)
	}
	if string(got) != string(original) {
		t.Fatalf("existing task.toml was modified")
	}
}

func TestRunConfigInitRejectsExtraArgs(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code, err := run([]string{"config", "init", "extra"}, &out, &errOut)
	if err == nil {
		t.Fatalf("expected error for extra args")
	}
	if code != 2 {
		t.Fatalf("run() code = %d, want 2", code)
	}
	if !strings.Contains(err.Error(), "does not accept positional arguments") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunUnknownCommand(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code, err := run([]string{"unknown"}, &out, &errOut)
	if err == nil {
		t.Fatalf("expected error for unknown command")
	}
	if code != 2 {
		t.Fatalf("run() code = %d, want 2", code)
	}
	if !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunConfigMissingSubcommand(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code, err := run([]string{"config"}, &out, &errOut)
	if err == nil {
		t.Fatalf("expected error for missing subcommand")
	}
	if code != 2 {
		t.Fatalf("run() code = %d, want 2", code)
	}
	if !strings.Contains(err.Error(), "missing config subcommand") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunConfigUnknownSubcommand(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code, err := run([]string{"config", "unknown"}, &out, &errOut)
	if err == nil {
		t.Fatalf("expected error for unknown subcommand")
	}
	if code != 2 {
		t.Fatalf("run() code = %d, want 2", code)
	}
	if !strings.Contains(err.Error(), "unknown config subcommand") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunDoctorCommand(t *testing.T) {
	oldWriter := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(oldWriter)

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db.sqlite")
	cfgPath := filepath.Join(dir, "task.toml")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open db error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create table error = %v", err)
	}

	content := strings.Join([]string{
		"[[databases]]",
		`name = "src"`,
		`type = "sqlite"`,
		`path = "` + dbPath + `"`,
		"",
		"[[databases]]",
		`name = "dst"`,
		`type = "sqlite"`,
		`path = "` + filepath.Join(dir, "target.db") + `"`,
		"",
		"[[tasks]]",
		`table_name = "users_copy"`,
		`sql = "SELECT id FROM users"`,
		`source_db = "src"`,
		`target_db = "dst"`,
		`mode = "replace"`,
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write config error = %v", err)
	}

	var out bytes.Buffer
	var errOut bytes.Buffer
	code, runErr := run([]string{"-config", cfgPath, "doctor"}, &out, &errOut)
	if runErr != nil {
		t.Fatalf("run() error = %v", runErr)
	}
	if code != 0 {
		t.Fatalf("run() code = %d, want 0\noutput:\n%s", code, out.String())
	}
	got := out.String()
	if !strings.Contains(got, "[PASS] TOML syntax") {
		t.Fatalf("expected doctor output to contain TOML pass, got:\n%s", got)
	}
	if !strings.Contains(got, "Ready to ferry.") {
		t.Fatalf("expected doctor output to contain ready message, got:\n%s", got)
	}
}

func TestRunDoctorRejectsExtraArgs(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code, err := run([]string{"doctor", "extra"}, &out, &errOut)
	if err == nil {
		t.Fatalf("expected error for extra args")
	}
	if code != 2 {
		t.Fatalf("run() code = %d, want 2", code)
	}
	if !strings.Contains(err.Error(), "unknown doctor argument") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInitConfigTemplateStatError(t *testing.T) {
	dir := t.TempDir()
	chdirForTest(t, dir)

	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatalf("chmod error = %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(dir, 0o755)
	})

	var out bytes.Buffer
	code, err := initConfigTemplate(&out)
	if err == nil {
		t.Fatalf("expected error when stat fails")
	}
	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if !strings.Contains(err.Error(), "failed to check") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunConfigInitInteractiveFlagLong(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code, err := run([]string{"config", "init", "--interactive"}, &out, &errOut)
	// Interactive wizard requires a terminal; it will fail with a specific error.
	// We just verify the flag is parsed and the wizard is invoked rather than the template copy.
	if err == nil {
		t.Fatalf("expected error when running interactive wizard without terminal")
	}
	if code != 1 {
		t.Fatalf("run() code = %d, want 1", code)
	}
	if strings.Contains(err.Error(), "does not accept positional arguments") || strings.Contains(err.Error(), "already exists") {
		t.Fatalf("interactive flag was not recognized: %v", err)
	}
}

func TestRunConfigInitInteractiveFlagShort(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code, err := run([]string{"config", "init", "-i"}, &out, &errOut)
	if err == nil {
		t.Fatalf("expected error when running interactive wizard without terminal")
	}
	if code != 1 {
		t.Fatalf("run() code = %d, want 1", code)
	}
	if strings.Contains(err.Error(), "does not accept positional arguments") || strings.Contains(err.Error(), "already exists") {
		t.Fatalf("interactive flag was not recognized: %v", err)
	}
}

func TestRunInvalidFlag(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code, err := run([]string{"-badflag"}, &out, &errOut)
	if err == nil {
		t.Fatalf("expected error for invalid flag")
	}
	if code != 2 {
		t.Fatalf("run() code = %d, want 2", code)
	}
}

func TestRunConfigInitInvalidFlag(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code, err := run([]string{"config", "init", "-badflag"}, &out, &errOut)
	if err == nil {
		t.Fatalf("expected error for invalid flag")
	}
	if code != 2 {
		t.Fatalf("run() code = %d, want 2", code)
	}
}

func TestRunProcessError(t *testing.T) {
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
		`sql = "SELECT id, name FROM nonexistent_table ORDER BY id"`,
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
	if runErr == nil {
		t.Fatalf("expected error for failed task")
	}
	if code != 1 {
		t.Fatalf("run() code = %d, want 1", code)
	}
	if !strings.Contains(runErr.Error(), "failed to process tasks") {
		t.Fatalf("unexpected error: %v", runErr)
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

func TestRunDryRun(t *testing.T) {
	oldWriter := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(oldWriter)

	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
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
		`path = "` + filepath.Join(dir, "target.db") + `"`,
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
	code, runErr := run([]string{"-config", cfgPath, "-dry-run"}, &out, &errOut)
	if runErr != nil {
		t.Fatalf("run() error = %v", runErr)
	}
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	got := out.String()
	if !strings.Contains(got, "[PLAN]") {
		t.Fatalf("expected [PLAN] output, got:\n%s", got)
	}
	if !strings.Contains(got, "dst_users") {
		t.Fatalf("expected table name in output, got:\n%s", got)
	}
	if !strings.Contains(got, "~2") {
		t.Fatalf("expected row count in output, got:\n%s", got)
	}

	targetDB, err := sql.Open("sqlite3", filepath.Join(dir, "target.db"))
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	t.Cleanup(func() { _ = targetDB.Close() })

	var tableExists int
	if err := targetDB.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='dst_users'`).Scan(&tableExists); err != nil {
		t.Fatalf("query target table existence error = %v", err)
	}
	if tableExists != 0 {
		t.Fatalf("dry-run should not create target table")
	}
}

func TestRunVerbose(t *testing.T) {
	oldFlags := log.Flags()
	t.Cleanup(func() { log.SetFlags(oldFlags) })

	var out bytes.Buffer
	var errOut bytes.Buffer
	code, err := run([]string{"-version", "-v"}, &out, &errOut)
	if err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}
}

func TestInitConfigTemplateWriteError(t *testing.T) {
	dir := t.TempDir()
	chdirForTest(t, dir)

	// Create a file where task.toml should be written, but make the parent
	// directory read-only so WriteFile fails.
	readOnlyDir := filepath.Join(dir, "readonly")
	if err := os.Mkdir(readOnlyDir, 0o555); err != nil {
		t.Fatalf("mkdir error = %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(readOnlyDir, 0o755) })

	// Override target so it points inside the read-only directory.
	origTarget := configTemplateTarget
	configTemplateTarget = filepath.Join(readOnlyDir, "task.toml")
	t.Cleanup(func() { configTemplateTarget = origTarget })

	var out bytes.Buffer
	code, err := initConfigTemplate(&out)
	if err == nil {
		t.Fatalf("expected error when write fails")
	}
	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if !strings.Contains(err.Error(), "failed to write") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func chdirForTest(t *testing.T, dir string) {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory error = %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("change working directory error = %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(wd); err != nil {
			t.Fatalf("restore working directory error = %v", err)
		}
	})
}
