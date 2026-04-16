package doctor

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestDoctorTOMLSyntaxFail(t *testing.T) {
	dir := t.TempDir()
	badPath := filepath.Join(dir, "bad.toml")
	if err := os.WriteFile(badPath, []byte("[[databases\nname = \"x\""), 0o644); err != nil {
		t.Fatalf("write bad toml error = %v", err)
	}

	var out bytes.Buffer
	doc := New(badPath)
	code := doc.Run(&out)
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	output := out.String()
	if !strings.Contains(output, "[FAIL] TOML syntax") {
		t.Fatalf("expected TOML syntax failure, got:\n%s", output)
	}
}

func TestDoctorConfigValidationFail(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "task.toml")
	content := strings.Join([]string{
		"[[databases]]",
		`name = "src"`,
		`type = "sqlite"`,
		`path = "/tmp/src.db"`,
		"",
		"[[tasks]]",
		`table_name = "users"`,
		`sql = "SELECT 1"`,
		`source_db = "missing"`,
		`target_db = "src"`,
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write config error = %v", err)
	}

	var out bytes.Buffer
	doc := New(cfgPath)
	code := doc.Run(&out)
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	output := out.String()
	if !strings.Contains(output, "[FAIL] Configuration validation") {
		t.Fatalf("expected config validation failure, got:\n%s", output)
	}
}

func TestDoctorConnectionFail(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "task.toml")
	content := strings.Join([]string{
		"[[databases]]",
		`name = "src"`,
		`type = "sqlite"`,
		`path = "/nonexistent/path/db.sqlite"`,
		"",
		"[[tasks]]",
		`table_name = "users"`,
		`sql = "SELECT 1"`,
		`source_db = "src"`,
		`target_db = "src"`,
		`allow_same_table = true`,
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write config error = %v", err)
	}

	var out bytes.Buffer
	doc := New(cfgPath)
	code := doc.Run(&out)
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	output := out.String()
	if !strings.Contains(output, "[FAIL] Database connection: src") {
		t.Fatalf("expected connection failure, got:\n%s", output)
	}
	if !strings.Contains(output, "[SKIP] Column existence: users") {
		t.Fatalf("expected skipped column existence, got:\n%s", output)
	}
	if !strings.Contains(output, "[SKIP] Target permission: users") {
		t.Fatalf("expected skipped target permission, got:\n%s", output)
	}
}

func TestDoctorHappyPath(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")
	cfgPath := filepath.Join(dir, "task.toml")

	srcDB, err := sql.Open("sqlite3", srcPath)
	if err != nil {
		t.Fatalf("open source db error = %v", err)
	}
	defer srcDB.Close()

	if _, err := srcDB.Exec(`CREATE TABLE src_users (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("create source table error = %v", err)
	}
	if _, err := srcDB.Exec(`INSERT INTO src_users(id, name) VALUES (1, 'alice')`); err != nil {
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
		`path = "` + targetPath + `"`,
		"",
		"[[tasks]]",
		`table_name = "dst_users"`,
		`sql = "SELECT id, name FROM src_users ORDER BY id"`,
		`source_db = "src"`,
		`target_db = "dst"`,
		`mode = "replace"`,
		`resume_key = "id"`,
		`resume_from = "0"`,
		"[[tasks.indexes]]",
		`name = "idx_name"`,
		`columns = ["name"]`,
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write config error = %v", err)
	}

	var out bytes.Buffer
	doc := New(cfgPath)
	code := doc.Run(&out)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d\noutput:\n%s", code, out.String())
	}
	output := out.String()
	checks := []string{
		"[PASS] TOML syntax",
		"[PASS] Configuration validation",
		"[PASS] Database connection: src",
		"[PASS] Database connection: dst",
		"[PASS] Source permission: dst_users",
		"[PASS] SQL syntax: dst_users",
		"[PASS] Column existence: dst_users",
		"[PASS] Target permission: dst_users",
		"[PASS] Disk space: dst",
		"Ready to ferry.",
	}
	for _, check := range checks {
		if !strings.Contains(output, check) {
			t.Fatalf("expected output to contain %q, got:\n%s", check, output)
		}
	}
}

func TestDoctorSourceSQLFail(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")
	cfgPath := filepath.Join(dir, "task.toml")

	srcDB, err := sql.Open("sqlite3", srcPath)
	if err != nil {
		t.Fatalf("open source db error = %v", err)
	}
	defer srcDB.Close()

	if _, err := srcDB.Exec(`CREATE TABLE src_users (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create source table error = %v", err)
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
		`path = "` + targetPath + `"`,
		"",
		"[[tasks]]",
		`table_name = "dst_users"`,
		`sql = "SELECT * FROM nonexistent_table"`,
		`source_db = "src"`,
		`target_db = "dst"`,
		`mode = "replace"`,
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write config error = %v", err)
	}

	var out bytes.Buffer
	doc := New(cfgPath)
	code := doc.Run(&out)
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d\noutput:\n%s", code, out.String())
	}
	output := out.String()
	if !strings.Contains(output, "[FAIL] SQL syntax: dst_users") {
		t.Fatalf("expected SQL syntax failure, got:\n%s", output)
	}
	if !strings.Contains(output, "[SKIP] Column existence: dst_users") {
		t.Fatalf("expected skipped column existence, got:\n%s", output)
	}
}

func TestDoctorColumnExistenceFail(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.db")
	targetPath := filepath.Join(dir, "target.db")
	cfgPath := filepath.Join(dir, "task.toml")

	srcDB, err := sql.Open("sqlite3", srcPath)
	if err != nil {
		t.Fatalf("open source db error = %v", err)
	}
	defer srcDB.Close()

	if _, err := srcDB.Exec(`CREATE TABLE src_users (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("create source table error = %v", err)
	}
	if _, err := srcDB.Exec(`INSERT INTO src_users(id, name) VALUES (1, 'alice')`); err != nil {
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
		`path = "` + targetPath + `"`,
		"",
		"[[tasks]]",
		`table_name = "dst_users"`,
		`sql = "SELECT id, name FROM src_users ORDER BY id"`,
		`source_db = "src"`,
		`target_db = "dst"`,
		`mode = "replace"`,
		`resume_key = "missing_column"`,
		`resume_from = "0"`,
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write config error = %v", err)
	}

	var out bytes.Buffer
	doc := New(cfgPath)
	code := doc.Run(&out)
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d\noutput:\n%s", code, out.String())
	}
	output := out.String()
	if !strings.Contains(output, "[FAIL] Column existence: dst_users") {
		t.Fatalf("expected column existence failure, got:\n%s", output)
	}
}

func TestDoctorSameDBMigrationWarning(t *testing.T) {
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
		`name = "db"`,
		`type = "sqlite"`,
		`path = "` + dbPath + `"`,
		"",
		"[[tasks]]",
		`table_name = "users_copy"`,
		`sql = "SELECT id FROM users"`,
		`source_db = "db"`,
		`target_db = "db"`,
		`mode = "replace"`,
		`allow_same_table = true`,
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write config error = %v", err)
	}

	var out bytes.Buffer
	doc := New(cfgPath)
	code := doc.Run(&out)
	if code != 0 {
		t.Fatalf("expected exit code 0 (warning only), got %d\noutput:\n%s", code, out.String())
	}
	output := out.String()
	if !strings.Contains(output, "[WARN] Same-database migration: users_copy") {
		t.Fatalf("expected same-db warning, got:\n%s", output)
	}
	if !strings.Contains(output, "Ready to ferry, but review warnings.") {
		t.Fatalf("expected review warnings message, got:\n%s", output)
	}
}

func TestDoctorDiskSpaceFail(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.db")
	// Create target DB in a writable directory first, then make it read-only.
	roDir := filepath.Join(dir, "readonly")
	if err := os.Mkdir(roDir, 0o755); err != nil {
		t.Fatalf("mkdir error = %v", err)
	}
	targetPath := filepath.Join(roDir, "target.db")

	// Create the target SQLite file so the connection check succeeds.
	dstDB, err := sql.Open("sqlite3", targetPath)
	if err != nil {
		t.Fatalf("open target db error = %v", err)
	}
	if _, err := dstDB.Exec(`CREATE TABLE dst_users (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create target table error = %v", err)
	}
	_ = dstDB.Close()

	// Now make the directory read-only so disk-space check fails.
	if err := os.Chmod(roDir, 0o555); err != nil {
		t.Fatalf("chmod error = %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(roDir, 0o755) })

	cfgPath := filepath.Join(dir, "task.toml")

	srcDB, err := sql.Open("sqlite3", srcPath)
	if err != nil {
		t.Fatalf("open source db error = %v", err)
	}
	defer srcDB.Close()

	if _, err := srcDB.Exec(`CREATE TABLE src_users (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create source table error = %v", err)
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
		`path = "` + targetPath + `"`,
		"",
		"[[tasks]]",
		`table_name = "dst_users"`,
		`sql = "SELECT id FROM src_users"`,
		`source_db = "src"`,
		`target_db = "dst"`,
		`mode = "replace"`,
	}, "\n")
	if err := os.WriteFile(cfgPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write config error = %v", err)
	}

	var out bytes.Buffer
	doc := New(cfgPath)
	code := doc.Run(&out)
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d\noutput:\n%s", code, out.String())
	}
	output := out.String()
	if !strings.Contains(output, "[FAIL] Disk space: dst") {
		t.Fatalf("expected disk space failure, got:\n%s", output)
	}
}

func TestTrimSQL(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"SELECT 1;", "SELECT 1"},
		{"  SELECT 1  ", "SELECT 1"},
		{"SELECT 1;;", "SELECT 1"},
		{"SELECT 1", "SELECT 1"},
	}
	for _, tc := range tests {
		got := trimSQL(tc.input)
		if got != tc.expected {
			t.Fatalf("trimSQL(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestContainsStringFold(t *testing.T) {
	haystack := []string{"Foo", "BAR", "baz"}
	if !containsStringFold(haystack, "foo") {
		t.Fatal("expected to find 'foo'")
	}
	if !containsStringFold(haystack, "bar") {
		t.Fatal("expected to find 'bar'")
	}
	if containsStringFold(haystack, "qux") {
		t.Fatal("expected not to find 'qux'")
	}
}

func TestDropTableSQL(t *testing.T) {
	if got := dropTableSQL("sqlite", "t"); !strings.Contains(got, "DROP TABLE IF EXISTS") {
		t.Fatalf("unexpected sqlite drop: %s", got)
	}
	if got := dropTableSQL("oracle", "t"); !strings.Contains(got, "EXECUTE IMMEDIATE") {
		t.Fatalf("unexpected oracle drop: %s", got)
	}
	if got := dropTableSQL("sqlserver", "t"); !strings.Contains(got, "OBJECT_ID") {
		t.Fatalf("unexpected sqlserver drop: %s", got)
	}
}

func TestStatusColor(t *testing.T) {
	cases := []struct {
		status Status
		want   string
	}{
		{StatusPass, "\033[32m"},
		{StatusWarn, "\033[33m"},
		{StatusFail, "\033[31m"},
		{StatusSkip, "\033[33m"},
		{Status(99), "\033[0m"},
	}
	for _, tc := range cases {
		if got := tc.status.color(); got != tc.want {
			t.Fatalf("color() for %s = %q, want %q", tc.status, got, tc.want)
		}
	}
}
