package web

import (
	"bytes"
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"db-ferry/daemon"
	"db-ferry/sse"

	"github.com/go-chi/chi/v5"
	_ "github.com/mattn/go-sqlite3"
)

func withChiParams(req *http.Request, pairs ...string) *http.Request {
	rctx := chi.NewRouteContext()
	for i := 0; i < len(pairs); i += 2 {
		rctx.URLParams.Add(pairs[i], pairs[i+1])
	}
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
}

func tempConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "task.toml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func newTestServer(t *testing.T, configContent string) (*Server, string) {
	t.Helper()
	cfgPath := tempConfig(t, configContent)
	d := daemon.New(daemon.Options{
		ConfigPath: cfgPath,
		Version:    "test",
	})
	srv := New(Options{
		ConfigPath: cfgPath,
		Daemon:     d,
		SSEServer:  sse.NewServer(),
		User:       "",
		Pass:       "",
	})
	return srv, cfgPath
}

func TestHandleGetConfig(t *testing.T) {
	srv, _ := newTestServer(t, "# test config\n")
	req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	rec := httptest.NewRecorder()

	srv.handleGetConfig(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "test config") {
		t.Fatalf("expected config content, got %q", body)
	}
}

func TestHandlePutConfig(t *testing.T) {
	srv, cfgPath := newTestServer(t, "# old\n")
	req := httptest.NewRequest(http.MethodPut, "/api/config", bytes.NewReader([]byte("# new\n")))
	rec := httptest.NewRecorder()

	srv.handlePutConfig(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "# new\n" {
		t.Fatalf("expected updated config, got %q", string(data))
	}
}

func TestHandleValidateConfig(t *testing.T) {
	srv, _ := newTestServer(t, "")

	// Valid TOML but invalid config
	req := httptest.NewRequest(http.MethodPost, "/api/config/validate", bytes.NewReader([]byte("title = \"hello\"")))
	rec := httptest.NewRecorder()
	srv.handleValidateConfig(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid config, got %d", rec.Code)
	}

	// Valid config
	valid := `
[[databases]]
name = "src"
type = "sqlite"
path = "test.db"

[[tasks]]
table_name = "test"
sql = "SELECT 1"
source_db = "src"
target_db = "src"
mode = "replace"
allow_same_table = true
`
	req = httptest.NewRequest(http.MethodPost, "/api/config/validate", bytes.NewReader([]byte(valid)))
	rec = httptest.NewRecorder()
	srv.handleValidateConfig(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for valid config, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"valid":"true"`) {
		t.Fatalf("expected valid=true, got %q", body)
	}
}

func TestHandleGetTasks(t *testing.T) {
	content := `
[[databases]]
name = "src"
type = "sqlite"
path = "test.db"

[[tasks]]
table_name = "users"
sql = "SELECT 1"
source_db = "src"
target_db = "src"
mode = "replace"
allow_same_table = true
`
	srv, _ := newTestServer(t, content)
	req := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	rec := httptest.NewRecorder()

	srv.handleGetTasks(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"table_name":"users"`) {
		t.Fatalf("expected task in response, got %q", body)
	}
}

func TestHandleGetTask(t *testing.T) {
	content := `
[[databases]]
name = "src"
type = "sqlite"
path = "test.db"

[[tasks]]
table_name = "users"
sql = "SELECT 1"
source_db = "src"
target_db = "src"
mode = "replace"
allow_same_table = true
`
	srv, _ := newTestServer(t, content)

	// Existing task
	req := withChiParams(httptest.NewRequest(http.MethodGet, "/api/tasks/users", nil), "name", "users")
	rec := httptest.NewRecorder()
	srv.handleGetTask(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	// Non-existent task
	req = withChiParams(httptest.NewRequest(http.MethodGet, "/api/tasks/missing", nil), "name", "missing")
	rec = httptest.NewRecorder()
	srv.handleGetTask(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestHandleTriggerTask(t *testing.T) {
	srv, _ := newTestServer(t, "")

	// Without daemon running
	req := httptest.NewRequest(http.MethodPost, "/api/tasks/trigger", nil)
	rec := httptest.NewRecorder()
	srv.handleTriggerTask(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestHandleGetDatabases(t *testing.T) {
	content := `
[[databases]]
name = "src"
type = "sqlite"
path = "test.db"
user = "admin"
password = "secret"

[[tasks]]
table_name = "test"
sql = "SELECT 1"
source_db = "src"
target_db = "src"
mode = "replace"
allow_same_table = true
`
	srv, _ := newTestServer(t, content)
	req := httptest.NewRequest(http.MethodGet, "/api/databases", nil)
	rec := httptest.NewRecorder()

	srv.handleGetDatabases(rec, req)

	body := rec.Body.String()
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body: %s", rec.Code, body)
	}
	if !strings.Contains(body, `"name":"src"`) {
		t.Fatalf("expected database in response, got %q", body)
	}
	if strings.Contains(body, "secret") {
		t.Fatal("password should be masked")
	}
}

func TestHandleRunDoctor(t *testing.T) {
	content := `
[[databases]]
name = "src"
type = "sqlite"
path = "test.db"

[[tasks]]
table_name = "test"
sql = "SELECT 1"
source_db = "src"
target_db = "src"
mode = "replace"
allow_same_table = true
`
	srv, _ := newTestServer(t, content)
	req := httptest.NewRequest(http.MethodPost, "/api/doctor", nil)
	rec := httptest.NewRecorder()

	srv.handleRunDoctor(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"Name"`) {
		t.Fatalf("expected check results, got %q", body)
	}
}

func TestHandleGetDaemonStatus(t *testing.T) {
	srv, _ := newTestServer(t, "")
	req := httptest.NewRequest(http.MethodGet, "/api/daemon/status", nil)
	rec := httptest.NewRecorder()

	srv.handleGetDaemonStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"running":false`) {
		t.Fatalf("expected running=false, got %q", body)
	}
}

func TestHandleTestDatabase(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE test (id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	db.Close()

	content := `
[[databases]]
name = "src"
type = "sqlite"
path = "` + dbPath + `"

[[tasks]]
table_name = "test"
sql = "SELECT 1"
source_db = "src"
target_db = "src"
mode = "replace"
allow_same_table = true
`
	srv, _ := newTestServer(t, content)

	req := withChiParams(httptest.NewRequest(http.MethodPost, "/api/databases/src/test", nil), "name", "src")
	rec := httptest.NewRecorder()
	srv.handleTestDatabase(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"status":"ok"`) {
		t.Fatalf("expected ok status, got %q", body)
	}
}

func TestHandleGetTables(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	db.Close()

	content := `
[[databases]]
name = "src"
type = "sqlite"
path = "` + dbPath + `"

[[tasks]]
table_name = "test"
sql = "SELECT 1"
source_db = "src"
target_db = "src"
mode = "replace"
allow_same_table = true
`
	srv, _ := newTestServer(t, content)

	req := withChiParams(httptest.NewRequest(http.MethodGet, "/api/databases/src/tables", nil), "name", "src")
	rec := httptest.NewRecorder()
	srv.handleGetTables(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"users"`) {
		t.Fatalf("expected users table, got %q", body)
	}
}

func TestHandleGetTableSchema(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	db.Close()

	content := `
[[databases]]
name = "src"
type = "sqlite"
path = "` + dbPath + `"

[[tasks]]
table_name = "test"
sql = "SELECT 1"
source_db = "src"
target_db = "src"
mode = "replace"
allow_same_table = true
`
	srv, _ := newTestServer(t, content)

	req := withChiParams(httptest.NewRequest(http.MethodGet, "/api/databases/src/tables/users/schema", nil), "name", "src", "table", "users")
	rec := httptest.NewRecorder()
	srv.handleGetTableSchema(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"name":"users"`) && !strings.Contains(body, `"columns"`) {
		t.Fatalf("expected schema, got %q", body)
	}
}

func TestHandleGetTableIndexes(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE INDEX idx_name ON users(name)"); err != nil {
		t.Fatal(err)
	}
	db.Close()

	content := `
[[databases]]
name = "src"
type = "sqlite"
path = "` + dbPath + `"

[[tasks]]
table_name = "test"
sql = "SELECT 1"
source_db = "src"
target_db = "src"
mode = "replace"
allow_same_table = true
`
	srv, _ := newTestServer(t, content)

	req := withChiParams(httptest.NewRequest(http.MethodGet, "/api/databases/src/tables/users/indexes", nil), "name", "src", "table", "users")
	rec := httptest.NewRecorder()
	srv.handleGetTableIndexes(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d, body: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleGetHistory(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "history.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	content := `
[[databases]]
name = "src"
type = "sqlite"
path = "` + dbPath + `"

[[tasks]]
table_name = "test"
sql = "SELECT 1"
source_db = "src"
target_db = "src"
mode = "replace"
allow_same_table = true

[history]
enabled = true
table_name = "db_ferry_migrations"
`
	srv, _ := newTestServer(t, content)

	req := httptest.NewRequest(http.MethodGet, "/api/history?limit=10", nil)
	rec := httptest.NewRecorder()
	srv.handleGetHistory(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestHandleCompareHistory(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "history.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE db_ferry_migrations (
		id TEXT PRIMARY KEY,
		config_hash TEXT,
		started_at TEXT,
		finished_at TEXT,
		task_name TEXT,
		source_db TEXT,
		target_db TEXT,
		mode TEXT,
		rows_processed INTEGER,
		rows_failed INTEGER,
		validation_result TEXT,
		error_message TEXT,
		version TEXT
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO db_ferry_migrations
		(id, config_hash, started_at, finished_at, task_name, source_db, target_db, mode, rows_processed, rows_failed, validation_result, error_message, version)
		VALUES ('id1', 'hash1', '2026-01-01T00:00:00Z', '2026-01-01T00:01:00Z', 'test', 'src', 'src', 'replace', 100, 0, 'ok', '', '1.0')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO db_ferry_migrations
		(id, config_hash, started_at, finished_at, task_name, source_db, target_db, mode, rows_processed, rows_failed, validation_result, error_message, version)
		VALUES ('id2', 'hash2', '2026-01-02T00:00:00Z', '2026-01-02T00:01:00Z', 'test', 'src', 'src', 'replace', 200, 0, 'ok', '', '1.0')`); err != nil {
		t.Fatal(err)
	}
	db.Close()

	content := `
[[databases]]
name = "src"
type = "sqlite"
path = "` + dbPath + `"

[[tasks]]
table_name = "test"
sql = "SELECT 1"
source_db = "src"
target_db = "src"
mode = "replace"
allow_same_table = true

[history]
enabled = true
table_name = "db_ferry_migrations"
`
	srv, _ := newTestServer(t, content)

	// Missing params
	req := httptest.NewRequest(http.MethodGet, "/api/history/compare", nil)
	rec := httptest.NewRecorder()
	srv.handleCompareHistory(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	// Valid compare
	req = httptest.NewRequest(http.MethodGet, "/api/history/compare?id1=id1&id2=id2", nil)
	rec = httptest.NewRecorder()
	srv.handleCompareHistory(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"left"`) || !strings.Contains(body, `"right"`) {
		t.Fatalf("expected left and right in response, got %q", body)
	}

	// Not found
	req = httptest.NewRequest(http.MethodGet, "/api/history/compare?id1=missing&id2=missing2", nil)
	rec = httptest.NewRecorder()
	srv.handleCompareHistory(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestHandleGetTasksWithSSE(t *testing.T) {
	content := `
[[databases]]
name = "src"
type = "sqlite"
path = "test.db"

[[tasks]]
table_name = "users"
sql = "SELECT 1"
source_db = "src"
target_db = "src"
mode = "replace"
allow_same_table = true
`
	srv, _ := newTestServer(t, content)
	if srv.sseServer != nil {
		srv.sseServer.Send(sse.Event{
			Type: sse.EventTaskProgress,
			Data: sse.TaskProgressData{
				Task:       "users",
				Processed:  50,
				Percentage: 50.0,
				DurationMs: 1000,
			},
		})
	}

	req := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	rec := httptest.NewRecorder()
	srv.handleGetTasks(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"processed":50`) {
		t.Fatalf("expected processed=50 in response, got %q", body)
	}
}

func TestHandleGetTaskWithSSE(t *testing.T) {
	content := `
[[databases]]
name = "src"
type = "sqlite"
path = "test.db"

[[tasks]]
table_name = "users"
sql = "SELECT 1"
source_db = "src"
target_db = "src"
mode = "replace"
allow_same_table = true
`
	srv, _ := newTestServer(t, content)
	if srv.sseServer != nil {
		srv.sseServer.Send(sse.Event{
			Type: sse.EventTaskComplete,
			Data: sse.TaskProgressData{
				Task:       "users",
				Processed:  100,
				Percentage: 100.0,
				DurationMs: 2000,
			},
		})
	}

	req := withChiParams(httptest.NewRequest(http.MethodGet, "/api/tasks/users", nil), "name", "users")
	rec := httptest.NewRecorder()
	srv.handleGetTask(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"status":"completed"`) {
		t.Fatalf("expected status=completed in response, got %q", body)
	}
}

func TestHandleTriggerTaskRunning(t *testing.T) {
	srv, _ := newTestServer(t, "")
	// Simulate a running daemon by setting the internal state
	srv.daemon = nil

	req := httptest.NewRequest(http.MethodPost, "/api/tasks/trigger", nil)
	rec := httptest.NewRecorder()
	srv.handleTriggerTask(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestHandleGetConfigError(t *testing.T) {
	srv := New(Options{ConfigPath: "/nonexistent/path/task.toml"})
	req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	rec := httptest.NewRecorder()
	srv.handleGetConfig(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}

func TestHandlePutConfigError(t *testing.T) {
	srv := New(Options{ConfigPath: "/nonexistent/path/task.toml"})
	req := httptest.NewRequest(http.MethodPut, "/api/config", bytes.NewReader([]byte("new")))
	rec := httptest.NewRecorder()
	srv.handlePutConfig(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}

func TestHandleGetTablesNotFound(t *testing.T) {
	content := `
[[databases]]
name = "src"
type = "sqlite"
path = "test.db"

[[tasks]]
table_name = "test"
sql = "SELECT 1"
source_db = "src"
target_db = "src"
mode = "replace"
allow_same_table = true
`
	srv, _ := newTestServer(t, content)
	req := withChiParams(httptest.NewRequest(http.MethodGet, "/api/databases/missing/tables", nil), "name", "missing")
	rec := httptest.NewRecorder()
	srv.handleGetTables(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestHandleGetTableSchemaNotFound(t *testing.T) {
	content := `
[[databases]]
name = "src"
type = "sqlite"
path = "test.db"

[[tasks]]
table_name = "test"
sql = "SELECT 1"
source_db = "src"
target_db = "src"
mode = "replace"
allow_same_table = true
`
	srv, _ := newTestServer(t, content)
	req := withChiParams(httptest.NewRequest(http.MethodGet, "/api/databases/missing/tables/users/schema", nil), "name", "missing", "table", "users")
	rec := httptest.NewRecorder()
	srv.handleGetTableSchema(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestRequestLogMiddleware(t *testing.T) {
	mw := requestLogMiddleware
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestServerStartStop(t *testing.T) {
	srv, _ := newTestServer(t, "")

	if err := srv.Start(":0"); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	if srv.Addr() == "" {
		t.Fatal("expected non-empty address")
	}

	// Give server a moment to start
	time.Sleep(50 * time.Millisecond)

	if err := srv.Stop(); err != nil {
		t.Fatalf("failed to stop server: %v", err)
	}
}

func TestBasicAuthMiddleware(t *testing.T) {
	mw := basicAuthMiddleware("admin", "admin")
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// No auth
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}

	// Wrong auth
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Basic "+string([]byte("wrong:creds")))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}

	// Correct auth
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Basic YWRtaW46YWRtaW4=")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}
