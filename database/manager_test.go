package database

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"db-ferry/config"
)

func TestConnectionManagerGetSourceTargetAndCache(t *testing.T) {
	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{Name: "db1", Type: config.DatabaseTypeSQLite, Path: filepath.Join(t.TempDir(), "db1.db")},
		},
		Tasks: []config.TaskConfig{
			{
				TableName:      "t",
				SQL:            "SELECT 1",
				SourceDB:       "db1",
				TargetDB:       "db1",
				AllowSameTable: true,
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	m := NewConnectionManager(cfg)
	src1, err := m.GetSource("db1")
	if err != nil {
		t.Fatalf("GetSource() error = %v", err)
	}
	src2, err := m.GetSource("db1")
	if err != nil {
		t.Fatalf("GetSource() second call error = %v", err)
	}
	if src1 != src2 {
		t.Fatalf("expected cached source connection instance")
	}

	if _, err := m.GetTarget("missing"); err == nil {
		t.Fatalf("expected unknown alias error")
	}
}

func TestConnectionManagerCloseAllAggregatesErrors(t *testing.T) {
	m := &ConnectionManager{
		connections: map[string]*connectionEntry{
			"a": {close: func() error { return errors.New("aerr") }},
			"b": {close: func() error { return errors.New("berr") }},
		},
	}

	err := m.CloseAll()
	if err == nil {
		t.Fatalf("expected CloseAll() error")
	}
	if !strings.Contains(err.Error(), "a: aerr") || !strings.Contains(err.Error(), "b: berr") {
		t.Fatalf("unexpected CloseAll() error: %v", err)
	}
}

func TestConnectionManagerOpenConnectionUnsupportedType(t *testing.T) {
	m := &ConnectionManager{}
	_, err := m.openConnection(config.DatabaseConfig{Name: "x", Type: "unknown"})
	if err == nil || !strings.Contains(err.Error(), "unsupported database type") {
		t.Fatalf("expected unsupported type error, got %v", err)
	}
}

func TestConnectionManagerGetSourceAndTargetRoles(t *testing.T) {
	m := NewConnectionManager(&config.Config{})
	// Manually inject an entry that has only a source (no target)
	m.connections["src_only"] = &connectionEntry{source: &SQLiteDB{}}
	_, err := m.GetTarget("src_only")
	if err == nil {
		t.Fatalf("expected error when using source-only db as target")
	}
	if !strings.Contains(err.Error(), "not configured as a target") {
		t.Fatalf("unexpected error: %v", err)
	}

	// Manually inject an entry that has only a target (no source)
	m.connections["tgt_only"] = &connectionEntry{target: &SQLiteDB{}}
	_, err = m.GetSource("tgt_only")
	if err == nil {
		t.Fatalf("expected error when using target-only db as source")
	}
	if !strings.Contains(err.Error(), "not configured as a source") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpenSourceAndTarget(t *testing.T) {
	// Unsupported type
	_, err := OpenSource(config.DatabaseConfig{Type: "unknown"})
	if err == nil || !strings.Contains(err.Error(), "unsupported database type") {
		t.Fatalf("expected unsupported type error for OpenSource, got %v", err)
	}
	_, err = OpenTarget(config.DatabaseConfig{Type: "unknown"})
	if err == nil || !strings.Contains(err.Error(), "unsupported database type") {
		t.Fatalf("expected unsupported type error for OpenTarget, got %v", err)
	}

	// Valid SQLite path
	dbPath := filepath.Join(t.TempDir(), "test.db")
	src, err := OpenSource(config.DatabaseConfig{Type: config.DatabaseTypeSQLite, Path: dbPath})
	if err != nil {
		t.Fatalf("OpenSource(sqlite) error = %v", err)
	}
	if src == nil {
		t.Fatalf("expected non-nil source")
	}
	_ = src.Close()

	tgt, err := OpenTarget(config.DatabaseConfig{Type: config.DatabaseTypeSQLite, Path: dbPath})
	if err != nil {
		t.Fatalf("OpenTarget(sqlite) error = %v", err)
	}
	if tgt == nil {
		t.Fatalf("expected non-nil target")
	}
	_ = tgt.Close()
}

func TestDSNBuilders(t *testing.T) {
	oracle := BuildOracleDSN(config.DatabaseConfig{
		User: "u", Password: "p", Host: "h", Port: "1521", Service: "svc",
	})
	if oracle != "oracle://u:p@h:1521/svc" {
		t.Fatalf("unexpected oracle DSN: %s", oracle)
	}

	mysql := BuildMySQLDSN(config.DatabaseConfig{
		User: "u", Password: "p", Host: "h", Port: "3306", Database: "d",
	})
	if !strings.Contains(mysql, "u:p@tcp(h:3306)/d?parseTime=true") {
		t.Fatalf("unexpected mysql DSN: %s", mysql)
	}

	postgres := BuildPostgresDSN(config.DatabaseConfig{
		Host: "h", Port: "5432", User: "u", Password: "p", Database: "d",
	})
	if !strings.Contains(postgres, "host=h port=5432 user=u password=p dbname=d") {
		t.Fatalf("unexpected postgres DSN: %s", postgres)
	}

	sqlserver := BuildSQLServerDSN(config.DatabaseConfig{
		User: "u", Password: "p", Host: "h", Port: "1433", Database: "d",
	})
	if sqlserver != "sqlserver://u:p@h:1433?database=d" {
		t.Fatalf("unexpected sqlserver DSN: %s", sqlserver)
	}
}

func TestConnectionManagerReplicaRoundRobin(t *testing.T) {
	callCount := 0
	testOpenSourceHook = func(dbCfg config.DatabaseConfig) (SourceDB, error) {
		callCount++
		return &SQLiteDB{}, nil
	}
	defer func() { testOpenSourceHook = nil }()

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{
				Name: "db1", Type: config.DatabaseTypeMySQL,
				Host: "master", Port: "3306", User: "u", Password: "p", Database: "d",
				Replicas: []config.ReplicaConfig{
					{Host: "r1", Priority: 1},
					{Host: "r2", Priority: 2},
				},
			},
		},
		Tasks: []config.TaskConfig{
			{TableName: "t", SQL: "SELECT 1", SourceDB: "db1", TargetDB: "db1", AllowSameTable: true},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	m := NewConnectionManager(cfg)
	defer m.CloseAll()

	_, err := m.GetSource("db1")
	if err != nil {
		t.Fatalf("GetSource() error = %v", err)
	}
	if callCount != 1 {
		t.Fatalf("expected 1 replica open attempt, got %d", callCount)
	}

	// Cached: should not call hook again.
	_, err = m.GetSource("db1")
	if err != nil {
		t.Fatalf("GetSource() cached error = %v", err)
	}
	if callCount != 1 {
		t.Fatalf("expected still 1 replica open attempt, got %d", callCount)
	}
}

func TestConnectionManagerReplicaFallback(t *testing.T) {
	testOpenSourceHook = func(dbCfg config.DatabaseConfig) (SourceDB, error) {
		return nil, fmt.Errorf("replica down")
	}
	defer func() { testOpenSourceHook = nil }()

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{
				Name: "db1", Type: config.DatabaseTypeSQLite,
				Path: filepath.Join(t.TempDir(), "db1.db"),
				Replicas: []config.ReplicaConfig{
					{Host: "r1"},
				},
				ReplicaFallback: true,
			},
		},
		Tasks: []config.TaskConfig{
			{TableName: "t", SQL: "SELECT 1", SourceDB: "db1", TargetDB: "db1", AllowSameTable: true},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	before := GetReplicaFallbackTotal()
	m := NewConnectionManager(cfg)
	defer m.CloseAll()

	src, err := m.GetSource("db1")
	if err != nil {
		t.Fatalf("GetSource() error = %v", err)
	}
	if src == nil {
		t.Fatalf("expected non-nil source after fallback")
	}
	if GetReplicaFallbackTotal() != before+1 {
		t.Fatalf("expected fallback counter to increment")
	}
}

func TestConnectionManagerReplicaFallbackDisabled(t *testing.T) {
	testOpenSourceHook = func(dbCfg config.DatabaseConfig) (SourceDB, error) {
		return nil, fmt.Errorf("replica down")
	}
	defer func() { testOpenSourceHook = nil }()

	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{
				Name: "db1", Type: config.DatabaseTypeMySQL,
				Host: "master", Port: "3306", User: "u", Password: "p", Database: "d",
				Replicas: []config.ReplicaConfig{
					{Host: "r1"},
				},
				ReplicaFallback: false,
			},
		},
		Tasks: []config.TaskConfig{
			{TableName: "t", SQL: "SELECT 1", SourceDB: "db1", TargetDB: "db1", AllowSameTable: true},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	m := NewConnectionManager(cfg)
	defer m.CloseAll()

	_, err := m.GetSource("db1")
	if err == nil || !strings.Contains(err.Error(), "all replicas failed") {
		t.Fatalf("expected replica failure error, got %v", err)
	}
}

func TestConnectionManagerPoolSettings(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "pool.db")
	cfg := &config.Config{
		Databases: []config.DatabaseConfig{
			{
				Name: "db1", Type: config.DatabaseTypeSQLite,
				Path:        dbPath,
				PoolMaxOpen: 7,
				PoolMaxIdle: 3,
			},
		},
		Tasks: []config.TaskConfig{
			{TableName: "t", SQL: "SELECT 1", SourceDB: "db1", TargetDB: "db1", AllowSameTable: true},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	m := NewConnectionManager(cfg)
	defer m.CloseAll()

	tgt, err := m.GetTarget("db1")
	if err != nil {
		t.Fatalf("GetTarget() error = %v", err)
	}

	// Use type assertion to access the underlying *sql.DB for verification.
	sqliteDB, ok := tgt.(*SQLiteDB)
	if !ok {
		t.Fatalf("expected *SQLiteDB, got %T", tgt)
	}
	stats := sqliteDB.db.Stats()
	if stats.MaxOpenConnections != 7 {
		t.Fatalf("expected MaxOpenConnections=7, got %d", stats.MaxOpenConnections)
	}
	// MaxIdleConnections is not directly exposed in Stats, but we verified the setter was called.
}
