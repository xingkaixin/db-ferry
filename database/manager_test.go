package database

import (
	"errors"
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

func TestDSNBuilders(t *testing.T) {
	oracle := buildOracleDSN(config.DatabaseConfig{
		User: "u", Password: "p", Host: "h", Port: "1521", Service: "svc",
	})
	if oracle != "oracle://u:p@h:1521/svc" {
		t.Fatalf("unexpected oracle DSN: %s", oracle)
	}

	mysql := buildMySQLDSN(config.DatabaseConfig{
		User: "u", Password: "p", Host: "h", Port: "3306", Database: "d",
	})
	if !strings.Contains(mysql, "u:p@tcp(h:3306)/d?parseTime=true") {
		t.Fatalf("unexpected mysql DSN: %s", mysql)
	}

	postgres := buildPostgresDSN(config.DatabaseConfig{
		Host: "h", Port: "5432", User: "u", Password: "p", Database: "d",
	})
	if !strings.Contains(postgres, "host=h port=5432 user=u password=p dbname=d") {
		t.Fatalf("unexpected postgres DSN: %s", postgres)
	}

	sqlserver := buildSQLServerDSN(config.DatabaseConfig{
		User: "u", Password: "p", Host: "h", Port: "1433", Database: "d",
	})
	if sqlserver != "sqlserver://u:p@h:1433?database=d" {
		t.Fatalf("unexpected sqlserver DSN: %s", sqlserver)
	}
}
