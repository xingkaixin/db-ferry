package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func baseConfig(t *testing.T) *Config {
	t.Helper()
	dir := t.TempDir()
	return &Config{
		Databases: []DatabaseConfig{
			{Name: "src", Type: DatabaseTypeSQLite, Path: filepath.Join(dir, "src.db")},
			{Name: "dst", Type: DatabaseTypeSQLite, Path: filepath.Join(dir, "dst.db")},
		},
		Tasks: []TaskConfig{
			{
				TableName: "users",
				SQL:       "SELECT id, name FROM users",
				SourceDB:  "src",
				TargetDB:  "dst",
			},
		},
	}
}

func TestIndexConfigParseColumns(t *testing.T) {
	t.Run("supports order shorthand", func(t *testing.T) {
		idx := IndexConfig{Columns: []string{"id", "name:DESC", "created_at:1"}}
		if err := idx.ParseColumns(); err != nil {
			t.Fatalf("ParseColumns() error = %v", err)
		}
		if got := idx.ParsedColumns[0].Order; got != "ASC" {
			t.Fatalf("expected ASC, got %s", got)
		}
		if got := idx.ParsedColumns[1].Order; got != "DESC" {
			t.Fatalf("expected DESC, got %s", got)
		}
	})

	t.Run("rejects invalid order specifier", func(t *testing.T) {
		idx := IndexConfig{Columns: []string{"id:foo"}}
		err := idx.ParseColumns()
		if err == nil || !strings.Contains(err.Error(), "invalid order specifier") {
			t.Fatalf("expected invalid order specifier error, got %v", err)
		}
	})
}

func TestNormalizeKeys(t *testing.T) {
	keys, err := normalizeKeys([]string{" id ", "Name"})
	if err != nil {
		t.Fatalf("normalizeKeys() error = %v", err)
	}
	if len(keys) != 2 || keys[0] != "id" || keys[1] != "Name" {
		t.Fatalf("unexpected normalized keys: %#v", keys)
	}

	if _, err := normalizeKeys([]string{"id", "ID"}); err == nil {
		t.Fatalf("expected duplicate key error")
	}
	if _, err := normalizeKeys([]string{"id", " "}); err == nil {
		t.Fatalf("expected empty key error")
	}
}

func TestValidateNormalizesTaskDefaults(t *testing.T) {
	cfg := baseConfig(t)
	cfg.Tasks[0].Mode = "upsert"
	cfg.Tasks[0].MergeKeys = []string{"id"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	task := cfg.Tasks[0]
	if task.Mode != TaskModeMerge {
		t.Fatalf("expected mode merge after normalization, got %s", task.Mode)
	}
	if task.Validate != TaskValidateNone {
		t.Fatalf("expected default validate none, got %s", task.Validate)
	}
}

func TestValidateRejectsInvalidTaskConstraints(t *testing.T) {
	t.Run("merge mode requires merge_keys", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Mode = TaskModeMerge
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "merge_keys is required") {
			t.Fatalf("expected merge_keys required error, got %v", err)
		}
	})

	t.Run("non-merge mode rejects merge_keys", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Mode = TaskModeReplace
		cfg.Tasks[0].MergeKeys = []string{"id"}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "merge_keys is only valid") {
			t.Fatalf("expected merge_keys mode error, got %v", err)
		}
	})

	t.Run("same source and target requires allow_same_table", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].TargetDB = "src"
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "allow_same_table") {
			t.Fatalf("expected allow_same_table error, got %v", err)
		}
	})

	t.Run("state_file requires resume_key", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].StateFile = "state.json"
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "state_file requires resume_key") {
			t.Fatalf("expected state_file requires resume_key error, got %v", err)
		}
	})

	t.Run("resume_key requires resume_from or state_file", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].ResumeKey = "id"
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "resume_key requires resume_from or state_file") {
			t.Fatalf("expected resume_key requires source error, got %v", err)
		}
	})
}

func TestValidateIndexRules(t *testing.T) {
	t.Run("duplicate index names rejected", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks = append(cfg.Tasks, TaskConfig{
			TableName: "orders",
			SQL:       "SELECT id FROM orders",
			SourceDB:  "src",
			TargetDB:  "dst",
			Indexes: []IndexConfig{
				{Name: "idx_dup", Columns: []string{"id"}},
			},
		})
		cfg.Tasks[0].Indexes = []IndexConfig{
			{Name: "idx_dup", Columns: []string{"id"}},
		}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "index name 'idx_dup' already used") {
			t.Fatalf("expected duplicate index error, got %v", err)
		}
	})

	t.Run("partial index only allowed for sqlite target", func(t *testing.T) {
		cfg := &Config{
			Databases: []DatabaseConfig{
				{Name: "src", Type: DatabaseTypeSQLite, Path: filepath.Join(t.TempDir(), "src.db")},
				{
					Name:     "mysql_dst",
					Type:     DatabaseTypeMySQL,
					Host:     "127.0.0.1",
					Port:     "3306",
					User:     "u",
					Password: "p",
					Database: "db",
				},
			},
			Tasks: []TaskConfig{
				{
					TableName: "users",
					SQL:       "SELECT id FROM users",
					SourceDB:  "src",
					TargetDB:  "mysql_dst",
					Indexes: []IndexConfig{
						{Name: "idx_users_id", Columns: []string{"id"}, Where: "id > 0"},
					},
				},
			},
		}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "partial indexes") {
			t.Fatalf("expected partial index restriction error, got %v", err)
		}
	})
}

func TestLoadConfigGetDatabaseAndMapCopy(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "task.toml")
	srcPath := filepath.Join(dir, "src.db")
	dstPath := filepath.Join(dir, "dst.db")

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
		`table_name = "users"`,
		`sql = "SELECT id FROM users"`,
		`source_db = "src"`,
		`target_db = "dst"`,
	}, "\n")

	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	db, ok := cfg.GetDatabase("src")
	if !ok || db.Name != "src" {
		t.Fatalf("GetDatabase() mismatch: ok=%v db=%+v", ok, db)
	}

	m := cfg.DatabasesMap()
	delete(m, "src")
	if _, stillExists := cfg.GetDatabase("src"); !stillExists {
		t.Fatalf("DatabasesMap() should return a copy")
	}
}

func TestValidateDatabaseConfig(t *testing.T) {
	cases := []struct {
		name    string
		db      DatabaseConfig
		wantErr bool
	}{
		{
			name:    "oracle valid",
			db:      DatabaseConfig{Type: DatabaseTypeOracle, Host: "h", User: "u", Password: "p", Service: "svc"},
			wantErr: false,
		},
		{
			name:    "oracle missing host",
			db:      DatabaseConfig{Type: DatabaseTypeOracle, User: "u", Password: "p", Service: "svc"},
			wantErr: true,
		},
		{
			name:    "mysql valid",
			db:      DatabaseConfig{Type: DatabaseTypeMySQL, Host: "h", User: "u", Password: "p", Database: "d"},
			wantErr: false,
		},
		{
			name:    "postgresql valid",
			db:      DatabaseConfig{Type: DatabaseTypePostgreSQL, Host: "h", User: "u", Password: "p", Database: "d"},
			wantErr: false,
		},
		{
			name:    "sqlserver valid",
			db:      DatabaseConfig{Type: DatabaseTypeSQLServer, Host: "h", User: "u", Password: "p", Database: "d"},
			wantErr: false,
		},
		{
			name:    "sqlite valid",
			db:      DatabaseConfig{Type: DatabaseTypeSQLite, Path: "x.db"},
			wantErr: false,
		},
		{
			name:    "duckdb valid",
			db:      DatabaseConfig{Type: DatabaseTypeDuckDB, Path: "x.duckdb"},
			wantErr: false,
		},
		{
			name:    "unsupported type",
			db:      DatabaseConfig{Type: "mongo"},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		err := validateDatabaseConfig(&tc.db)
		if tc.wantErr && err == nil {
			t.Fatalf("%s: expected error", tc.name)
		}
		if !tc.wantErr && err != nil {
			t.Fatalf("%s: unexpected error: %v", tc.name, err)
		}
	}
}

func TestEnsureDatabaseSupportsSourceAndTarget(t *testing.T) {
	okDB := DatabaseConfig{Name: "src", Type: DatabaseTypeMySQL}
	if err := ensureDatabaseSupportsSource(&okDB); err != nil {
		t.Fatalf("ensureDatabaseSupportsSource() error = %v", err)
	}
	if err := ensureDatabaseSupportsTarget(&okDB); err != nil {
		t.Fatalf("ensureDatabaseSupportsTarget() error = %v", err)
	}

	bad := DatabaseConfig{Name: "x", Type: "mongo"}
	if err := ensureDatabaseSupportsSource(&bad); err == nil {
		t.Fatalf("expected source support error")
	}
	if err := ensureDatabaseSupportsTarget(&bad); err == nil {
		t.Fatalf("expected target support error")
	}
}

func TestValidateGeneralFailuresAndDefaults(t *testing.T) {
	t.Run("requires databases", func(t *testing.T) {
		cfg := &Config{Tasks: []TaskConfig{{TableName: "t", SQL: "SELECT 1", SourceDB: "a", TargetDB: "b"}}}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("expected missing databases error")
		}
	})

	t.Run("requires tasks", func(t *testing.T) {
		cfg := &Config{Databases: []DatabaseConfig{{Name: "a", Type: DatabaseTypeSQLite, Path: "a.db"}}}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("expected missing tasks error")
		}
	})

	t.Run("applies default ports", func(t *testing.T) {
		cfg := &Config{
			Databases: []DatabaseConfig{
				{Name: "ora", Type: DatabaseTypeOracle, Host: "h", User: "u", Password: "p", Service: "svc"},
				{Name: "my", Type: DatabaseTypeMySQL, Host: "h", User: "u", Password: "p", Database: "d"},
				{Name: "pg", Type: DatabaseTypePostgreSQL, Host: "h", User: "u", Password: "p", Database: "d"},
				{Name: "ss", Type: DatabaseTypeSQLServer, Host: "h", User: "u", Password: "p", Database: "d"},
			},
			Tasks: []TaskConfig{
				{TableName: "t", SQL: "SELECT 1", SourceDB: "ora", TargetDB: "my", AllowSameTable: false},
			},
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
		dbs := cfg.DatabasesMap()
		if dbs["ora"].Port != "1521" || dbs["my"].Port != "3306" || dbs["pg"].Port != "5432" || dbs["ss"].Port != "1433" {
			t.Fatalf("unexpected default ports: %+v", dbs)
		}
	})
}

func TestValidateTaskFieldErrors(t *testing.T) {
	t.Run("invalid mode", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Mode = "invalid"
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "mode must be") {
			t.Fatalf("expected invalid mode error, got %v", err)
		}
	})

	t.Run("invalid validate mode", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Validate = "bad"
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "validate must be") {
			t.Fatalf("expected invalid validate mode error, got %v", err)
		}
	})

	t.Run("negative batch size", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].BatchSize = -1
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "batch_size must be >= 0") {
			t.Fatalf("expected negative batch size error, got %v", err)
		}
	})

	t.Run("negative max retries", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].MaxRetries = -1
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "max_retries must be >= 0") {
			t.Fatalf("expected negative max retries error, got %v", err)
		}
	})
}

func TestValidateDatabaseDefinitionErrors(t *testing.T) {
	t.Run("database name required", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Databases[0].Name = ""
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "name is required") {
			t.Fatalf("expected database name required error, got %v", err)
		}
	})

	t.Run("duplicate database name", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Databases[1].Name = cfg.Databases[0].Name
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate database name") {
			t.Fatalf("expected duplicate database name error, got %v", err)
		}
	})

	t.Run("missing source database alias", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].SourceDB = "missing"
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "source_db 'missing' is not defined") {
			t.Fatalf("expected missing source_db alias error, got %v", err)
		}
	})

	t.Run("missing target database alias", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].TargetDB = "missing"
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "target_db 'missing' is not defined") {
			t.Fatalf("expected missing target_db alias error, got %v", err)
		}
	})
}
