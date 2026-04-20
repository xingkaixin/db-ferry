package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

	t.Run("shard requires resume_key", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Shard = ShardConfig{Enabled: true, Shards: 4}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "shard requires resume_key") {
			t.Fatalf("expected shard requires resume_key error, got %v", err)
		}
	})

	t.Run("shard requires shards > 1", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].ResumeKey = "id"
		cfg.Tasks[0].ResumeFrom = "0"
		cfg.Tasks[0].Shard = ShardConfig{Enabled: true, Shards: 1}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "shard.shards must be > 1") {
			t.Fatalf("expected shard.shards error, got %v", err)
		}
	})

	t.Run("shard rejected in replace mode", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Mode = TaskModeReplace
		cfg.Tasks[0].ResumeKey = "id"
		cfg.Tasks[0].ResumeFrom = "0"
		cfg.Tasks[0].Shard = ShardConfig{Enabled: true, Shards: 4}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "shard is not supported in replace mode") {
			t.Fatalf("expected shard replace mode error, got %v", err)
		}
	})

	t.Run("shard rejected with state_file", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Mode = TaskModeAppend
		cfg.Tasks[0].ResumeKey = "id"
		cfg.Tasks[0].StateFile = "state.json"
		cfg.Tasks[0].Shard = ShardConfig{Enabled: true, Shards: 4}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "state_file is not supported with shard") {
			t.Fatalf("expected shard state_file error, got %v", err)
		}
	})

	t.Run("shard valid in append mode", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Mode = TaskModeAppend
		cfg.Tasks[0].ResumeKey = "id"
		cfg.Tasks[0].ResumeFrom = "0"
		cfg.Tasks[0].Shard = ShardConfig{Enabled: true, Shards: 4}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected shard append mode to pass, got %v", err)
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

func TestLoadConfigInvalidFile(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/task.toml")
	if err == nil {
		t.Fatalf("expected error for missing file")
	}

	dir := t.TempDir()
	badPath := filepath.Join(dir, "bad.toml")
	if err := os.WriteFile(badPath, []byte("not valid toml = ["), 0o644); err != nil {
		t.Fatalf("write file error = %v", err)
	}
	_, err = LoadConfig(badPath)
	if err == nil {
		t.Fatalf("expected error for invalid toml")
	}
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

func TestResolveReplicaConfig(t *testing.T) {
	primary := DatabaseConfig{
		Name:     "prod",
		Type:     DatabaseTypeMySQL,
		Host:     "master.internal",
		Port:     "3306",
		Database: "db",
		User:     "u",
		Password: "p",
		Replicas: []ReplicaConfig{
			{Host: "replica1.internal", Port: "3307", Priority: 1},
		},
	}

	resolved := primary.ResolveReplicaConfig(primary.Replicas[0])
	if resolved.Host != "replica1.internal" {
		t.Fatalf("expected host replica1.internal, got %s", resolved.Host)
	}
	if resolved.Port != "3307" {
		t.Fatalf("expected port 3307, got %s", resolved.Port)
	}
	if resolved.User != "u" {
		t.Fatalf("expected user u, got %s", resolved.User)
	}
	if resolved.Password != "p" {
		t.Fatalf("expected password p, got %s", resolved.Password)
	}
	if resolved.Database != "db" {
		t.Fatalf("expected database db, got %s", resolved.Database)
	}
	if len(resolved.Replicas) != 0 {
		t.Fatalf("expected replicas to be cleared, got %v", resolved.Replicas)
	}
}

func TestValidateDatabaseConfigWithReplicas(t *testing.T) {
	valid := DatabaseConfig{
		Type:     DatabaseTypeMySQL,
		Host:     "h",
		User:     "u",
		Password: "p",
		Database: "d",
		Replicas: []ReplicaConfig{
			{Host: "r1", Priority: 1},
		},
	}
	if err := validateDatabaseConfig(&valid); err != nil {
		t.Fatalf("expected valid config with replicas, got %v", err)
	}

	invalid := DatabaseConfig{
		Type:     DatabaseTypeMySQL,
		Host:     "h",
		User:     "u",
		Password: "p",
		Database: "d",
		Replicas: []ReplicaConfig{
			{Host: ""},
		},
	}
	if err := validateDatabaseConfig(&invalid); err == nil || !strings.Contains(err.Error(), "replica 1: host is required") {
		t.Fatalf("expected replica host required error, got %v", err)
	}
}

func TestValidateTLSConfig(t *testing.T) {
	t.Run("invalid ssl_mode rejected", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Databases[0].SSLMode = "invalid"
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "unsupported ssl_mode") {
			t.Fatalf("expected unsupported ssl_mode error, got %v", err)
		}
	})

	t.Run("missing certificate file fails", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Databases[0].SSLMode = SSLModeRequire
		cfg.Databases[0].SSLCert = "/nonexistent/cert.pem"
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "ssl_cert file not found") {
			t.Fatalf("expected ssl_cert file not found error, got %v", err)
		}
	})

	t.Run("valid tls config passes", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Databases[0].SSLMode = SSLModeRequire
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected valid TLS config to pass, got %v", err)
		}
	})

	t.Run("ssl_mode defaults to disable", func(t *testing.T) {
		cfg := baseConfig(t)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
		db, _ := cfg.GetDatabase("src")
		if db.SSLMode != SSLModeDisable {
			t.Fatalf("expected default ssl_mode disable, got %s", db.SSLMode)
		}
	})
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

	t.Run("checksum validate ok", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Validate = TaskValidateChecksum
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected checksum validate to pass, got %v", err)
		}
	})

	t.Run("sample validate requires sample size", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Validate = TaskValidateSample
		cfg.Tasks[0].ValidateSampleSize = 0
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "validate_sample_size must be > 0") {
			t.Fatalf("expected sample size error, got %v", err)
		}
	})

	t.Run("sample validate ok", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Validate = TaskValidateSample
		cfg.Tasks[0].ValidateSampleSize = 500
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected sample validate to pass, got %v", err)
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

	t.Run("adaptive batch min_size must be > 0 when enabled", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].AdaptiveBatch = AdaptiveBatchConfig{Enabled: true, MinSize: 0, MaxSize: 100, TargetLatencyMs: 100, MemoryLimitMB: 10}
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "adaptive_batch.min_size must be > 0") {
			t.Fatalf("expected adaptive batch min_size error, got %v", err)
		}
	})

	t.Run("adaptive batch max_size must be >= min_size", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].AdaptiveBatch = AdaptiveBatchConfig{Enabled: true, MinSize: 200, MaxSize: 100, TargetLatencyMs: 100, MemoryLimitMB: 10}
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "adaptive_batch.max_size must be >= min_size") {
			t.Fatalf("expected adaptive batch max_size error, got %v", err)
		}
	})

	t.Run("adaptive batch target_latency_ms must be > 0", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].AdaptiveBatch = AdaptiveBatchConfig{Enabled: true, MinSize: 100, MaxSize: 1000, TargetLatencyMs: 0, MemoryLimitMB: 10}
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "adaptive_batch.target_latency_ms must be > 0") {
			t.Fatalf("expected adaptive batch target_latency_ms error, got %v", err)
		}
	})

	t.Run("adaptive batch memory_limit_mb must be > 0", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].AdaptiveBatch = AdaptiveBatchConfig{Enabled: true, MinSize: 100, MaxSize: 1000, TargetLatencyMs: 100, MemoryLimitMB: 0}
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "adaptive_batch.memory_limit_mb must be > 0") {
			t.Fatalf("expected adaptive batch memory_limit_mb error, got %v", err)
		}
	})

	t.Run("adaptive batch valid config passes", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].AdaptiveBatch = AdaptiveBatchConfig{Enabled: true, MinSize: 100, MaxSize: 1000, TargetLatencyMs: 100, MemoryLimitMB: 10}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected adaptive batch config to pass, got %v", err)
		}
	})

	t.Run("invalid dlq format", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].DLQFormat = "xml"
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "dlq_format must be") {
			t.Fatalf("expected invalid dlq_format error, got %v", err)
		}
	})

	t.Run("dlq format defaults to jsonl", func(t *testing.T) {
		cfg := baseConfig(t)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
		if cfg.Tasks[0].DLQFormat != DLQFormatJSONL {
			t.Fatalf("expected DLQFormat default %q, got %q", DLQFormatJSONL, cfg.Tasks[0].DLQFormat)
		}
	})

	t.Run("dlq format normalization", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].DLQFormat = "CSV"
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
		if cfg.Tasks[0].DLQFormat != DLQFormatCSV {
			t.Fatalf("expected normalized DLQFormat %q, got %q", DLQFormatCSV, cfg.Tasks[0].DLQFormat)
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

func TestValidateColumnMapping(t *testing.T) {
	t.Run("empty source rejected", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Columns = []ColumnMapping{{Source: "", Target: "id"}}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "source is required") {
			t.Fatalf("expected source is required error, got %v", err)
		}
	})

	t.Run("empty target rejected", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Columns = []ColumnMapping{{Source: "id", Target: "  "}}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "target is required") {
			t.Fatalf("expected target is required error, got %v", err)
		}
	})

	t.Run("duplicate target rejected", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Columns = []ColumnMapping{
			{Source: "id", Target: "pk"},
			{Source: "name", Target: "pk"},
		}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "duplicate target column") {
			t.Fatalf("expected duplicate target column error, got %v", err)
		}
	})

	t.Run("valid column mapping passes", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Columns = []ColumnMapping{
			{Source: "id", Target: "user_id", Transform: "UPPER(?)"},
			{Source: "name", Target: "user_name"},
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
		if len(cfg.Tasks[0].Columns) != 2 {
			t.Fatalf("expected 2 columns, got %d", len(cfg.Tasks[0].Columns))
		}
	})
}

func TestValidateDependsOn(t *testing.T) {
	t.Run("depends_on references missing table", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].DependsOn = []string{"missing"}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "depends_on table 'missing' not found") {
			t.Fatalf("expected depends_on missing error, got %v", err)
		}
	})

	t.Run("two-task cycle detected", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks = append(cfg.Tasks, TaskConfig{
			TableName: "orders",
			SQL:       "SELECT id FROM orders",
			SourceDB:  "src",
			TargetDB:  "dst",
			DependsOn: []string{"users"},
		})
		cfg.Tasks[0].DependsOn = []string{"orders"}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "circular dependency") {
			t.Fatalf("expected circular dependency error, got %v", err)
		}
	})

	t.Run("three-task cycle detected", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks = append(cfg.Tasks,
			TaskConfig{
				TableName: "orders",
				SQL:       "SELECT id FROM orders",
				SourceDB:  "src",
				TargetDB:  "dst",
				DependsOn: []string{"items"},
			},
			TaskConfig{
				TableName: "items",
				SQL:       "SELECT id FROM items",
				SourceDB:  "src",
				TargetDB:  "dst",
				DependsOn: []string{"users"},
			},
		)
		cfg.Tasks[0].DependsOn = []string{"orders"}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "circular dependency") {
			t.Fatalf("expected circular dependency error, got %v", err)
		}
	})

	t.Run("valid dependency chain passes", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks = append(cfg.Tasks, TaskConfig{
			TableName: "orders",
			SQL:       "SELECT id FROM orders",
			SourceDB:  "src",
			TargetDB:  "dst",
			DependsOn: []string{"users"},
		})
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
	})

	t.Run("ignored task does not participate in cycle", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks = append(cfg.Tasks, TaskConfig{
			TableName: "orders",
			SQL:       "SELECT id FROM orders",
			SourceDB:  "src",
			TargetDB:  "dst",
			Ignore:    true,
			DependsOn: []string{"users"},
		})
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
	})
}

func TestMetricsConfigValidation(t *testing.T) {
	t.Run("enabled with defaults", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Metrics = MetricsConfig{Enabled: true}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
		if cfg.Metrics.Interval != "30s" {
			t.Fatalf("expected default interval 30s, got %s", cfg.Metrics.Interval)
		}
	})

	t.Run("invalid interval rejected", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Metrics = MetricsConfig{Enabled: true, Interval: "not-a-duration"}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "invalid metrics interval") {
			t.Fatalf("expected invalid interval error, got %v", err)
		}
	})

	t.Run("valid interval passes", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Metrics = MetricsConfig{Enabled: true, Interval: "1m", ListenAddr: ":9090"}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
		if cfg.Metrics.Interval != "1m" {
			t.Fatalf("expected interval 1m, got %s", cfg.Metrics.Interval)
		}
	})

	t.Run("disabled metrics skips validation", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Metrics = MetricsConfig{Enabled: false, Interval: "bad"}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
	})
}
func TestValidateNotifyConfig(t *testing.T) {
	t.Run("valid notify config passes", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Notify = NotifyConfig{
			OnSuccess: []string{"https://hooks.slack.com/services/xxx"},
			OnFailure: []string{"https://hooks.example.com/fail"},
			Timeout:   10 * time.Second,
			Retry:     2,
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
	})

	t.Run("invalid URL rejected", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Notify = NotifyConfig{
			OnSuccess: []string{"://bad-url"},
		}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "invalid URL") {
			t.Fatalf("expected invalid URL error, got %v", err)
		}
	})

	t.Run("empty URL rejected", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Notify = NotifyConfig{
			OnFailure: []string{""},
		}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "URL is required") {
			t.Fatalf("expected URL required error, got %v", err)
		}
	})

	t.Run("negative timeout rejected", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Notify = NotifyConfig{
			Timeout: -1 * time.Second,
		}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "timeout must be >= 0") {
			t.Fatalf("expected timeout error, got %v", err)
		}
	})

	t.Run("negative retry rejected", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Notify = NotifyConfig{
			Retry: -1,
		}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "retry must be >= 0") {
			t.Fatalf("expected retry error, got %v", err)
		}
	})

	t.Run("empty notify config passes", func(t *testing.T) {
		cfg := baseConfig(t)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
	})
}

func TestNotifyConfigHasURLs(t *testing.T) {
	t.Run("has URLs when on_success is set", func(t *testing.T) {
		n := NotifyConfig{OnSuccess: []string{"https://example.com"}}
		if !n.HasURLs() {
			t.Fatal("expected HasURLs() to be true")
		}
	})

	t.Run("has URLs when on_failure is set", func(t *testing.T) {
		n := NotifyConfig{OnFailure: []string{"https://example.com"}}
		if !n.HasURLs() {
			t.Fatal("expected HasURLs() to be true")
		}
	})

	t.Run("no URLs when empty", func(t *testing.T) {
		n := NotifyConfig{}
		if n.HasURLs() {
			t.Fatal("expected HasURLs() to be false")
		}
	})
}

func TestHistoryTable(t *testing.T) {
	t.Run("default table name", func(t *testing.T) {
		h := HistoryConfig{Enabled: true}
		if got := h.Table(); got != "db_ferry_migrations" {
			t.Fatalf("expected default table name, got %q", got)
		}
	})

	t.Run("custom table name", func(t *testing.T) {
		h := HistoryConfig{Enabled: true, TableName: "custom_history"}
		if got := h.Table(); got != "custom_history" {
			t.Fatalf("expected custom table name, got %q", got)
		}
	})
}

func TestValidatePluginConfig(t *testing.T) {
	t.Run("valid lua plugin passes", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Plugin = PluginConfig{Engine: PluginEngineLua, Script: "function transform(row) return row end", TimeoutMs: 10}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
		if cfg.Tasks[0].Plugin.TimeoutMs != 10 {
			t.Fatalf("expected timeout 10, got %d", cfg.Tasks[0].Plugin.TimeoutMs)
		}
	})

	t.Run("valid javascript plugin passes", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Plugin = PluginConfig{Engine: "JavaScript", Script: "function transform(row) { return row; }"}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
		if cfg.Tasks[0].Plugin.Engine != PluginEngineJavaScript {
			t.Fatalf("expected normalized engine %s, got %s", PluginEngineJavaScript, cfg.Tasks[0].Plugin.Engine)
		}
	})

	t.Run("empty engine skips validation", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Plugin = PluginConfig{Engine: "", Script: ""}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
	})

	t.Run("invalid engine rejected", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Plugin = PluginConfig{Engine: "python", Script: "print(1)"}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "plugin.engine must be") {
			t.Fatalf("expected invalid engine error, got %v", err)
		}
	})

	t.Run("engine set but empty script rejected", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Plugin = PluginConfig{Engine: PluginEngineLua, Script: "   "}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "plugin.script is required") {
			t.Fatalf("expected script required error, got %v", err)
		}
	})

	t.Run("timeout defaults to 5 when zero", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Plugin = PluginConfig{Engine: PluginEngineLua, Script: "return row", TimeoutMs: 0}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
		if cfg.Tasks[0].Plugin.TimeoutMs != 5 {
			t.Fatalf("expected default timeout 5, got %d", cfg.Tasks[0].Plugin.TimeoutMs)
		}
	})

	t.Run("timeout defaults to 5 when negative", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Plugin = PluginConfig{Engine: PluginEngineLua, Script: "return row", TimeoutMs: -1}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
		if cfg.Tasks[0].Plugin.TimeoutMs != 5 {
			t.Fatalf("expected default timeout 5, got %d", cfg.Tasks[0].Plugin.TimeoutMs)
		}
	})
}

func TestValidateFederatedTasks(t *testing.T) {
	baseFederatedConfig := func(t *testing.T) *Config {
		t.Helper()
		dir := t.TempDir()
		return &Config{
			Databases: []DatabaseConfig{
				{Name: "orders_db", Type: DatabaseTypeSQLite, Path: filepath.Join(dir, "orders.db")},
				{Name: "users_db", Type: DatabaseTypeSQLite, Path: filepath.Join(dir, "users.db")},
				{Name: "dst", Type: DatabaseTypeSQLite, Path: filepath.Join(dir, "dst.db")},
			},
			Tasks: []TaskConfig{
				{
					TableName: "order_user_wide",
					TargetDB:  "dst",
					Sources: []SourceConfig{
						{Alias: "orders", DB: "orders_db", SQL: "SELECT order_id, user_id, amount FROM orders"},
						{Alias: "users", DB: "users_db", SQL: "SELECT user_id, user_name, region FROM users"},
					},
					Join: JoinConfig{
						Keys: []string{"user_id"},
						Type: "inner",
					},
				},
			},
		}
	}

	t.Run("valid federated config passes", func(t *testing.T) {
		cfg := baseFederatedConfig(t)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected valid federated config to pass, got %v", err)
		}
		task := cfg.Tasks[0]
		if task.Join.Type != "inner" {
			t.Fatalf("expected join type inner, got %s", task.Join.Type)
		}
	})

	t.Run("federated with source_db rejected", func(t *testing.T) {
		cfg := baseFederatedConfig(t)
		cfg.Tasks[0].SourceDB = "orders_db"
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "source_db is not allowed in federated mode") {
			t.Fatalf("expected source_db rejection error, got %v", err)
		}
	})

	t.Run("federated with sql rejected", func(t *testing.T) {
		cfg := baseFederatedConfig(t)
		cfg.Tasks[0].SQL = "SELECT 1"
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "sql is not allowed in federated mode") {
			t.Fatalf("expected sql rejection error, got %v", err)
		}
	})

	t.Run("federated requires at least 2 sources", func(t *testing.T) {
		cfg := baseFederatedConfig(t)
		cfg.Tasks[0].Sources = cfg.Tasks[0].Sources[:1]
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "at least 2 sources") {
			t.Fatalf("expected at least 2 sources error, got %v", err)
		}
	})

	t.Run("federated source alias required", func(t *testing.T) {
		cfg := baseFederatedConfig(t)
		cfg.Tasks[0].Sources[0].Alias = ""
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "alias is required") {
			t.Fatalf("expected alias required error, got %v", err)
		}
	})

	t.Run("federated source alias must be unique", func(t *testing.T) {
		cfg := baseFederatedConfig(t)
		cfg.Tasks[0].Sources[1].Alias = "orders"
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "duplicate alias") {
			t.Fatalf("expected duplicate alias error, got %v", err)
		}
	})

	t.Run("federated source db required", func(t *testing.T) {
		cfg := baseFederatedConfig(t)
		cfg.Tasks[0].Sources[0].DB = ""
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "db is required") {
			t.Fatalf("expected db required error, got %v", err)
		}
	})

	t.Run("federated source db must be defined", func(t *testing.T) {
		cfg := baseFederatedConfig(t)
		cfg.Tasks[0].Sources[0].DB = "missing"
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "db 'missing' is not defined") {
			t.Fatalf("expected undefined db error, got %v", err)
		}
	})

	t.Run("federated source sql required", func(t *testing.T) {
		cfg := baseFederatedConfig(t)
		cfg.Tasks[0].Sources[0].SQL = ""
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "sql is required") {
			t.Fatalf("expected sql required error, got %v", err)
		}
	})

	t.Run("federated join keys required", func(t *testing.T) {
		cfg := baseFederatedConfig(t)
		cfg.Tasks[0].Join.Keys = nil
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "join.keys is required") {
			t.Fatalf("expected join.keys required error, got %v", err)
		}
	})

	t.Run("federated invalid join type rejected", func(t *testing.T) {
		cfg := baseFederatedConfig(t)
		cfg.Tasks[0].Join.Type = "full"
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "join.type must be") {
			t.Fatalf("expected invalid join type error, got %v", err)
		}
	})

	t.Run("federated resume_key rejected", func(t *testing.T) {
		cfg := baseFederatedConfig(t)
		cfg.Tasks[0].ResumeKey = "id"
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "resume_key is not supported in federated mode") {
			t.Fatalf("expected resume_key rejection error, got %v", err)
		}
	})

	t.Run("federated state_file rejected", func(t *testing.T) {
		cfg := baseFederatedConfig(t)
		cfg.Tasks[0].StateFile = "state.json"
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "state_file is not supported in federated mode") {
			t.Fatalf("expected state_file rejection error, got %v", err)
		}
	})

	t.Run("federated shard rejected", func(t *testing.T) {
		cfg := baseFederatedConfig(t)
		cfg.Tasks[0].Shard = ShardConfig{Enabled: true, Shards: 4}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "shard is not supported in federated mode") {
			t.Fatalf("expected shard rejection error, got %v", err)
		}
	})

	t.Run("federated target_db required", func(t *testing.T) {
		cfg := baseFederatedConfig(t)
		cfg.Tasks[0].TargetDB = ""
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "target_db is required") {
			t.Fatalf("expected target_db required error, got %v", err)
		}
	})

	t.Run("federated target_db must be defined", func(t *testing.T) {
		cfg := baseFederatedConfig(t)
		cfg.Tasks[0].TargetDB = "missing"
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "target_db 'missing' is not defined") {
			t.Fatalf("expected undefined target_db error, got %v", err)
		}
	})

	t.Run("federated left and right join types accepted", func(t *testing.T) {
		for _, joinType := range []string{"left", "right"} {
			cfg := baseFederatedConfig(t)
			cfg.Tasks[0].Join.Type = joinType
			if err := cfg.Validate(); err != nil {
				t.Fatalf("expected join type %s to pass, got %v", joinType, err)
			}
			if cfg.Tasks[0].Join.Type != joinType {
				t.Fatalf("expected normalized join type %s, got %s", joinType, cfg.Tasks[0].Join.Type)
			}
		}
	})
}

func TestValidateMaskingRules(t *testing.T) {
	t.Run("valid masking passes", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Masking = []MaskingConfig{
			{Column: "phone", Rule: MaskRulePhoneCN},
			{Column: "email", Rule: MaskRuleEmail},
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
		if cfg.Tasks[0].Masking[0].Rule != MaskRulePhoneCN {
			t.Fatalf("expected normalized rule, got %s", cfg.Tasks[0].Masking[0].Rule)
		}
	})

	t.Run("missing column rejected", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Masking = []MaskingConfig{{Column: "", Rule: MaskRulePhoneCN}}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "column is required") {
			t.Fatalf("expected column required error, got %v", err)
		}
	})

	t.Run("missing rule rejected", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Masking = []MaskingConfig{{Column: "phone", Rule: ""}}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "rule is required") {
			t.Fatalf("expected rule required error, got %v", err)
		}
	})

	t.Run("unsupported rule rejected", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Masking = []MaskingConfig{{Column: "phone", Rule: "unsupported"}}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "unsupported rule") {
			t.Fatalf("expected unsupported rule error, got %v", err)
		}
	})

	t.Run("duplicate masking column rejected", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Masking = []MaskingConfig{
			{Column: "phone", Rule: MaskRulePhoneCN},
			{Column: "phone", Rule: MaskRuleHash},
		}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "duplicate masking column") {
			t.Fatalf("expected duplicate masking column error, got %v", err)
		}
	})

	t.Run("random_numeric requires range", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Masking = []MaskingConfig{{Column: "score", Rule: MaskRuleRandomNumeric}}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "requires exactly 2 range values") {
			t.Fatalf("expected range error, got %v", err)
		}
	})

	t.Run("fixed_value requires value", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Masking = []MaskingConfig{{Column: "status", Rule: MaskRuleFixedValue}}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "requires value") {
			t.Fatalf("expected value error, got %v", err)
		}
	})
}

func TestAssertionValidation(t *testing.T) {
	minVal := 0.0
	maxVal := 100.0

	t.Run("valid assertions pass", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Assertions = []AssertionConfig{
			{Column: "id", Rule: AssertionRuleNotNull},
			{Column: "score", Rule: AssertionRuleRange, Min: &minVal, Max: &maxVal},
			{Column: "status", Rule: AssertionRuleInSet, Values: []string{"a", "b"}},
			{Columns: []string{"id", "name"}, Rule: AssertionRuleUnique},
			{Column: "email", Rule: AssertionRuleRegex, Pattern: `^.*@.*$`},
			{Column: "name", Rule: AssertionRuleMinLength, Length: 1},
			{Column: "code", Rule: AssertionRuleMaxLength, Length: 10},
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected valid config, got: %v", err)
		}
	})

	t.Run("unsupported rule fails", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Assertions = []AssertionConfig{{Column: "id", Rule: "unknown"}}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "unsupported rule") {
			t.Fatalf("expected unsupported rule error, got: %v", err)
		}
	})

	t.Run("unique requires columns", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Assertions = []AssertionConfig{{Rule: AssertionRuleUnique}}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "requires columns") {
			t.Fatalf("expected columns error, got: %v", err)
		}
	})

	t.Run("non-unique requires column", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Assertions = []AssertionConfig{{Rule: AssertionRuleNotNull}}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "requires column") {
			t.Fatalf("expected column error, got: %v", err)
		}
	})

	t.Run("range requires min or max", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Assertions = []AssertionConfig{{Column: "score", Rule: AssertionRuleRange}}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "requires min or max") {
			t.Fatalf("expected range error, got: %v", err)
		}
	})

	t.Run("in_set requires values", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Assertions = []AssertionConfig{{Column: "status", Rule: AssertionRuleInSet}}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "requires values") {
			t.Fatalf("expected values error, got: %v", err)
		}
	})

	t.Run("regex requires pattern", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Assertions = []AssertionConfig{{Column: "email", Rule: AssertionRuleRegex}}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "requires pattern") {
			t.Fatalf("expected pattern error, got: %v", err)
		}
	})

	t.Run("min_length requires non-negative length", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Assertions = []AssertionConfig{{Column: "name", Rule: AssertionRuleMinLength, Length: -1}}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "non-negative length") {
			t.Fatalf("expected length error, got: %v", err)
		}
	})

	t.Run("invalid on_fail fails", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Assertions = []AssertionConfig{{Column: "id", Rule: AssertionRuleNotNull, OnFail: "panic"}}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "on_fail") {
			t.Fatalf("expected on_fail error, got: %v", err)
		}
	})

	t.Run("default on_fail is abort", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Assertions = []AssertionConfig{{Column: "id", Rule: AssertionRuleNotNull}}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected valid config, got: %v", err)
		}
		if cfg.Tasks[0].Assertions[0].OnFail != AssertionActionAbort {
			t.Fatalf("expected default on_fail=abort, got: %s", cfg.Tasks[0].Assertions[0].OnFail)
		}
	})

	t.Run("duplicate assertion fails", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Assertions = []AssertionConfig{
			{Column: "id", Rule: AssertionRuleNotNull},
			{Column: "id", Rule: AssertionRuleNotNull},
		}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "duplicate assertion") {
			t.Fatalf("expected duplicate error, got: %v", err)
		}
	})
}

func TestValidateCDCConfig(t *testing.T) {
	t.Run("valid cdc config passes", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Mode = TaskModeAppend
		cfg.Tasks[0].StateFile = "./state.json"
		cfg.Tasks[0].CDC = CDCConfig{
			Enabled:      true,
			CursorColumn: "updated_at",
			PollInterval: "5m",
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected valid CDC config to pass, got %v", err)
		}
		if cfg.Tasks[0].ResumeKey != "updated_at" {
			t.Fatalf("expected ResumeKey set to cursor_column, got %s", cfg.Tasks[0].ResumeKey)
		}
	})

	t.Run("cdc requires cursor_column", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Mode = TaskModeAppend
		cfg.Tasks[0].StateFile = "./state.json"
		cfg.Tasks[0].CDC = CDCConfig{Enabled: true, PollInterval: "5m"}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "cdc.cursor_column is required") {
			t.Fatalf("expected cursor_column required error, got %v", err)
		}
	})

	t.Run("cdc requires append or merge mode", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Mode = TaskModeReplace
		cfg.Tasks[0].StateFile = "./state.json"
		cfg.Tasks[0].CDC = CDCConfig{Enabled: true, CursorColumn: "id", PollInterval: "5m"}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "cdc requires mode") {
			t.Fatalf("expected mode error, got %v", err)
		}
	})

	t.Run("cdc requires state_file", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Mode = TaskModeAppend
		cfg.Tasks[0].CDC = CDCConfig{Enabled: true, CursorColumn: "id", PollInterval: "5m"}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "cdc requires state_file") {
			t.Fatalf("expected state_file required error, got %v", err)
		}
	})

	t.Run("cdc requires poll_interval", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Mode = TaskModeAppend
		cfg.Tasks[0].StateFile = "./state.json"
		cfg.Tasks[0].CDC = CDCConfig{Enabled: true, CursorColumn: "id"}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "cdc.poll_interval is required") {
			t.Fatalf("expected poll_interval required error, got %v", err)
		}
	})

	t.Run("cdc rejects invalid poll_interval", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Mode = TaskModeAppend
		cfg.Tasks[0].StateFile = "./state.json"
		cfg.Tasks[0].CDC = CDCConfig{Enabled: true, CursorColumn: "id", PollInterval: "not-a-duration"}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "invalid cdc.poll_interval") {
			t.Fatalf("expected invalid poll_interval error, got %v", err)
		}
	})

	t.Run("cdc not supported in federated mode", func(t *testing.T) {
		dir := t.TempDir()
		cfg := &Config{
			Databases: []DatabaseConfig{
				{Name: "db1", Type: DatabaseTypeSQLite, Path: filepath.Join(dir, "db1.db")},
				{Name: "db2", Type: DatabaseTypeSQLite, Path: filepath.Join(dir, "db2.db")},
				{Name: "dst", Type: DatabaseTypeSQLite, Path: filepath.Join(dir, "dst.db")},
			},
			Tasks: []TaskConfig{
				{
					TableName: "wide",
					TargetDB:  "dst",
					Sources: []SourceConfig{
						{Alias: "a", DB: "db1", SQL: "SELECT id FROM t1"},
						{Alias: "b", DB: "db2", SQL: "SELECT id FROM t2"},
					},
					Join: JoinConfig{Keys: []string{"id"}, Type: "inner"},
					CDC:  CDCConfig{Enabled: true, CursorColumn: "id", PollInterval: "5m"},
				},
			},
		}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "cdc is not supported in federated mode") {
			t.Fatalf("expected federated CDC error, got %v", err)
		}
	})

	t.Run("cdc not supported with shard", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Mode = TaskModeAppend
		cfg.Tasks[0].StateFile = "./state.json"
		cfg.Tasks[0].ResumeKey = "id"
		cfg.Tasks[0].Shard = ShardConfig{Enabled: true, Shards: 4}
		cfg.Tasks[0].CDC = CDCConfig{Enabled: true, CursorColumn: "id", PollInterval: "5m"}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "cdc is not supported with shard") {
			t.Fatalf("expected shard CDC error, got %v", err)
		}
	})

	t.Run("cdc initial_cursor sets resume_from", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Mode = TaskModeAppend
		cfg.Tasks[0].StateFile = "./state.json"
		cfg.Tasks[0].CDC = CDCConfig{
			Enabled:       true,
			CursorColumn:  "updated_at",
			PollInterval:  "5m",
			InitialCursor: "2024-01-01",
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected valid CDC config to pass, got %v", err)
		}
		if cfg.Tasks[0].ResumeFrom != "2024-01-01" {
			t.Fatalf("expected ResumeFrom set to initial_cursor, got %s", cfg.Tasks[0].ResumeFrom)
		}
	})

	t.Run("cdc resume_key must match cursor_column", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Tasks[0].Mode = TaskModeAppend
		cfg.Tasks[0].StateFile = "./state.json"
		cfg.Tasks[0].ResumeKey = "created_at"
		cfg.Tasks[0].CDC = CDCConfig{Enabled: true, CursorColumn: "updated_at", PollInterval: "5m"}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "resume_key and cdc.cursor_column must match") {
			t.Fatalf("expected resume_key mismatch error, got %v", err)
		}
	})
}

func TestValidateScheduleConfig(t *testing.T) {
	t.Run("valid schedule passes", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Schedule = ScheduleConfig{Cron: "0 2 * * *", Timezone: "UTC", MaxRetry: 3}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
	})

	t.Run("schedule with start_at and end_at passes", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Schedule = ScheduleConfig{
			Cron:    "0 * * * *",
			StartAt: "2025-01-01T00:00:00Z",
			EndAt:   "2025-12-31T23:59:59Z",
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() error = %v", err)
		}
	})

	t.Run("empty cron skips schedule validation", func(t *testing.T) {
		cfg := baseConfig(t)
		cfg.Schedule = ScheduleConfig{Timezone: "bad", MaxRetry: -1}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected empty cron to skip validation, got %v", err)
		}
	})
}

func TestValidateScheduleInvalidCron(t *testing.T) {
	cfg := baseConfig(t)
	cfg.Schedule = ScheduleConfig{Cron: "invalid"}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "invalid schedule.cron") {
		t.Fatalf("expected invalid cron error, got %v", err)
	}
}

func TestValidateScheduleInvalidTimezone(t *testing.T) {
	cfg := baseConfig(t)
	cfg.Schedule = ScheduleConfig{Cron: "0 2 * * *", Timezone: "Mars/Zone"}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "invalid schedule.timezone") {
		t.Fatalf("expected invalid timezone error, got %v", err)
	}
}

func TestValidateScheduleEndBeforeStart(t *testing.T) {
	cfg := baseConfig(t)
	cfg.Schedule = ScheduleConfig{
		Cron:    "0 2 * * *",
		StartAt: "2025-12-31T00:00:00Z",
		EndAt:   "2025-01-01T00:00:00Z",
	}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "schedule.end_at must be after schedule.start_at") {
		t.Fatalf("expected end before start error, got %v", err)
	}
}

func TestValidateScheduleMaxRetry(t *testing.T) {
	cfg := baseConfig(t)
	cfg.Schedule = ScheduleConfig{Cron: "0 2 * * *", MaxRetry: -1}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "schedule.max_retry must be >= 0") {
		t.Fatalf("expected max_retry error, got %v", err)
	}
}
