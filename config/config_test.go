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
