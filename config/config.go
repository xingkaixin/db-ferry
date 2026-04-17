package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
)

// Supported database types.
const (
	DatabaseTypeOracle     = "oracle"
	DatabaseTypeMySQL      = "mysql"
	DatabaseTypeSQLite     = "sqlite"
	DatabaseTypeDuckDB     = "duckdb"
	DatabaseTypePostgreSQL = "postgresql"
	DatabaseTypeSQLServer  = "sqlserver"
)

// Supported task modes.
const (
	TaskModeReplace = "replace"
	TaskModeAppend  = "append"
	TaskModeMerge   = "merge"
	TaskModeUpsert  = "upsert"
)

// Supported validation modes.
const (
	TaskValidateNone     = "none"
	TaskValidateRowCount = "row_count"
	TaskValidateChecksum = "checksum"
	TaskValidateSample   = "sample"
)

// Supported DLQ formats.
const (
	DLQFormatJSONL = "jsonl"
	DLQFormatCSV   = "csv"
)

// Supported SSL modes.
const (
	SSLModeDisable    = "disable"
	SSLModeRequire    = "require"
	SSLModeVerifyCA   = "verify-ca"
	SSLModeVerifyFull = "verify-full"
)

// Supported masking rules.
const (
	MaskRulePhoneCN       = "phone_cn"
	MaskRulePhoneUS       = "phone_us"
	MaskRuleEmail         = "email"
	MaskRuleIDCardCN      = "id_card_cn"
	MaskRuleNameCN        = "name_cn"
	MaskRuleRandomNumeric = "random_numeric"
	MaskRuleRandomDate    = "random_date"
	MaskRuleFixedValue    = "fixed_value"
	MaskRuleHash          = "hash"
)

var supportedMaskRules = map[string]struct{}{
	MaskRulePhoneCN:       {},
	MaskRulePhoneUS:       {},
	MaskRuleEmail:         {},
	MaskRuleIDCardCN:      {},
	MaskRuleNameCN:        {},
	MaskRuleRandomNumeric: {},
	MaskRuleRandomDate:    {},
	MaskRuleFixedValue:    {},
	MaskRuleHash:          {},
}

// ShardConfig defines range-based sharding for a single table.
type ShardConfig struct {
	Enabled bool `toml:"enabled"`
	Shards  int  `toml:"shards"`
}

// ReplicaConfig describes a read-only replica connection.
type ReplicaConfig struct {
	Host     string `toml:"host"`
	Port     string `toml:"port,omitempty"`
	Priority int    `toml:"priority,omitempty"`
}

// DatabaseConfig describes a named database connection definition.
type DatabaseConfig struct {
	Name string `toml:"name"`
	Type string `toml:"type"`

	Host     string `toml:"host,omitempty"`
	Port     string `toml:"port,omitempty"`
	Service  string `toml:"service,omitempty"`
	Database string `toml:"database,omitempty"`
	User     string `toml:"user,omitempty"`
	Password string `toml:"password,omitempty"`
	Path     string `toml:"path,omitempty"`

	Replicas        []ReplicaConfig `toml:"replicas,omitempty"`
	PoolMaxOpen     int             `toml:"pool_max_open,omitempty"`
	PoolMaxIdle     int             `toml:"pool_max_idle,omitempty"`
	ReplicaFallback bool            `toml:"replica_fallback,omitempty"`

	// TLS/SSL configuration
	SSLMode     string `toml:"ssl_mode,omitempty"`
	SSLCert     string `toml:"ssl_cert,omitempty"`
	SSLKey      string `toml:"ssl_key,omitempty"`
	SSLRootCert string `toml:"ssl_root_cert,omitempty"`

	// Encryption for file-based databases (e.g., SQLCipher)
	EncryptionKey string `toml:"encryption_key,omitempty"`
}

// IndexColumn represents a column definition for index creation with order information.
type IndexColumn struct {
	Name  string
	Order string
}

// IndexConfig captures index information for a task.
type IndexConfig struct {
	Name          string        `toml:"name"`
	Columns       []string      `toml:"columns"`
	Unique        bool          `toml:"unique"`
	Where         string        `toml:"where"`
	ParsedColumns []IndexColumn `toml:"-"`
}

// ParseColumns converts shorthand column definitions into structured data.
func (ic *IndexConfig) ParseColumns() error {
	ic.ParsedColumns = make([]IndexColumn, len(ic.Columns))

	for i, col := range ic.Columns {
		if strings.Contains(col, ":") {
			parts := strings.Split(col, ":")
			if len(parts) != 2 {
				return fmt.Errorf("invalid column format: %s", col)
			}

			orderSpecifier := strings.TrimSpace(parts[1])
			switch orderSpecifier {
			case "1", "ASC", "asc":
				ic.ParsedColumns[i] = IndexColumn{Name: strings.TrimSpace(parts[0]), Order: "ASC"}
			case "-1", "DESC", "desc":
				ic.ParsedColumns[i] = IndexColumn{Name: strings.TrimSpace(parts[0]), Order: "DESC"}
			default:
				return fmt.Errorf("invalid order specifier: %s (must be 1, -1, ASC, or DESC)", orderSpecifier)
			}
		} else {
			ic.ParsedColumns[i] = IndexColumn{Name: strings.TrimSpace(col), Order: "ASC"}
		}
	}

	return nil
}

// MaskingConfig defines a column masking rule for a task.
type MaskingConfig struct {
	Column string    `toml:"column"`
	Rule   string    `toml:"rule"`
	Range  []float64 `toml:"range,omitempty"`
	Value  string    `toml:"value,omitempty"`
}

// AdaptiveBatchConfig configures dynamic batch-size tuning for a task.
type AdaptiveBatchConfig struct {
	Enabled         bool `toml:"enabled"`
	MinSize         int  `toml:"min_size"`
	MaxSize         int  `toml:"max_size"`
	TargetLatencyMs int  `toml:"target_latency_ms"`
	MemoryLimitMB   int  `toml:"memory_limit_mb"`
}

// ColumnMapping defines a source-to-target column mapping with an optional transform.
type ColumnMapping struct {
	Source    string `toml:"source"`
	Target    string `toml:"target"`
	Transform string `toml:"transform"`
}

// TaskConfig defines a single migration job.
type TaskConfig struct {
	TableName          string              `toml:"table_name"`
	SQL                string              `toml:"sql"`
	SourceDB           string              `toml:"source_db"`
	TargetDB           string              `toml:"target_db"`
	Ignore             bool                `toml:"ignore"`
	Mode               string              `toml:"mode"`
	BatchSize          int                 `toml:"batch_size"`
	MaxRetries         int                 `toml:"max_retries"`
	Validate           string              `toml:"validate"`
	ValidateSampleSize int                 `toml:"validate_sample_size"`
	MergeKeys          []string            `toml:"merge_keys"`
	ResumeKey          string              `toml:"resume_key"`
	ResumeFrom         string              `toml:"resume_from"`
	StateFile          string              `toml:"state_file"`
	AdaptiveBatch      AdaptiveBatchConfig `toml:"adaptive_batch"`
	Columns            []ColumnMapping     `toml:"columns,omitempty"`
	// AllowSameTable 明确允许同库执行并覆盖目标表（存在数据丢失风险）。
	AllowSameTable bool `toml:"allow_same_table"`
	// SkipCreateTable 跳过目标表的 drop/create 操作。
	SkipCreateTable bool `toml:"skip_create_table"`
	// SchemaEvolution 在 append/merge 模式下自动为目标表添加源端新增列。
	SchemaEvolution bool `toml:"schema_evolution"`
	// DLQPath 死信队列文件路径，用于保存插入失败的行。
	DLQPath string `toml:"dlq_path,omitempty"`
	// DLQFormat 死信队列文件格式，支持 jsonl 和 csv，默认为 jsonl。
	DLQFormat string          `toml:"dlq_format,omitempty"`
	Indexes   []IndexConfig   `toml:"indexes,omitempty"`
	Masking   []MaskingConfig `toml:"masking,omitempty"`
	PreSQL    []string        `toml:"pre_sql,omitempty"`
	PostSQL   []string        `toml:"post_sql,omitempty"`
	DependsOn []string        `toml:"depends_on"`
	Shard     ShardConfig     `toml:"shard,omitempty"`
}

// HistoryConfig controls migration audit logging.
type HistoryConfig struct {
	Enabled   bool   `toml:"enabled"`
	TableName string `toml:"table_name"`
}

// Table returns the configured history table name or the default.
func (h *HistoryConfig) Table() string {
	if h.TableName == "" {
		return "db_ferry_migrations"
	}
	return h.TableName
}

// Config is the top-level configuration structure decoded from task.toml.
type Config struct {
	Databases          []DatabaseConfig `toml:"databases"`
	Tasks              []TaskConfig     `toml:"tasks"`
	MaxConcurrentTasks int              `toml:"max_concurrent_tasks"`
	History            HistoryConfig    `toml:"history"`

	databaseMap map[string]DatabaseConfig
}

// LoadConfig decodes the TOML configuration file and validates its content.
func LoadConfig(tomlPath string) (*Config, error) {
	cfg := &Config{}
	if _, err := toml.DecodeFile(tomlPath, cfg); err != nil {
		return nil, fmt.Errorf("error decoding TOML file: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// Validate ensures configuration integrity and populates runtime helpers.
func (c *Config) Validate() error {
	if len(c.Databases) == 0 {
		return fmt.Errorf("at least one database must be defined under [[databases]]")
	}
	if len(c.Tasks) == 0 {
		return fmt.Errorf("at least one task must be defined under [[tasks]]")
	}

	c.databaseMap = make(map[string]DatabaseConfig, len(c.Databases))
	for i, db := range c.Databases {
		if db.Name == "" {
			return fmt.Errorf("database definition %d: name is required", i+1)
		}
		if _, exists := c.databaseMap[db.Name]; exists {
			return fmt.Errorf("database definition %d: duplicate database name '%s'", i+1, db.Name)
		}
		db.Type = strings.ToLower(db.Type)
		switch db.Type {
		case DatabaseTypeOracle:
			if db.Port == "" {
				db.Port = "1521"
			}
		case DatabaseTypeMySQL:
			if db.Port == "" {
				db.Port = "3306"
			}
		case DatabaseTypePostgreSQL:
			if db.Port == "" {
				db.Port = "5432"
			}
		case DatabaseTypeSQLServer:
			if db.Port == "" {
				db.Port = "1433"
			}
		}

		if err := validateDatabaseConfig(&db); err != nil {
			return fmt.Errorf("database '%s': %w", db.Name, err)
		}

		c.databaseMap[db.Name] = db
	}

	indexNames := make(map[string]string)

	for i, task := range c.Tasks {
		if task.TableName == "" {
			return fmt.Errorf("task %d: table_name is required", i+1)
		}
		if task.SQL == "" {
			return fmt.Errorf("task %d: sql is required", i+1)
		}
		if task.SourceDB == "" {
			return fmt.Errorf("task %d: source_db is required", i+1)
		}
		if task.TargetDB == "" {
			return fmt.Errorf("task %d: target_db is required", i+1)
		}

		sourceDB, ok := c.databaseMap[task.SourceDB]
		if !ok {
			return fmt.Errorf("task %d: source_db '%s' is not defined", i+1, task.SourceDB)
		}
		targetDB, ok := c.databaseMap[task.TargetDB]
		if !ok {
			return fmt.Errorf("task %d: target_db '%s' is not defined", i+1, task.TargetDB)
		}
		if task.SourceDB == task.TargetDB && !task.AllowSameTable {
			return fmt.Errorf("task %d: source_db and target_db are both '%s'; set allow_same_table = true to allow same-database migrations", i+1, task.SourceDB)
		}

		task.Mode = strings.ToLower(strings.TrimSpace(task.Mode))
		if task.Mode == "" {
			task.Mode = TaskModeReplace
		}
		switch task.Mode {
		case TaskModeReplace, TaskModeAppend, TaskModeMerge, TaskModeUpsert:
		default:
			return fmt.Errorf("task %d: mode must be %q, %q, %q, or %q", i+1, TaskModeReplace, TaskModeAppend, TaskModeMerge, TaskModeUpsert)
		}
		if task.Mode == TaskModeUpsert {
			task.Mode = TaskModeMerge
		}

		normalizedKeys, err := normalizeKeys(task.MergeKeys)
		if err != nil {
			return fmt.Errorf("task %d: %w", i+1, err)
		}
		task.MergeKeys = normalizedKeys
		if task.Mode == TaskModeMerge && len(task.MergeKeys) == 0 {
			return fmt.Errorf("task %d: merge_keys is required when mode is %q", i+1, TaskModeMerge)
		}
		if task.Mode != TaskModeMerge && len(task.MergeKeys) > 0 {
			return fmt.Errorf("task %d: merge_keys is only valid when mode is %q", i+1, TaskModeMerge)
		}

		task.Validate = strings.ToLower(strings.TrimSpace(task.Validate))
		if task.Validate == "" {
			task.Validate = TaskValidateNone
		}
		switch task.Validate {
		case TaskValidateNone, TaskValidateRowCount, TaskValidateChecksum, TaskValidateSample:
		default:
			return fmt.Errorf("task %d: validate must be %q, %q, %q, or %q", i+1, TaskValidateNone, TaskValidateRowCount, TaskValidateChecksum, TaskValidateSample)
		}
		if task.Validate == TaskValidateSample && task.ValidateSampleSize <= 0 {
			return fmt.Errorf("task %d: validate_sample_size must be > 0 when validate is %q", i+1, TaskValidateSample)
		}

		if task.BatchSize < 0 {
			return fmt.Errorf("task %d: batch_size must be >= 0", i+1)
		}
		if task.MaxRetries < 0 {
			return fmt.Errorf("task %d: max_retries must be >= 0", i+1)
		}

		if task.AdaptiveBatch.Enabled {
			if task.AdaptiveBatch.MinSize <= 0 {
				return fmt.Errorf("task %d: adaptive_batch.min_size must be > 0 when enabled", i+1)
			}
			if task.AdaptiveBatch.MaxSize < task.AdaptiveBatch.MinSize {
				return fmt.Errorf("task %d: adaptive_batch.max_size must be >= min_size", i+1)
			}
			if task.AdaptiveBatch.TargetLatencyMs <= 0 {
				return fmt.Errorf("task %d: adaptive_batch.target_latency_ms must be > 0 when enabled", i+1)
			}
			if task.AdaptiveBatch.MemoryLimitMB <= 0 {
				return fmt.Errorf("task %d: adaptive_batch.memory_limit_mb must be > 0 when enabled", i+1)
			}
		}

		task.DLQFormat = strings.ToLower(strings.TrimSpace(task.DLQFormat))
		if task.DLQFormat == "" {
			task.DLQFormat = DLQFormatJSONL
		}
		if task.DLQFormat != DLQFormatJSONL && task.DLQFormat != DLQFormatCSV {
			return fmt.Errorf("task %d: dlq_format must be %q or %q", i+1, DLQFormatJSONL, DLQFormatCSV)
		}

		task.ResumeKey = strings.TrimSpace(task.ResumeKey)
		task.ResumeFrom = strings.TrimSpace(task.ResumeFrom)
		task.StateFile = strings.TrimSpace(task.StateFile)
		if task.StateFile != "" && task.ResumeKey == "" {
			return fmt.Errorf("task %d: state_file requires resume_key", i+1)
		}
		if task.ResumeKey != "" && task.StateFile == "" && task.ResumeFrom == "" {
			return fmt.Errorf("task %d: resume_key requires resume_from or state_file", i+1)
		}


		if task.Shard.Enabled {
			if task.ResumeKey == "" {
				return fmt.Errorf("task %d: shard requires resume_key", i+1)
			}
			if task.Shard.Shards <= 1 {
				return fmt.Errorf("task %d: shard.shards must be > 1", i+1)
			}
			if task.Mode == TaskModeReplace {
				return fmt.Errorf("task %d: shard is not supported in replace mode; use append or merge", i+1)
			}
			if task.StateFile != "" {
				return fmt.Errorf("task %d: state_file is not supported with shard", i+1)
			}
		}

		seenTarget := make(map[string]struct{})
		for j, col := range task.Columns {
			if strings.TrimSpace(col.Source) == "" {
				return fmt.Errorf("task %d, column %d: source is required", i+1, j+1)
			}
			if strings.TrimSpace(col.Target) == "" {
				return fmt.Errorf("task %d, column %d: target is required", i+1, j+1)
			}
			lowerTarget := strings.ToLower(strings.TrimSpace(col.Target))
			if _, exists := seenTarget[lowerTarget]; exists {
				return fmt.Errorf("task %d, column %d: duplicate target column '%s'", i+1, j+1, col.Target)
			}
			seenTarget[lowerTarget] = struct{}{}
		}

		if err := ensureDatabaseSupportsSource(&sourceDB); err != nil {
			return fmt.Errorf("task %d: %w", i+1, err)
		}
		if err := ensureDatabaseSupportsTarget(&targetDB); err != nil {
			return fmt.Errorf("task %d: %w", i+1, err)
		}

		for j, index := range task.Indexes {
			if index.Name == "" {
				return fmt.Errorf("task %d, index %d: index name is required", i+1, j+1)
			}
			if len(index.Columns) == 0 {
				return fmt.Errorf("task %d, index %d: at least one column is required", i+1, j+1)
			}

			if existingTable, exists := indexNames[index.Name]; exists {
				if existingTable == task.TableName {
					return fmt.Errorf("task %d, index %d: index name '%s' already defined for table '%s'", i+1, j+1, index.Name, task.TableName)
				}
				return fmt.Errorf("task %d, index %d: index name '%s' already used by table '%s'", i+1, j+1, index.Name, existingTable)
			}
			indexNames[index.Name] = task.TableName

			if err := index.ParseColumns(); err != nil {
				return fmt.Errorf("task %d, index %d: %w", i+1, j+1, err)
			}

			if targetDB.Type != DatabaseTypeSQLite && index.Where != "" {
				return fmt.Errorf("task %d, index %d: partial indexes (where clause) are only supported for SQLite targets", i+1, j+1)
			}
		}

		seenMaskCols := make(map[string]struct{}, len(task.Masking))
		for j, m := range task.Masking {
			if m.Column == "" {
				return fmt.Errorf("task %d, masking %d: column is required", i+1, j+1)
			}
			colLower := strings.ToLower(m.Column)
			if _, exists := seenMaskCols[colLower]; exists {
				return fmt.Errorf("task %d, masking %d: duplicate masking column '%s'", i+1, j+1, m.Column)
			}
			seenMaskCols[colLower] = struct{}{}

			m.Rule = strings.ToLower(strings.TrimSpace(m.Rule))
			if m.Rule == "" {
				return fmt.Errorf("task %d, masking %d: rule is required for column '%s'", i+1, j+1, m.Column)
			}
			if _, ok := supportedMaskRules[m.Rule]; !ok {
				return fmt.Errorf("task %d, masking %d: unsupported rule '%s' for column '%s'", i+1, j+1, m.Rule, m.Column)
			}
			if m.Rule == MaskRuleRandomNumeric && len(m.Range) != 2 {
				return fmt.Errorf("task %d, masking %d: rule '%s' requires exactly 2 range values [min, max]", i+1, j+1, m.Rule)
			}
			if m.Rule == MaskRuleFixedValue && m.Value == "" {
				return fmt.Errorf("task %d, masking %d: rule '%s' requires value", i+1, j+1, m.Rule)
			}
			task.Masking[j] = m
		}

		c.Tasks[i] = task
	}

	if err := validateTaskDependencies(c.Tasks); err != nil {
		return err
	}

	if err := detectTaskCycle(c.Tasks); err != nil {
		return err
	}

	return nil
}

// ResolveReplicaConfig returns a full DatabaseConfig for a replica by inheriting
// missing fields from the primary configuration.
func (dbCfg DatabaseConfig) ResolveReplicaConfig(r ReplicaConfig) DatabaseConfig {
	resolved := dbCfg
	resolved.Replicas = nil
	if r.Host != "" {
		resolved.Host = r.Host
	}
	if r.Port != "" {
		resolved.Port = r.Port
	}
	return resolved
}

func validateDatabaseConfig(db *DatabaseConfig) error {
	if db.Type == "" {
		return fmt.Errorf("type is required for database")
	}
	switch strings.ToLower(db.Type) {
	case DatabaseTypeOracle:
		if db.Host == "" {
			return fmt.Errorf("host is required for Oracle database")
		}
		if db.User == "" {
			return fmt.Errorf("user is required for Oracle database")
		}
		if db.Password == "" {
			return fmt.Errorf("password is required for Oracle database")
		}
		if db.Service == "" {
			return fmt.Errorf("service is required for Oracle database")
		}
	case DatabaseTypeMySQL:
		if db.Host == "" {
			return fmt.Errorf("host is required for MySQL database")
		}
		if db.User == "" {
			return fmt.Errorf("user is required for MySQL database")
		}
		if db.Password == "" {
			return fmt.Errorf("password is required for MySQL database")
		}
		if db.Database == "" {
			return fmt.Errorf("database is required for MySQL database")
		}
	case DatabaseTypePostgreSQL:
		if db.Host == "" {
			return fmt.Errorf("host is required for PostgreSQL database")
		}
		if db.User == "" {
			return fmt.Errorf("user is required for PostgreSQL database")
		}
		if db.Password == "" {
			return fmt.Errorf("password is required for PostgreSQL database")
		}
		if db.Database == "" {
			return fmt.Errorf("database is required for PostgreSQL database")
		}
	case DatabaseTypeSQLServer:
		if db.Host == "" {
			return fmt.Errorf("host is required for SQL Server database")
		}
		if db.User == "" {
			return fmt.Errorf("user is required for SQL Server database")
		}
		if db.Password == "" {
			return fmt.Errorf("password is required for SQL Server database")
		}
		if db.Database == "" {
			return fmt.Errorf("database is required for SQL Server database")
		}
	case DatabaseTypeSQLite, DatabaseTypeDuckDB:
		if db.Path == "" {
			return fmt.Errorf("path is required for %s database", db.Type)
		}
	default:
		return fmt.Errorf("unsupported database type '%s'", db.Type)
	}

	for i, r := range db.Replicas {
		if r.Host == "" {
			return fmt.Errorf("replica %d: host is required", i+1)
		}
	}

	if err := validateTLSConfig(db); err != nil {
		return err
	}

	return nil
}

func validateTLSConfig(db *DatabaseConfig) error {
	db.SSLMode = strings.ToLower(strings.TrimSpace(db.SSLMode))
	if db.SSLMode == "" {
		db.SSLMode = SSLModeDisable
	}
	switch db.SSLMode {
	case SSLModeDisable, SSLModeRequire, SSLModeVerifyCA, SSLModeVerifyFull:
	default:
		return fmt.Errorf("unsupported ssl_mode '%s' (must be %q, %q, %q, or %q)", db.SSLMode, SSLModeDisable, SSLModeRequire, SSLModeVerifyCA, SSLModeVerifyFull)
	}

	checkFile := func(path, name string) error {
		if path == "" {
			return nil
		}
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("%s file not found: %w", name, err)
		}
		return nil
	}

	if err := checkFile(db.SSLCert, "ssl_cert"); err != nil {
		return err
	}
	if err := checkFile(db.SSLKey, "ssl_key"); err != nil {
		return err
	}
	if err := checkFile(db.SSLRootCert, "ssl_root_cert"); err != nil {
		return err
	}

	return nil
}

func ensureDatabaseSupportsSource(db *DatabaseConfig) error {
	switch strings.ToLower(db.Type) {
	case DatabaseTypeOracle, DatabaseTypeMySQL, DatabaseTypeSQLite, DatabaseTypeDuckDB, DatabaseTypePostgreSQL, DatabaseTypeSQLServer:
		return nil
	default:
		return fmt.Errorf("database '%s' of type '%s' cannot be used as source", db.Name, db.Type)
	}
}

func ensureDatabaseSupportsTarget(db *DatabaseConfig) error {
	switch strings.ToLower(db.Type) {
	case DatabaseTypeOracle, DatabaseTypeMySQL, DatabaseTypeSQLite, DatabaseTypeDuckDB, DatabaseTypePostgreSQL, DatabaseTypeSQLServer:
		return nil
	default:
		return fmt.Errorf("database '%s' of type '%s' cannot be used as target", db.Name, db.Type)
	}
}

// GetDatabase retrieves a database configuration by name.
func (c *Config) GetDatabase(name string) (DatabaseConfig, bool) {
	db, ok := c.databaseMap[name]
	return db, ok
}

// DatabasesMap exposes the internal alias map (read-only copy) for consumers.
func (c *Config) DatabasesMap() map[string]DatabaseConfig {
	out := make(map[string]DatabaseConfig, len(c.databaseMap))
	for k, v := range c.databaseMap {
		out[k] = v
	}
	return out
}

func validateTaskDependencies(tasks []TaskConfig) error {
	tableNames := make(map[string]struct{})
	for _, task := range tasks {
		if !task.Ignore {
			tableNames[task.TableName] = struct{}{}
		}
	}

	for i, task := range tasks {
		if task.Ignore {
			continue
		}
		for _, dep := range task.DependsOn {
			if _, ok := tableNames[dep]; !ok {
				return fmt.Errorf("task %d: depends_on table '%s' not found", i+1, dep)
			}
		}
	}

	return nil
}

func detectTaskCycle(tasks []TaskConfig) error {
	taskIndex := make(map[string][]int)
	var filtered []TaskConfig
	for _, task := range tasks {
		if task.Ignore {
			continue
		}
		idx := len(filtered)
		filtered = append(filtered, task)
		taskIndex[task.TableName] = append(taskIndex[task.TableName], idx)
	}

	n := len(filtered)
	if n == 0 {
		return nil
	}

	adj := make([][]int, n)
	inDegree := make([]int, n)

	for i, task := range filtered {
		for _, depName := range task.DependsOn {
			if depIndices, ok := taskIndex[depName]; ok {
				for _, depIdx := range depIndices {
					adj[depIdx] = append(adj[depIdx], i)
					inDegree[i]++
				}
			}
		}
	}

	queue := make([]int, 0, n)
	for i := 0; i < n; i++ {
		if inDegree[i] == 0 {
			queue = append(queue, i)
		}
	}

	visited := 0
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		visited++
		for _, v := range adj[u] {
			inDegree[v]--
			if inDegree[v] == 0 {
				queue = append(queue, v)
			}
		}
	}

	if visited != n {
		return fmt.Errorf("circular dependency detected among tasks")
	}

	return nil
}

func normalizeKeys(keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	normalized := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			return nil, fmt.Errorf("merge_keys contains empty value")
		}
		lower := strings.ToLower(trimmed)
		if _, exists := seen[lower]; exists {
			return nil, fmt.Errorf("merge_keys contains duplicate key '%s'", trimmed)
		}
		seen[lower] = struct{}{}
		normalized = append(normalized, trimmed)
	}

	return normalized, nil
}
