package config

import (
	"fmt"
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
)

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

// TaskConfig defines a single migration job.
type TaskConfig struct {
	TableName  string   `toml:"table_name"`
	SQL        string   `toml:"sql"`
	SourceDB   string   `toml:"source_db"`
	TargetDB   string   `toml:"target_db"`
	Ignore     bool     `toml:"ignore"`
	Mode       string   `toml:"mode"`
	BatchSize  int      `toml:"batch_size"`
	MaxRetries int      `toml:"max_retries"`
	Validate   string   `toml:"validate"`
	MergeKeys  []string `toml:"merge_keys"`
	ResumeKey  string   `toml:"resume_key"`
	ResumeFrom string   `toml:"resume_from"`
	StateFile  string   `toml:"state_file"`
	// AllowSameTable 明确允许同库执行并覆盖目标表（存在数据丢失风险）。
	AllowSameTable bool `toml:"allow_same_table"`
	// SkipCreateTable 跳过目标表的 drop/create 操作。
	SkipCreateTable bool          `toml:"skip_create_table"`
	Indexes         []IndexConfig `toml:"indexes,omitempty"`
}

// Config is the top-level configuration structure decoded from task.toml.
type Config struct {
	Databases []DatabaseConfig `toml:"databases"`
	Tasks     []TaskConfig     `toml:"tasks"`

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
		case TaskValidateNone, TaskValidateRowCount:
		default:
			return fmt.Errorf("task %d: validate must be %q or %q", i+1, TaskValidateNone, TaskValidateRowCount)
		}

		if task.BatchSize < 0 {
			return fmt.Errorf("task %d: batch_size must be >= 0", i+1)
		}
		if task.MaxRetries < 0 {
			return fmt.Errorf("task %d: max_retries must be >= 0", i+1)
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

		c.Tasks[i] = task
	}

	return nil
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
