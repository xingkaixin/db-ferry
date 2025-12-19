package config

import (
	"fmt"
	"strings"

	"github.com/BurntSushi/toml"
)

// Supported database types.
const (
	DatabaseTypeOracle = "oracle"
	DatabaseTypeMySQL  = "mysql"
	DatabaseTypeSQLite = "sqlite"
	DatabaseTypeDuckDB = "duckdb"
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
	TableName string `toml:"table_name"`
	SQL       string `toml:"sql"`
	SourceDB  string `toml:"source_db"`
	TargetDB  string `toml:"target_db"`
	Ignore    bool   `toml:"ignore"`
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
	case DatabaseTypeOracle, DatabaseTypeMySQL, DatabaseTypeSQLite, DatabaseTypeDuckDB:
		return nil
	default:
		return fmt.Errorf("database '%s' of type '%s' cannot be used as source", db.Name, db.Type)
	}
}

func ensureDatabaseSupportsTarget(db *DatabaseConfig) error {
	switch strings.ToLower(db.Type) {
	case DatabaseTypeOracle, DatabaseTypeMySQL, DatabaseTypeSQLite, DatabaseTypeDuckDB:
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
