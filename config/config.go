package config

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/BurntSushi/toml"
)

type OracleConfig struct {
	Host     string
	Port     string
	Service  string
	User     string
	Password string
}

type MySQLConfig struct {
	Host     string
	Port     string
	Database string
	User     string
	Password string
}

type IndexColumn struct {
	Name  string // 列名
	Order string // "ASC" 或 "DESC"
}

type IndexConfig struct {
	Name          string         `toml:"name"`          // 索引名
	Columns       []string       `toml:"columns"`       // 列配置: "column" 或 "column:1" 或 "column:-1"
	Unique        bool           `toml:"unique"`        // 是否唯一索引
	Where         string         `toml:"where"`         // 可选的 WHERE 子句
	ParsedColumns []IndexColumn `toml:"-"`             // 运行时解析的列信息
}

// ParseColumns 解析列配置字符串，将 "column:1" 转换为 IndexColumn
func (ic *IndexConfig) ParseColumns() error {
	ic.ParsedColumns = make([]IndexColumn, len(ic.Columns))

	for i, col := range ic.Columns {
		if strings.Contains(col, ":") {
			parts := strings.Split(col, ":")
			if len(parts) != 2 {
				return fmt.Errorf("invalid column format: %s", col)
			}

			orderSpecifier := strings.TrimSpace(parts[1])
			if orderSpecifier != "1" && orderSpecifier != "-1" {
				return fmt.Errorf("invalid order specifier: %s (must be 1 or -1)", orderSpecifier)
			}

			ic.ParsedColumns[i] = IndexColumn{
				Name:  strings.TrimSpace(parts[0]),
				Order: map[string]string{"1": "ASC", "-1": "DESC"}[orderSpecifier],
			}
		} else {
			// 默认升序
			ic.ParsedColumns[i] = IndexColumn{
				Name:  strings.TrimSpace(col),
				Order: "ASC",
			}
		}
	}

	return nil
}

type TaskConfig struct {
	TableName  string         `toml:"table_name"`
	SQL        string         `toml:"sql"`
	SourceType string         `toml:"source_type,omitempty"` // "oracle" 或 "mysql"，默认为 "oracle"
	Ignore     bool           `toml:"ignore"`
	Indexes    []IndexConfig  `toml:"indexes,omitempty"`     // 表级别的索引配置
}

type Config struct {
	OracleConfig  OracleConfig
	MySQLConfig   MySQLConfig
	SQLiteDBPath  string
	Tasks         []TaskConfig
}

func LoadConfig(envPath, tomlPath string) (*Config, error) {
	// Load .env file
	if err := godotenv.Load(envPath); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("error loading .env file: %w", err)
		}
		log.Printf("Warning: .env file not found at %s", envPath)
	}

	// Load TOML configuration
	config := &Config{}
	if _, err := toml.DecodeFile(tomlPath, config); err != nil {
		return nil, fmt.Errorf("error decoding TOML file: %w", err)
	}

	// Parse Oracle configuration from environment
	config.OracleConfig = OracleConfig{
		Host:     getEnvOrDefault("ORACLE_HOST", "localhost"),
		Port:     getEnvOrDefault("ORACLE_PORT", "1521"),
		Service:  getEnvOrDefault("ORACLE_SERVICE", "ORCL"),
		User:     getEnvOrDefault("ORACLE_USER", ""),
		Password: getEnvOrDefault("ORACLE_PASSWORD", ""),
	}

	// Parse MySQL configuration from environment
	config.MySQLConfig = MySQLConfig{
		Host:     getEnvOrDefault("MYSQL_HOST", "localhost"),
		Port:     getEnvOrDefault("MYSQL_PORT", "3306"),
		Database: getEnvOrDefault("MYSQL_DATABASE", ""),
		User:     getEnvOrDefault("MYSQL_USER", ""),
		Password: getEnvOrDefault("MYSQL_PASSWORD", ""),
	}

	// Get SQLite database path
	config.SQLiteDBPath = getEnvOrDefault("SQLITE_DB_PATH", "./data.db")

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return config, nil
}

func (c *Config) Validate() error {
	if c.SQLiteDBPath == "" {
		return fmt.Errorf("SQLITE_DB_PATH is required")
	}
	if len(c.Tasks) == 0 {
		return fmt.Errorf("at least one task must be defined in %s", "task.toml")
	}

	// 检查是否有任务使用Oracle，如果有则验证Oracle配置
	hasOracleTasks := false
	hasMySQLTasks := false
	for _, task := range c.Tasks {
		if task.Ignore {
			continue
		}
		sourceType := task.SourceType
		if sourceType == "" {
			sourceType = "oracle" // 默认值
		}
		if sourceType == "oracle" {
			hasOracleTasks = true
		} else if sourceType == "mysql" {
			hasMySQLTasks = true
		} else {
			return fmt.Errorf("invalid source_type '%s' in task '%s', must be 'oracle' or 'mysql'", sourceType, task.TableName)
		}
	}

	if hasOracleTasks {
		if c.OracleConfig.User == "" {
			return fmt.Errorf("ORACLE_USER is required when using Oracle source database")
		}
		if c.OracleConfig.Password == "" {
			return fmt.Errorf("ORACLE_PASSWORD is required when using Oracle source database")
		}
	}

	if hasMySQLTasks {
		if c.MySQLConfig.User == "" {
			return fmt.Errorf("MYSQL_USER is required when using MySQL source database")
		}
		if c.MySQLConfig.Password == "" {
			return fmt.Errorf("MYSQL_PASSWORD is required when using MySQL source database")
		}
		if c.MySQLConfig.Database == "" {
			return fmt.Errorf("MYSQL_DATABASE is required when using MySQL source database")
		}
	}

	// 用于检查索引名重复的 map
	indexNames := make(map[string]string)

	for i, task := range c.Tasks {
		if task.TableName == "" {
			return fmt.Errorf("task %d: table_name is required", i+1)
		}
		if task.SQL == "" {
			return fmt.Errorf("task %d: sql is required", i+1)
		}

		// 验证索引配置
		for j, index := range task.Indexes {
			if index.Name == "" {
				return fmt.Errorf("task %d, index %d: index name is required", i+1, j+1)
			}
			if len(index.Columns) == 0 {
				return fmt.Errorf("task %d, index %d: at least one column is required", i+1, j+1)
			}

			// 检查索引名是否重复
			if existingTable, exists := indexNames[index.Name]; exists {
				if existingTable == task.TableName {
					return fmt.Errorf("task %d, index %d: index name '%s' is already defined for table '%s'", i+1, j+1, index.Name, task.TableName)
				} else {
					return fmt.Errorf("task %d, index %d: index name '%s' is already used by table '%s'", i+1, j+1, index.Name, existingTable)
				}
			}
			indexNames[index.Name] = task.TableName

			// 验证列配置格式
			if err := index.ParseColumns(); err != nil {
				return fmt.Errorf("task %d, index %d: %w", i+1, j+1, err)
			}
		}
	}
	return nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func (c *Config) GetOracleConnectionString() string {
	// go-ora/v2 connection string format
	return fmt.Sprintf("oracle://%s:%s@%s:%s/%s",
		c.OracleConfig.User,
		c.OracleConfig.Password,
		c.OracleConfig.Host,
		c.OracleConfig.Port,
		c.OracleConfig.Service,
	)
}

func (c *Config) GetMySQLConnectionString() string {
	// go-sql-driver/mysql connection string format
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		c.MySQLConfig.User,
		c.MySQLConfig.Password,
		c.MySQLConfig.Host,
		c.MySQLConfig.Port,
		c.MySQLConfig.Database,
	)
}