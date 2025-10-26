package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"cbd_data_go/config"
	"cbd_data_go/database"
	"cbd_data_go/processor"
)

const (
	defaultEnvPath  = ".env"
	defaultTomlPath = "task.toml"
)

func main() {
	var (
		envPath  = flag.String("env", defaultEnvPath, "Path to .env file")
		tomlPath = flag.String("config", defaultTomlPath, "Path to task.toml configuration file")
		verbose  = flag.Bool("v", false, "Enable verbose logging")
		version  = flag.Bool("version", false, "Show version information")
	)
	flag.Parse()

	if *version {
		fmt.Println("Multi-Source to SQLite Migration Tool v1.1.0 (Oracle/MySQL Support)")
		os.Exit(0)
	}

	if *verbose {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
	} else {
		log.SetFlags(0)
	}

	log.Println("Starting multi-source to SQLite migration tool...")

	// Load configuration
	cfg, err := config.LoadConfig(*envPath, *tomlPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("Loaded %d tasks from configuration", len(cfg.Tasks))

	// Initialize source databases
	var oracleDB *database.OracleDB
	var mysqlDB *database.MySQLDB

	// Check if we need Oracle connection
	hasOracleTasks := false
	hasMySQLTasks := false
	for _, task := range cfg.Tasks {
		if task.Ignore {
			continue
		}
		sourceType := task.SourceType
		if sourceType == "" {
			sourceType = "oracle"
		}
		if sourceType == "oracle" {
			hasOracleTasks = true
		} else if sourceType == "mysql" {
			hasMySQLTasks = true
		}
	}

	// Connect to Oracle database if needed
	if hasOracleTasks {
		oracleDB, err = database.NewOracleDB(cfg.GetOracleConnectionString())
		if err != nil {
			log.Fatalf("Failed to connect to Oracle database: %v", err)
		}
		defer oracleDB.Close()
	}

	// Connect to MySQL database if needed
	if hasMySQLTasks {
		mysqlDB, err = database.NewMySQLDB(cfg.GetMySQLConnectionString())
		if err != nil {
			log.Fatalf("Failed to connect to MySQL database: %v", err)
		}
		defer mysqlDB.Close()
	}

	// Connect to SQLite database
	sqliteDB, err := database.NewSQLiteDB(cfg.SQLiteDBPath)
	if err != nil {
		log.Fatalf("Failed to connect to SQLite database: %v", err)
	}
	defer sqliteDB.Close()

	// Create processor and run tasks
	processor := processor.NewProcessor(oracleDB, mysqlDB, sqliteDB, cfg)

	if err := processor.ProcessAllTasks(); err != nil {
		log.Fatalf("Failed to process tasks: %v", err)
	}

	log.Println("All tasks completed successfully!")
}