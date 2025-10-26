package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"db-ferry/config"
	"db-ferry/database"
	"db-ferry/processor"
)

const (
	defaultTomlPath = "task.toml"
)

func main() {
	var (
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

	log.Println("Starting multi-database migration tool...")

	cfg, err := config.LoadConfig(*tomlPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("Loaded %d tasks from configuration", len(cfg.Tasks))

	manager := database.NewConnectionManager(cfg)
	proc := processor.NewProcessor(manager, cfg)
	defer func() {
		if err := proc.Close(); err != nil {
			log.Printf("Warning: failed to close resources: %v", err)
		}
	}()

	if err := proc.ProcessAllTasks(); err != nil {
		log.Fatalf("Failed to process tasks: %v", err)
	}

	log.Println("All tasks completed successfully!")
}
