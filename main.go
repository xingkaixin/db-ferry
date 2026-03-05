package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"db-ferry/config"
	"db-ferry/database"
	"db-ferry/processor"
)

const (
	defaultTomlPath = "task.toml"
)

var exitFn = os.Exit

func main() {
	code, err := run(os.Args[1:], os.Stdout, os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	exitFn(code)
}

func run(args []string, stdout io.Writer, stderr io.Writer) (int, error) {
	flags := flag.NewFlagSet("db-ferry", flag.ContinueOnError)
	flags.SetOutput(stderr)

	var (
		tomlPath = flags.String("config", defaultTomlPath, "Path to task.toml configuration file")
		verbose  = flags.Bool("v", false, "Enable verbose logging")
		version  = flags.Bool("version", false, "Show version information")
	)

	if err := flags.Parse(args); err != nil {
		return 2, err
	}

	if *version {
		fmt.Fprintln(stdout, "Multi-Source to SQLite Migration Tool v0.5.0 (Oracle/MySQL Support)")
		return 0, nil
	}

	if *verbose {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
	} else {
		log.SetFlags(0)
	}

	log.Println("Starting multi-database migration tool...")

	cfg, err := config.LoadConfig(*tomlPath)
	if err != nil {
		return 1, fmt.Errorf("failed to load configuration: %w", err)
	}

	log.Printf("Loaded %d tasks from configuration", len(cfg.Tasks))

	manager := database.NewConnectionManager(cfg)
	proc := processor.NewProcessor(manager, cfg)
	defer func() {
		if closeErr := proc.Close(); closeErr != nil {
			log.Printf("Warning: failed to close resources: %v", closeErr)
		}
	}()

	if err := proc.ProcessAllTasks(); err != nil {
		return 1, fmt.Errorf("failed to process tasks: %w", err)
	}

	log.Println("All tasks completed successfully!")
	return 0, nil
}
