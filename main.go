package main

import (
	_ "embed"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"db-ferry/config"
	"db-ferry/database"
	"db-ferry/doctor"
	"db-ferry/processor"
)

const (
	defaultTomlPath      = "task.toml"
	configCommandName    = "config"
	configInitCommand    = "init"
	doctorCommandName    = "doctor"
	configTemplateTarget = "task.toml"
)

var version = "dev"

var exitFn = os.Exit

//go:embed task.toml.sample
var defaultTaskTemplate string

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
		tomlPath    = flags.String("config", defaultTomlPath, "Path to task.toml configuration file")
		verbose     = flags.Bool("v", false, "Enable verbose logging")
		showVersion = flags.Bool("version", false, "Show version information")
		dryRun      = flags.Bool("dry-run", false, "Preview the migration plan without executing")
	)

	if err := flags.Parse(args); err != nil {
		return 2, err
	}

	if *showVersion {
		fmt.Fprintf(stdout, "db-ferry %s\n", version)
		return 0, nil
	}

	if *verbose {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
	} else {
		log.SetFlags(0)
	}

	if remainingArgs := flags.Args(); len(remainingArgs) > 0 {
		return runCommand(remainingArgs, *tomlPath, stdout)
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

	if *dryRun {
		if err := proc.PlanAllTasks(stdout); err != nil {
			return 1, fmt.Errorf("failed to generate plan: %w", err)
		}
		return 0, nil
	}

	if err := proc.ProcessAllTasks(); err != nil {
		return 1, fmt.Errorf("failed to process tasks: %w", err)
	}

	log.Println("All tasks completed successfully!")
	return 0, nil
}

func runCommand(args []string, tomlPath string, stdout io.Writer) (int, error) {
	switch args[0] {
	case configCommandName:
		return runConfigCommand(args[1:], stdout)
	case doctorCommandName:
		return runDoctorCommand(args[1:], tomlPath, stdout)
	default:
		return 2, fmt.Errorf("unknown command: %s", args[0])
	}
}

func runConfigCommand(args []string, stdout io.Writer) (int, error) {
	if len(args) == 0 {
		return 2, fmt.Errorf("missing config subcommand")
	}

	switch args[0] {
	case configInitCommand:
		return runConfigInitCommand(args[1:], stdout)
	default:
		return 2, fmt.Errorf("unknown config subcommand: %s", args[0])
	}
}

func runDoctorCommand(args []string, tomlPath string, stdout io.Writer) (int, error) {
	if len(args) > 0 {
		return 2, fmt.Errorf("unknown doctor argument: %s", args[0])
	}

	doc := doctor.New(tomlPath)
	return doc.Run(stdout), nil
}

func runConfigInitCommand(args []string, stdout io.Writer) (int, error) {
	flags := flag.NewFlagSet("config init", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)

	interactive := flags.Bool("interactive", false, "Run interactive configuration wizard")
	flags.BoolVar(interactive, "i", false, "Run interactive configuration wizard (shorthand)")

	if err := flags.Parse(args); err != nil {
		return 2, err
	}

	if len(flags.Args()) > 0 {
		return 2, fmt.Errorf("config init does not accept positional arguments")
	}

	if *interactive {
		return runInteractiveWizard(stdout)
	}
	return initConfigTemplate(stdout)
}

func initConfigTemplate(stdout io.Writer) (int, error) {
	if _, err := os.Stat(configTemplateTarget); err == nil {
		return 1, fmt.Errorf("%s already exists in current directory", configTemplateTarget)
	} else if !os.IsNotExist(err) {
		return 1, fmt.Errorf("failed to check %s: %w", configTemplateTarget, err)
	}

	if err := os.WriteFile(configTemplateTarget, []byte(defaultTaskTemplate), 0o644); err != nil {
		return 1, fmt.Errorf("failed to write %s: %w", configTemplateTarget, err)
	}

	fmt.Fprintf(stdout, "Created %s in current directory\n", configTemplateTarget)
	return 0, nil
}
