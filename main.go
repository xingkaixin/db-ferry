package main

import (
	"context"
	_ "embed"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"db-ferry/config"
	"db-ferry/daemon"
	"db-ferry/database"
	"db-ferry/diff"
	"db-ferry/doctor"
	mcpserver "db-ferry/mcp"
	"db-ferry/metrics"
	"db-ferry/notify"
	"db-ferry/processor"
	"db-ferry/sse"
)

const (
	defaultTomlPath    = "task.toml"
	configCommandName  = "config"
	configInitCommand  = "init"
	doctorCommandName  = "doctor"
	historyCommandName = "history"
	diffCommandName    = "diff"
	mcpCommandName     = "mcp"
	mcpServeCommand    = "serve"
	daemonCommandName  = "daemon"
)

var configTemplateTarget = "task.toml"

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
		tomlPath             = flags.String("config", defaultTomlPath, "Path to task.toml configuration file")
		verbose              = flags.Bool("v", false, "Enable verbose logging")
		showVersion          = flags.Bool("version", false, "Show version information")
		dryRun               = flags.Bool("dry-run", false, "Preview the migration plan without executing")
		federatedMemoryLimit = flags.Int("federated-memory-limit", 1000000, "Max rows per source for federated in-memory JOIN")
		ssePort              = flags.String("sse-port", "", "SSE server listen address (e.g., :8080) for real-time progress streaming")
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var rec metrics.Recorder = metrics.NewNoopRecorder()
	if cfg.Metrics.Enabled {
		rec = metrics.NewPrometheusRecorder(version, cfg.Metrics.Endpoint)
		if cfg.Metrics.ListenAddr != "" {
			if err := rec.ServeHTTP(cfg.Metrics.ListenAddr); err != nil {
				return 1, fmt.Errorf("failed to start metrics server: %w", err)
			}
			log.Printf("Metrics server listening on %s", cfg.Metrics.ListenAddr)
		}
		if cfg.Metrics.Endpoint != "" {
			interval, err := time.ParseDuration(cfg.Metrics.Interval)
			if err != nil {
				return 1, fmt.Errorf("invalid metrics interval: %w", err)
			}
			go metrics.StartPushLoop(ctx, rec, interval)
		}
		if cfg.Metrics.ListenAddr == "" && cfg.Metrics.Endpoint == "" {
			log.Println("Warning: metrics enabled but no listen_addr or endpoint configured")
		}
	}

	manager := database.NewConnectionManager(cfg)
	proc := processor.NewProcessorWithVersion(manager, cfg, version, rec)
	proc.SetFederatedMemoryLimit(*federatedMemoryLimit)

	var sseServer *sse.Server
	if *ssePort != "" {
		sseServer = sse.NewServer()
		if err := sseServer.Start(*ssePort); err != nil {
			return 1, fmt.Errorf("failed to start SSE server: %w", err)
		}
		log.Printf("SSE server listening on %s", sseServer.Addr())
		proc.SetProgressNotifier(func(event processor.ProgressEvent) {
			var evtType sse.EventType
			switch event.Type {
			case "task.start":
				evtType = sse.EventTaskStart
			case "task.progress":
				evtType = sse.EventTaskProgress
			case "task.complete":
				evtType = sse.EventTaskComplete
			case "task.error":
				evtType = sse.EventTaskError
			default:
				return
			}
			percentage := 0.0
			if event.TotalRows > 0 {
				percentage = float64(event.Processed) / float64(event.TotalRows) * 100
			}
			sseServer.Send(sse.Event{
				Type: evtType,
				Data: sse.TaskProgressData{
					Task:          event.TaskName,
					SourceDB:      event.SourceDB,
					TargetDB:      event.TargetDB,
					EstimatedRows: event.TotalRows,
					Processed:     event.Processed,
					Percentage:    percentage,
					DurationMs:    event.DurationMs,
					Error:         event.Error,
				},
				Time: time.Now(),
			})
		})
		defer func() {
			if err := sseServer.Stop(); err != nil {
				log.Printf("Warning: failed to stop SSE server: %v", err)
			}
		}()
	}

	defer func() {
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := rec.Shutdown(shutdownCtx); err != nil {
			log.Printf("Warning: failed to shutdown metrics: %v", err)
		}
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

	startedAt := time.Now()
	var processErr error

	if hasCDCTasks(cfg) {
		// CDC mode: run initial sync then poll continuously.
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigCh
			log.Println("Received shutdown signal, stopping CDC polling...")
			cancel()
		}()
		processErr = proc.ProcessCDCTasksContext(ctx)
	} else {
		processErr = proc.ProcessAllTasksContext(ctx)
	}
	duration := time.Since(startedAt)

	if cfg.Notify.HasURLs() {
		event := "migration.success"
		if processErr != nil {
			event = "migration.failure"
		}
		client := notify.NewClient(cfg.Notify)
		if err := client.Send(event, *tomlPath, proc.TaskResults(), duration); err != nil {
			log.Printf("Warning: failed to send notification: %v", err)
		}
	}

	if processErr != nil {
		return 1, fmt.Errorf("failed to process tasks: %w", processErr)
	}

	log.Println("All tasks completed successfully!")
	return 0, nil
}

func hasCDCTasks(cfg *config.Config) bool {
	for _, task := range cfg.Tasks {
		if !task.Ignore && task.CDC.Enabled {
			return true
		}
	}
	return false
}

func runCommand(args []string, tomlPath string, stdout io.Writer) (int, error) {
	switch args[0] {
	case configCommandName:
		return runConfigCommand(args[1:], stdout)
	case doctorCommandName:
		return runDoctorCommand(args[1:], tomlPath, stdout)
	case historyCommandName:
		return runHistoryCommand(args[1:], tomlPath, stdout)
	case diffCommandName:
		return runDiffCommand(args[1:], tomlPath, stdout)
	case mcpCommandName:
		return runMCPCommand(args[1:], stdout)
	case daemonCommandName:
		return runDaemonCommand(args[1:], tomlPath, stdout)
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

func runHistoryCommand(args []string, tomlPath string, stdout io.Writer) (int, error) {
	flags := flag.NewFlagSet("history", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	limit := flags.Int("n", 10, "Number of recent migrations to show")
	if err := flags.Parse(args); err != nil {
		return 2, err
	}
	if len(flags.Args()) > 0 {
		return 2, fmt.Errorf("unknown history argument: %s", flags.Args()[0])
	}

	cfg, err := config.LoadConfig(tomlPath)
	if err != nil {
		return 1, fmt.Errorf("failed to load configuration: %w", err)
	}

	targetAliases := make(map[string]struct{})
	for _, task := range cfg.Tasks {
		if !task.Ignore {
			targetAliases[task.TargetDB] = struct{}{}
		}
	}
	if len(targetAliases) == 0 {
		fmt.Fprintln(stdout, "No target databases found.")
		return 0, nil
	}

	manager := database.NewConnectionManager(cfg)
	defer func() { _ = manager.CloseAll() }()

	var allRecords []database.MigrationRecord
	for alias := range targetAliases {
		targetDB, err := manager.GetTarget(alias)
		if err != nil {
			log.Printf("Warning: failed to connect to target %s: %v", alias, err)
			continue
		}
		dbCfg, ok := cfg.GetDatabase(alias)
		if !ok {
			continue
		}
		recorder := database.NewHistoryRecorder(dbCfg.Type, cfg.History.Table())
		records, err := recorder.List(targetDB, *limit)
		if err != nil {
			continue
		}
		allRecords = append(allRecords, records...)
	}

	if len(allRecords) == 0 {
		fmt.Fprintln(stdout, "No migration history found.")
		return 0, nil
	}

	sort.Slice(allRecords, func(i, j int) bool {
		return allRecords[i].StartedAt.After(allRecords[j].StartedAt)
	})
	if len(allRecords) > *limit {
		allRecords = allRecords[:*limit]
	}

	w := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "STARTED\tTASK\tMODE\tROWS\tFAILED\tRESULT\tSOURCE\tTARGET")
	for _, rec := range allRecords {
		started := rec.StartedAt.Format(time.RFC3339)
		if rec.StartedAt.IsZero() {
			started = "-"
		}
		result := rec.ValidationResult
		if result == "" {
			result = "-"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%d\t%s\t%s\t%s\n",
			started, rec.TaskName, rec.Mode, rec.RowsProcessed, rec.RowsFailed, result, rec.SourceDB, rec.TargetDB)
	}
	_ = w.Flush()
	return 0, nil
}

func runDiffCommand(args []string, tomlPath string, stdout io.Writer) (int, error) {
	flags := flag.NewFlagSet("diff", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)

	taskName := flags.String("task", "", "Task table_name to diff (required)")
	output := flags.String("output", "", "Output file path (default: stdout)")
	format := flags.String("format", "json", "Output format: json, csv, html")
	where := flags.String("where", "", "Optional WHERE clause applied to both sides")
	limit := flags.Int("limit", 0, "Max rows to compare on each side (0 = unlimited)")
	keys := flags.String("keys", "", "Comma-separated diff keys (overrides merge_keys)")

	if err := flags.Parse(args); err != nil {
		return 2, err
	}

	if len(flags.Args()) > 0 {
		return 2, fmt.Errorf("diff does not accept positional arguments")
	}

	if *taskName == "" {
		return 2, fmt.Errorf("-task is required")
	}

	cfg, err := config.LoadConfig(tomlPath)
	if err != nil {
		return 1, fmt.Errorf("failed to load configuration: %w", err)
	}

	var keyList []string
	if *keys != "" {
		keyList = strings.Split(*keys, ",")
		for i := range keyList {
			keyList[i] = strings.TrimSpace(keyList[i])
		}
	}

	opts := diff.Options{
		TaskName: *taskName,
		Output:   *output,
		Format:   *format,
		Where:    *where,
		Limit:    *limit,
		Keys:     keyList,
	}

	if err := diff.Run(cfg, opts, stdout); err != nil {
		return 1, err
	}
	return 0, nil
}

func runMCPCommand(args []string, stdout io.Writer) (int, error) {
	if len(args) == 0 {
		return 2, fmt.Errorf("missing mcp subcommand")
	}

	switch args[0] {
	case mcpServeCommand:
		return runMCPServeCommand(args[1:], stdout)
	default:
		return 2, fmt.Errorf("unknown mcp subcommand: %s", args[0])
	}
}

func runMCPServeCommand(args []string, stdout io.Writer) (int, error) {
	flags := flag.NewFlagSet("mcp serve", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)

	if err := flags.Parse(args); err != nil {
		return 2, err
	}

	if len(flags.Args()) > 0 {
		return 2, fmt.Errorf("mcp serve does not accept positional arguments")
	}

	srv := mcpserver.NewServer(version)
	if err := srv.ServeStdio(); err != nil {
		return 1, fmt.Errorf("mcp server error: %w", err)
	}
	return 0, nil
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

func runDaemonCommand(args []string, tomlPath string, stdout io.Writer) (int, error) {
	flags := flag.NewFlagSet("daemon", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)

	watch := flags.Bool("watch", false, "Watch config file for changes and auto-reload")
	healthAddr := flags.String("health-addr", ":8080", "HTTP health check listen address")

	if err := flags.Parse(args); err != nil {
		return 2, err
	}
	if len(flags.Args()) > 0 {
		return 2, fmt.Errorf("unknown daemon argument: %s", flags.Args()[0])
	}

	d := daemon.New(daemon.Options{
		ConfigPath:   tomlPath,
		HealthAddr:   *healthAddr,
		WatchEnabled: *watch,
		Version:      version,
	})

	hsrv := daemon.NewHealthServer(*healthAddr, d)
	hsrv.Start()
	defer func() { _ = hsrv.Stop() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Received shutdown signal, stopping daemon...")
		d.Stop()
	}()

	fmt.Fprintf(stdout, "Daemon started (watch=%v, health=%s)\n", *watch, *healthAddr)
	if err := d.Run(); err != nil {
		return 1, fmt.Errorf("daemon error: %w", err)
	}
	fmt.Fprintln(stdout, "Daemon stopped")
	return 0, nil
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
