package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"db-ferry/config"
	"db-ferry/database"

	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/huh/spinner"
)

// wizardState holds all user inputs during the interactive session.
type wizardState struct {
	SourceDB config.DatabaseConfig
	TargetDB config.DatabaseConfig

	SourceTables   []string
	SelectedTables []string

	Mode       string
	BatchSize  int
	MaxRetries int
	Validate   string
	StateFile  string
	ResumeKey  string
	MergeKeys  []string
}

func runInteractiveWizard(stdout io.Writer) (int, error) {
	state := &wizardState{}

	// Step 1: source database
	if err := collectSourceDB(state); err != nil {
		return 1, err
	}

	// Step 2: test source connection & list tables
	sourceTables, sourceConn, err := testSourceAndListTables(state.SourceDB)
	if err != nil {
		return 1, err
	}
	if sourceConn != nil {
		defer sourceConn.Close()
	}
	state.SourceTables = sourceTables

	if len(state.SourceTables) == 0 {
		return 1, fmt.Errorf("source database has no tables or views")
	}

	// Step 3: select tables
	if err := selectTables(state); err != nil {
		return 1, err
	}
	if len(state.SelectedTables) == 0 {
		return 1, fmt.Errorf("no tables selected")
	}

	// Step 4: target database
	if err := collectTargetDB(state); err != nil {
		return 1, err
	}

	// Step 5: test target connection
	targetConn, err := testTargetConnection(state.TargetDB)
	if err != nil {
		return 1, err
	}
	if targetConn != nil {
		defer targetConn.Close()
	}

	// Step 6: advanced options
	if err := collectAdvancedOptions(state); err != nil {
		return 1, err
	}

	// Step 7: confirm & write
	tomlContent, err := generateTOML(state)
	if err != nil {
		return 1, err
	}

	confirmed := false
	if err := huh.NewConfirm().
		Title("Ready to create task.toml?").
		Description("The configuration will be written to the current directory.").
		Value(&confirmed).
		Run(); err != nil {
		return 1, err
	}
	if !confirmed {
		fmt.Fprintln(stdout, "Aborted.")
		return 0, nil
	}

	if _, err := os.Stat(configTemplateTarget); err == nil {
		overwrite := false
		if err := huh.NewConfirm().
			Title(fmt.Sprintf("%s already exists. Overwrite?", configTemplateTarget)).
			Value(&overwrite).
			Run(); err != nil {
			return 1, err
		}
		if !overwrite {
			fmt.Fprintln(stdout, "Aborted.")
			return 0, nil
		}
	}

	if err := os.WriteFile(configTemplateTarget, []byte(tomlContent), 0o644); err != nil {
		return 1, fmt.Errorf("failed to write %s: %w", configTemplateTarget, err)
	}

	fmt.Fprintf(stdout, "Created %s in current directory\n", configTemplateTarget)
	return 0, nil
}

func collectSourceDB(state *wizardState) error {
	dbType := ""
	if err := huh.NewSelect[string]().
		Title("Select source database type").
		Options(
			huh.NewOption("Oracle", config.DatabaseTypeOracle),
			huh.NewOption("MySQL", config.DatabaseTypeMySQL),
			huh.NewOption("PostgreSQL", config.DatabaseTypePostgreSQL),
			huh.NewOption("SQL Server", config.DatabaseTypeSQLServer),
			huh.NewOption("SQLite", config.DatabaseTypeSQLite),
			huh.NewOption("DuckDB", config.DatabaseTypeDuckDB),
		).
		Value(&dbType).
		Run(); err != nil {
		return err
	}
	state.SourceDB.Type = dbType
	state.SourceDB.Name = "source_db"

	fields := []huh.Field{
		huh.NewInput().
			Title("Database alias name").
			Description("A unique name to reference this database in tasks").
			Value(&state.SourceDB.Name).
			Validate(func(s string) error {
				if strings.TrimSpace(s) == "" {
					return fmt.Errorf("alias name is required")
				}
				return nil
			}),
	}

	if needsHostPort(dbType) {
		fields = append(fields,
			huh.NewInput().Title("Host").Value(&state.SourceDB.Host).Validate(nonEmpty("host is required")),
			huh.NewInput().Title("Port").Value(&state.SourceDB.Port).Placeholder(defaultPort(dbType)).Validate(nonEmpty("port is required")),
		)
		if dbType == config.DatabaseTypeOracle {
			fields = append(fields, huh.NewInput().Title("Service Name").Value(&state.SourceDB.Service).Validate(nonEmpty("service is required")))
		} else {
			fields = append(fields, huh.NewInput().Title("Database Name").Value(&state.SourceDB.Database).Validate(nonEmpty("database name is required")))
		}
		fields = append(fields,
			huh.NewInput().Title("User").Value(&state.SourceDB.User).Validate(nonEmpty("user is required")),
			huh.NewInput().Title("Password").Value(&state.SourceDB.Password).Validate(nonEmpty("password is required")),
		)
	} else {
		fields = append(fields, huh.NewInput().Title("File Path").Value(&state.SourceDB.Path).Validate(nonEmpty("path is required")))
	}

	return huh.NewForm(huh.NewGroup(fields...)).Run()
}

func collectTargetDB(state *wizardState) error {
	dbType := ""
	if err := huh.NewSelect[string]().
		Title("Select target database type").
		Options(
			huh.NewOption("Oracle", config.DatabaseTypeOracle),
			huh.NewOption("MySQL", config.DatabaseTypeMySQL),
			huh.NewOption("PostgreSQL", config.DatabaseTypePostgreSQL),
			huh.NewOption("SQL Server", config.DatabaseTypeSQLServer),
			huh.NewOption("SQLite", config.DatabaseTypeSQLite),
			huh.NewOption("DuckDB", config.DatabaseTypeDuckDB),
		).
		Value(&dbType).
		Run(); err != nil {
		return err
	}
	state.TargetDB.Type = dbType
	state.TargetDB.Name = "target_db"

	fields := []huh.Field{
		huh.NewInput().
			Title("Database alias name").
			Description("A unique name to reference this database in tasks").
			Value(&state.TargetDB.Name).
			Validate(func(s string) error {
				if strings.TrimSpace(s) == "" {
					return fmt.Errorf("alias name is required")
				}
				if strings.TrimSpace(s) == state.SourceDB.Name {
					return fmt.Errorf("target alias must differ from source alias")
				}
				return nil
			}),
	}

	if needsHostPort(dbType) {
		fields = append(fields,
			huh.NewInput().Title("Host").Value(&state.TargetDB.Host).Validate(nonEmpty("host is required")),
			huh.NewInput().Title("Port").Value(&state.TargetDB.Port).Placeholder(defaultPort(dbType)).Validate(nonEmpty("port is required")),
		)
		if dbType == config.DatabaseTypeOracle {
			fields = append(fields, huh.NewInput().Title("Service Name").Value(&state.TargetDB.Service).Validate(nonEmpty("service is required")))
		} else {
			fields = append(fields, huh.NewInput().Title("Database Name").Value(&state.TargetDB.Database).Validate(nonEmpty("database name is required")))
		}
		fields = append(fields,
			huh.NewInput().Title("User").Value(&state.TargetDB.User).Validate(nonEmpty("user is required")),
			huh.NewInput().Title("Password").Value(&state.TargetDB.Password).Validate(nonEmpty("password is required")),
		)
	} else {
		fields = append(fields, huh.NewInput().Title("File Path").Value(&state.TargetDB.Path).Validate(nonEmpty("path is required")))
	}

	return huh.NewForm(huh.NewGroup(fields...)).Run()
}

func testSourceAndListTables(dbCfg config.DatabaseConfig) ([]string, database.SourceDB, error) {
	var tables []string
	var src database.SourceDB
	var err error

	action := func() {
		src, err = database.OpenSource(dbCfg)
		if err != nil {
			return
		}
		tables, err = src.GetTables()
	}

	if spinErr := spinner.New().Title("Testing source connection...").Action(action).Run(); spinErr != nil {
		return nil, nil, spinErr
	}
	if err != nil {
		return nil, src, fmt.Errorf("source connection failed: %w", err)
	}
	return tables, src, nil
}

func testTargetConnection(dbCfg config.DatabaseConfig) (database.TargetDB, error) {
	var tgt database.TargetDB
	var err error

	action := func() {
		tgt, err = database.OpenTarget(dbCfg)
	}

	if spinErr := spinner.New().Title("Testing target connection...").Action(action).Run(); spinErr != nil {
		return nil, spinErr
	}
	if err != nil {
		return tgt, fmt.Errorf("target connection failed: %w", err)
	}
	return tgt, nil
}

func selectTables(state *wizardState) error {
	options := make([]huh.Option[string], len(state.SourceTables))
	for i, t := range state.SourceTables {
		options[i] = huh.NewOption(t, t)
	}

	return huh.NewMultiSelect[string]().
		Title("Select tables/views to migrate").
		Description(fmt.Sprintf("Found %d object(s) in the source database", len(state.SourceTables))).
		Options(options...).
		Value(&state.SelectedTables).
		Run()
}

func collectAdvancedOptions(state *wizardState) error {
	mode := "replace"
	if err := huh.NewSelect[string]().
		Title("Default migration mode").
		Description("replace = drop & recreate; append = insert only; merge = upsert").
		Options(
			huh.NewOption("replace", config.TaskModeReplace),
			huh.NewOption("append", config.TaskModeAppend),
			huh.NewOption("merge", config.TaskModeMerge),
		).
		Value(&mode).
		Run(); err != nil {
		return err
	}
	state.Mode = mode

	batchSize := "1000"
	maxRetries := "2"
	validate := "none"
	stateFile := ""

	fields := []huh.Field{
		huh.NewInput().Title("Batch size").Value(&batchSize).Validate(nonEmpty("batch size is required")),
		huh.NewInput().Title("Max retries").Value(&maxRetries).Validate(nonEmpty("max retries is required")),
		huh.NewSelect[string]().Title("Validation").Options(
			huh.NewOption("none", config.TaskValidateNone),
			huh.NewOption("row_count", config.TaskValidateRowCount),
		).Value(&validate),
		huh.NewInput().Title("State file path (optional, for resume)").Value(&stateFile),
	}

	if err := huh.NewForm(huh.NewGroup(fields...)).Run(); err != nil {
		return err
	}

	state.BatchSize = parseInt(batchSize, 1000)
	state.MaxRetries = parseInt(maxRetries, 2)
	state.Validate = validate
	state.StateFile = strings.TrimSpace(stateFile)

	if state.StateFile != "" {
		resumeKey := ""
		if err := huh.NewInput().
			Title("Resume key (required with state_file)").
			Value(&resumeKey).
			Validate(nonEmpty("resume key is required")).
			Run(); err != nil {
			return err
		}
		state.ResumeKey = strings.TrimSpace(resumeKey)
	}

	if state.Mode == config.TaskModeMerge {
		keysStr := ""
		if err := huh.NewInput().
			Title("Merge keys (comma-separated)").
			Value(&keysStr).
			Validate(nonEmpty("merge keys are required")).
			Run(); err != nil {
			return err
		}
		state.MergeKeys = parseStringList(keysStr)
	}

	return nil
}

func generateTOML(state *wizardState) (string, error) {
	var b strings.Builder

	b.WriteString("#########################\n")
	b.WriteString("# Database Definitions  #\n")
	b.WriteString("#########################\n\n")

	writeDatabase(&b, state.SourceDB)
	b.WriteString("\n")
	writeDatabase(&b, state.TargetDB)
	b.WriteString("\n")

	b.WriteString("#########################\n")
	b.WriteString("# Task Definitions      #\n")
	b.WriteString("#########################\n\n")

	for _, table := range state.SelectedTables {
		writeTask(&b, table, state)
	}

	return b.String(), nil
}

func writeDatabase(b *strings.Builder, db config.DatabaseConfig) {
	b.WriteString(fmt.Sprintf("[[databases]]\n"))
	b.WriteString(fmt.Sprintf("name = %q\n", db.Name))
	b.WriteString(fmt.Sprintf("type = %q\n", db.Type))
	if needsHostPort(db.Type) {
		b.WriteString(fmt.Sprintf("host = %q\n", db.Host))
		b.WriteString(fmt.Sprintf("port = %q\n", db.Port))
		if db.Type == config.DatabaseTypeOracle {
			b.WriteString(fmt.Sprintf("service = %q\n", db.Service))
		} else {
			b.WriteString(fmt.Sprintf("database = %q\n", db.Database))
		}
		b.WriteString(fmt.Sprintf("user = %q\n", db.User))
		b.WriteString(fmt.Sprintf("password = %q\n", db.Password))
	} else {
		b.WriteString(fmt.Sprintf("path = %q\n", db.Path))
	}
}

func writeTask(b *strings.Builder, table string, state *wizardState) {
	b.WriteString(fmt.Sprintf("[[tasks]]\n"))
	b.WriteString(fmt.Sprintf("table_name = %q\n", table))
	sql := fmt.Sprintf("SELECT * FROM %s", quoteSQLIdentifier(state.SourceDB.Type, table))
	b.WriteString(fmt.Sprintf("sql = %q\n", sql))
	b.WriteString(fmt.Sprintf("source_db = %q\n", state.SourceDB.Name))
	b.WriteString(fmt.Sprintf("target_db = %q\n", state.TargetDB.Name))
	b.WriteString(fmt.Sprintf("ignore = false\n"))
	b.WriteString(fmt.Sprintf("mode = %q\n", state.Mode))
	b.WriteString(fmt.Sprintf("batch_size = %d\n", state.BatchSize))
	b.WriteString(fmt.Sprintf("max_retries = %d\n", state.MaxRetries))
	if state.Validate != config.TaskValidateNone {
		b.WriteString(fmt.Sprintf("validate = %q\n", state.Validate))
	}
	if state.StateFile != "" {
		b.WriteString(fmt.Sprintf("state_file = %q\n", state.StateFile))
		b.WriteString(fmt.Sprintf("resume_key = %q\n", state.ResumeKey))
	}
	if state.Mode == config.TaskModeMerge && len(state.MergeKeys) > 0 {
		b.WriteString(fmt.Sprintf("merge_keys = %s\n", tomlStringArray(state.MergeKeys)))
	}
	b.WriteString("\n")
}

func needsHostPort(dbType string) bool {
	switch dbType {
	case config.DatabaseTypeOracle, config.DatabaseTypeMySQL, config.DatabaseTypePostgreSQL, config.DatabaseTypeSQLServer:
		return true
	default:
		return false
	}
}

func defaultPort(dbType string) string {
	switch dbType {
	case config.DatabaseTypeOracle:
		return "1521"
	case config.DatabaseTypeMySQL:
		return "3306"
	case config.DatabaseTypePostgreSQL:
		return "5432"
	case config.DatabaseTypeSQLServer:
		return "1433"
	default:
		return ""
	}
}

func nonEmpty(msg string) func(string) error {
	return func(s string) error {
		if strings.TrimSpace(s) == "" {
			return errors.New(msg)
		}
		return nil
	}
}

func parseInt(s string, fallback int) int {
	var n int
	if _, err := fmt.Sscanf(strings.TrimSpace(s), "%d", &n); err != nil || n < 0 {
		return fallback
	}
	return n
}

func parseStringList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func tomlStringArray(items []string) string {
	quoted := make([]string, len(items))
	for i, item := range items {
		quoted[i] = fmt.Sprintf("%q", item)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

func quoteSQLIdentifier(dbType, name string) string {
	switch dbType {
	case config.DatabaseTypeMySQL:
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	case config.DatabaseTypeSQLServer:
		return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
	default:
		// postgresql, oracle, sqlite, duckdb
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
}
