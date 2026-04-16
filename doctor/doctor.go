package doctor

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"db-ferry/config"
	"db-ferry/database"

	"github.com/BurntSushi/toml"
	"golang.org/x/term"
)

// Status represents the result of a diagnostic check.
type Status int

const (
	StatusPass Status = iota
	StatusWarn
	StatusFail
	StatusSkip
)

func (s Status) String() string {
	switch s {
	case StatusPass:
		return "PASS"
	case StatusWarn:
		return "WARN"
	case StatusFail:
		return "FAIL"
	case StatusSkip:
		return "SKIP"
	default:
		return "UNKNOWN"
	}
}

func (s Status) color() string {
	switch s {
	case StatusPass:
		return "\033[32m"
	case StatusWarn, StatusSkip:
		return "\033[33m"
	case StatusFail:
		return "\033[31m"
	default:
		return "\033[0m"
	}
}

// CheckResult captures the outcome of a single diagnostic check.
type CheckResult struct {
	Name    string
	Status  Status
	Message string
}

// Doctor runs diagnostic checks against a db-ferry configuration.
type Doctor struct {
	tomlPath string
}

// New creates a new Doctor instance.
func New(tomlPath string) *Doctor {
	return &Doctor{tomlPath: tomlPath}
}

// Run executes all diagnostic checks and writes formatted results to stdout.
// It returns an exit code: 0 if no failures, 1 otherwise.
func (d *Doctor) Run(stdout io.Writer) int {
	results := d.runChecks()
	d.printResults(stdout, results)

	for _, r := range results {
		if r.Status == StatusFail {
			return 1
		}
	}
	return 0
}

func (d *Doctor) runChecks() []CheckResult {
	var results []CheckResult

	// 1. TOML syntax
	cfg, err := parseTOML(d.tomlPath)
	results = append(results, CheckResult{
		Name:    "TOML syntax",
		Status:  statusFromErr(err),
		Message: errMsg(err),
	})
	if err != nil {
		return results
	}

	// 2. Configuration validation
	err = cfg.Validate()
	results = append(results, CheckResult{
		Name:    "Configuration validation",
		Status:  statusFromErr(err),
		Message: errMsg(err),
	})
	if err != nil {
		return results
	}

	// 3. Database connections
	manager := database.NewConnectionManager(cfg)
	defer manager.CloseAll()

	connected := make(map[string]bool)
	for _, dbCfg := range cfg.Databases {
		_, err := manager.GetSource(dbCfg.Name)
		status := statusFromErr(err)
		msg := errMsg(err)
		if status == StatusPass {
			connected[dbCfg.Name] = true
			msg = fmt.Sprintf("type=%s", dbCfg.Type)
			// Also explicitly verify GetTarget for future source-only/target-only support.
			if _, err := manager.GetTarget(dbCfg.Name); err != nil {
				status = StatusFail
				msg = err.Error()
				connected[dbCfg.Name] = false
			}
		}
		results = append(results, CheckResult{
			Name:    fmt.Sprintf("Database connection: %s", dbCfg.Name),
			Status:  status,
			Message: msg,
		})
	}

	// Track checked items to avoid duplicates
	diskSpaceChecked := make(map[string]bool)
	targetPermissionChecked := make(map[string]bool)

	// Per-task checks
	for _, task := range cfg.Tasks {
		if task.Ignore {
			continue
		}

		// 4. Source permission / SQL syntax (combined execution)
		sourceOK := connected[task.SourceDB]
		var sourceErr error
		if sourceOK {
			sourceErr = checkSourceSQL(manager, task)
		}

		results = append(results, CheckResult{
			Name:    fmt.Sprintf("Source permission: %s", task.TableName),
			Status:  statusFromErr(sourceErr),
			Message: errMsg(sourceErr),
		})
		results = append(results, CheckResult{
			Name:    fmt.Sprintf("SQL syntax: %s", task.TableName),
			Status:  statusFromErr(sourceErr),
			Message: errMsg(sourceErr),
		})

		// 5. Column existence
		if sourceOK && sourceErr == nil {
			err := checkColumns(manager, task)
			results = append(results, CheckResult{
				Name:    fmt.Sprintf("Column existence: %s", task.TableName),
				Status:  statusFromErr(err),
				Message: errMsg(err),
			})
		} else {
			results = append(results, CheckResult{
				Name:    fmt.Sprintf("Column existence: %s", task.TableName),
				Status:  StatusSkip,
				Message: "skipped because source connection or SQL check failed",
			})
		}

		// 6. Target permission
		if connected[task.TargetDB] && !targetPermissionChecked[task.TargetDB] {
			targetPermissionChecked[task.TargetDB] = true
			err := checkTargetPermissions(manager, task, cfg)
			results = append(results, CheckResult{
				Name:    fmt.Sprintf("Target permission: %s", task.TableName),
				Status:  statusFromErr(err),
				Message: errMsg(err),
			})
		} else {
			reason := "skipped because target database connection failed"
			if targetPermissionChecked[task.TargetDB] {
				reason = "skipped because target permission already checked"
			}
			results = append(results, CheckResult{
				Name:    fmt.Sprintf("Target permission: %s", task.TableName),
				Status:  StatusSkip,
				Message: reason,
			})
		}

		// 7. Same-database migration
		if task.SourceDB == task.TargetDB {
			results = append(results, CheckResult{
				Name:   fmt.Sprintf("Same-database migration: %s", task.TableName),
				Status: StatusWarn,
				Message: fmt.Sprintf("source_db and target_db are both '%s'; ensure this is intentional",
					task.SourceDB),
			})
		}

		// 8. Disk space for file-based DBs
		targetDBCfg, _ := cfg.GetDatabase(task.TargetDB)
		if connected[task.TargetDB] && (targetDBCfg.Type == config.DatabaseTypeSQLite || targetDBCfg.Type == config.DatabaseTypeDuckDB) {
			if !diskSpaceChecked[task.TargetDB] {
				diskSpaceChecked[task.TargetDB] = true
				err := checkDiskSpace(targetDBCfg.Path)
				results = append(results, CheckResult{
					Name:    fmt.Sprintf("Disk space: %s", task.TargetDB),
					Status:  statusFromErr(err),
					Message: errMsg(err),
				})
			}
		}
	}

	return results
}

func (d *Doctor) printResults(w io.Writer, results []CheckResult) {
	useColor := false
	if f, ok := w.(*os.File); ok {
		useColor = term.IsTerminal(int(f.Fd()))
	}

	passCount, warnCount, failCount := 0, 0, 0
	for _, r := range results {
		switch r.Status {
		case StatusPass:
			passCount++
		case StatusWarn:
			warnCount++
		case StatusFail:
			failCount++
		}

		statusStr := r.Status.String()
		if useColor {
			statusStr = r.Status.color() + statusStr + "\033[0m"
		}

		if r.Message != "" {
			fmt.Fprintf(w, "[%s] %s: %s\n", statusStr, r.Name, r.Message)
		} else {
			fmt.Fprintf(w, "[%s] %s\n", statusStr, r.Name)
		}
	}

	fmt.Fprintf(w, "\n%d checks passed, %d warning, %d failure. ", passCount, warnCount, failCount)

	if failCount > 0 {
		fmt.Fprintln(w, "Fix issues before running db-ferry.")
	} else if warnCount > 0 {
		fmt.Fprintln(w, "Ready to ferry, but review warnings.")
	} else {
		fmt.Fprintln(w, "Ready to ferry.")
	}
}

func parseTOML(path string) (*config.Config, error) {
	cfg := &config.Config{}
	if _, err := toml.DecodeFile(path, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func checkSourceSQL(manager *database.ConnectionManager, task config.TaskConfig) error {
	sourceDB, err := manager.GetSource(task.SourceDB)
	if err != nil {
		return err
	}

	wrapped := fmt.Sprintf("SELECT * FROM (%s) db_ferry_check WHERE 1=0", trimSQL(task.SQL))
	rows, err := sourceDB.Query(wrapped)
	if err != nil {
		return err
	}
	return rows.Close()
}

func checkColumns(manager *database.ConnectionManager, task config.TaskConfig) error {
	sourceDB, err := manager.GetSource(task.SourceDB)
	if err != nil {
		return err
	}

	wrapped := fmt.Sprintf("SELECT * FROM (%s) db_ferry_check WHERE 1=0", trimSQL(task.SQL))
	rows, err := sourceDB.Query(wrapped)
	if err != nil {
		return err
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	columns := make([]string, len(columnTypes))
	for i, ct := range columnTypes {
		columns[i] = ct.Name()
	}

	if task.ResumeKey != "" {
		if !containsStringFold(columns, task.ResumeKey) {
			return fmt.Errorf("resume_key '%s' not found in query columns", task.ResumeKey)
		}
	}

	for _, key := range task.MergeKeys {
		if !containsStringFold(columns, key) {
			return fmt.Errorf("merge_key '%s' not found in query columns", key)
		}
	}

	for _, idx := range task.Indexes {
		for _, col := range idx.Columns {
			colName := col
			if strings.Contains(col, ":") {
				parts := strings.SplitN(col, ":", 2)
				colName = strings.TrimSpace(parts[0])
			}
			if !containsStringFold(columns, colName) {
				return fmt.Errorf("index column '%s' not found in query columns", colName)
			}
		}
	}

	return nil
}

func checkTargetPermissions(manager *database.ConnectionManager, task config.TaskConfig, cfg *config.Config) error {
	targetDB, err := manager.GetTarget(task.TargetDB)
	if err != nil {
		return err
	}

	targetDBCfg, _ := cfg.GetDatabase(task.TargetDB)
	tempTable := fmt.Sprintf("db_ferry_doctor_test_%d", time.Now().UnixNano())

	// Pre-cleanup in case a previous interrupted run left the table behind.
	_ = targetDB.Exec(dropTableSQL(targetDBCfg.Type, tempTable))

	columns := []database.ColumnMetadata{
		{Name: "doctor_value", DatabaseType: "INT", GoType: "int"},
	}

	if err := targetDB.CreateTable(tempTable, columns); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	if err := targetDB.InsertData(tempTable, columns, [][]any{{1}}); err != nil {
		_ = targetDB.Exec(dropTableSQL(targetDBCfg.Type, tempTable))
		return fmt.Errorf("failed to insert data: %w", err)
	}

	indexes := []config.IndexConfig{
		{Name: "idx_doctor_test", Columns: []string{"doctor_value"}},
	}
	for i := range indexes {
		if err := indexes[i].ParseColumns(); err != nil {
			_ = targetDB.Exec(dropTableSQL(targetDBCfg.Type, tempTable))
			return err
		}
	}

	if err := targetDB.CreateIndexes(tempTable, indexes); err != nil {
		_ = targetDB.Exec(dropTableSQL(targetDBCfg.Type, tempTable))
		return fmt.Errorf("failed to create index: %w", err)
	}

	_ = targetDB.Exec(dropTableSQL(targetDBCfg.Type, tempTable))
	return nil
}

func checkDiskSpace(path string) error {
	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}
	f, err := os.CreateTemp(dir, ".db_ferry_write_test_")
	if err != nil {
		return fmt.Errorf("directory not writable: %w", err)
	}
	_ = f.Close()
	_ = os.Remove(f.Name())
	return nil
}

func dropTableSQL(dbType, tableName string) string {
	switch dbType {
	case config.DatabaseTypeOracle:
		return fmt.Sprintf("BEGIN EXECUTE IMMEDIATE 'DROP TABLE %s'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;", tableName)
	case config.DatabaseTypeSQLServer:
		return fmt.Sprintf("IF OBJECT_ID(N'%s', 'U') IS NOT NULL DROP TABLE %s", tableName, tableName)
	default:
		return fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	}
}

func trimSQL(sqlText string) string {
	trimmed := strings.TrimSpace(sqlText)
	for strings.HasSuffix(trimmed, ";") {
		trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
	}
	return trimmed
}

func containsStringFold(haystack []string, needle string) bool {
	for _, s := range haystack {
		if strings.EqualFold(s, needle) {
			return true
		}
	}
	return false
}

func statusFromErr(err error) Status {
	if err == nil {
		return StatusPass
	}
	return StatusFail
}

func errMsg(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
