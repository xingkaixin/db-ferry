package processor

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"db-ferry/config"
	"db-ferry/database"
	"db-ferry/utils"
)

type Processor struct {
	manager    *database.ConnectionManager
	config     *config.Config
	stateFiles map[string]*stateFile
}

func NewProcessor(manager *database.ConnectionManager, cfg *config.Config) *Processor {
	return &Processor{
		manager:    manager,
		config:     cfg,
		stateFiles: make(map[string]*stateFile),
	}
}

func (p *Processor) ProcessAllTasks() error {
	totalTasks := 0
	for _, task := range p.config.Tasks {
		if !task.Ignore {
			totalTasks++
		}
	}

	var taskProgress *utils.ProgressManager
	if totalTasks > 0 {
		taskProgress = utils.NewProgressManagerWithUnit(int64(totalTasks), "Processing tasks", "tasks")
		defer taskProgress.Finish()
	}

	for i, task := range p.config.Tasks {
		if task.Ignore {
			log.Printf("Skipping ignored task: %s", task.TableName)
			continue
		}

		log.Printf("Processing task %d/%d: %s", i+1, len(p.config.Tasks), task.TableName)
		if err := p.processTask(task); err != nil {
			return fmt.Errorf("failed to process task %s: %w", task.TableName, err)
		}
		log.Printf("Successfully completed task: %s", task.TableName)
		if taskProgress != nil {
			taskProgress.Increment()
		}
	}

	return nil
}

func (p *Processor) processTask(task config.TaskConfig) error {
	log.Printf("Executing query for table %s", task.TableName)

	sourceDB, err := p.manager.GetSource(task.SourceDB)
	if err != nil {
		return err
	}

	targetDB, err := p.manager.GetTarget(task.TargetDB)
	if err != nil {
		return err
	}

	resumeLiteral, err := p.resolveResumeLiteral(task)
	if err != nil {
		return err
	}

	querySQL, countSQL := buildTaskSQL(task.SQL, task.ResumeKey, resumeLiteral)
	if task.ResumeKey != "" {
		if resumeLiteral != "" {
			log.Printf("Resume enabled for %s: %s > %s", task.TableName, task.ResumeKey, resumeLiteral)
		} else {
			log.Printf("Resume enabled for %s with key %s", task.TableName, task.ResumeKey)
		}
	}

	rows, err := sourceDB.Query(querySQL)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columnsMeta, err := p.extractColumnMetadata(rows)
	if err != nil {
		return fmt.Errorf("failed to extract column metadata: %w", err)
	}

	resumeIndex := -1
	if task.ResumeKey != "" {
		resumeIndex = findColumnIndex(columnsMeta, task.ResumeKey)
		if resumeIndex < 0 {
			return fmt.Errorf("resume_key '%s' not found in query columns for table %s", task.ResumeKey, task.TableName)
		}
	}

	if task.SkipCreateTable {
		log.Printf("Skipping table creation for %s", task.TableName)
	} else {
		switch task.Mode {
		case config.TaskModeAppend:
			if err := targetDB.EnsureTable(task.TableName, columnsMeta); err != nil {
				return fmt.Errorf("failed to ensure target table: %w", err)
			}
		default:
			if err := targetDB.CreateTable(task.TableName, columnsMeta); err != nil {
				return fmt.Errorf("failed to prepare target table: %w", err)
			}
		}
	}

	validateRowCount := task.Validate == config.TaskValidateRowCount
	targetCountBefore := 0
	if validateRowCount {
		count, err := targetDB.GetTableRowCount(task.TableName)
		if err != nil {
			return fmt.Errorf("failed to get target row count before insert: %w", err)
		}
		targetCountBefore = count
	}

	var totalRows int
	if count, err := sourceDB.GetRowCount(countSQL); err != nil {
		log.Printf("Warning: Could not get row count for progress tracking: %v", err)
		totalRows = -1
	} else {
		totalRows = count
		log.Printf("Found %d rows to process for table %s", totalRows, task.TableName)
	}

	var progress *utils.ProgressManager
	if totalRows > 0 {
		progress = utils.NewProgressManager(int64(totalRows), fmt.Sprintf("Processing %s", task.TableName))
	} else {
		progress = utils.NewProgressManager(-1, fmt.Sprintf("Processing %s (unknown row count)", task.TableName))
	}
	defer progress.Finish()

	batchSize := task.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	var batch [][]any
	processedRows := 0
	var lastResumeValue any

	for rows.Next() {
		row, err := p.scanRow(rows, columnsMeta)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		if resumeIndex >= 0 {
			lastResumeValue = row[resumeIndex]
		}

		batch = append(batch, row)
		processedRows++

		if totalRows > 0 {
			progress.SetCurrent(int64(processedRows))
		} else {
			progress.Increment()
		}

		if len(batch) >= batchSize {
			if err := p.insertBatchWithRetry(targetDB, task, columnsMeta, batch); err != nil {
				return fmt.Errorf("failed to insert batch: %w", err)
			}
			if err := p.updateResumeState(task, lastResumeValue); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := p.insertBatchWithRetry(targetDB, task, columnsMeta, batch); err != nil {
			return fmt.Errorf("failed to insert final batch: %w", err)
		}
		if err := p.updateResumeState(task, lastResumeValue); err != nil {
			return err
		}
	}

	if totalRows > 0 {
		progress.SetCurrent(int64(processedRows))
		if processedRows < totalRows {
			log.Printf("Warning: processed %d rows but expected %d for table %s", processedRows, totalRows, task.TableName)
		}
		progress.SetCurrent(int64(totalRows))
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error during row iteration: %w", err)
	}

	if len(task.Indexes) > 0 {
		log.Printf("Creating %d indexes for table %s", len(task.Indexes), task.TableName)
		if err := targetDB.CreateIndexes(task.TableName, task.Indexes); err != nil {
			return fmt.Errorf("failed to create indexes for table %s: %w", task.TableName, err)
		}
		log.Printf("Successfully created all indexes for table %s", task.TableName)
	}

	if validateRowCount {
		targetCountAfter, err := targetDB.GetTableRowCount(task.TableName)
		if err != nil {
			return fmt.Errorf("failed to get target row count after insert: %w", err)
		}
		inserted := targetCountAfter - targetCountBefore
		if inserted != processedRows {
			return fmt.Errorf("row count validation failed for table %s: expected %d inserted rows but got %d", task.TableName, processedRows, inserted)
		}
	}

	log.Printf("Successfully processed %d rows for table %s", processedRows, task.TableName)
	return nil
}

func (p *Processor) resolveResumeLiteral(task config.TaskConfig) (string, error) {
	if task.ResumeKey == "" {
		return "", nil
	}
	if task.StateFile == "" {
		return task.ResumeFrom, nil
	}

	state, err := p.loadStateFile(task.StateFile)
	if err != nil {
		return "", err
	}

	if literal, ok := state.Tasks[p.taskKey(task)]; ok && literal != "" {
		return literal, nil
	}

	return task.ResumeFrom, nil
}

func (p *Processor) updateResumeState(task config.TaskConfig, value any) error {
	if task.ResumeKey == "" || task.StateFile == "" {
		return nil
	}
	if value == nil {
		return fmt.Errorf("resume_key '%s' value is nil for table %s", task.ResumeKey, task.TableName)
	}

	literal, err := formatResumeLiteral(value)
	if err != nil {
		return fmt.Errorf("failed to format resume value for table %s: %w", task.TableName, err)
	}

	state, err := p.loadStateFile(task.StateFile)
	if err != nil {
		return err
	}
	state.Tasks[p.taskKey(task)] = literal
	if err := p.saveStateFile(task.StateFile, state); err != nil {
		return fmt.Errorf("failed to save state file %s: %w", task.StateFile, err)
	}

	return nil
}

func (p *Processor) insertBatchWithRetry(targetDB database.TargetDB, task config.TaskConfig, columns []database.ColumnMetadata, batch [][]any) error {
	attempts := task.MaxRetries + 1
	for attempt := 1; attempt <= attempts; attempt++ {
		err := targetDB.InsertData(task.TableName, columns, batch)
		if err == nil {
			return nil
		}
		if attempt == attempts {
			return err
		}
		wait := time.Duration(attempt) * time.Second
		log.Printf("Insert batch failed (attempt %d/%d): %v; retrying in %s", attempt, attempts, err, wait)
		time.Sleep(wait)
	}

	return nil
}

func buildTaskSQL(baseSQL, resumeKey, resumeLiteral string) (string, string) {
	normalized := trimSQL(baseSQL)
	if resumeKey == "" {
		return normalized, normalized
	}

	wrapped := fmt.Sprintf("SELECT * FROM (%s) src", normalized)
	if resumeLiteral != "" {
		wrapped = fmt.Sprintf("%s WHERE %s > %s", wrapped, resumeKey, resumeLiteral)
	}

	dataSQL := fmt.Sprintf("%s ORDER BY %s", wrapped, resumeKey)
	return dataSQL, wrapped
}

func trimSQL(sqlText string) string {
	trimmed := strings.TrimSpace(sqlText)
	for strings.HasSuffix(trimmed, ";") {
		trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
	}
	return trimmed
}

func formatResumeLiteral(value any) (string, error) {
	switch v := value.(type) {
	case int:
		return fmt.Sprintf("%d", v), nil
	case int8:
		return fmt.Sprintf("%d", v), nil
	case int16:
		return fmt.Sprintf("%d", v), nil
	case int32:
		return fmt.Sprintf("%d", v), nil
	case int64:
		return fmt.Sprintf("%d", v), nil
	case uint:
		return fmt.Sprintf("%d", v), nil
	case uint8:
		return fmt.Sprintf("%d", v), nil
	case uint16:
		return fmt.Sprintf("%d", v), nil
	case uint32:
		return fmt.Sprintf("%d", v), nil
	case uint64:
		return fmt.Sprintf("%d", v), nil
	case float32:
		return fmt.Sprintf("%v", v), nil
	case float64:
		return fmt.Sprintf("%v", v), nil
	case bool:
		if v {
			return "1", nil
		}
		return "0", nil
	case time.Time:
		return quoteSQLString(v.Format("2006-01-02 15:04:05")), nil
	case []byte:
		return quoteSQLString(string(v)), nil
	case string:
		return quoteSQLString(v), nil
	default:
		return quoteSQLString(fmt.Sprint(value)), nil
	}
}

func quoteSQLString(value string) string {
	escaped := strings.ReplaceAll(value, "'", "''")
	return "'" + escaped + "'"
}

func findColumnIndex(columns []database.ColumnMetadata, name string) int {
	for i, col := range columns {
		if strings.EqualFold(col.Name, name) {
			return i
		}
	}
	return -1
}

func (p *Processor) extractColumnMetadata(rows *sql.Rows) ([]database.ColumnMetadata, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	metadata := make([]database.ColumnMetadata, len(columnTypes))
	for i, ct := range columnTypes {
		scanType := ct.ScanType()
		goType := ""
		if scanType != nil {
			goType = scanType.String()
		}
		meta := database.ColumnMetadata{
			Name:         ct.Name(),
			DatabaseType: ct.DatabaseTypeName(),
			GoType:       goType,
		}

		if length, ok := ct.Length(); ok {
			meta.Length = length
			meta.LengthValid = true
		}

		if precision, scale, ok := ct.DecimalSize(); ok {
			meta.Precision = precision
			meta.Scale = scale
			meta.PrecisionScaleValid = true
		}

		if nullable, ok := ct.Nullable(); ok {
			meta.Nullable = nullable
			meta.NullableValid = true
		}

		metadata[i] = meta
	}

	return metadata, nil
}

func (p *Processor) scanRow(rows *sql.Rows, columns []database.ColumnMetadata) ([]any, error) {
	columnCount := len(columns)
	values := make([]any, columnCount)
	valuePtrs := make([]any, columnCount)

	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	for i, value := range values {
		if value == nil {
			continue
		}

		if bytes, ok := value.([]byte); ok {
			if isTextualColumn(columns[i]) {
				values[i] = string(bytes)
			} else {
				copied := make([]byte, len(bytes))
				copy(copied, bytes)
				values[i] = copied
			}
		}
	}

	return values, nil
}

func isTextualColumn(column database.ColumnMetadata) bool {
	typeName := strings.ToUpper(column.DatabaseType)
	if typeName == "" {
		typeName = strings.ToUpper(column.GoType)
	}

	switch {
	case strings.Contains(typeName, "CHAR"), strings.Contains(typeName, "TEXT"), strings.Contains(typeName, "CLOB"), strings.Contains(typeName, "STRING"):
		return true
	default:
		return false
	}
}

func (p *Processor) Close() error {
	return p.manager.CloseAll()
}
