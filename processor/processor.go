package processor

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"db-ferry/config"
	"db-ferry/database"
	"db-ferry/utils"
)

type Processor struct {
	manager           *database.ConnectionManager
	config            *config.Config
	stateFiles        map[string]*stateFile
	stateMu           sync.Mutex
	historyRecorders  map[string]*database.HistoryRecorder
	historyMu         sync.Mutex
	version           string
}

var sleepFn = time.Sleep

type dlqWriter struct {
	path      string
	format    string
	file      *os.File
	csvWriter *csv.Writer
	mu        sync.Mutex
}

func newDLQWriter(path, format string, columns []database.ColumnMetadata) (*dlqWriter, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create DLQ directory %s: %w", dir, err)
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open DLQ file %s: %w", path, err)
	}

	w := &dlqWriter{
		path:   path,
		format: format,
		file:   file,
	}

	if format == config.DLQFormatCSV {
		stat, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("failed to stat DLQ file: %w", err)
		}
		w.csvWriter = csv.NewWriter(file)
		if stat.Size() == 0 {
			headers := make([]string, len(columns)+3)
			for i, col := range columns {
				headers[i] = col.Name
			}
			headers[len(columns)] = "_dlq_error"
			headers[len(columns)+1] = "_dlq_table_name"
			headers[len(columns)+2] = "_dlq_timestamp"
			if err := w.csvWriter.Write(headers); err != nil {
				_ = file.Close()
				return nil, fmt.Errorf("failed to write CSV header: %w", err)
			}
			w.csvWriter.Flush()
		}
	}

	return w, nil
}

func (w *dlqWriter) write(row []any, errMsg, taskKey, tableName string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	timestamp := time.Now().Format(time.RFC3339)

	if w.format == config.DLQFormatCSV {
		record := make([]string, len(row)+3)
		for i, v := range row {
			if v == nil {
				record[i] = ""
			} else {
				record[i] = fmt.Sprint(v)
			}
		}
		record[len(row)] = errMsg
		record[len(row)+1] = tableName
		record[len(row)+2] = timestamp
		if err := w.csvWriter.Write(record); err != nil {
			return err
		}
		w.csvWriter.Flush()
		return w.csvWriter.Error()
	}

	entry := map[string]any{
		"row":        row,
		"error":      errMsg,
		"task_name":  taskKey,
		"table_name": tableName,
		"timestamp":  timestamp,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if _, err := w.file.Write(append(data, '\n')); err != nil {
		return err
	}
	return nil
}

func (w *dlqWriter) close() error {
	if w == nil {
		return nil
	}
	if w.csvWriter != nil {
		w.csvWriter.Flush()
	}
	return w.file.Close()
}

func NewProcessor(manager *database.ConnectionManager, cfg *config.Config) *Processor {
	return NewProcessorWithVersion(manager, cfg, "dev")
}

// NewProcessorWithVersion creates a processor with an explicit version string.
func NewProcessorWithVersion(manager *database.ConnectionManager, cfg *config.Config, version string) *Processor {
	return &Processor{
		manager:          manager,
		config:           cfg,
		stateFiles:       make(map[string]*stateFile),
		historyRecorders: make(map[string]*database.HistoryRecorder),
		version:          version,
	}
}

func (p *Processor) ProcessAllTasks() error {
	tasks, _, deps, children, inDegree := p.buildTaskGraph()
	totalTasks := len(tasks)

	var taskProgress *utils.ProgressManager
	if totalTasks > 0 {
		taskProgress = utils.NewProgressManagerWithUnit(int64(totalTasks), "Processing tasks", "tasks")
		defer taskProgress.Finish()
	}

	maxConcurrent := p.config.MaxConcurrentTasks
	if maxConcurrent <= 0 {
		maxConcurrent = 3
	}

	// Fast path: single task or explicitly serial.
	if totalTasks <= 1 || maxConcurrent == 1 {
		for _, task := range p.config.Tasks {
			if task.Ignore {
				log.Printf("Skipping ignored task: %s", task.TableName)
				continue
			}
			log.Printf("Processing task: %s", task.TableName)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sched := &taskScheduler{
		p:             p,
		tasks:         tasks,
		deps:          deps,
		children:      children,
		sem:           make(chan struct{}, maxConcurrent),
		ctx:           ctx,
		cancel:        cancel,
		results:       make([]error, totalTasks),
		resultReady:   make([]chan struct{}, totalTasks),
		remainingDeps: make([]int, totalTasks),
	}
	copy(sched.remainingDeps, inDegree)
	for i := 0; i < totalTasks; i++ {
		sched.resultReady[i] = make(chan struct{}, 1)
	}

	for i := 0; i < totalTasks; i++ {
		if inDegree[i] == 0 {
			sched.wg.Add(1)
			go sched.runTask(i)
		}
	}

	sched.wg.Wait()

	var firstErr error
	for i := 0; i < totalTasks; i++ {
		<-sched.resultReady[i]
		if sched.results[i] != nil && firstErr == nil {
			firstErr = sched.results[i]
		}
		if taskProgress != nil {
			taskProgress.Increment()
		}
	}

	return firstErr
}

type taskScheduler struct {
	p             *Processor
	tasks         []config.TaskConfig
	deps          map[int][]int
	children      map[int][]int
	sem           chan struct{}
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	results       []error
	resultReady   []chan struct{}
	resultsMu     sync.Mutex
	remainingDeps []int
	depMu         sync.Mutex
}

func (s *taskScheduler) setResult(idx int, err error) {
	s.resultsMu.Lock()
	s.results[idx] = err
	close(s.resultReady[idx])
	s.resultsMu.Unlock()
}

func (s *taskScheduler) waitResult(depIdx int) error {
	<-s.resultReady[depIdx]
	s.resultsMu.Lock()
	err := s.results[depIdx]
	s.resultsMu.Unlock()
	return err
}

func (s *taskScheduler) runTask(idx int) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in task %s: %v", s.tasks[idx].TableName, r)
			s.setResult(idx, fmt.Errorf("panic in task %s: %v", s.tasks[idx].TableName, r))
			s.cancel()
		}
		s.onTaskComplete(idx)
		s.wg.Done()
	}()

	for _, depIdx := range s.deps[idx] {
		select {
		case <-s.ctx.Done():
			s.setResult(idx, fmt.Errorf("cancelled due to upstream failure"))
			return
		default:
			if err := s.waitResult(depIdx); err != nil {
				s.setResult(idx, fmt.Errorf("upstream task %s failed: %w", s.tasks[depIdx].TableName, err))
				return
			}
		}
	}

	select {
	case <-s.ctx.Done():
		s.setResult(idx, fmt.Errorf("cancelled due to upstream failure"))
		return
	case s.sem <- struct{}{}:
	}
	defer func() { <-s.sem }()

	log.Printf("Processing task: %s", s.tasks[idx].TableName)
	err := s.p.processTaskInternal(s.tasks[idx], true)
	if err != nil {
		log.Printf("Task %s failed: %v", s.tasks[idx].TableName, err)
		s.cancel()
	} else {
		log.Printf("Successfully completed task: %s", s.tasks[idx].TableName)
	}
	s.setResult(idx, err)
}

func (s *taskScheduler) onTaskComplete(idx int) {
	s.depMu.Lock()
	for _, child := range s.children[idx] {
		s.remainingDeps[child]--
		if s.remainingDeps[child] == 0 {
			s.depMu.Unlock()
			s.wg.Add(1)
			go s.runTask(child)
			s.depMu.Lock()
		}
	}
	s.depMu.Unlock()
}

func (p *Processor) buildTaskGraph() (tasks []config.TaskConfig, taskIndex map[string][]int, deps map[int][]int, children map[int][]int, inDegree []int) {
	taskIndex = make(map[string][]int)
	for _, task := range p.config.Tasks {
		if task.Ignore {
			continue
		}
		idx := len(tasks)
		tasks = append(tasks, task)
		taskIndex[task.TableName] = append(taskIndex[task.TableName], idx)
	}

	n := len(tasks)
	deps = make(map[int][]int)
	children = make(map[int][]int)
	inDegree = make([]int, n)

	for i, task := range tasks {
		for _, depName := range task.DependsOn {
			if depIndices, ok := taskIndex[depName]; ok {
				for _, depIdx := range depIndices {
					deps[i] = append(deps[i], depIdx)
					children[depIdx] = append(children[depIdx], i)
					inDegree[i]++
				}
			}
		}
	}

	return
}

func (p *Processor) processTask(task config.TaskConfig) error {
	return p.processTaskInternal(task, false)
}

func (p *Processor) processTaskInternal(task config.TaskConfig, silent bool) (err error) {
	log.Printf("Executing query for table %s", task.TableName)

	sourceDB, err := p.manager.GetSource(task.SourceDB)
	if err != nil {
		return err
	}

	targetDB, err := p.manager.GetTarget(task.TargetDB)
	if err != nil {
		return err
	}

	var historyID string
	var recorder *database.HistoryRecorder
	if p.config.History.Enabled {
		recorder = p.getHistoryRecorder(task.TargetDB)
		if ensureErr := recorder.EnsureTable(targetDB); ensureErr != nil {
			log.Printf("Warning: failed to ensure history table: %v", ensureErr)
		} else {
			rec := &database.MigrationRecord{
				TaskName: task.TableName,
				SourceDB: task.SourceDB,
				TargetDB: task.TargetDB,
				Mode:     task.Mode,
				Version:  p.version,
			}
			historyID, _ = recorder.Start(targetDB, rec)
		}
	}

	sourceDBCfg, ok := p.config.GetDatabase(task.SourceDB)
	if !ok {
		return fmt.Errorf("source_db '%s' is not defined", task.SourceDB)
	}

	targetDBCfg, ok := p.config.GetDatabase(task.TargetDB)
	if !ok {
		return fmt.Errorf("target_db '%s' is not defined", task.TargetDB)
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

	mergeKeys, err := resolveMergeKeys(columnsMeta, task.MergeKeys)
	if err != nil {
		return err
	}

	masker := newMaskEngine(task.Masking, columnsMeta)
	if masker != nil {
		log.Printf("Applying %d masking rules for table %s", len(task.Masking), task.TableName)
	}

	var dlqw *dlqWriter
	if task.DLQPath != "" {
		dlqw, err = newDLQWriter(task.DLQPath, task.DLQFormat, columnsMeta)
		if err != nil {
			return fmt.Errorf("failed to initialize DLQ writer: %w", err)
		}
		defer dlqw.close()
	}

	if task.SkipCreateTable {
		log.Printf("Skipping table creation for %s", task.TableName)
	} else {
		switch task.Mode {
		case config.TaskModeAppend, config.TaskModeMerge:
			if err := targetDB.EnsureTable(task.TableName, columnsMeta); err != nil {
				return fmt.Errorf("failed to ensure target table: %w", err)
			}
			if task.SchemaEvolution {
				if err := database.SyncSchema(targetDB, targetDBCfg.Type, task.TableName, columnsMeta); err != nil {
					return fmt.Errorf("failed to sync schema: %w", err)
				}
			}
		default:
			if err := targetDB.CreateTable(task.TableName, columnsMeta); err != nil {
				return fmt.Errorf("failed to prepare target table: %w", err)
			}
		}
	}

	if len(task.PreSQL) > 0 {
		log.Printf("Executing %d pre_sql hooks for table %s", len(task.PreSQL), task.TableName)
		if err := execHookSQLs(targetDB, task.PreSQL); err != nil {
			return fmt.Errorf("pre_sql hook failed for table %s: %w", task.TableName, err)
		}
		log.Printf("Successfully executed all pre_sql hooks for table %s", task.TableName)
	}

	targetCountBefore := 0
	if task.Validate == config.TaskValidateRowCount || task.Validate == config.TaskValidateChecksum || task.Validate == config.TaskValidateSample {
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
	if !silent {
		if totalRows > 0 {
			progress = utils.NewProgressManager(int64(totalRows), fmt.Sprintf("Processing %s", task.TableName))
		} else {
			progress = utils.NewProgressManager(-1, fmt.Sprintf("Processing %s (unknown row count)", task.TableName))
		}
		defer progress.Finish()
	}

	batchSize := task.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	var batch [][]any
	processedRows := 0
	totalDLQ := 0
	var lastResumeValue any

	if historyID != "" && recorder != nil {
		defer func() {
			validationResult := "success"
			errMsg := ""
			if err != nil {
				errMsg = err.Error()
				validationResult = "failed"
			}
			if finishErr := recorder.Finish(targetDB, historyID, int64(processedRows), int64(totalDLQ), validationResult, errMsg); finishErr != nil {
				log.Printf("Warning: failed to finish history record: %v", finishErr)
			}
		}()
	}

	for rows.Next() {
		row, err := p.scanRow(rows, columnsMeta)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		row = masker.apply(row, columnsMeta)

		if resumeIndex >= 0 {
			lastResumeValue = row[resumeIndex]
		}

		batch = append(batch, row)
		processedRows++

		if progress != nil {
			if totalRows > 0 {
				progress.SetCurrent(int64(processedRows))
			} else {
				progress.Increment()
			}
		}

		if len(batch) >= batchSize {
			dlqCount, err := p.insertBatchWithRetry(targetDB, task, columnsMeta, batch, mergeKeys, dlqw)
			if err != nil {
				return fmt.Errorf("failed to insert batch: %w", err)
			}
			totalDLQ += dlqCount
			if err := p.updateResumeState(task, lastResumeValue); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		dlqCount, err := p.insertBatchWithRetry(targetDB, task, columnsMeta, batch, mergeKeys, dlqw)
		if err != nil {
			return fmt.Errorf("failed to insert final batch: %w", err)
		}
		totalDLQ += dlqCount
		if err := p.updateResumeState(task, lastResumeValue); err != nil {
			return err
		}
	}

	if progress != nil && totalRows > 0 {
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

	if len(task.PostSQL) > 0 {
		log.Printf("Executing %d post_sql hooks for table %s", len(task.PostSQL), task.TableName)
		if err := execHookSQLs(targetDB, task.PostSQL); err != nil {
			return fmt.Errorf("post_sql hook failed for table %s: %w", task.TableName, err)
		}
		log.Printf("Successfully executed all post_sql hooks for table %s", task.TableName)
	}

	targetDBCfg, ok = p.config.GetDatabase(task.TargetDB)
	if !ok {
		return fmt.Errorf("target_db '%s' is not defined", task.TargetDB)
	}

	validationTask := task
	reportedProcessedRows := processedRows - totalDLQ
	if reportedProcessedRows < 0 {
		reportedProcessedRows = 0
	}
	if err := database.ValidateTask(sourceDB, targetDB, sourceDBCfg.Type, targetDBCfg.Type, validationTask, columnsMeta, countSQL, reportedProcessedRows, targetCountBefore); err != nil {
		return err
	}

	if task.DLQPath != "" {
		log.Printf("Processed %d rows, %d rows written to DLQ for table %s", processedRows, totalDLQ, task.TableName)
	} else {
		log.Printf("Successfully processed %d rows for table %s", processedRows, task.TableName)
	}
	return nil
}

func (p *Processor) getHistoryRecorder(targetDBAlias string) *database.HistoryRecorder {
	p.historyMu.Lock()
	defer p.historyMu.Unlock()

	if r, ok := p.historyRecorders[targetDBAlias]; ok {
		return r
	}

	dbCfg, ok := p.config.GetDatabase(targetDBAlias)
	if !ok {
		// Fallback to generic recorder if config missing (should not happen).
		r := database.NewHistoryRecorder(config.DatabaseTypeSQLite, p.config.History.Table())
		p.historyRecorders[targetDBAlias] = r
		return r
	}

	r := database.NewHistoryRecorder(dbCfg.Type, p.config.History.Table())
	p.historyRecorders[targetDBAlias] = r
	return r
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

func (p *Processor) insertBatchWithRetry(targetDB database.TargetDB, task config.TaskConfig, columns []database.ColumnMetadata, batch [][]any, mergeKeys []string, dlqw *dlqWriter) (int, error) {
	var lastErr error
	attempts := task.MaxRetries + 1
	for attempt := 1; attempt <= attempts; attempt++ {
		switch task.Mode {
		case config.TaskModeMerge:
			lastErr = targetDB.UpsertData(task.TableName, columns, batch, mergeKeys)
		default:
			lastErr = targetDB.InsertData(task.TableName, columns, batch)
		}
		if lastErr == nil {
			return 0, nil
		}
		if attempt < attempts {
			wait := time.Duration(attempt) * time.Second
			log.Printf("Insert batch failed (attempt %d/%d): %v; retrying in %s", attempt, attempts, lastErr, wait)
			sleepFn(wait)
		}
	}

	if dlqw == nil {
		return 0, lastErr
	}

	log.Printf("Batch insert failed after %d attempts, falling back to row-by-row for table %s: %v", attempts, task.TableName, lastErr)
	taskKey := p.taskKey(task)
	dlqCount := 0
	for _, row := range batch {
		var rowErr error
		switch task.Mode {
		case config.TaskModeMerge:
			rowErr = targetDB.UpsertData(task.TableName, columns, [][]any{row}, mergeKeys)
		default:
			rowErr = targetDB.InsertData(task.TableName, columns, [][]any{row})
		}
		if rowErr != nil {
			if err := dlqw.write(row, rowErr.Error(), taskKey, task.TableName); err != nil {
				return dlqCount, fmt.Errorf("failed to write to DLQ: %w", err)
			}
			dlqCount++
		}
	}
	if dlqCount > 0 {
		log.Printf("Wrote %d/%d rows to DLQ for table %s", dlqCount, len(batch), task.TableName)
	}
	return dlqCount, nil
}

func execHookSQLs(targetDB database.TargetDB, sqls []string) error {
	for _, sqlText := range sqls {
		if err := targetDB.Exec(sqlText); err != nil {
			return fmt.Errorf("failed to execute hook sql %q: %w", sqlText, err)
		}
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

func resolveMergeKeys(columns []database.ColumnMetadata, mergeKeys []string) ([]string, error) {
	if len(mergeKeys) == 0 {
		return nil, nil
	}

	resolved := make([]string, len(mergeKeys))
	for i, key := range mergeKeys {
		found := ""
		for _, col := range columns {
			if strings.EqualFold(col.Name, key) {
				found = col.Name
				break
			}
		}
		if found == "" {
			return nil, fmt.Errorf("merge_key '%s' not found in query columns", key)
		}
		resolved[i] = found
	}

	return resolved, nil
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

func (p *Processor) PlanAllTasks(w io.Writer) error {
	totalTasks := 0
	for _, task := range p.config.Tasks {
		if !task.Ignore {
			totalTasks++
		}
	}

	taskIndex := 0
	for i, task := range p.config.Tasks {
		if task.Ignore {
			continue
		}
		taskIndex++
		if err := p.planTask(w, taskIndex, totalTasks, i+1, task); err != nil {
			return fmt.Errorf("failed to plan task %s: %w", task.TableName, err)
		}
	}

	return nil
}

func (p *Processor) planTask(w io.Writer, taskIndex, totalTasks, overallIndex int, task config.TaskConfig) error {
	sourceDB, err := p.manager.GetSource(task.SourceDB)
	if err != nil {
		return err
	}

	targetDBCfg, ok := p.config.GetDatabase(task.TargetDB)
	if !ok {
		return fmt.Errorf("target_db '%s' is not defined", task.TargetDB)
	}

	resumeLiteral, err := p.resolveResumeLiteral(task)
	if err != nil {
		return err
	}

	querySQL, countSQL := buildTaskSQL(task.SQL, task.ResumeKey, resumeLiteral)

	rows, err := sourceDB.Query(querySQL)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columnsMeta, err := p.extractColumnMetadata(rows)
	if err != nil {
		return fmt.Errorf("failed to extract column metadata: %w", err)
	}

	if task.ResumeKey != "" {
		resumeIndex := findColumnIndex(columnsMeta, task.ResumeKey)
		if resumeIndex < 0 {
			return fmt.Errorf("resume_key '%s' not found in query columns for table %s", task.ResumeKey, task.TableName)
		}
	}

	if _, err := resolveMergeKeys(columnsMeta, task.MergeKeys); err != nil {
		return err
	}

	var rowCount int
	if count, err := sourceDB.GetRowCount(countSQL); err != nil {
		rowCount = -1
	} else {
		rowCount = count
	}

	batchSize := task.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	ddlStmts, err := database.GeneratePlanDDL(targetDBCfg.Type, task.TableName, columnsMeta, task.Mode, task.SkipCreateTable, task.Indexes)
	if err != nil {
		return fmt.Errorf("failed to generate DDL: %w", err)
	}

	fmt.Fprintf(w, "[PLAN] Task %d/%d: %s\n", taskIndex, totalTasks, task.TableName)
	fmt.Fprintf(w, "  Source:  %s  →  Target:  %s\n", task.SourceDB, task.TargetDB)
	fmt.Fprintf(w, "  Mode:    %s\n", task.Mode)
	if len(ddlStmts) > 0 {
		fmt.Fprintln(w, "  DDL:")
		for _, stmt := range ddlStmts {
			fmt.Fprintf(w, "    %s\n", stmt)
		}
	} else {
		fmt.Fprintln(w, "  DDL:     (none)")
	}
	if rowCount >= 0 {
		fmt.Fprintf(w, "  Rows:    ~%s\n", formatNumber(rowCount))
	} else {
		fmt.Fprintln(w, "  Rows:    (unknown)")
	}
	fmt.Fprintf(w, "  Batch:   %d\n", batchSize)
	if task.SchemaEvolution && (task.Mode == config.TaskModeAppend || task.Mode == config.TaskModeMerge) {
		fmt.Fprintln(w, "  Schema:  evolution enabled (missing columns will be added via ALTER TABLE)")
	}

	if overallIndex < len(p.config.Tasks) {
		fmt.Fprintln(w)
	}

	return nil
}

func formatNumber(n int) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	s := fmt.Sprintf("%d", n)
	var parts []string
	for len(s) > 3 {
		parts = append([]string{s[len(s)-3:]}, parts...)
		s = s[:len(s)-3]
	}
	parts = append([]string{s}, parts...)
	return strings.Join(parts, ",")
}
