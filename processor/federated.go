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

// sourceData holds the result of querying a single federated source.
type sourceData struct {
	alias   string
	columns []database.ColumnMetadata
	rows    []map[string]any
}

// joinResult holds the result of an in-memory JOIN.
type joinResult struct {
	columns []database.ColumnMetadata
	rows    [][]any
}

func (p *Processor) processFederatedTask(task config.TaskConfig, silent bool) error {
	start := time.Now()
	log.Printf("Executing federated query for table %s with %d sources", task.TableName, len(task.Sources))

	p.notify(ProgressEvent{
		Type:     "task.start",
		TaskName: task.TableName,
		SourceDB: task.SourceDB,
		TargetDB: task.TargetDB,
	})

	targetDB, err := p.manager.GetTarget(task.TargetDB)
	if err != nil {
		p.notify(ProgressEvent{
			Type:       "task.error",
			TaskName:   task.TableName,
			SourceDB:   task.SourceDB,
			TargetDB:   task.TargetDB,
			Error:      err.Error(),
			DurationMs: time.Since(start).Milliseconds(),
		})
		return err
	}

	var historyID string
	var recorder *database.HistoryRecorder
	if p.config.History.Enabled {
		recorder = p.getHistoryRecorder(task.TargetDB)
		if ensureErr := recorder.EnsureTable(targetDB); ensureErr != nil {
			log.Printf("Warning: failed to ensure history table: %v", ensureErr)
		} else {
			var sourceDBs []string
			for _, s := range task.Sources {
				sourceDBs = append(sourceDBs, s.DB)
			}
			rec := &database.MigrationRecord{
				TaskName: task.TableName,
				SourceDB: strings.Join(sourceDBs, ","),
				TargetDB: task.TargetDB,
				Mode:     task.Mode,
				Version:  p.version,
			}
			historyID, _ = recorder.Start(targetDB, rec)
		}
	}

	// Load all source data
	var sources []*sourceData
	for _, src := range task.Sources {
		sd, err := p.loadSourceData(src)
		if err != nil {
			return fmt.Errorf("failed to load source %q: %w", src.Alias, err)
		}
		sources = append(sources, sd)
		log.Printf("Loaded %d rows from source %q (%s) for table %s", len(sd.rows), src.Alias, src.DB, task.TableName)
	}

	// For now, only support exactly 2 sources
	if len(sources) != 2 {
		return fmt.Errorf("federated task %s: exactly 2 sources are required (got %d)", task.TableName, len(sources))
	}

	// Determine probe side (preserved side for outer joins) and build side.
	// For LEFT JOIN, probe must be the left table (sources[0]).
	// For RIGHT JOIN, probe must be the right table (sources[1]).
	// For INNER JOIN, build the hash table on the smaller side for efficiency.
	var probe, build *sourceData
	switch task.Join.Type {
	case "left":
		probe, build = sources[0], sources[1]
	case "right":
		probe, build = sources[1], sources[0]
	default: // inner
		if len(sources[0].rows) <= len(sources[1].rows) {
			probe, build = sources[0], sources[1]
		} else {
			probe, build = sources[1], sources[0]
		}
	}

	// Check column conflicts before joining
	if err := checkColumnConflicts(probe, build, task.Join.Keys); err != nil {
		return fmt.Errorf("federated task %s: %w", task.TableName, err)
	}

	// Perform hash join
	joinResult, err := p.performHashJoin(probe, build, task.Join.Keys, task.Join.Type)
	if err != nil {
		return fmt.Errorf("failed to perform JOIN for table %s: %w", task.TableName, err)
	}

	log.Printf("JOIN result for table %s: %d rows, %d columns", task.TableName, len(joinResult.rows), len(joinResult.columns))

	// Resolve merge keys for target write
	mergeKeys, err := resolveMergeKeys(joinResult.columns, task.MergeKeys)
	if err != nil {
		return err
	}

	// Create target table
	if task.SkipCreateTable {
		log.Printf("Skipping table creation for %s", task.TableName)
	} else {
		switch task.Mode {
		case config.TaskModeAppend, config.TaskModeMerge:
			if err := targetDB.EnsureTable(task.TableName, joinResult.columns); err != nil {
				return fmt.Errorf("failed to ensure target table: %w", err)
			}
		default:
			if err := targetDB.CreateTable(task.TableName, joinResult.columns); err != nil {
				return fmt.Errorf("failed to prepare target table: %w", err)
			}
		}
	}

	// Pre SQL hooks
	if len(task.PreSQL) > 0 {
		log.Printf("Executing %d pre_sql hooks for table %s", len(task.PreSQL), task.TableName)
		if err := execHookSQLs(targetDB, task.PreSQL); err != nil {
			return fmt.Errorf("pre_sql hook failed for table %s: %w", task.TableName, err)
		}
		log.Printf("Successfully executed all pre_sql hooks for table %s", task.TableName)
	}

	var dlqw *dlqWriter
	if task.DLQPath != "" {
		dlqw, err = newDLQWriter(task.DLQPath, task.DLQFormat, joinResult.columns)
		if err != nil {
			return fmt.Errorf("failed to initialize DLQ writer: %w", err)
		}
		defer dlqw.close()
	}

	var progress *utils.ProgressManager
	if !silent {
		progress = utils.NewProgressManager(int64(len(joinResult.rows)), fmt.Sprintf("Processing %s", task.TableName))
		defer progress.Finish()
	}

	batchSize := task.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	processedRows := 0
	totalDLQ := 0
	var batch [][]any

	for _, row := range joinResult.rows {
		batch = append(batch, row)
		processedRows++

		if progress != nil {
			progress.Increment()
		}

		if len(batch) >= batchSize {
			dlqCount, err := p.insertBatchWithRetry(targetDB, task, joinResult.columns, batch, mergeKeys, dlqw)
			if err != nil {
				return fmt.Errorf("failed to insert batch: %w", err)
			}
			totalDLQ += dlqCount
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		dlqCount, err := p.insertBatchWithRetry(targetDB, task, joinResult.columns, batch, mergeKeys, dlqw)
		if err != nil {
			return fmt.Errorf("failed to insert final batch: %w", err)
		}
		totalDLQ += dlqCount
	}

	// Create indexes
	if len(task.Indexes) > 0 {
		log.Printf("Creating %d indexes for table %s", len(task.Indexes), task.TableName)
		if err := targetDB.CreateIndexes(task.TableName, task.Indexes); err != nil {
			return fmt.Errorf("failed to create indexes for table %s: %w", task.TableName, err)
		}
		log.Printf("Successfully created all indexes for table %s", task.TableName)
	}

	// Post SQL hooks
	if len(task.PostSQL) > 0 {
		log.Printf("Executing %d post_sql hooks for table %s", len(task.PostSQL), task.TableName)
		if err := execHookSQLs(targetDB, task.PostSQL); err != nil {
			return fmt.Errorf("post_sql hook failed for table %s: %w", task.TableName, err)
		}
		log.Printf("Successfully executed all post_sql hooks for table %s", task.TableName)
	}

	if historyID != "" && recorder != nil {
		validationResult := "success"
		errMsg := ""
		if err != nil {
			errMsg = err.Error()
			validationResult = "failed"
		}
		if finishErr := recorder.Finish(targetDB, historyID, int64(processedRows), int64(totalDLQ), validationResult, errMsg); finishErr != nil {
			log.Printf("Warning: failed to finish history record: %v", finishErr)
		}
	}

	if task.DLQPath != "" {
		log.Printf("Processed %d rows, %d rows written to DLQ for table %s", processedRows, totalDLQ, task.TableName)
	} else {
		log.Printf("Successfully processed %d rows for table %s", processedRows, task.TableName)
	}

	p.notify(ProgressEvent{
		Type:       "task.complete",
		TaskName:   task.TableName,
		SourceDB:   task.SourceDB,
		TargetDB:   task.TargetDB,
		Processed:  processedRows,
		DurationMs: time.Since(start).Milliseconds(),
	})
	return nil
}

func (p *Processor) loadSourceData(source config.SourceConfig) (*sourceData, error) {
	db, err := p.manager.GetSource(source.DB)
	if err != nil {
		return nil, err
	}

	// Check row count against memory limit
	limit := p.federatedMemoryLimit
	if limit <= 0 {
		limit = 1000000
	}
	count, err := db.GetRowCount(trimSQL(source.SQL))
	if err != nil {
		return nil, fmt.Errorf("failed to count rows: %w", err)
	}
	if count > limit {
		return nil, fmt.Errorf("source %q has %s rows, exceeding --federated-memory-limit (%s). Consider using an ETL engine", source.Alias, formatNumber(count), formatNumber(limit))
	}

	rows, err := db.Query(source.SQL)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columns, err := p.extractColumnMetadata(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to extract column metadata: %w", err)
	}

	var result []map[string]any
	for rows.Next() {
		rowMap, err := p.scanRowMap(rows, columns)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		result = append(result, rowMap)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during row iteration: %w", err)
	}

	return &sourceData{
		alias:   source.Alias,
		columns: columns,
		rows:    result,
	}, nil
}

func (p *Processor) scanRowMap(rows *sql.Rows, columns []database.ColumnMetadata) (map[string]any, error) {
	row, err := p.scanRow(rows, columns)
	if err != nil {
		return nil, err
	}
	rowMap := make(map[string]any, len(columns))
	for i, col := range columns {
		rowMap[col.Name] = row[i]
	}
	return rowMap, nil
}

func (p *Processor) performHashJoin(probe, build *sourceData, joinKeys []string, joinType string) (*joinResult, error) {
	// Build hash table on build side (skip rows with NULL join keys)
	hashTable := make(map[string][]int) // join key -> indices into build.rows
	for i := range build.rows {
		if hasNullKey(build.rows[i], joinKeys) {
			continue
		}
		key, err := buildJoinKey(build.rows[i], joinKeys)
		if err != nil {
			return nil, fmt.Errorf("build side %q: %w", build.alias, err)
		}
		hashTable[key] = append(hashTable[key], i)
	}

	// Build output column metadata
	joinKeySet := make(map[string]struct{})
	for _, k := range joinKeys {
		joinKeySet[strings.ToLower(k)] = struct{}{}
	}

	var outColumns []database.ColumnMetadata
	var probeKeyCols, probeNonKeyCols, buildNonKeyCols []string

	// Join keys from probe side
	for _, key := range joinKeys {
		for _, col := range probe.columns {
			if strings.EqualFold(col.Name, key) {
				outColumns = append(outColumns, col)
				probeKeyCols = append(probeKeyCols, col.Name)
				break
			}
		}
	}

	// Non-join-key probe columns
	for _, col := range probe.columns {
		if _, isKey := joinKeySet[strings.ToLower(col.Name)]; !isKey {
			outColumns = append(outColumns, col)
			probeNonKeyCols = append(probeNonKeyCols, col.Name)
		}
	}

	// Non-join-key build columns
	for _, col := range build.columns {
		if _, isKey := joinKeySet[strings.ToLower(col.Name)]; !isKey {
			outColumns = append(outColumns, col)
			buildNonKeyCols = append(buildNonKeyCols, col.Name)
		}
	}

	var result [][]any

	// Probe phase: iterate over probe rows.
	// For LEFT/RIGHT joins, probe is the preserved side (all probe rows appear).
	// For INNER join, only matched rows appear.
	isOuter := joinType == "left" || joinType == "right"
	for _, probeRow := range probe.rows {
		if hasNullKey(probeRow, joinKeys) {
			if isOuter {
				merged := buildMergedRow(probeRow, nil, probeKeyCols, probeNonKeyCols, buildNonKeyCols)
				result = append(result, merged)
			}
			continue
		}
		key, err := buildJoinKey(probeRow, joinKeys)
		if err != nil {
			return nil, fmt.Errorf("probe side %q: %w", probe.alias, err)
		}
		buildIndices := hashTable[key]
		if len(buildIndices) > 0 {
			for _, idx := range buildIndices {
				merged := buildMergedRow(probeRow, build.rows[idx], probeKeyCols, probeNonKeyCols, buildNonKeyCols)
				result = append(result, merged)
			}
		} else if isOuter {
			merged := buildMergedRow(probeRow, nil, probeKeyCols, probeNonKeyCols, buildNonKeyCols)
			result = append(result, merged)
		}
	}

	return &joinResult{
		columns: outColumns,
		rows:    result,
	}, nil
}

func buildJoinKey(row map[string]any, joinKeys []string) (string, error) {
	parts := make([]string, 0, len(joinKeys))
	for _, key := range joinKeys {
		val := lookupValue(row, key)
		if val == nil {
			return "", fmt.Errorf("join key %q not found or is nil in row", key)
		}
		parts = append(parts, fmt.Sprint(val))
	}
	return strings.Join(parts, "\x00"), nil
}

func hasNullKey(row map[string]any, joinKeys []string) bool {
	for _, key := range joinKeys {
		val := lookupValue(row, key)
		if val == nil {
			return true
		}
	}
	return false
}

func buildMergedRow(probe, build map[string]any, probeKeyCols, probeNonKeyCols, buildNonKeyCols []string) []any {
	row := make([]any, 0, len(probeKeyCols)+len(probeNonKeyCols)+len(buildNonKeyCols))

	// Join keys: prefer probe side, fallback to build side
	for _, col := range probeKeyCols {
		if probe != nil {
			row = append(row, lookupValue(probe, col))
		} else {
			row = append(row, lookupValue(build, col))
		}
	}

	// Non-join-key probe columns
	for _, col := range probeNonKeyCols {
		if probe != nil {
			row = append(row, lookupValue(probe, col))
		} else {
			row = append(row, nil)
		}
	}

	// Non-join-key build columns
	for _, col := range buildNonKeyCols {
		if build != nil {
			row = append(row, lookupValue(build, col))
		} else {
			row = append(row, nil)
		}
	}

	return row
}

func lookupValue(row map[string]any, key string) any {
	if v, ok := row[key]; ok {
		return v
	}
	for k, v := range row {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return nil
}

func checkColumnConflicts(probe, build *sourceData, joinKeys []string) error {
	joinKeySet := make(map[string]struct{})
	for _, k := range joinKeys {
		joinKeySet[strings.ToLower(k)] = struct{}{}
	}

	probeCols := make(map[string]string) // lower -> original
	for _, col := range probe.columns {
		lower := strings.ToLower(col.Name)
		if _, isKey := joinKeySet[lower]; isKey {
			continue
		}
		probeCols[lower] = col.Name
	}

	for _, col := range build.columns {
		lower := strings.ToLower(col.Name)
		if _, isKey := joinKeySet[lower]; isKey {
			continue
		}
		if origProbe, exists := probeCols[lower]; exists {
			return fmt.Errorf("column %q conflicts between sources %q and %q; use SQL alias to disambiguate", origProbe, probe.alias, build.alias)
		}
	}

	return nil
}
