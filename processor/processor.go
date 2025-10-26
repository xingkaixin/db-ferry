package processor

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"db-ferry/config"
	"db-ferry/database"
	"db-ferry/utils"
)

type Processor struct {
	manager *database.ConnectionManager
	config  *config.Config
}

func NewProcessor(manager *database.ConnectionManager, cfg *config.Config) *Processor {
	return &Processor{
		manager: manager,
		config:  cfg,
	}
}

func (p *Processor) ProcessAllTasks() error {
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

	rows, err := sourceDB.Query(task.SQL)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columnsMeta, err := p.extractColumnMetadata(rows)
	if err != nil {
		return fmt.Errorf("failed to extract column metadata: %w", err)
	}

	if err := targetDB.CreateTable(task.TableName, columnsMeta); err != nil {
		return fmt.Errorf("failed to prepare target table: %w", err)
	}

	var totalRows int
	if count, err := sourceDB.GetRowCount(task.SQL); err != nil {
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

	batchSize := 1000
	var batch [][]any
	processedRows := 0

	for rows.Next() {
		row, err := p.scanRow(rows, columnsMeta)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		batch = append(batch, row)
		processedRows++

		if totalRows > 0 {
			progress.SetCurrent(int64(processedRows))
		} else {
			progress.Increment()
		}

		if len(batch) >= batchSize {
			if err := targetDB.InsertData(task.TableName, columnsMeta, batch); err != nil {
				return fmt.Errorf("failed to insert batch: %w", err)
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := targetDB.InsertData(task.TableName, columnsMeta, batch); err != nil {
			return fmt.Errorf("failed to insert final batch: %w", err)
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

	log.Printf("Successfully processed %d rows for table %s", processedRows, task.TableName)
	return nil
}

func (p *Processor) extractColumnMetadata(rows *sql.Rows) ([]database.ColumnMetadata, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	metadata := make([]database.ColumnMetadata, len(columnTypes))
	for i, ct := range columnTypes {
		meta := database.ColumnMetadata{
			Name:         ct.Name(),
			DatabaseType: ct.DatabaseTypeName(),
			GoType:       ct.ScanType().String(),
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
