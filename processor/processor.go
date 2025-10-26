package processor

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"cbd_data_go/config"
	"cbd_data_go/database"
	"cbd_data_go/utils"
)

type Processor struct {
	oracle *database.OracleDB
	mysql  *database.MySQLDB
	sqlite *database.SQLiteDB
	config *config.Config
}

func NewProcessor(oracle *database.OracleDB, mysql *database.MySQLDB, sqlite *database.SQLiteDB, config *config.Config) *Processor {
	return &Processor{
		oracle: oracle,
		mysql:  mysql,
		sqlite: sqlite,
		config: config,
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

// getSourceDB 根据任务配置返回相应的源数据库连接
func (p *Processor) getSourceDB(task config.TaskConfig) (database.SourceDB, error) {
	sourceType := task.SourceType
	if sourceType == "" {
		sourceType = "oracle" // 默认值
	}

	switch sourceType {
	case "oracle":
		if p.oracle == nil {
			return nil, fmt.Errorf("Oracle database not configured but task '%s' requires it", task.TableName)
		}
		return p.oracle, nil
	case "mysql":
		if p.mysql == nil {
			return nil, fmt.Errorf("MySQL database not configured but task '%s' requires it", task.TableName)
		}
		return p.mysql, nil
	default:
		return nil, fmt.Errorf("unsupported source type: %s", sourceType)
	}
}

func (p *Processor) processTask(task config.TaskConfig) error {
	log.Printf("Executing query for table %s", task.TableName)

	// Get the appropriate source database connection
	sourceDB, err := p.getSourceDB(task)
	if err != nil {
		return err
	}

	// Execute query first to get column information and validate SQL
	rows, err := sourceDB.Query(task.SQL)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	columnTypes, err := p.getColumnTypes(rows)
	if err != nil {
		return fmt.Errorf("failed to get column types: %w", err)
	}

	// Create SQLite table (drops existing table and recreates)
	if err := p.sqlite.CreateTable(task.TableName, columns, columnTypes); err != nil {
		return fmt.Errorf("failed to create SQLite table: %w", err)
	}

	// Try to get row count for progress tracking (fallback if it fails)
	var totalRows int
	if count, err := sourceDB.GetRowCount(task.SQL); err != nil {
		log.Printf("Warning: Could not get row count for progress tracking: %v", err)
		totalRows = -1 // Unknown row count
	} else {
		totalRows = count
		log.Printf("Found %d rows to process for table %s", totalRows, task.TableName)
	}

	// Process data with progress bar
	var progress *utils.ProgressManager
	if totalRows > 0 {
		progress = utils.NewProgressManager(int64(totalRows), fmt.Sprintf("Processing %s", task.TableName))
	} else {
		// Create an indeterminate progress bar for unknown row count
		progress = utils.NewProgressManager(-1, fmt.Sprintf("Processing %s (unknown row count)", task.TableName))
	}
	defer progress.Finish()

	batchSize := 1000
	var batch [][]any
	processedRows := 0

	for rows.Next() {
		row, err := p.scanRow(rows, len(columns))
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		batch = append(batch, row)
		processedRows++

		// Insert batch when it reaches the batch size
		if len(batch) >= batchSize {
			if err := p.sqlite.InsertData(task.TableName, columns, batch); err != nil {
				return fmt.Errorf("failed to insert batch: %w", err)
			}
			if totalRows > 0 {
				progress.SetCurrent(int64(processedRows))
			} else {
				progress.Increment()
			}
			batch = batch[:0] // Clear batch
		}
	}

	// Insert remaining rows in batch
	if len(batch) > 0 {
		if err := p.sqlite.InsertData(task.TableName, columns, batch); err != nil {
			return fmt.Errorf("failed to insert final batch: %w", err)
		}
	}

	// Update progress to 100%
	if totalRows > 0 {
		progress.SetCurrent(int64(processedRows))
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error during row iteration: %w", err)
	}

	// Create indexes after data insertion
	if len(task.Indexes) > 0 {
		log.Printf("Creating %d indexes for table %s", len(task.Indexes), task.TableName)
		if err := p.sqlite.CreateIndexes(task.TableName, task.Indexes); err != nil {
			return fmt.Errorf("failed to create indexes for table %s: %w", task.TableName, err)
		}
		log.Printf("Successfully created all indexes for table %s", task.TableName)
	}

	log.Printf("Successfully processed %d rows for table %s", processedRows, task.TableName)
	return nil
}

func (p *Processor) getColumnTypes(rows *sql.Rows) ([]string, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	var types []string
	for _, ct := range columnTypes {
		// Get database type name
		typeName := ct.DatabaseTypeName()
		if typeName == "" {
			// Fallback to scan type if database type name is not available
			switch ct.ScanType().String() {
			case "int64":
				typeName = "NUMBER"
			case "float64":
				typeName = "NUMBER"
			case "string":
				typeName = "VARCHAR2"
			case "[]uint8":
				typeName = "BLOB"
			case "time.Time":
				typeName = "DATE"
			default:
				typeName = "VARCHAR2"
			}
		}
		types = append(types, typeName)
	}

	return types, nil
}

func (p *Processor) scanRow(rows *sql.Rows, columnCount int) ([]any, error) {
	values := make([]any, columnCount)
	valuePtrs := make([]any, columnCount)

	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	// Handle NULL values and type conversions
	for i, value := range values {
		if value == nil {
			continue // Keep NULL values as is
		}

		// Convert []byte to string for TEXT fields
		if bytes, ok := value.([]byte); ok {
			values[i] = string(bytes)
		}
	}

	return values, nil
}

func (p *Processor) Close() error {
	var errs []string

	if p.oracle != nil {
		if err := p.oracle.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("oracle close error: %v", err))
		}
	}

	if p.mysql != nil {
		if err := p.mysql.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("mysql close error: %v", err))
		}
	}

	if p.sqlite != nil {
		if err := p.sqlite.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("sqlite close error: %v", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("close errors: %s", strings.Join(errs, "; "))
	}

	return nil
}