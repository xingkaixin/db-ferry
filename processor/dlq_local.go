package processor

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"db-ferry/config"
	"db-ferry/database"
)

type localDLQStore struct {
	path      string
	format    string
	file      *os.File
	csvWriter *csv.Writer
}

func newLocalDLQStore(path, format string, columns []database.ColumnMetadata) (*localDLQStore, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create DLQ directory %s: %w", dir, err)
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open DLQ file %s: %w", path, err)
	}

	s := &localDLQStore{
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
		s.csvWriter = csv.NewWriter(file)
		if stat.Size() == 0 {
			headers := make([]string, len(columns)+3)
			for i, col := range columns {
				headers[i] = col.Name
			}
			headers[len(columns)] = "_dlq_error"
			headers[len(columns)+1] = "_dlq_table_name"
			headers[len(columns)+2] = "_dlq_timestamp"
			if err := s.csvWriter.Write(headers); err != nil {
				_ = file.Close()
				return nil, fmt.Errorf("failed to write CSV header: %w", err)
			}
			s.csvWriter.Flush()
		}
	}

	return s, nil
}

func (s *localDLQStore) write(row []any, errMsg, taskKey, tableName string) error {
	timestamp := time.Now().Format(time.RFC3339)

	if s.format == config.DLQFormatCSV {
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
		if err := s.csvWriter.Write(record); err != nil {
			return fmt.Errorf("failed to write CSV record: %w", err)
		}
		s.csvWriter.Flush()
		return s.csvWriter.Error()
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
		return fmt.Errorf("failed to marshal DLQ JSONL record: %w", err)
	}
	if _, err := s.file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write DLQ record: %w", err)
	}
	return nil
}

func (s *localDLQStore) close() error {
	if s.csvWriter != nil {
		s.csvWriter.Flush()
	}
	return s.file.Close()
}
