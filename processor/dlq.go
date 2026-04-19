package processor

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"text/template"
	"time"

	"db-ferry/config"
	"db-ferry/database"
)

// dlqStore abstracts the storage backend for dead-letter queue records.
type dlqStore interface {
	write(row []any, errMsg, taskKey, tableName string) error
	close() error
}

// dlqWriter wraps a dlqStore with format-aware serialization and concurrency safety.
type dlqWriter struct {
	store   dlqStore
	format  string
	columns []database.ColumnMetadata
	mu      sync.Mutex
}

func newDLQWriter(path, format string, columns []database.ColumnMetadata) (*dlqWriter, error) {
	resolvedPath, err := resolveDLQPath(path)
	if err != nil {
		return nil, err
	}

	var store dlqStore
	switch {
	case strings.HasPrefix(resolvedPath, "s3://"):
		store, err = newS3DLQStore(resolvedPath, format, columns)
	case strings.HasPrefix(resolvedPath, "gs://"):
		store, err = newGCSDLQStore(resolvedPath, format, columns)
	default:
		store, err = newLocalDLQStore(resolvedPath, format, columns)
	}
	if err != nil {
		return nil, err
	}

	return &dlqWriter{
		store:   store,
		format:  format,
		columns: columns,
	}, nil
}

func (w *dlqWriter) write(row []any, errMsg, taskKey, tableName string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.store.write(row, errMsg, taskKey, tableName)
}

func (w *dlqWriter) close() error {
	if w == nil {
		return nil
	}
	return w.store.close()
}

func resolveDLQPath(path string) (string, error) {
	if !strings.Contains(path, "{{") {
		return path, nil
	}

	tmpl, err := template.New("dlq").Parse(path)
	if err != nil {
		return "", fmt.Errorf("invalid DLQ path template: %w", err)
	}

	var buf strings.Builder
	data := struct {
		Date string
	}{
		Date: time.Now().Format("2006-01-02"),
	}
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to execute DLQ path template: %w", err)
	}
	return buf.String(), nil
}

// cloudDLQBuffer provides in-memory buffering and format-aware serialization
// shared by S3 and GCS backends.
type cloudDLQBuffer struct {
	format    string
	columns   []database.ColumnMetadata
	buf       *bytes.Buffer
	csvWriter *csv.Writer
}

func newCloudDLQBuffer(format string, columns []database.ColumnMetadata) (*cloudDLQBuffer, error) {
	b := &cloudDLQBuffer{
		format:  format,
		columns: columns,
		buf:     &bytes.Buffer{},
	}

	if format == config.DLQFormatCSV {
		b.csvWriter = csv.NewWriter(b.buf)
		headers := make([]string, len(columns)+3)
		for i, col := range columns {
			headers[i] = col.Name
		}
		headers[len(columns)] = "_dlq_error"
		headers[len(columns)+1] = "_dlq_table_name"
		headers[len(columns)+2] = "_dlq_timestamp"
		if err := b.csvWriter.Write(headers); err != nil {
			return nil, fmt.Errorf("failed to write CSV header: %w", err)
		}
		b.csvWriter.Flush()
		if err := b.csvWriter.Error(); err != nil {
			return nil, fmt.Errorf("failed to flush CSV header: %w", err)
		}
	}

	return b, nil
}

func (b *cloudDLQBuffer) writeJSONL(row []any, errMsg, taskKey, tableName string) error {
	timestamp := time.Now().Format(time.RFC3339)
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
	b.buf.Write(data)
	b.buf.WriteByte('\n')
	return nil
}

func (b *cloudDLQBuffer) writeCSV(row []any, errMsg, taskKey, tableName string) error {
	timestamp := time.Now().Format(time.RFC3339)
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
	if err := b.csvWriter.Write(record); err != nil {
		return fmt.Errorf("failed to write CSV record: %w", err)
	}
	b.csvWriter.Flush()
	return b.csvWriter.Error()
}

func (b *cloudDLQBuffer) bytes() []byte {
	return b.buf.Bytes()
}
