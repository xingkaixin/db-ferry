package processor

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/storage"

	"db-ferry/config"
	"db-ferry/database"
)

type gcsDLQStore struct {
	buffer *cloudDLQBuffer
	client *storage.Client
	bucket string
	key    string
}

func newGCSDLQStore(path, format string, columns []database.ColumnMetadata) (*gcsDLQStore, error) {
	path = strings.TrimPrefix(path, "gs://")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid GCS path: gs://%s", path)
	}
	bucket := parts[0]
	key := parts[1]

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	buffer, err := newCloudDLQBuffer(format, columns)
	if err != nil {
		return nil, err
	}

	return &gcsDLQStore{
		buffer: buffer,
		client: client,
		bucket: bucket,
		key:    key,
	}, nil
}

func (s *gcsDLQStore) write(row []any, errMsg, taskKey, tableName string) error {
	if s.buffer.format == config.DLQFormatCSV {
		return s.buffer.writeCSV(row, errMsg, taskKey, tableName)
	}
	return s.buffer.writeJSONL(row, errMsg, taskKey, tableName)
}

func (s *gcsDLQStore) close() error {
	ctx := context.Background()
	w := s.client.Bucket(s.bucket).Object(s.key).NewWriter(ctx)

	contentType := "application/jsonl"
	if s.buffer.format == config.DLQFormatCSV {
		contentType = "text/csv"
	}
	w.ContentType = contentType

	if _, err := w.Write(s.buffer.bytes()); err != nil {
		_ = w.Close()
		return fmt.Errorf("failed to write DLQ to GCS gs://%s/%s: %w", s.bucket, s.key, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer gs://%s/%s: %w", s.bucket, s.key, err)
	}
	return nil
}
