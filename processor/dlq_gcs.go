package processor

import (
	"context"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/storage"

	"db-ferry/config"
	"db-ferry/database"
)

type gcsUploader interface {
	newWriter(ctx context.Context, bucket, key, contentType string) (io.WriteCloser, error)
}

type realGCSUploader struct {
	client *storage.Client
}

func (r *realGCSUploader) newWriter(ctx context.Context, bucket, key, contentType string) (io.WriteCloser, error) {
	w := r.client.Bucket(bucket).Object(key).NewWriter(ctx)
	w.ContentType = contentType
	return w, nil
}

type gcsDLQStore struct {
	buffer   *cloudDLQBuffer
	uploader gcsUploader
	bucket   string
	key      string
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

	return newGCSDLQStoreWithUploader(&realGCSUploader{client: client}, bucket, key, format, columns)
}

func newGCSDLQStoreWithUploader(uploader gcsUploader, bucket, key, format string, columns []database.ColumnMetadata) (*gcsDLQStore, error) {
	buffer, err := newCloudDLQBuffer(format, columns)
	if err != nil {
		return nil, err
	}

	return &gcsDLQStore{
		buffer:   buffer,
		uploader: uploader,
		bucket:   bucket,
		key:      key,
	}, nil
}

func (s *gcsDLQStore) write(row []any, errMsg, taskKey, tableName string) error {
	if s.buffer.format == config.DLQFormatCSV {
		return s.buffer.writeCSV(row, errMsg, taskKey, tableName)
	}
	return s.buffer.writeJSONL(row, errMsg, taskKey, tableName)
}

func (s *gcsDLQStore) close() error {
	contentType := "application/jsonl"
	if s.buffer.format == config.DLQFormatCSV {
		contentType = "text/csv"
	}

	ctx := context.Background()
	w, err := s.uploader.newWriter(ctx, s.bucket, s.key, contentType)
	if err != nil {
		return fmt.Errorf("failed to create GCS writer gs://%s/%s: %w", s.bucket, s.key, err)
	}

	if _, err := w.Write(s.buffer.bytes()); err != nil {
		_ = w.Close()
		return fmt.Errorf("failed to write DLQ to GCS gs://%s/%s: %w", s.bucket, s.key, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer gs://%s/%s: %w", s.bucket, s.key, err)
	}
	return nil
}
