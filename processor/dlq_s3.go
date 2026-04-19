package processor

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"db-ferry/config"
	"db-ferry/database"
)

type s3DLQStore struct {
	buffer *cloudDLQBuffer
	client *s3.Client
	bucket string
	key    string
}

func newS3DLQStore(path, format string, columns []database.ColumnMetadata) (*s3DLQStore, error) {
	path = strings.TrimPrefix(path, "s3://")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid S3 path: s3://%s", path)
	}
	bucket := parts[0]
	key := parts[1]

	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	buffer, err := newCloudDLQBuffer(format, columns)
	if err != nil {
		return nil, err
	}

	return &s3DLQStore{
		buffer: buffer,
		client: client,
		bucket: bucket,
		key:    key,
	}, nil
}

func (s *s3DLQStore) write(row []any, errMsg, taskKey, tableName string) error {
	if s.buffer.format == config.DLQFormatCSV {
		return s.buffer.writeCSV(row, errMsg, taskKey, tableName)
	}
	return s.buffer.writeJSONL(row, errMsg, taskKey, tableName)
}

func (s *s3DLQStore) close() error {
	contentType := "application/jsonl"
	if s.buffer.format == config.DLQFormatCSV {
		contentType = "text/csv"
	}

	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:      &s.bucket,
		Key:         &s.key,
		Body:        bytes.NewReader(s.buffer.bytes()),
		ContentType: &contentType,
	})
	if err != nil {
		return fmt.Errorf("failed to upload DLQ to S3 s3://%s/%s: %w", s.bucket, s.key, err)
	}
	return nil
}
