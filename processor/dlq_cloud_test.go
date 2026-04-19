package processor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"db-ferry/config"
	"db-ferry/database"
)

// ----- S3 mocks -----

type mockS3Client struct {
	putErr error
}

func (m *mockS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, m.putErr
}

func TestNewS3DLQStoreInvalidPath(t *testing.T) {
	_, err := newS3DLQStore("s3://bucket-only", config.DLQFormatJSONL, nil)
	if err == nil {
		t.Fatal("expected error for invalid S3 path")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("invalid S3 path")) {
		t.Fatalf("expected invalid S3 path error, got: %v", err)
	}
}

func TestS3DLQStoreWriteAndClose(t *testing.T) {
	client := &mockS3Client{}
	store, err := newS3DLQStoreWithClient(client, "bucket", "key.jsonl", config.DLQFormatJSONL, []database.ColumnMetadata{{Name: "id"}})
	if err != nil {
		t.Fatalf("newS3DLQStoreWithClient() error = %v", err)
	}

	if err := store.write([]any{1}, "fail", "task1", "t1"); err != nil {
		t.Fatalf("write() error = %v", err)
	}
	if err := store.close(); err != nil {
		t.Fatalf("close() error = %v", err)
	}
}

func TestS3DLQStoreCSV(t *testing.T) {
	client := &mockS3Client{}
	store, err := newS3DLQStoreWithClient(client, "bucket", "key.csv", config.DLQFormatCSV, []database.ColumnMetadata{{Name: "id"}})
	if err != nil {
		t.Fatalf("newS3DLQStoreWithClient() error = %v", err)
	}

	if err := store.write([]any{1}, "fail", "task1", "t1"); err != nil {
		t.Fatalf("write() error = %v", err)
	}
	if err := store.close(); err != nil {
		t.Fatalf("close() error = %v", err)
	}
}

func TestS3DLQStoreCloseError(t *testing.T) {
	client := &mockS3Client{putErr: errors.New("network error")}
	store, err := newS3DLQStoreWithClient(client, "bucket", "key.jsonl", config.DLQFormatJSONL, []database.ColumnMetadata{{Name: "id"}})
	if err != nil {
		t.Fatalf("newS3DLQStoreWithClient() error = %v", err)
	}

	if err := store.write([]any{1}, "fail", "task1", "t1"); err != nil {
		t.Fatalf("write() error = %v", err)
	}
	if err := store.close(); err == nil {
		t.Fatal("expected close error")
	}
}

// ----- GCS mocks -----

type mockGCSWriter struct {
	data     bytes.Buffer
	writeErr error
	closeErr error
}

func (m *mockGCSWriter) Write(p []byte) (int, error) {
	if m.writeErr != nil {
		return 0, m.writeErr
	}
	return m.data.Write(p)
}

func (m *mockGCSWriter) Close() error {
	return m.closeErr
}

type mockGCSUploader struct {
	writer *mockGCSWriter
	newErr error
}

func (m *mockGCSUploader) newWriter(ctx context.Context, bucket, key, contentType string) (io.WriteCloser, error) {
	if m.newErr != nil {
		return nil, m.newErr
	}
	return m.writer, nil
}

func TestNewGCSDLQStoreInvalidPath(t *testing.T) {
	_, err := newGCSDLQStore("gs://bucket-only", config.DLQFormatJSONL, nil)
	if err == nil {
		t.Fatal("expected error for invalid GCS path")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("invalid GCS path")) {
		t.Fatalf("expected invalid GCS path error, got: %v", err)
	}
}

func TestGCSDLQStoreWriteAndClose(t *testing.T) {
	writer := &mockGCSWriter{}
	uploader := &mockGCSUploader{writer: writer}
	store, err := newGCSDLQStoreWithUploader(uploader, "bucket", "key.jsonl", config.DLQFormatJSONL, []database.ColumnMetadata{{Name: "id"}})
	if err != nil {
		t.Fatalf("newGCSDLQStoreWithUploader() error = %v", err)
	}

	if err := store.write([]any{1}, "fail", "task1", "t1"); err != nil {
		t.Fatalf("write() error = %v", err)
	}
	if err := store.close(); err != nil {
		t.Fatalf("close() error = %v", err)
	}
	if !bytes.Contains(writer.data.Bytes(), []byte("fail")) {
		t.Fatalf("expected uploaded data to contain error, got: %s", writer.data.String())
	}
}

func TestGCSDLQStoreCSV(t *testing.T) {
	writer := &mockGCSWriter{}
	uploader := &mockGCSUploader{writer: writer}
	store, err := newGCSDLQStoreWithUploader(uploader, "bucket", "key.csv", config.DLQFormatCSV, []database.ColumnMetadata{{Name: "id"}})
	if err != nil {
		t.Fatalf("newGCSDLQStoreWithUploader() error = %v", err)
	}

	if err := store.write([]any{1}, "fail", "task1", "t1"); err != nil {
		t.Fatalf("write() error = %v", err)
	}
	if err := store.close(); err != nil {
		t.Fatalf("close() error = %v", err)
	}
	if !bytes.Contains(writer.data.Bytes(), []byte("id")) {
		t.Fatalf("expected uploaded CSV data to contain header, got: %s", writer.data.String())
	}
}

func TestGCSDLQStoreWriteError(t *testing.T) {
	writer := &mockGCSWriter{writeErr: errors.New("write error")}
	uploader := &mockGCSUploader{writer: writer}
	store, err := newGCSDLQStoreWithUploader(uploader, "bucket", "key.jsonl", config.DLQFormatJSONL, []database.ColumnMetadata{{Name: "id"}})
	if err != nil {
		t.Fatalf("newGCSDLQStoreWithUploader() error = %v", err)
	}

	if err := store.write([]any{1}, "fail", "task1", "t1"); err != nil {
		t.Fatalf("write() error = %v", err)
	}
	if err := store.close(); err == nil {
		t.Fatal("expected close error due to write failure")
	}
}

func TestGCSDLQStoreCloseWriterError(t *testing.T) {
	writer := &mockGCSWriter{closeErr: errors.New("close error")}
	uploader := &mockGCSUploader{writer: writer}
	store, err := newGCSDLQStoreWithUploader(uploader, "bucket", "key.jsonl", config.DLQFormatJSONL, []database.ColumnMetadata{{Name: "id"}})
	if err != nil {
		t.Fatalf("newGCSDLQStoreWithUploader() error = %v", err)
	}

	if err := store.write([]any{1}, "fail", "task1", "t1"); err != nil {
		t.Fatalf("write() error = %v", err)
	}
	if err := store.close(); err == nil {
		t.Fatal("expected close error")
	}
}

func TestGCSDLQStoreNewWriterError(t *testing.T) {
	uploader := &mockGCSUploader{newErr: errors.New("new writer error")}
	store, err := newGCSDLQStoreWithUploader(uploader, "bucket", "key.jsonl", config.DLQFormatJSONL, []database.ColumnMetadata{{Name: "id"}})
	if err != nil {
		t.Fatalf("newGCSDLQStoreWithUploader() error = %v", err)
	}

	if err := store.write([]any{1}, "fail", "task1", "t1"); err != nil {
		t.Fatalf("write() error = %v", err)
	}
	if err := store.close(); err == nil {
		t.Fatal("expected close error due to newWriter failure")
	}
}

func TestNewDLQWriterInvalidCloudPaths(t *testing.T) {
	t.Run("invalid s3 path", func(t *testing.T) {
		_, err := newDLQWriter("s3://bucket-only", config.DLQFormatJSONL, nil)
		if err == nil {
			t.Fatal("expected error for invalid S3 path")
		}
	})

	t.Run("invalid gcs path", func(t *testing.T) {
		_, err := newDLQWriter("gs://bucket-only", config.DLQFormatJSONL, nil)
		if err == nil {
			t.Fatal("expected error for invalid GCS path")
		}
	})
}
