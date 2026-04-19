package processor

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"db-ferry/config"
	"db-ferry/database"
)

func TestResolveDLQPath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		want    string
		wantErr bool
	}{
		{
			name: "plain local path",
			path: "./dlq/failed.jsonl",
			want: "./dlq/failed.jsonl",
		},
		{
			name: "s3 path without template",
			path: "s3://my-bucket/db-ferry-dlq/events_failed.jsonl",
			want: "s3://my-bucket/db-ferry-dlq/events_failed.jsonl",
		},
		{
			name: "gs path without template",
			path: "gs://my-bucket/db-ferry-dlq/events_failed.jsonl",
			want: "gs://my-bucket/db-ferry-dlq/events_failed.jsonl",
		},
		{
			name: "date template local",
			path: "./dlq/{{.Date}}/events_failed.jsonl",
			want: "./dlq/" + time.Now().Format("2006-01-02") + "/events_failed.jsonl",
		},
		{
			name: "date template s3",
			path: "s3://bucket/prefix/{{.Date}}/events_failed.jsonl",
			want: "s3://bucket/prefix/" + time.Now().Format("2006-01-02") + "/events_failed.jsonl",
		},
		{
			name:    "invalid template field",
			path:    "./dlq/{{.Unknown}}/events.jsonl",
			wantErr: true, // Go template errors on missing fields
		},
		{
			name:    "malformed template",
			path:    "./dlq/{{.Date/events.jsonl",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveDLQPath(tt.path)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("resolveDLQPath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestNewDLQWriterLocal(t *testing.T) {
	dir := t.TempDir()

	t.Run("jsonl", func(t *testing.T) {
		path := filepath.Join(dir, "test.jsonl")
		w, err := newDLQWriter(path, config.DLQFormatJSONL, []database.ColumnMetadata{{Name: "id"}})
		if err != nil {
			t.Fatalf("newDLQWriter() error = %v", err)
		}
		defer w.close()

		if err := w.write([]any{1}, "error msg", "task1", "t1"); err != nil {
			t.Fatalf("write() error = %v", err)
		}

		// close to flush
		if err := w.close(); err != nil {
			t.Fatalf("close() error = %v", err)
		}

		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile() error = %v", err)
		}
		if !strings.Contains(string(data), `"error":"error msg"`) {
			t.Fatalf("expected JSONL to contain error field, got: %s", string(data))
		}
	})

	t.Run("csv", func(t *testing.T) {
		path := filepath.Join(dir, "test.csv")
		w, err := newDLQWriter(path, config.DLQFormatCSV, []database.ColumnMetadata{{Name: "id"}, {Name: "name"}})
		if err != nil {
			t.Fatalf("newDLQWriter() error = %v", err)
		}
		defer w.close()

		if err := w.write([]any{1, "alice"}, "error msg", "task1", "t1"); err != nil {
			t.Fatalf("write() error = %v", err)
		}

		if err := w.close(); err != nil {
			t.Fatalf("close() error = %v", err)
		}

		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile() error = %v", err)
		}
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		if len(lines) != 2 {
			t.Fatalf("expected 2 lines (header + data), got %d: %v", len(lines), lines)
		}
		if !strings.Contains(lines[0], "id") || !strings.Contains(lines[0], "_dlq_error") {
			t.Fatalf("expected CSV header with id and _dlq_error, got: %s", lines[0])
		}
		if !strings.Contains(lines[1], "alice") || !strings.Contains(lines[1], "error msg") {
			t.Fatalf("expected CSV data with alice and error msg, got: %s", lines[1])
		}
	})
}

func TestNewDLQWriterLocalAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "existing.jsonl")

	// Pre-create file with content
	if err := os.WriteFile(path, []byte("existing line\n"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	w, err := newDLQWriter(path, config.DLQFormatJSONL, []database.ColumnMetadata{{Name: "id"}})
	if err != nil {
		t.Fatalf("newDLQWriter() error = %v", err)
	}

	if err := w.write([]any{42}, "new error", "task1", "t1"); err != nil {
		t.Fatalf("write() error = %v", err)
	}
	w.close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	if lines[0] != "existing line" {
		t.Fatalf("expected first line preserved, got: %s", lines[0])
	}
}

func TestCloudDLQBufferJSONL(t *testing.T) {
	buf, err := newCloudDLQBuffer(config.DLQFormatJSONL, []database.ColumnMetadata{{Name: "id"}, {Name: "name"}})
	if err != nil {
		t.Fatalf("newCloudDLQBuffer() error = %v", err)
	}

	if err := buf.writeJSONL([]any{1, "alice"}, "fail", "task1", "t1"); err != nil {
		t.Fatalf("writeJSONL() error = %v", err)
	}
	if err := buf.writeJSONL([]any{2, "bob"}, "fail2", "task1", "t1"); err != nil {
		t.Fatalf("writeJSONL() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(buf.bytes())), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 JSONL lines, got %d", len(lines))
	}
	if !strings.Contains(lines[0], `"task_name":"task1"`) {
		t.Fatalf("expected task_name in first line, got: %s", lines[0])
	}
	if !strings.Contains(lines[1], `"error":"fail2"`) {
		t.Fatalf("expected fail2 error in second line, got: %s", lines[1])
	}
}

func TestCloudDLQBufferCSV(t *testing.T) {
	buf, err := newCloudDLQBuffer(config.DLQFormatCSV, []database.ColumnMetadata{{Name: "id"}, {Name: "name"}})
	if err != nil {
		t.Fatalf("newCloudDLQBuffer() error = %v", err)
	}

	if err := buf.writeCSV([]any{1, "alice"}, "fail", "task1", "t1"); err != nil {
		t.Fatalf("writeCSV() error = %v", err)
	}

	output := string(buf.bytes())
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 CSV lines (header + data), got %d: %v", len(lines), lines)
	}
	if !strings.Contains(lines[0], "id") || !strings.Contains(lines[0], "_dlq_error") {
		t.Fatalf("expected CSV header, got: %s", lines[0])
	}
	if !strings.Contains(lines[1], "alice") || !strings.Contains(lines[1], "fail") {
		t.Fatalf("expected CSV data, got: %s", lines[1])
	}
}
