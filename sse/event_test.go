package sse

import (
	"strings"
	"testing"
	"time"
)

func TestEventEncode(t *testing.T) {
	evt := Event{
		Type: EventTaskStart,
		Data: TaskProgressData{
			Task:          "orders",
			EstimatedRows: 1000,
		},
		Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	encoded := string(evt.Encode())
	if !strings.HasPrefix(encoded, "event: task.start\n") {
		t.Fatalf("expected SSE event prefix, got: %q", encoded)
	}
	if !strings.Contains(encoded, `"task":"orders"`) {
		t.Fatalf("expected task name in data, got: %q", encoded)
	}
	if !strings.Contains(encoded, `"estimated_rows":1000`) {
		t.Fatalf("expected estimated_rows in data, got: %q", encoded)
	}
	if !strings.HasSuffix(encoded, "\n\n") {
		t.Fatalf("expected double newline suffix, got: %q", encoded)
	}
}

func TestEventEncodeProgress(t *testing.T) {
	evt := Event{
		Type: EventTaskProgress,
		Data: TaskProgressData{
			Task:       "users",
			Processed:  500,
			Percentage: 50.0,
		},
	}

	encoded := string(evt.Encode())
	if !strings.HasPrefix(encoded, "event: task.progress\n") {
		t.Fatalf("expected task.progress event, got: %q", encoded)
	}
	if !strings.Contains(encoded, `"processed":500`) {
		t.Fatalf("expected processed count, got: %q", encoded)
	}
}

func TestEventEncodeError(t *testing.T) {
	evt := Event{
		Type: EventTaskError,
		Data: TaskProgressData{
			Task:  "orders",
			Error: "connection timeout",
		},
	}

	encoded := string(evt.Encode())
	if !strings.HasPrefix(encoded, "event: task.error\n") {
		t.Fatalf("expected task.error event, got: %q", encoded)
	}
	if !strings.Contains(encoded, `"error":"connection timeout"`) {
		t.Fatalf("expected error message, got: %q", encoded)
	}
}
