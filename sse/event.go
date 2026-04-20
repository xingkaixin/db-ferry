package sse

import (
	"bytes"
	"encoding/json"
	"time"
)

// EventType defines the type of SSE event.
type EventType string

const (
	EventTaskStart    EventType = "task.start"
	EventTaskProgress EventType = "task.progress"
	EventTaskComplete EventType = "task.complete"
	EventTaskError    EventType = "task.error"
)

// TaskProgressData contains progress information for a single task.
type TaskProgressData struct {
	Task          string  `json:"task"`
	SourceDB      string  `json:"source_db,omitempty"`
	TargetDB      string  `json:"target_db,omitempty"`
	EstimatedRows int     `json:"estimated_rows,omitempty"`
	Processed     int     `json:"processed,omitempty"`
	Percentage    float64 `json:"percentage,omitempty"`
	DurationMs    int64   `json:"duration_ms,omitempty"`
	Error         string  `json:"error,omitempty"`
}

// Event is a single SSE event to be broadcast.
type Event struct {
	Type EventType
	Data any
	Time time.Time
}

// Encode returns the SSE-formatted event bytes.
func (e Event) Encode() []byte {
	data, _ := json.Marshal(e.Data)
	var buf bytes.Buffer
	buf.WriteString("event: ")
	buf.WriteString(string(e.Type))
	buf.WriteString("\ndata: ")
	buf.Write(data)
	buf.WriteString("\n\n")
	return buf.Bytes()
}
