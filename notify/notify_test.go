package notify

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"db-ferry/config"
	"db-ferry/processor"
)

func TestSendSuccessPayload(t *testing.T) {
	var received *Payload
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("expected Content-Type application/json, got %q", ct)
		}
		data, _ := io.ReadAll(r.Body)
		var p Payload
		if err := json.Unmarshal(data, &p); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		received = &p
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	cfg := config.NotifyConfig{
		OnSuccess: []string{ts.URL},
		Timeout:   5 * time.Second,
		Retry:     0,
	}
	client := NewClient(cfg)

	results := []processor.TaskResult{
		{Name: "users", Rows: 100, Status: "success"},
		{Name: "orders", Rows: 50, Status: "failed", Error: "connection timeout"},
	}

	if err := client.Send("migration.success", "task.toml", results, 2*time.Second); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	if received == nil {
		t.Fatal("expected payload to be received")
	}
	if received.Event != "migration.success" {
		t.Fatalf("expected event migration.success, got %s", received.Event)
	}
	if received.Config != "task.toml" {
		t.Fatalf("expected config task.toml, got %s", received.Config)
	}
	if received.Project != "db-ferry" {
		t.Fatalf("expected project db-ferry, got %s", received.Project)
	}
	if received.Summary.TotalTasks != 2 {
		t.Fatalf("expected total_tasks 2, got %d", received.Summary.TotalTasks)
	}
	if received.Summary.Success != 1 {
		t.Fatalf("expected success 1, got %d", received.Summary.Success)
	}
	if received.Summary.Failed != 1 {
		t.Fatalf("expected failed 1, got %d", received.Summary.Failed)
	}
	if received.Summary.DurationMs != 2000 {
		t.Fatalf("expected duration_ms 2000, got %d", received.Summary.DurationMs)
	}
	if len(received.Tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(received.Tasks))
	}
	if received.Tasks[0].Name != "users" || received.Tasks[0].Rows != 100 {
		t.Fatalf("unexpected first task: %+v", received.Tasks[0])
	}
	if received.Tasks[1].Error != "connection timeout" {
		t.Fatalf("unexpected second task error: %q", received.Tasks[1].Error)
	}
}

func TestSendNoURLs(t *testing.T) {
	cfg := config.NotifyConfig{}
	client := NewClient(cfg)
	if err := client.Send("migration.success", "task.toml", nil, time.Second); err != nil {
		t.Fatalf("Send() with no URLs should return nil, got %v", err)
	}
}

func TestSendUnknownEvent(t *testing.T) {
	cfg := config.NotifyConfig{
		OnSuccess: []string{"http://localhost:99999"},
	}
	client := NewClient(cfg)
	if err := client.Send("unknown.event", "task.toml", nil, time.Second); err == nil {
		t.Fatal("expected error for unknown event")
	}
}

func TestSendRetrySuccess(t *testing.T) {
	attempts := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	cfg := config.NotifyConfig{
		OnSuccess: []string{ts.URL},
		Timeout:   5 * time.Second,
		Retry:     2,
	}
	client := NewClient(cfg)

	if err := client.Send("migration.success", "task.toml", nil, time.Second); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
}

func TestSendRetryExhausted(t *testing.T) {
	attempts := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	cfg := config.NotifyConfig{
		OnSuccess: []string{ts.URL},
		Timeout:   5 * time.Second,
		Retry:     1,
	}
	client := NewClient(cfg)

	if err := client.Send("migration.success", "task.toml", nil, time.Second); err == nil {
		t.Fatal("expected error after retry exhaustion")
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
}

func TestSendTimeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	cfg := config.NotifyConfig{
		OnSuccess: []string{ts.URL},
		Timeout:   50 * time.Millisecond,
		Retry:     0,
	}
	client := NewClient(cfg)

	if err := client.Send("migration.success", "task.toml", nil, time.Second); err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestSendPartialFailure(t *testing.T) {
	okServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer okServer.Close()

	failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failServer.Close()

	cfg := config.NotifyConfig{
		OnSuccess: []string{okServer.URL, failServer.URL},
		Timeout:   5 * time.Second,
		Retry:     0,
	}
	client := NewClient(cfg)

	err := client.Send("migration.success", "task.toml", nil, time.Second)
	if err == nil {
		t.Fatal("expected error for partial failure")
	}
	if !strings.Contains(err.Error(), "failed to send notification to 1/2 URLs") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestSendFailureEventUsesOnFailure(t *testing.T) {
	var receivedEvent string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		var p Payload
		_ = json.Unmarshal(data, &p)
		receivedEvent = p.Event
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	cfg := config.NotifyConfig{
		OnFailure: []string{ts.URL},
		Timeout:   5 * time.Second,
		Retry:     0,
	}
	client := NewClient(cfg)

	if err := client.Send("migration.failure", "task.toml", nil, time.Second); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if receivedEvent != "migration.failure" {
		t.Fatalf("expected event migration.failure, got %s", receivedEvent)
	}
}

func TestSendDefaultsTimeout(t *testing.T) {
	cfg := config.NotifyConfig{}
	client := NewClient(cfg)
	if client.client.Timeout != 10*time.Second {
		t.Fatalf("expected default timeout 10s, got %v", client.client.Timeout)
	}
}
