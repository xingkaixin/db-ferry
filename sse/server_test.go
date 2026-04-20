package sse

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestNewServer(t *testing.T) {
	s := NewServer()
	if s == nil {
		t.Fatal("NewServer returned nil")
	}
	if s.clients == nil {
		t.Fatal("expected clients map to be initialized")
	}
	if s.taskStates == nil {
		t.Fatal("expected taskStates map to be initialized")
	}
}

func TestServerStartStop(t *testing.T) {
	s := NewServer()
	if err := s.Start(":0"); err != nil {
		t.Fatalf("Start error = %v", err)
	}
	if s.server == nil {
		t.Fatal("expected server to be initialized after Start")
	}
	if s.Addr() == "" {
		t.Fatal("expected non-empty addr after Start")
	}

	if err := s.Stop(); err != nil {
		t.Fatalf("Stop error = %v", err)
	}
}

func TestServerStopBeforeStart(t *testing.T) {
	s := NewServer()
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop before Start error = %v", err)
	}
}

func TestServerSendAndBroadcast(t *testing.T) {
	s := NewServer()
	if err := s.Start(":0"); err != nil {
		t.Fatalf("Start error = %v", err)
	}
	defer s.Stop()

	baseURL := "http://" + s.Addr()
	waitForServer(t, baseURL+"/status")

	// Connect two SSE clients
	resp1, err := http.Get(baseURL + "/events")
	if err != nil {
		t.Fatalf("GET /events error = %v", err)
	}
	defer resp1.Body.Close()

	resp2, err := http.Get(baseURL + "/events")
	if err != nil {
		t.Fatalf("GET /events (2nd client) error = %v", err)
	}
	defer resp2.Body.Close()

	// Send an event
	s.Send(Event{
		Type: EventTaskStart,
		Data: TaskProgressData{Task: "orders", EstimatedRows: 1000},
		Time: time.Now(),
	})

	// Read from first client
	reader1 := bufio.NewReader(resp1.Body)
	line, err := readSSELine(reader1)
	if err != nil {
		t.Fatalf("failed to read SSE from client 1: %v", err)
	}
	if !strings.Contains(line, "task.start") {
		t.Fatalf("expected task.start event, got: %q", line)
	}

	// Read data line from client 1
	dataLine, err := readSSELine(reader1)
	if err != nil {
		t.Fatalf("failed to read SSE data from client 1: %v", err)
	}
	if !strings.Contains(dataLine, `"task":"orders"`) {
		t.Fatalf("expected task name in data, got: %q", dataLine)
	}

	// Read from second client
	reader2 := bufio.NewReader(resp2.Body)
	line2, err := readSSELine(reader2)
	if err != nil {
		t.Fatalf("failed to read SSE from client 2: %v", err)
	}
	if !strings.Contains(line2, "task.start") {
		t.Fatalf("expected task.start event on client 2, got: %q", line2)
	}
}

func TestServerStatusEndpoint(t *testing.T) {
	s := NewServer()
	if err := s.Start(":0"); err != nil {
		t.Fatalf("Start error = %v", err)
	}
	defer s.Stop()

	// Pre-seed state
	s.Send(Event{
		Type: EventTaskProgress,
		Data: TaskProgressData{Task: "orders", Processed: 500, EstimatedRows: 1000},
		Time: time.Now(),
	})

	baseURL := "http://" + s.Addr()
	waitForServer(t, baseURL+"/status")

	resp, err := http.Get(baseURL + "/status")
	if err != nil {
		t.Fatalf("GET /status error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var result map[string]TaskProgressData
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("unmarshal status response error = %v", err)
	}

	state, ok := result["orders"]
	if !ok {
		t.Fatalf("expected task 'orders' in status, got: %v", result)
	}
	if state.Processed != 500 {
		t.Fatalf("expected processed 500, got %d", state.Processed)
	}
}

func TestServerStatusMethodNotAllowed(t *testing.T) {
	s := NewServer()
	if err := s.Start(":0"); err != nil {
		t.Fatalf("Start error = %v", err)
	}
	defer s.Stop()

	baseURL := "http://" + s.Addr()
	waitForServer(t, baseURL+"/status")

	req, _ := http.NewRequest(http.MethodPost, baseURL+"/status", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /status error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected status 405, got %d", resp.StatusCode)
	}
}

func TestServerEventsMethodNotAllowed(t *testing.T) {
	s := NewServer()
	if err := s.Start(":0"); err != nil {
		t.Fatalf("Start error = %v", err)
	}
	defer s.Stop()

	baseURL := "http://" + s.Addr()
	waitForServer(t, baseURL+"/status")

	req, _ := http.NewRequest(http.MethodPost, baseURL+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /events error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected status 405, got %d", resp.StatusCode)
	}
}

func TestServerCORSHeaders(t *testing.T) {
	s := NewServer()
	if err := s.Start(":0"); err != nil {
		t.Fatalf("Start error = %v", err)
	}
	defer s.Stop()

	baseURL := "http://" + s.Addr()
	waitForServer(t, baseURL+"/status")

	resp, err := http.Get(baseURL + "/status")
	if err != nil {
		t.Fatalf("GET /status error = %v", err)
	}
	defer resp.Body.Close()

	origin := resp.Header.Get("Access-Control-Allow-Origin")
	if origin != "*" {
		t.Fatalf("expected CORS header *, got %q", origin)
	}
}

func TestServerSnapshotForNewClient(t *testing.T) {
	s := NewServer()
	if err := s.Start(":0"); err != nil {
		t.Fatalf("Start error = %v", err)
	}
	defer s.Stop()

	// Send event before client connects
	s.Send(Event{
		Type: EventTaskProgress,
		Data: TaskProgressData{Task: "users", Processed: 100, EstimatedRows: 200},
		Time: time.Now(),
	})

	baseURL := "http://" + s.Addr()
	waitForServer(t, baseURL+"/status")

	// New client should receive snapshot
	resp, err := http.Get(baseURL + "/events")
	if err != nil {
		t.Fatalf("GET /events error = %v", err)
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	line, err := readSSELine(reader)
	if err != nil {
		t.Fatalf("failed to read snapshot: %v", err)
	}
	if !strings.Contains(line, "task.progress") {
		t.Fatalf("expected snapshot event, got: %q", line)
	}
}

func readSSELine(r *bufio.Reader) (string, error) {
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return "", err
		}
		line = strings.TrimRight(line, "\r\n")
		if line != "" {
			return line, nil
		}
	}
}

func waitForServer(t *testing.T, url string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("server did not become ready at %s", url)
		case <-ticker.C:
			resp, err := http.Get(url)
			if err == nil {
				resp.Body.Close()
				return
			}
		}
	}
}
