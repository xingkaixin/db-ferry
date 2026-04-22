package daemon

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestNewHealthServer(t *testing.T) {
	d := New(Options{ConfigPath: "test.toml"})
	hs := NewHealthServer(":0", d)
	//nolint:staticcheck
	if hs == nil {
		t.Fatal("NewHealthServer returned nil")
	}
	if hs.addr == "" {
		t.Fatal("expected non-empty addr")
	}
	if hs.daemon != d {
		t.Fatal("daemon reference mismatch")
	}
}

func TestHealthServerStartStop(t *testing.T) {
	d := New(Options{ConfigPath: "test.toml"})
	hs := NewHealthServer(":0", d)

	hs.Start()
	if hs.server == nil {
		t.Fatal("expected server to be initialized after Start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Wait briefly for server to be ready.
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	ready := false
	for {
		select {
		case <-ctx.Done():
			t.Fatal("server did not become ready")
		case <-ticker.C:
			if hs.server != nil {
				ready = true
			}
		}
		if ready {
			break
		}
	}

	if err := hs.Stop(); err != nil {
		t.Fatalf("Stop error = %v", err)
	}
}

func TestHealthServerStopBeforeStart(t *testing.T) {
	d := New(Options{ConfigPath: "test.toml"})
	hs := NewHealthServer(":0", d)

	if err := hs.Stop(); err != nil {
		t.Fatalf("Stop before Start error = %v", err)
	}
}

func TestHealthServerHandleHealth(t *testing.T) {
	d := New(Options{ConfigPath: "test.toml"})
	// Simulate running state.
	d.mu.Lock()
	d.running = true
	d.mu.Unlock()

	hs := NewHealthServer(":0", d)
	hs.Start()
	defer hs.Stop()

	baseURL := "http://" + hs.server.Addr

	// Wait for server readiness.
	waitForServer(t, baseURL+"/health")

	// GET /health while running.
	resp, err := http.Get(baseURL + "/health")
	if err != nil {
		t.Fatalf("GET /health error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("unmarshal health response error = %v", err)
	}
	if result["status"] != "healthy" {
		t.Fatalf("expected status healthy, got %v", result["status"])
	}
}

func TestHealthServerHandleHealthNotRunning(t *testing.T) {
	d := New(Options{ConfigPath: "test.toml"})
	// Leave running as false.

	hs := NewHealthServer(":0", d)
	hs.Start()
	defer hs.Stop()

	baseURL := "http://" + hs.server.Addr
	waitForServer(t, baseURL+"/health")

	resp, err := http.Get(baseURL + "/health")
	if err != nil {
		t.Fatalf("GET /health error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("unmarshal health response error = %v", err)
	}
	if result["status"] != "not running" {
		t.Fatalf("expected status not running, got %v", result["status"])
	}
}

func TestHealthServerHandleHealthMethodNotAllowed(t *testing.T) {
	d := New(Options{ConfigPath: "test.toml"})
	d.mu.Lock()
	d.running = true
	d.mu.Unlock()

	hs := NewHealthServer(":0", d)
	hs.Start()
	defer hs.Stop()

	baseURL := "http://" + hs.server.Addr
	waitForServer(t, baseURL+"/health")

	req, _ := http.NewRequest(http.MethodPost, baseURL+"/health", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /health error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected status 405, got %d", resp.StatusCode)
	}
}

func TestHealthServerHandleHealthWithLastError(t *testing.T) {
	d := New(Options{ConfigPath: "test.toml"})
	d.mu.Lock()
	d.running = true
	d.lastErr = context.Canceled
	d.mu.Unlock()

	hs := NewHealthServer(":0", d)
	hs.Start()
	defer hs.Stop()

	baseURL := "http://" + hs.server.Addr
	waitForServer(t, baseURL+"/health")

	resp, err := http.Get(baseURL + "/health")
	if err != nil {
		t.Fatalf("GET /health error = %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("unmarshal health response error = %v", err)
	}
	if result["last_error"] != context.Canceled.Error() {
		t.Fatalf("expected last_error %q, got %v", context.Canceled.Error(), result["last_error"])
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
