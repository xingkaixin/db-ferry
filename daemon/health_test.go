package daemon

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestHealthServerStartAndStop(t *testing.T) {
	d := New(Options{ConfigPath: "task.toml"})
	addr := "127.0.0.1:0"
	hsrv := NewHealthServer(addr, d)
	hsrv.Start()

	// Give it a moment to bind.
	time.Sleep(50 * time.Millisecond)

	if err := hsrv.Stop(); err != nil {
		t.Fatalf("Stop error = %v", err)
	}
}

func TestHealthEndpoint(t *testing.T) {
	d := New(Options{ConfigPath: "task.toml"})
	d.mu.Lock()
	d.running = true
	d.mu.Unlock()

	addr := "127.0.0.1:0"
	hsrv := NewHealthServer(addr, d)
	hsrv.Start()

	// Give it a moment to bind.
	time.Sleep(50 * time.Millisecond)

	// Use the actual bound address.
	url := fmt.Sprintf("http://%s/health", hsrv.server.Addr)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("health request error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	body, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("unmarshal body error = %v", err)
	}
	if result["status"] != "healthy" {
		t.Fatalf("status = %v, want healthy", result["status"])
	}

	_ = hsrv.Stop()
}

func TestHealthEndpointNotRunning(t *testing.T) {
	d := New(Options{ConfigPath: "task.toml"})
	addr := "127.0.0.1:0"
	hsrv := NewHealthServer(addr, d)
	hsrv.Start()

	time.Sleep(50 * time.Millisecond)

	d.Stop()
	// Wait for daemon to stop.
	time.Sleep(50 * time.Millisecond)

	url := fmt.Sprintf("http://%s/health", hsrv.server.Addr)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("health request error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}

	_ = hsrv.Stop()
}

func TestHealthEndpointMethodNotAllowed(t *testing.T) {
	d := New(Options{ConfigPath: "task.toml"})
	addr := "127.0.0.1:0"
	hsrv := NewHealthServer(addr, d)
	hsrv.Start()

	time.Sleep(50 * time.Millisecond)

	url := fmt.Sprintf("http://%s/health", hsrv.server.Addr)
	resp, err := http.Post(url, "application/json", nil)
	if err != nil {
		t.Fatalf("health request error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusMethodNotAllowed)
	}

	_ = hsrv.Stop()
}

func TestHealthEndpointWithLastError(t *testing.T) {
	d := New(Options{ConfigPath: "task.toml"})
	d.mu.Lock()
	d.running = true
	d.lastErr = fmt.Errorf("something went wrong")
	d.mu.Unlock()

	addr := "127.0.0.1:0"
	hsrv := NewHealthServer(addr, d)
	hsrv.Start()

	time.Sleep(50 * time.Millisecond)

	url := fmt.Sprintf("http://%s/health", hsrv.server.Addr)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("health request error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	body, _ := io.ReadAll(resp.Body)
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("unmarshal body error = %v", err)
	}
	if result["last_error"] != "something went wrong" {
		t.Fatalf("last_error = %v, want something went wrong", result["last_error"])
	}

	_ = hsrv.Stop()
}
