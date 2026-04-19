package metrics

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNoopRecorder(t *testing.T) {
	n := NewNoopRecorder()
	n.RecordRowsProcessed("t", "s", "d", 1)
	n.RecordBatch("t", "s", "d", true)
	n.RecordBatchDuration("t", "s", "d", 1.0)
	n.RecordDLQRows("t", "s", "d", 1)
	n.RecordValidationMismatch("t", "s", "d", "row_count")
	n.RecordTaskDuration("t", "s", "d", 1.0)
	if err := n.ServeHTTP(":0"); err != nil {
		t.Fatalf("ServeHTTP error: %v", err)
	}
	if err := n.Push(context.Background()); err != nil {
		t.Fatalf("Push error: %v", err)
	}
	if err := n.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown error: %v", err)
	}
}

func TestPrometheusRecorderCounterConcurrency(t *testing.T) {
	r := NewPrometheusRecorder("test", "")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				r.RecordRowsProcessed("task", "src", "dst", 1)
			}
		}()
	}
	wg.Wait()

	k := r.key("task", "src", "dst")
	v := r.getCounter(&r.rowsProcessed, k).Load()
	if v != 100000 {
		t.Fatalf("expected 100000, got %d", v)
	}
}

func TestPrometheusRecorderHistogramConcurrency(t *testing.T) {
	r := NewPrometheusRecorder("test", "")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				r.RecordBatchDuration("task", "src", "dst", float64(j%100))
			}
		}()
	}
	wg.Wait()

	k := r.key("task", "src", "dst")
	e := r.getHistogram(r.batchDuration, &r.batchDurationMu, k)
	if e.count.Load() != 100000 {
		t.Fatalf("expected count 100000, got %d", e.count.Load())
	}
}

func TestPrometheusTextFormat(t *testing.T) {
	r := NewPrometheusRecorder("v1", "")
	r.RecordRowsProcessed("users", "src", "dst", 42)
	r.RecordBatch("users", "src", "dst", true)
	r.RecordBatch("users", "src", "dst", false)
	r.RecordBatchDuration("users", "src", "dst", 50.0)
	r.RecordDLQRows("users", "src", "dst", 3)
	r.RecordValidationMismatch("users", "src", "dst", "row_count")
	r.RecordTaskDuration("users", "src", "dst", 1000.0)

	var buf strings.Builder
	r.writePrometheus(&buf)
	out := buf.String()

	checks := []string{
		`db_ferry_task_rows_processed{source_db="src",target_db="dst",task_name="users",version="v1"} 42`,
		`db_ferry_task_batches_total{source_db="src",status="success",target_db="dst",task_name="users",version="v1"} 1`,
		`db_ferry_task_batches_total{source_db="src",status="failure",target_db="dst",task_name="users",version="v1"} 1`,
		`db_ferry_task_dlq_rows_total{source_db="src",target_db="dst",task_name="users",version="v1"} 3`,
		`db_ferry_task_validation_mismatches_total{source_db="src",target_db="dst",task_name="users",validate_type="row_count",version="v1"} 1`,
		`db_ferry_task_batch_duration_ms_bucket{le="50",source_db="src",target_db="dst",task_name="users",version="v1"} 1`,
		`db_ferry_task_batch_duration_ms_sum{source_db="src",target_db="dst",task_name="users",version="v1"} 50`,
		`db_ferry_task_duration_ms_bucket{le="+Inf",source_db="src",target_db="dst",task_name="users",version="v1"} 1`,
		`db_ferry_task_duration_ms_sum{source_db="src",target_db="dst",task_name="users",version="v1"} 1000`,
	}

	for _, check := range checks {
		if !strings.Contains(out, check) {
			t.Fatalf("expected output to contain %q, got:\n%s", check, out)
		}
	}
}

func TestPrometheusHTTPHandler(t *testing.T) {
	r := NewPrometheusRecorder("v1", "")
	r.RecordRowsProcessed("users", "src", "dst", 10)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	r.handleMetrics(w, req)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Fatalf("expected text/plain content type, got %s", ct)
	}
	if !strings.Contains(string(body), `db_ferry_task_rows_processed`) {
		t.Fatalf("expected metrics in response, got: %s", string(body))
	}
}

func TestPrometheusServerLifecycle(t *testing.T) {
	r := NewPrometheusRecorder("v1", "")

	if err := r.ServeHTTP("127.0.0.1:19091"); err != nil {
		t.Fatalf("ServeHTTP error: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get("http://127.0.0.1:19091/metrics")
	if err != nil {
		t.Fatalf("GET error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown error: %v", err)
	}
}

func TestPrometheusRecorderPush(t *testing.T) {
	var received []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		received, _ = io.ReadAll(req.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	r := NewPrometheusRecorder("v1", srv.URL)
	r.RecordRowsProcessed("users", "src", "dst", 5)
	r.RecordBatchDuration("users", "src", "dst", 10.0)

	if err := r.Push(context.Background()); err != nil {
		t.Fatalf("Push error: %v", err)
	}

	if len(received) == 0 {
		t.Fatal("expected non-empty payload")
	}

	var payload map[string]any
	if err := json.Unmarshal(received, &payload); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	rms, ok := payload["resourceMetrics"].([]any)
	if !ok || len(rms) == 0 {
		t.Fatal("expected resourceMetrics")
	}
}

func TestStartPushLoop(t *testing.T) {
	var count int
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		count++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	r := NewPrometheusRecorder("v1", srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go StartPushLoop(ctx, r, 50*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	cancel()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	c := count
	mu.Unlock()
	if c < 2 {
		t.Fatalf("expected at least 2 pushes, got %d", c)
	}
}

func TestPrometheusRecorderPushNoEndpoint(t *testing.T) {
	r := NewPrometheusRecorder("v1", "")
	if err := r.Push(context.Background()); err != nil {
		t.Fatalf("Push with empty endpoint should return nil, got %v", err)
	}
}

func TestPrometheusRecorderShutdownWithoutServer(t *testing.T) {
	r := NewPrometheusRecorder("v1", "")
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown without server should return nil, got %v", err)
	}
}

func TestPrometheusRecorderPushFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	r := NewPrometheusRecorder("v1", srv.URL)
	if err := r.Push(context.Background()); err == nil {
		t.Fatal("expected error for bad status code")
	}
}
