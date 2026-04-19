package metrics

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Recorder captures migration metrics and optionally exposes or pushes them.
type Recorder interface {
	RecordRowsProcessed(taskName, sourceDB, targetDB string, count int64)
	RecordBatch(taskName, sourceDB, targetDB string, success bool)
	RecordBatchDuration(taskName, sourceDB, targetDB string, ms float64)
	RecordDLQRows(taskName, sourceDB, targetDB string, count int64)
	RecordValidationMismatch(taskName, sourceDB, targetDB, validateType string)
	RecordTaskDuration(taskName, sourceDB, targetDB string, ms float64)
	ServeHTTP(listenAddr string) error
	Push(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

// NoopRecorder is a no-op implementation of Recorder.
type NoopRecorder struct{}

func NewNoopRecorder() *NoopRecorder { return &NoopRecorder{} }

func (n *NoopRecorder) RecordRowsProcessed(_, _, _ string, _ int64)   {}
func (n *NoopRecorder) RecordBatch(_, _, _ string, _ bool)            {}
func (n *NoopRecorder) RecordBatchDuration(_, _, _ string, _ float64) {}
func (n *NoopRecorder) RecordDLQRows(_, _, _ string, _ int64)         {}
func (n *NoopRecorder) RecordValidationMismatch(_, _, _, _ string)    {}
func (n *NoopRecorder) RecordTaskDuration(_, _, _ string, _ float64)  {}
func (n *NoopRecorder) ServeHTTP(_ string) error                      { return nil }
func (n *NoopRecorder) Push(_ context.Context) error                  { return nil }
func (n *NoopRecorder) Shutdown(_ context.Context) error              { return nil }

var defaultBuckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}

// PrometheusRecorder collects metrics using atomic counters and histograms
// and can expose them in Prometheus text format or push via OTLP/HTTP.
type PrometheusRecorder struct {
	version  string
	endpoint string

	rowsProcessed        sync.Map // string -> *atomic.Int64
	batchesTotal         sync.Map // string -> *atomic.Int64
	dlqRows              sync.Map // string -> *atomic.Int64
	validationMismatches sync.Map // string -> *atomic.Int64

	batchDurationMu sync.RWMutex
	batchDuration   map[string]*histogramEntry

	taskDurationMu sync.RWMutex
	taskDuration   map[string]*histogramEntry

	server *http.Server
}

// NewPrometheusRecorder creates a new PrometheusRecorder.
func NewPrometheusRecorder(version, endpoint string) *PrometheusRecorder {
	return &PrometheusRecorder{
		version:       version,
		endpoint:      endpoint,
		batchDuration: make(map[string]*histogramEntry),
		taskDuration:  make(map[string]*histogramEntry),
	}
}

type histogramEntry struct {
	buckets []float64
	counts  []atomic.Int64
	sum     atomic.Uint64
	count   atomic.Int64
}

func newHistogramEntry(buckets []float64) *histogramEntry {
	return &histogramEntry{
		buckets: buckets,
		counts:  make([]atomic.Int64, len(buckets)),
	}
}

func (h *histogramEntry) addSum(v float64) {
	for {
		old := math.Float64frombits(h.sum.Load())
		newVal := old + v
		if h.sum.CompareAndSwap(math.Float64bits(old), math.Float64bits(newVal)) {
			return
		}
	}
}

func (h *histogramEntry) loadSum() float64 {
	return math.Float64frombits(h.sum.Load())
}

func (h *histogramEntry) observe(v float64) {
	for i, b := range h.buckets {
		if v <= b {
			h.counts[i].Add(1)
			break
		}
	}
	h.addSum(v)
	h.count.Add(1)
}

func (r *PrometheusRecorder) key(taskName, sourceDB, targetDB string) string {
	return taskName + "\x00" + sourceDB + "\x00" + targetDB
}

func (r *PrometheusRecorder) getCounter(m *sync.Map, key string) *atomic.Int64 {
	if v, ok := m.Load(key); ok {
		return v.(*atomic.Int64)
	}
	actual, _ := m.LoadOrStore(key, new(atomic.Int64))
	return actual.(*atomic.Int64)
}

func (r *PrometheusRecorder) getHistogram(m map[string]*histogramEntry, mu *sync.RWMutex, key string) *histogramEntry {
	mu.RLock()
	if e, ok := m[key]; ok {
		mu.RUnlock()
		return e
	}
	mu.RUnlock()

	mu.Lock()
	if e, ok := m[key]; ok {
		mu.Unlock()
		return e
	}
	e := newHistogramEntry(defaultBuckets)
	m[key] = e
	mu.Unlock()
	return e
}

// RecordRowsProcessed increments the rows processed counter.
func (r *PrometheusRecorder) RecordRowsProcessed(taskName, sourceDB, targetDB string, count int64) {
	k := r.key(taskName, sourceDB, targetDB)
	r.getCounter(&r.rowsProcessed, k).Add(count)
}

// RecordBatch records a batch attempt result.
func (r *PrometheusRecorder) RecordBatch(taskName, sourceDB, targetDB string, success bool) {
	k := r.key(taskName, sourceDB, targetDB)
	if success {
		k += "\x00success"
	} else {
		k += "\x00failure"
	}
	r.getCounter(&r.batchesTotal, k).Add(1)
}

// RecordBatchDuration records batch insert latency.
func (r *PrometheusRecorder) RecordBatchDuration(taskName, sourceDB, targetDB string, ms float64) {
	k := r.key(taskName, sourceDB, targetDB)
	r.getHistogram(r.batchDuration, &r.batchDurationMu, k).observe(ms)
}

// RecordDLQRows increments the DLQ rows counter.
func (r *PrometheusRecorder) RecordDLQRows(taskName, sourceDB, targetDB string, count int64) {
	k := r.key(taskName, sourceDB, targetDB)
	r.getCounter(&r.dlqRows, k).Add(count)
}

// RecordValidationMismatch records a validation mismatch.
func (r *PrometheusRecorder) RecordValidationMismatch(taskName, sourceDB, targetDB, validateType string) {
	k := r.key(taskName, sourceDB, targetDB) + "\x00" + validateType
	r.getCounter(&r.validationMismatches, k).Add(1)
}

// RecordTaskDuration records total task latency.
func (r *PrometheusRecorder) RecordTaskDuration(taskName, sourceDB, targetDB string, ms float64) {
	k := r.key(taskName, sourceDB, targetDB)
	r.getHistogram(r.taskDuration, &r.taskDurationMu, k).observe(ms)
}

// ServeHTTP starts a background HTTP server exposing /metrics in Prometheus text format.
func (r *PrometheusRecorder) ServeHTTP(listenAddr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", r.handleMetrics)
	srv := &http.Server{
		Addr:    listenAddr,
		Handler: mux,
	}
	r.server = srv
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server error: %v", err)
		}
	}()
	return nil
}

func (r *PrometheusRecorder) handleMetrics(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	r.writePrometheus(w)
}

func escapeLabelValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	v = strings.ReplaceAll(v, "\n", `\n`)
	return v
}

func writeMetricLine(w io.Writer, name string, labels map[string]string, value string) {
	fmt.Fprintf(w, "%s", name)
	if len(labels) > 0 {
		keys := make([]string, 0, len(labels))
		for k := range labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(labels))
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf(`%s="%s"`, k, escapeLabelValue(labels[k])))
		}
		fmt.Fprintf(w, "{%s}", strings.Join(parts, ","))
	}
	fmt.Fprintf(w, " %s\n", value)
}

func (r *PrometheusRecorder) writePrometheus(w io.Writer) {
	r.writeCounterMetrics(w, "db_ferry_task_rows_processed", "Total rows processed per task.", &r.rowsProcessed)
	r.writeCounterMetrics(w, "db_ferry_task_batches_total", "Total batches processed per task.", &r.batchesTotal)
	r.writeCounterMetrics(w, "db_ferry_task_dlq_rows_total", "Total DLQ rows per task.", &r.dlqRows)
	r.writeCounterMetrics(w, "db_ferry_task_validation_mismatches_total", "Total validation mismatches per task.", &r.validationMismatches)

	r.writeHistogramMetrics(w, "db_ferry_task_batch_duration_ms", "Batch insert duration in milliseconds.", r.batchDuration, &r.batchDurationMu)
	r.writeHistogramMetrics(w, "db_ferry_task_duration_ms", "Task duration in milliseconds.", r.taskDuration, &r.taskDurationMu)
}

func (r *PrometheusRecorder) writeCounterMetrics(w io.Writer, name, help string, m *sync.Map) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s counter\n", name)
	m.Range(func(key, value any) bool {
		k := key.(string)
		v := value.(*atomic.Int64).Load()
		parts := strings.Split(k, "\x00")
		labels := map[string]string{"version": r.version}
		if len(parts) >= 1 {
			labels["task_name"] = parts[0]
		}
		if len(parts) >= 2 {
			labels["source_db"] = parts[1]
		}
		if len(parts) >= 3 {
			labels["target_db"] = parts[2]
		}
		if len(parts) >= 4 {
			if name == "db_ferry_task_batches_total" {
				labels["status"] = parts[3]
			} else if name == "db_ferry_task_validation_mismatches_total" {
				labels["validate_type"] = parts[3]
			}
		}
		writeMetricLine(w, name, labels, fmt.Sprintf("%d", v))
		return true
	})
}

func (r *PrometheusRecorder) writeHistogramMetrics(w io.Writer, name, help string, m map[string]*histogramEntry, mu *sync.RWMutex) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s histogram\n", name)

	mu.RLock()
	defer mu.RUnlock()

	for k, e := range m {
		parts := strings.Split(k, "\x00")
		labels := map[string]string{"version": r.version}
		if len(parts) >= 1 {
			labels["task_name"] = parts[0]
		}
		if len(parts) >= 2 {
			labels["source_db"] = parts[1]
		}
		if len(parts) >= 3 {
			labels["target_db"] = parts[2]
		}

		for i, b := range e.buckets {
			bucketLabels := make(map[string]string, len(labels)+1)
			for lk, lv := range labels {
				bucketLabels[lk] = lv
			}
			bucketLabels["le"] = fmt.Sprintf("%g", b)
			writeMetricLine(w, name+"_bucket", bucketLabels, fmt.Sprintf("%d", e.counts[i].Load()))
		}

		infLabels := make(map[string]string, len(labels)+1)
		for lk, lv := range labels {
			infLabels[lk] = lv
		}
		infLabels["le"] = "+Inf"
		writeMetricLine(w, name+"_bucket", infLabels, fmt.Sprintf("%d", e.count.Load()))

		writeMetricLine(w, name+"_sum", labels, fmt.Sprintf("%g", e.loadSum()))
		writeMetricLine(w, name+"_count", labels, fmt.Sprintf("%d", e.count.Load()))
	}
}

// Push sends the current metrics to the configured OTLP endpoint.
func (r *PrometheusRecorder) Push(ctx context.Context) error {
	if r.endpoint == "" {
		return nil
	}
	return r.pushOTLP(ctx)
}

// Shutdown performs a final push and gracefully shuts down the HTTP server.
func (r *PrometheusRecorder) Shutdown(ctx context.Context) error {
	var pushErr error
	if r.endpoint != "" {
		pushErr = r.Push(ctx)
	}
	var serverErr error
	if r.server != nil {
		serverErr = r.server.Shutdown(ctx)
	}
	if pushErr != nil {
		return pushErr
	}
	return serverErr
}

// StartPushLoop runs a background goroutine that pushes metrics at the given interval.
func StartPushLoop(ctx context.Context, rec Recorder, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := rec.Push(ctx); err != nil {
				// Silent failure for background push to avoid disrupting migration.
			}
		}
	}
}
