package processor

import (
	"testing"
	"time"

	"db-ferry/config"
)

func TestAdaptiveBatchControllerGrowAndShrink(t *testing.T) {
	cfg := config.AdaptiveBatchConfig{
		Enabled:         true,
		MinSize:         100,
		MaxSize:         1000,
		TargetLatencyMs: 100,
		MemoryLimitMB:   10,
	}
	c := newAdaptiveBatchController(cfg, 0)

	if c.currentSize != 100 {
		t.Fatalf("expected initial size 100, got %d", c.currentSize)
	}

	// fill window with fast, low-memory batches → should grow
	for i := 0; i < adaptiveWindowSize; i++ {
		c.record(10*time.Millisecond, 1)
	}
	size := c.nextBatchSize(nil)
	if size != 200 {
		t.Fatalf("expected grow to 200, got %d", size)
	}

	// fill window with slow batches → should shrink
	for i := 0; i < adaptiveWindowSize; i++ {
		c.record(200*time.Millisecond, 1)
	}
	size = c.nextBatchSize(nil)
	if size != 100 {
		t.Fatalf("expected shrink back to 100, got %d", size)
	}
}

func TestAdaptiveBatchControllerMaxMinBoundaries(t *testing.T) {
	cfg := config.AdaptiveBatchConfig{
		Enabled:         true,
		MinSize:         100,
		MaxSize:         400,
		TargetLatencyMs: 100,
		MemoryLimitMB:   10,
	}
	c := newAdaptiveBatchController(cfg, 0)

	// grow past max
	c.currentSize = 400
	c.record(10*time.Millisecond, 1)
	c.record(10*time.Millisecond, 1)
	c.record(10*time.Millisecond, 1)
	c.record(10*time.Millisecond, 1)
	c.record(10*time.Millisecond, 1)
	size := c.nextBatchSize(nil)
	if size != 400 {
		t.Fatalf("expected cap at max 400, got %d", size)
	}

	// shrink below min
	c.currentSize = 100
	c.record(200*time.Millisecond, 1)
	c.record(200*time.Millisecond, 1)
	c.record(200*time.Millisecond, 1)
	c.record(200*time.Millisecond, 1)
	c.record(200*time.Millisecond, 1)
	size = c.nextBatchSize(nil)
	if size != 100 {
		t.Fatalf("expected floor at min 100, got %d", size)
	}
}

func TestAdaptiveBatchControllerErrorShrinksImmediately(t *testing.T) {
	cfg := config.AdaptiveBatchConfig{
		Enabled:         true,
		MinSize:         100,
		MaxSize:         1000,
		TargetLatencyMs: 100,
		MemoryLimitMB:   10,
	}
	c := newAdaptiveBatchController(cfg, 0)
	c.currentSize = 400

	size := c.nextBatchSize(assertionError{"boom"})
	if size != 200 {
		t.Fatalf("expected immediate shrink to 200 on error, got %d", size)
	}
}

func TestAdaptiveBatchControllerMemoryPressure(t *testing.T) {
	cfg := config.AdaptiveBatchConfig{
		Enabled:         true,
		MinSize:         100,
		MaxSize:         1000,
		TargetLatencyMs: 1000,
		MemoryLimitMB:   5,
	}
	c := newAdaptiveBatchController(cfg, 0)
	c.currentSize = 200

	// latency is fine but memory exceeds limit
	for i := 0; i < adaptiveWindowSize; i++ {
		c.record(10*time.Millisecond, 10)
	}
	size := c.nextBatchSize(nil)
	if size != 100 {
		t.Fatalf("expected shrink due to memory pressure, got %d", size)
	}
}

func TestAdaptiveBatchControllerFallbackDefaults(t *testing.T) {
	cfg := config.AdaptiveBatchConfig{
		Enabled:         true,
		MinSize:         0,
		MaxSize:         0,
		TargetLatencyMs: 0,
		MemoryLimitMB:   0,
	}
	c := newAdaptiveBatchController(cfg, 500)
	if c.minSize != 500 {
		t.Fatalf("expected minSize fallback to 500, got %d", c.minSize)
	}
	if c.maxSize != 10000 {
		t.Fatalf("expected maxSize fallback to 10000, got %d", c.maxSize)
	}
	if c.targetLatency != 1000*time.Millisecond {
		t.Fatalf("expected targetLatency fallback to 1000ms, got %v", c.targetLatency)
	}
	if c.memoryLimitMB != 512 {
		t.Fatalf("expected memoryLimit fallback to 512MB, got %f", c.memoryLimitMB)
	}
}

func TestEstimateBatchMemoryMB(t *testing.T) {
	batch := [][]any{
		{"hello", []byte("world"), 42},
		{"foo", []byte("bar"), 99},
	}
	mb := estimateBatchMemoryMB(batch)
	expectedBytes := len("hello") + len("world") + 16 + len("foo") + len("bar") + 16
	expectedMB := float64(expectedBytes) / 1024 / 1024
	if mb != expectedMB {
		t.Fatalf("expected %.6f MB, got %.6f", expectedMB, mb)
	}
}

type assertionError struct{ msg string }

func (e assertionError) Error() string { return e.msg }
