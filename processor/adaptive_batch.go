package processor

import (
	"fmt"
	"time"

	"db-ferry/config"
)

const adaptiveWindowSize = 5

// adaptiveBatchController dynamically tunes batch size based on latency and memory.
type adaptiveBatchController struct {
	minSize       int
	maxSize       int
	targetLatency time.Duration
	memoryLimitMB float64
	currentSize   int
	latencyWindow []time.Duration
	memoryWindow  []float64
}

func newAdaptiveBatchController(cfg config.AdaptiveBatchConfig, fallbackSize int) *adaptiveBatchController {
	minSize := cfg.MinSize
	if minSize <= 0 {
		minSize = fallbackSize
		if minSize <= 0 {
			minSize = 1000
		}
	}
	maxSize := cfg.MaxSize
	if maxSize <= 0 {
		maxSize = minSize * 10
		if maxSize < 10000 {
			maxSize = 10000
		}
	}

	targetLatency := time.Duration(cfg.TargetLatencyMs) * time.Millisecond
	if targetLatency <= 0 {
		targetLatency = 1000 * time.Millisecond
	}

	memoryLimitMB := float64(cfg.MemoryLimitMB)
	if memoryLimitMB <= 0 {
		memoryLimitMB = 512
	}

	return &adaptiveBatchController{
		minSize:       minSize,
		maxSize:       maxSize,
		targetLatency: targetLatency,
		memoryLimitMB: memoryLimitMB,
		currentSize:   minSize,
		latencyWindow: make([]time.Duration, 0, adaptiveWindowSize),
		memoryWindow:  make([]float64, 0, adaptiveWindowSize),
	}
}

func (c *adaptiveBatchController) record(latency time.Duration, memoryMB float64) {
	c.latencyWindow = append(c.latencyWindow, latency)
	if len(c.latencyWindow) > adaptiveWindowSize {
		c.latencyWindow = c.latencyWindow[1:]
	}
	c.memoryWindow = append(c.memoryWindow, memoryMB)
	if len(c.memoryWindow) > adaptiveWindowSize {
		c.memoryWindow = c.memoryWindow[1:]
	}
}

func (c *adaptiveBatchController) shouldAdjust() bool {
	return len(c.latencyWindow) >= adaptiveWindowSize
}

func (c *adaptiveBatchController) nextBatchSize(err error) int {
	if err != nil {
		c.shrink()
		return c.currentSize
	}

	if !c.shouldAdjust() {
		return c.currentSize
	}

	avgLatency := c.avgLatency()
	avgMemory := c.avgMemory()

	if avgLatency < c.targetLatency && avgMemory < c.memoryLimitMB {
		c.grow()
	} else if avgLatency > c.targetLatency || avgMemory > c.memoryLimitMB {
		c.shrink()
	}

	return c.currentSize
}

func (c *adaptiveBatchController) grow() {
	if c.currentSize < c.maxSize {
		c.currentSize *= 2
		if c.currentSize > c.maxSize {
			c.currentSize = c.maxSize
		}
	}
}

func (c *adaptiveBatchController) shrink() {
	if c.currentSize > c.minSize {
		c.currentSize /= 2
		if c.currentSize < c.minSize {
			c.currentSize = c.minSize
		}
	}
}

func (c *adaptiveBatchController) avgLatency() time.Duration {
	if len(c.latencyWindow) == 0 {
		return 0
	}
	var sum time.Duration
	for _, v := range c.latencyWindow {
		sum += v
	}
	return sum / time.Duration(len(c.latencyWindow))
}

func (c *adaptiveBatchController) avgMemory() float64 {
	if len(c.memoryWindow) == 0 {
		return 0
	}
	var sum float64
	for _, v := range c.memoryWindow {
		sum += v
	}
	return sum / float64(len(c.memoryWindow))
}

func estimateBatchMemoryMB(batch [][]any) float64 {
	var bytes int64
	for _, row := range batch {
		for _, val := range row {
			switch v := val.(type) {
			case string:
				bytes += int64(len(v))
			case []byte:
				bytes += int64(len(v))
			default:
				bytes += 16
			}
		}
	}
	return float64(bytes) / 1024 / 1024
}

func (c *adaptiveBatchController) debugInfo() string {
	return fmt.Sprintf("adaptive batch: current=%d min=%d max=%d avg_latency=%s avg_memory=%.2fMB",
		c.currentSize, c.minSize, c.maxSize, c.avgLatency(), c.avgMemory())
}
