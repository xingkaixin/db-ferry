package database

import "sync/atomic"

var replicaFallbackTotal int64

// IncReplicaFallbackTotal increments the counter for replica fallback events.
func IncReplicaFallbackTotal() {
	atomic.AddInt64(&replicaFallbackTotal, 1)
}

// GetReplicaFallbackTotal returns the total number of replica fallback events.
func GetReplicaFallbackTotal() int64 {
	return atomic.LoadInt64(&replicaFallbackTotal)
}
