package daemon

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"time"
)

// HealthServer exposes a lightweight HTTP health endpoint.
type HealthServer struct {
	addr   string
	daemon *Daemon
	server *http.Server
}

// NewHealthServer creates a health server bound to the given daemon.
func NewHealthServer(addr string, d *Daemon) *HealthServer {
	return &HealthServer{
		addr:   addr,
		daemon: d,
	}
}

// Start begins serving the health endpoint in a background goroutine.
func (h *HealthServer) Start() {
	ln, err := net.Listen("tcp", h.addr)
	if err != nil {
		log.Printf("Health server listen error: %v", err)
		return
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", h.handleHealth)

	h.server = &http.Server{
		Addr:         ln.Addr().String(),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		if err := h.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("Health server error: %v", err)
		}
	}()
}

// Stop shuts down the health server gracefully.
func (h *HealthServer) Stop() error {
	if h.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return h.server.Shutdown(ctx)
}

func (h *HealthServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := http.StatusOK
	msg := "healthy"
	if !h.daemon.IsRunning() {
		status = http.StatusServiceUnavailable
		msg = "not running"
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	resp := map[string]any{
		"status": msg,
		"time":   time.Now().UTC().Format(time.RFC3339),
	}
	if err := h.daemon.LastError(); err != nil {
		resp["last_error"] = err.Error()
	}

	_ = json.NewEncoder(w).Encode(resp)
}
