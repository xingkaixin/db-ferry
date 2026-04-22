package sse

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

// Server is an SSE server that broadcasts migration progress events.
type Server struct {
	mu         sync.RWMutex
	clients    map[chan Event]struct{}
	taskStates map[string]TaskProgressData
	server     *http.Server
	addr       string
}

// NewServer creates a new SSE server.
func NewServer() *Server {
	return &Server{
		clients:    make(map[chan Event]struct{}),
		taskStates: make(map[string]TaskProgressData),
	}
}

// Start begins serving the SSE endpoint in a background goroutine.
func (s *Server) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/events", s.handleEvents)
	mux.HandleFunc("/status", s.handleStatus)

	s.server = &http.Server{
		Addr:    ln.Addr().String(),
		Handler: mux,
	}
	s.addr = ln.Addr().String()

	go func() {
		if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("SSE server error: %v", err)
		}
	}()

	return nil
}

// Stop shuts down the SSE server gracefully.
func (s *Server) Stop() error {
	if s.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

// Send broadcasts an event to all connected SSE clients and updates the internal state snapshot.
func (s *Server) Send(event Event) {
	s.mu.Lock()
	if data, ok := event.Data.(TaskProgressData); ok {
		s.taskStates[data.Task] = data
	}
	clients := make([]chan Event, 0, len(s.clients))
	for ch := range s.clients {
		clients = append(clients, ch)
	}
	s.mu.Unlock()

	for _, ch := range clients {
		select {
		case ch <- event:
		default:
		}
	}
}

// Addr returns the actual listen address (useful when using :0).
func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.addr
}

// GetStates returns a snapshot of the current task progress states.
func (s *Server) GetStates() map[string]TaskProgressData {
	s.mu.RLock()
	defer s.mu.RUnlock()
	states := make(map[string]TaskProgressData, len(s.taskStates))
	for k, v := range s.taskStates {
		states[k] = v
	}
	return states
}

// Handler returns an http.Handler with the SSE endpoints mounted at /events and /status.
// This allows the SSE server to be embedded into an existing router without starting
// a separate HTTP server.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/events", s.handleEvents)
	mux.HandleFunc("/status", s.handleStatus)
	return mux
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	ch := make(chan Event, 16)
	s.mu.Lock()
	snapshot := make([]TaskProgressData, 0, len(s.taskStates))
	for _, state := range s.taskStates {
		snapshot = append(snapshot, state)
	}
	s.clients[ch] = struct{}{}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.clients, ch)
		s.mu.Unlock()
		close(ch)
	}()

	for _, state := range snapshot {
		_, _ = w.Write(Event{Type: EventTaskProgress, Data: state, Time: time.Now()}.Encode())
	}
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			_, _ = w.Write(event.Encode())
			flusher.Flush()
		}
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	s.mu.RLock()
	states := make(map[string]TaskProgressData, len(s.taskStates))
	for k, v := range s.taskStates {
		states[k] = v
	}
	s.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(states)
}
