package web

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"db-ferry/daemon"
	"db-ferry/sse"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
)

// Server is the embedded web dashboard HTTP server.
type Server struct {
	configPath string
	daemon     *daemon.Daemon
	sseServer  *sse.Server
	user       string
	pass       string
	server     *http.Server
}

// Options configures the web server.
type Options struct {
	ConfigPath string
	Daemon     *daemon.Daemon
	SSEServer  *sse.Server
	User       string
	Pass       string
}

// New creates a new web server.
func New(opts Options) *Server {
	return &Server{
		configPath: opts.ConfigPath,
		daemon:     opts.Daemon,
		sseServer:  opts.SSEServer,
		user:       opts.User,
		pass:       opts.Pass,
	}
}

// Start begins serving the dashboard API and SPA.
func (s *Server) Start(addr string) error {
	router := chi.NewRouter()
	router.Use(middleware.Recoverer)
	router.Use(cors.Handler(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders: []string{"Accept", "Authorization", "Content-Type"},
		ExposedHeaders: []string{"Content-Type"},
	}))
	router.Use(requestLogMiddleware)

	auth := basicAuthMiddleware(s.user, s.pass)

	// API routes (protected by auth)
	router.Route("/api", func(r chi.Router) {
		r.Use(auth)
		r.Get("/tasks", s.handleGetTasks)
		r.Get("/tasks/{name}", s.handleGetTask)
		r.Post("/tasks/trigger", s.handleTriggerTask)
		r.Get("/config", s.handleGetConfig)
		r.Put("/config", s.handlePutConfig)
		r.Post("/config/validate", s.handleValidateConfig)
		r.Get("/history", s.handleGetHistory)
		r.Get("/history/compare", s.handleCompareHistory)
		r.Get("/databases", s.handleGetDatabases)
		r.Post("/databases/{name}/test", s.handleTestDatabase)
		r.Get("/databases/{name}/tables", s.handleGetTables)
		r.Get("/databases/{name}/tables/{table}/schema", s.handleGetTableSchema)
		r.Get("/databases/{name}/tables/{table}/indexes", s.handleGetTableIndexes)
		r.Post("/doctor", s.handleRunDoctor)
		r.Get("/daemon/status", s.handleGetDaemonStatus)
	})

	// SSE endpoint (protected by auth)
	if s.sseServer != nil {
		router.With(auth).Mount("/api/events", s.sseServer.Handler())
	}

	// SPA static files (protected by auth)
	dist, err := getDistFS()
	if err != nil {
		return fmt.Errorf("failed to get dist fs: %w", err)
	}
	staticServer := auth(http.FileServer(http.FS(dist)))
	router.Get("/*", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" || path == "/" {
			path = "index.html"
		}
		_, err := fs.Stat(dist, path)
		if err != nil {
			// SPA fallback: serve index.html for unknown routes
			f, err := dist.Open("index.html")
			if err != nil {
				http.NotFound(w, r)
				return
			}
			defer f.Close()
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = io.Copy(w, f)
			return
		}
		staticServer.ServeHTTP(w, r)
	})

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	s.server = &http.Server{
		Addr:         ln.Addr().String(),
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("Web server error: %v", err)
		}
	}()

	return nil
}

// Stop shuts down the web server gracefully.
func (s *Server) Stop() error {
	if s.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

// Addr returns the actual listen address.
func (s *Server) Addr() string {
	if s.server == nil {
		return ""
	}
	return s.server.Addr
}
