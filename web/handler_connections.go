package web

import (
	"encoding/json"
	"net/http"

	"db-ferry/config"
	"db-ferry/database"

	"github.com/go-chi/chi/v5"
)

func (s *Server) handleGetDatabases(w http.ResponseWriter, r *http.Request) {
	cfg, err := config.LoadConfig(s.configPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	dbs := make([]map[string]interface{}, 0, len(cfg.Databases))
	for _, db := range cfg.Databases {
		dbs = append(dbs, map[string]interface{}{
			"name":          db.Name,
			"type":          db.Type,
			"host":          db.Host,
			"port":          db.Port,
			"service":       db.Service,
			"database":      db.Database,
			"user":          db.User,
			"password":      "***",
			"path":          db.Path,
			"ssl_mode":      db.SSLMode,
			"pool_max_open": db.PoolMaxOpen,
			"pool_max_idle": db.PoolMaxIdle,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(dbs)
}

func (s *Server) handleTestDatabase(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	cfg, err := config.LoadConfig(s.configPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, ok := cfg.GetDatabase(name)
	if !ok {
		http.Error(w, "database not found", http.StatusNotFound)
		return
	}

	manager := database.NewConnectionManager(cfg)
	defer func() { _ = manager.CloseAll() }()

	_, err = manager.GetSource(name)
	status := "ok"
	msg := ""
	if err != nil {
		status = "error"
		msg = err.Error()
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  status,
		"message": msg,
	})
}

func (s *Server) handleGetTables(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	cfg, err := config.LoadConfig(s.configPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, ok := cfg.GetDatabase(name)
	if !ok {
		http.Error(w, "database not found", http.StatusNotFound)
		return
	}

	manager := database.NewConnectionManager(cfg)
	defer func() { _ = manager.CloseAll() }()

	src, err := manager.GetSource(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	tables, err := src.GetTables()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(tables)
}

func (s *Server) handleGetTableSchema(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	table := chi.URLParam(r, "table")

	cfg, err := config.LoadConfig(s.configPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	dbCfg, ok := cfg.GetDatabase(name)
	if !ok {
		http.Error(w, "database not found", http.StatusNotFound)
		return
	}

	manager := database.NewConnectionManager(cfg)
	defer func() { _ = manager.CloseAll() }()

	src, err := manager.GetSource(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	columns, err := database.GetTableSchema(src, table)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	pk, _ := database.GetTablePrimaryKey(src, dbCfg.Type, table)
	indexes, _ := database.GetTableIndexes(src, dbCfg.Type, table)

	schema := database.TableSchema{
		Columns:    columns,
		PrimaryKey: pk,
		Indexes:    indexes,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(schema)
}

func (s *Server) handleGetTableIndexes(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	table := chi.URLParam(r, "table")

	cfg, err := config.LoadConfig(s.configPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	dbCfg, ok := cfg.GetDatabase(name)
	if !ok {
		http.Error(w, "database not found", http.StatusNotFound)
		return
	}

	manager := database.NewConnectionManager(cfg)
	defer func() { _ = manager.CloseAll() }()

	src, err := manager.GetSource(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	indexes, err := database.GetTableIndexes(src, dbCfg.Type, table)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(indexes)
}
