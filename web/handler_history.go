package web

import (
	"encoding/json"
	"net/http"
	"strconv"

	"db-ferry/config"
	"db-ferry/database"
)

func (s *Server) handleGetHistory(w http.ResponseWriter, r *http.Request) {
	limitStr := r.URL.Query().Get("limit")
	limit := 50
	if limitStr != "" {
		if n, err := strconv.Atoi(limitStr); err == nil && n > 0 {
			limit = n
		}
	}

	cfg, err := config.LoadConfig(s.configPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	manager := database.NewConnectionManager(cfg)
	defer func() { _ = manager.CloseAll() }()

	targetAliases := make(map[string]struct{})
	for _, task := range cfg.Tasks {
		if !task.Ignore {
			targetAliases[task.TargetDB] = struct{}{}
		}
	}

	var allRecords []database.MigrationRecord
	for alias := range targetAliases {
		targetDB, err := manager.GetTarget(alias)
		if err != nil {
			continue
		}
		dbCfg, ok := cfg.GetDatabase(alias)
		if !ok {
			continue
		}
		recorder := database.NewHistoryRecorder(dbCfg.Type, cfg.History.Table())
		records, err := recorder.List(targetDB, limit)
		if err != nil {
			continue
		}
		allRecords = append(allRecords, records...)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(allRecords)
}

func (s *Server) handleCompareHistory(w http.ResponseWriter, r *http.Request) {
	id1 := r.URL.Query().Get("id1")
	id2 := r.URL.Query().Get("id2")
	if id1 == "" || id2 == "" {
		http.Error(w, "id1 and id2 are required", http.StatusBadRequest)
		return
	}

	cfg, err := config.LoadConfig(s.configPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	manager := database.NewConnectionManager(cfg)
	defer func() { _ = manager.CloseAll() }()

	targetAliases := make(map[string]struct{})
	for _, task := range cfg.Tasks {
		if !task.Ignore {
			targetAliases[task.TargetDB] = struct{}{}
		}
	}

	var rec1, rec2 *database.MigrationRecord
	for alias := range targetAliases {
		targetDB, err := manager.GetTarget(alias)
		if err != nil {
			continue
		}
		dbCfg, ok := cfg.GetDatabase(alias)
		if !ok {
			continue
		}
		recorder := database.NewHistoryRecorder(dbCfg.Type, cfg.History.Table())
		records, err := recorder.List(targetDB, 1000)
		if err != nil {
			continue
		}
		for i := range records {
			if records[i].ID == id1 {
				rec1 = &records[i]
			}
			if records[i].ID == id2 {
				rec2 = &records[i]
			}
		}
	}

	if rec1 == nil || rec2 == nil {
		http.Error(w, "records not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"left":  rec1,
		"right": rec2,
	})
}
