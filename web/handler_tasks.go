package web

import (
	"encoding/json"
	"net/http"

	"db-ferry/config"
	"db-ferry/sse"

	"github.com/go-chi/chi/v5"
)

func taskToMap(t config.TaskConfig) map[string]interface{} {
	indexes := make([]map[string]interface{}, 0, len(t.Indexes))
	for _, idx := range t.Indexes {
		indexes = append(indexes, map[string]interface{}{
			"name":    idx.Name,
			"columns": idx.Columns,
		})
	}
	masking := make([]map[string]interface{}, 0, len(t.Masking))
	for _, m := range t.Masking {
		masking = append(masking, map[string]interface{}{
			"column": m.Column,
			"rule":   m.Rule,
		})
	}
	return map[string]interface{}{
		"table_name":           t.TableName,
		"sql":                  t.SQL,
		"source_db":            t.SourceDB,
		"target_db":            t.TargetDB,
		"ignore":               t.Ignore,
		"mode":                 t.Mode,
		"batch_size":           t.BatchSize,
		"max_retries":          t.MaxRetries,
		"validate":             t.Validate,
		"validate_sample_size": t.ValidateSampleSize,
		"merge_keys":           t.MergeKeys,
		"resume_key":           t.ResumeKey,
		"resume_from":          t.ResumeFrom,
		"state_file":           t.StateFile,
		"allow_same_table":     t.AllowSameTable,
		"skip_create_table":    t.SkipCreateTable,
		"schema_evolution":     t.SchemaEvolution,
		"dlq_path":             t.DLQPath,
		"dlq_format":           t.DLQFormat,
		"indexes":              indexes,
		"masking":              masking,
		"pre_sql":              t.PreSQL,
		"post_sql":             t.PostSQL,
		"depends_on":           t.DependsOn,
	}
}

func (s *Server) handleGetTasks(w http.ResponseWriter, r *http.Request) {
	cfg, err := config.LoadConfig(s.configPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var states map[string]sse.TaskProgressData
	if s.sseServer != nil {
		states = s.sseServer.GetStates()
	}

	tasks := make([]map[string]interface{}, 0, len(cfg.Tasks))
	for _, t := range cfg.Tasks {
		m := taskToMap(t)
		if state, ok := states[t.TableName]; ok {
			m["processed"] = state.Processed
			m["percentage"] = state.Percentage
			m["duration_ms"] = state.DurationMs
			if state.Error != "" {
				m["status"] = "error"
			} else if state.Percentage >= 100 {
				m["status"] = "completed"
			} else if state.Processed > 0 {
				m["status"] = "running"
			} else {
				m["status"] = ""
			}
		} else {
			m["processed"] = 0
			m["percentage"] = 0.0
			m["duration_ms"] = int64(0)
			m["status"] = ""
		}
		tasks = append(tasks, m)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(tasks)
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	cfg, err := config.LoadConfig(s.configPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var found config.TaskConfig
	for _, t := range cfg.Tasks {
		if t.TableName == name {
			found = t
			break
		}
	}
	if found.TableName == "" {
		http.Error(w, "task not found", http.StatusNotFound)
		return
	}

	m := taskToMap(found)
	if s.sseServer != nil {
		states := s.sseServer.GetStates()
		if state, ok := states[name]; ok {
			m["processed"] = state.Processed
			m["percentage"] = state.Percentage
			m["duration_ms"] = state.DurationMs
			if state.Error != "" {
				m["status"] = "error"
			} else if state.Percentage >= 100 {
				m["status"] = "completed"
			} else if state.Processed > 0 {
				m["status"] = "running"
			} else {
				m["status"] = ""
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(m)
}

func (s *Server) handleTriggerTask(w http.ResponseWriter, r *http.Request) {
	if s.daemon == nil || !s.daemon.IsRunning() {
		http.Error(w, "daemon not running", http.StatusServiceUnavailable)
		return
	}

	s.daemon.TriggerRound()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "triggered"})
}
