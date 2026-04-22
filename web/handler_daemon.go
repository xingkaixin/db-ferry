package web

import (
	"encoding/json"
	"net/http"
)

func (s *Server) handleGetDaemonStatus(w http.ResponseWriter, r *http.Request) {
	status := map[string]interface{}{
		"running": false,
	}

	if s.daemon != nil {
		status["running"] = s.daemon.IsRunning()
		if err := s.daemon.LastError(); err != nil {
			status["last_error"] = err.Error()
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}
