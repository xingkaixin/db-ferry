package web

import (
	"encoding/json"
	"net/http"

	"db-ferry/doctor"
)

func (s *Server) handleRunDoctor(w http.ResponseWriter, r *http.Request) {
	d := doctor.New(s.configPath)
	results := d.RunChecks()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(results)
}
