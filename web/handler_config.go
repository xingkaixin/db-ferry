package web

import (
	"encoding/json"
	"io"
	"net/http"
	"os"

	"db-ferry/config"
)

func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	data, err := os.ReadFile(s.configPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write(data)
}

func (s *Server) handlePutConfig(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if err := os.WriteFile(s.configPath, data, 0o644); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "saved"})
}

func (s *Server) handleValidateConfig(w http.ResponseWriter, r *http.Request) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	tmpFile, err := os.CreateTemp("", "db-ferry-validate-*.toml")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = tmpFile.Close()

	cfg, err := config.LoadConfig(tmpFile.Name())
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"valid": "false", "error": err.Error()})
		return
	}

	if err := cfg.Validate(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"valid": "false", "error": err.Error()})
		return
	}

	_ = json.NewEncoder(w).Encode(map[string]string{"valid": "true"})
}
