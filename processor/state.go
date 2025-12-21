package processor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"db-ferry/config"
)

type stateFile struct {
	Tasks map[string]string `json:"tasks"`
}

func (p *Processor) loadStateFile(path string) (*stateFile, error) {
	if path == "" {
		return &stateFile{Tasks: make(map[string]string)}, nil
	}
	if state, ok := p.stateFiles[path]; ok {
		return state, nil
	}

	state := &stateFile{Tasks: make(map[string]string)}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			p.stateFiles[path] = state
			return state, nil
		}
		return nil, fmt.Errorf("failed to read state file %s: %w", path, err)
	}

	if len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, state); err != nil {
			return nil, fmt.Errorf("failed to parse state file %s: %w", path, err)
		}
	}
	if state.Tasks == nil {
		state.Tasks = make(map[string]string)
	}

	p.stateFiles[path] = state
	return state, nil
}

func (p *Processor) saveStateFile(path string, state *stateFile) error {
	if path == "" || state == nil {
		return nil
	}

	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("failed to create state directory %s: %w", dir, err)
		}
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to encode state file %s: %w", path, err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("failed to write state file %s: %w", path, err)
	}

	return nil
}

func (p *Processor) taskKey(task config.TaskConfig) string {
	return fmt.Sprintf("%s:%s:%s", task.SourceDB, task.TargetDB, task.TableName)
}
