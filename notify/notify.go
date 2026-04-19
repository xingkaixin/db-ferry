package notify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"db-ferry/config"
	"db-ferry/processor"
)

// Payload is the JSON structure sent to each webhook URL.
type Payload struct {
	Event   string     `json:"event"`
	Project string     `json:"project"`
	Config  string     `json:"config"`
	Summary Summary    `json:"summary"`
	Tasks   []TaskInfo `json:"tasks"`
}

// Summary captures high-level execution statistics.
type Summary struct {
	TotalTasks int   `json:"total_tasks"`
	Success    int   `json:"success"`
	Failed     int   `json:"failed"`
	DurationMs int64 `json:"duration_ms"`
}

// TaskInfo describes a single task outcome.
type TaskInfo struct {
	Name   string `json:"name"`
	Rows   int    `json:"rows"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// Client sends webhook notifications.
type Client struct {
	cfg    config.NotifyConfig
	client *http.Client
}

// NewClient creates a notification client from configuration.
func NewClient(cfg config.NotifyConfig) *Client {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Client{
		cfg:    cfg,
		client: &http.Client{Timeout: timeout},
	}
}

// Send delivers the notification payload to all URLs configured for the given event.
func (c *Client) Send(event string, configPath string, results []processor.TaskResult, duration time.Duration) error {
	var urls []string
	switch event {
	case "migration.success":
		urls = c.cfg.OnSuccess
	case "migration.failure":
		urls = c.cfg.OnFailure
	default:
		return fmt.Errorf("unknown event type: %s", event)
	}

	if len(urls) == 0 {
		return nil
	}

	payload := c.buildPayload(event, configPath, results, duration)
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	var errs []error
	for _, u := range urls {
		if err := c.sendWithRetry(u, data); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", u, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to send notification to %d/%d URLs: %v", len(errs), len(urls), errs)
	}
	return nil
}

func (c *Client) buildPayload(event, configPath string, results []processor.TaskResult, duration time.Duration) Payload {
	summary := Summary{
		TotalTasks: len(results),
		DurationMs: duration.Milliseconds(),
	}

	tasks := make([]TaskInfo, 0, len(results))
	for _, r := range results {
		if r.Status == "success" {
			summary.Success++
		} else {
			summary.Failed++
		}
		tasks = append(tasks, TaskInfo{
			Name:   r.Name,
			Rows:   r.Rows,
			Status: r.Status,
			Error:  r.Error,
		})
	}

	return Payload{
		Event:   event,
		Project: "db-ferry",
		Config:  configPath,
		Summary: summary,
		Tasks:   tasks,
	}
}

func (c *Client) sendWithRetry(u string, data []byte) error {
	attempts := c.cfg.Retry + 1
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			wait := time.Duration(1<<(attempt-1)) * time.Second
			log.Printf("Retrying webhook %s in %s (attempt %d/%d)", u, wait, attempt+1, attempts)
			time.Sleep(wait)
		}

		lastErr = c.post(u, data)
		if lastErr == nil {
			return nil
		}
	}
	return lastErr
}

func (c *Client) post(u string, data []byte) error {
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}
