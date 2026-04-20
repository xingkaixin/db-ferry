package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"db-ferry/config"
	"db-ferry/database"
	"db-ferry/processor"

	"github.com/fsnotify/fsnotify"
	"github.com/robfig/cron/v3"
)

// Daemon runs db-ferry in persistent mode, optionally watching for config changes.
type Daemon struct {
	configPath   string
	healthAddr   string
	watchEnabled bool
	version      string

	mu       sync.Mutex
	cancel   context.CancelFunc
	cfgHash  string
	running  bool
	stopCh   chan struct{}
	stopOnce sync.Once
	lastErr  error
	cron     *cron.Cron
}

// Options configures the daemon.
type Options struct {
	ConfigPath   string
	HealthAddr   string
	WatchEnabled bool
	Version      string
}

// New creates a new Daemon.
func New(opts Options) *Daemon {
	return &Daemon{
		configPath:   opts.ConfigPath,
		healthAddr:   opts.HealthAddr,
		watchEnabled: opts.WatchEnabled,
		version:      opts.Version,
		stopCh:       make(chan struct{}),
	}
}

// Run starts the daemon and blocks until Stop is called.
func (d *Daemon) Run() error {
	d.mu.Lock()
	d.running = true
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		d.running = false
		d.mu.Unlock()
	}()

	cfg, err := config.LoadConfig(d.configPath)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	if cfg.Schedule.Cron != "" {
		return d.runWithSchedule(cfg)
	}

	if d.watchEnabled {
		return d.runWithWatch()
	}
	return d.runOnce()
}

func (d *Daemon) runOnce() error {
	ctx, cancel := context.WithCancel(context.Background())
	d.mu.Lock()
	d.cancel = cancel
	d.mu.Unlock()

	defer cancel()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.stopCh:
		cancel()
		return nil
	default:
		err := d.executeRound(ctx)
		if err != nil {
			d.mu.Lock()
			d.lastErr = err
			d.mu.Unlock()
		}
		return err
	}
}

func (d *Daemon) runWithWatch() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create file watcher: %w", err)
	}
	defer watcher.Close()

	if err := watcher.Add(d.configPath); err != nil {
		return fmt.Errorf("failed to watch config file %s: %w", d.configPath, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	d.mu.Lock()
	d.cancel = cancel
	d.mu.Unlock()

	defer cancel()

	if err := d.executeRound(ctx); err != nil {
		log.Printf("Daemon initial round failed: %v", err)
		d.mu.Lock()
		d.lastErr = err
		d.mu.Unlock()
	}

	debounce := time.NewTimer(0)
	<-debounce.C

	for {
		select {
		case <-d.stopCh:
			cancel()
			return nil

		case event, ok := <-watcher.Events:
			if !ok {
				return fmt.Errorf("watcher event channel closed")
			}
			if !event.Has(fsnotify.Write) && !event.Has(fsnotify.Create) && !event.Has(fsnotify.Rename) {
				continue
			}
			if event.Name != d.configPath {
				continue
			}
			debounce.Reset(500 * time.Millisecond)

		case err, ok := <-watcher.Errors:
			if !ok {
				return fmt.Errorf("watcher error channel closed")
			}
			log.Printf("Watcher error: %v", err)

		case <-debounce.C:
			newHash, err := d.hashConfig(d.configPath)
			if err != nil {
				log.Printf("Failed to hash config after change: %v", err)
				continue
			}

			d.mu.Lock()
			oldHash := d.cfgHash
			d.mu.Unlock()

			if newHash == oldHash {
				continue
			}

			log.Printf("Config change detected (hash changed), reloading...")
			cancel()

			ctx, cancel = context.WithCancel(context.Background())
			d.mu.Lock()
			d.cancel = cancel
			d.mu.Unlock()

			if err := d.executeRound(ctx); err != nil {
				log.Printf("Daemon round failed after reload: %v", err)
				d.mu.Lock()
				d.lastErr = err
				d.mu.Unlock()
			}

			_ = watcher.Remove(d.configPath)
			if err := watcher.Add(d.configPath); err != nil {
				log.Printf("Failed to re-watch config file: %v", err)
			}
		}
	}
}

func (d *Daemon) executeRound(ctx context.Context) error {
	cfg, err := config.LoadConfig(d.configPath)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	hash, err := d.hashConfig(d.configPath)
	if err != nil {
		return fmt.Errorf("failed to hash config: %w", err)
	}
	d.mu.Lock()
	d.cfgHash = hash
	d.mu.Unlock()

	log.Printf("[daemon] Starting migration round with %d tasks", len(cfg.Tasks))

	manager := database.NewConnectionManager(cfg)
	proc := processor.NewProcessorWithVersion(manager, cfg, d.version)

	err = proc.ProcessAllTasksContext(ctx)

	if closeErr := proc.Close(); closeErr != nil {
		log.Printf("[daemon] Warning: failed to close resources: %v", closeErr)
	}

	if err != nil {
		if ctx.Err() != nil {
			log.Printf("[daemon] Migration round cancelled")
			return ctx.Err()
		}
		return fmt.Errorf("migration round failed: %w", err)
	}

	log.Printf("[daemon] Migration round completed successfully")
	return nil
}

// Stop signals the daemon to shut down gracefully.
func (d *Daemon) Stop() {
	d.stopOnce.Do(func() {
		close(d.stopCh)
	})

	d.mu.Lock()
	if d.cancel != nil {
		d.cancel()
	}
	if d.cron != nil {
		ctx := d.cron.Stop()
		<-ctx.Done()
	}
	d.mu.Unlock()
}

// IsRunning reports whether the daemon is currently running.
func (d *Daemon) IsRunning() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.running
}

// LastError returns the error from the most recent failed round.
func (d *Daemon) LastError() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastErr
}

func (d *Daemon) runWithSchedule(cfg *config.Config) error {
	loc := time.Local
	if cfg.Schedule.Timezone != "" {
		var err error
		loc, err = time.LoadLocation(cfg.Schedule.Timezone)
		if err != nil {
			return fmt.Errorf("failed to load timezone: %w", err)
		}
	}

	if cfg.Schedule.MissedCatchup {
		if err := d.handleMissedCatchup(cfg, loc); err != nil {
			log.Printf("[schedule] Missed catchup failed: %v", err)
		}
	}

	c := cron.New(cron.WithLocation(loc))
	job := &scheduledJob{d: d, cfg: cfg, loc: loc}
	if _, err := c.AddJob(cfg.Schedule.Cron, job); err != nil {
		return fmt.Errorf("failed to add cron job: %w", err)
	}
	c.Start()

	d.mu.Lock()
	d.cron = c
	d.mu.Unlock()

	if d.watchEnabled {
		go d.watchConfigForSchedule(loc)
	}

	<-d.stopCh

	ctx := c.Stop()
	<-ctx.Done()
	return nil
}

// scheduleRetryDelay controls the fixed interval between retry attempts in schedule mode.
// It is exposed as a package-level variable so tests can override it.
var scheduleRetryDelay = 1 * time.Minute

type scheduledJob struct {
	d   *Daemon
	cfg *config.Config
	loc *time.Location
}

func (j *scheduledJob) Run() {
	now := time.Now().In(j.loc)
	if j.cfg.Schedule.StartAt != "" {
		startAt, err := parseDaemonScheduleTime(j.cfg.Schedule.StartAt)
		if err == nil && now.Before(startAt) {
			log.Printf("[schedule] Current time %s before start_at %s, skipping", now.Format(time.RFC3339), startAt.Format(time.RFC3339))
			return
		}
	}
	if j.cfg.Schedule.EndAt != "" {
		endAt, err := parseDaemonScheduleTime(j.cfg.Schedule.EndAt)
		if err == nil && now.After(endAt) {
			log.Printf("[schedule] Current time %s after end_at %s, skipping", now.Format(time.RFC3339), endAt.Format(time.RFC3339))
			return
		}
	}

	logDir := "logs"
	_ = os.MkdirAll(logDir, 0o755)
	logFileName := filepath.Join(logDir, now.Format("2006-01-02")+".log")
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Printf("[schedule] Failed to open log file %s: %v", logFileName, err)
	} else {
		oldOutput := log.Writer()
		log.SetOutput(logFile)
		defer func() {
			log.SetOutput(oldOutput)
			_ = logFile.Close()
		}()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-j.d.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	var execErr error
	maxRetry := j.cfg.Schedule.MaxRetry
	for attempt := 0; attempt <= maxRetry; attempt++ {
		if attempt > 0 {
			log.Printf("[schedule] Retry attempt %d/%d after failure", attempt, maxRetry)
			select {
			case <-time.After(scheduleRetryDelay):
			case <-j.d.stopCh:
				return
			}
		}
		execErr = j.d.executeRound(ctx)
		if execErr == nil {
			break
		}
		log.Printf("[schedule] Migration round failed: %v", execErr)
		j.d.mu.Lock()
		j.d.lastErr = execErr
		j.d.mu.Unlock()
		if !j.cfg.Schedule.RetryOnFailure || attempt == maxRetry {
			break
		}
	}

	if j.cfg.Schedule.MissedCatchup {
		if err := j.d.recordLastRun(j.d.scheduleStatePath(), time.Now()); err != nil {
			log.Printf("[schedule] Failed to record last run: %v", err)
		}
	}
}

func (d *Daemon) scheduleStatePath() string {
	dir := filepath.Dir(d.configPath)
	return filepath.Join(dir, ".db-ferry-schedule-state.json")
}

type scheduleState struct {
	LastRun time.Time `json:"last_run"`
}

func (d *Daemon) loadLastRun(path string) (time.Time, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return time.Time{}, nil
		}
		return time.Time{}, err
	}
	var state scheduleState
	if err := json.Unmarshal(data, &state); err != nil {
		return time.Time{}, err
	}
	return state.LastRun, nil
}

func (d *Daemon) recordLastRun(path string, t time.Time) error {
	state := scheduleState{LastRun: t}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func (d *Daemon) handleMissedCatchup(cfg *config.Config, loc *time.Location) error {
	path := d.scheduleStatePath()
	lastRun, err := d.loadLastRun(path)
	if err != nil {
		return fmt.Errorf("failed to load last run: %w", err)
	}
	if lastRun.IsZero() {
		return nil
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	schedule, err := parser.Parse(cfg.Schedule.Cron)
	if err != nil {
		return fmt.Errorf("failed to parse cron: %w", err)
	}
	next := schedule.Next(lastRun)
	now := time.Now().In(loc)
	if next.Before(now) || next.Equal(now) {
		log.Printf("[schedule] Missed catchup: last run %s, next scheduled %s, now %s", lastRun.Format(time.RFC3339), next.Format(time.RFC3339), now.Format(time.RFC3339))

		logDir := "logs"
		_ = os.MkdirAll(logDir, 0o755)
		logFileName := filepath.Join(logDir, now.Format("2006-01-02")+".log")
		logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			log.Printf("[schedule] Failed to open log file %s: %v", logFileName, err)
		} else {
			oldOutput := log.Writer()
			log.SetOutput(logFile)
			defer func() {
				log.SetOutput(oldOutput)
				_ = logFile.Close()
			}()
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			<-d.stopCh
			cancel()
		}()
		if err := d.executeRound(ctx); err != nil {
			return fmt.Errorf("missed catchup execution failed: %w", err)
		}
		if err := d.recordLastRun(path, time.Now()); err != nil {
			log.Printf("[schedule] Failed to record last run after catchup: %v", err)
		}
	}
	return nil
}

func (d *Daemon) watchConfigForSchedule(currentLoc *time.Location) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("[schedule] Failed to create file watcher: %v", err)
		return
	}
	defer watcher.Close()

	if err := watcher.Add(d.configPath); err != nil {
		log.Printf("[schedule] Failed to watch config file %s: %v", d.configPath, err)
		return
	}

	debounce := time.NewTimer(0)
	<-debounce.C

	for {
		select {
		case <-d.stopCh:
			return
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			if !event.Has(fsnotify.Write) && !event.Has(fsnotify.Create) && !event.Has(fsnotify.Rename) {
				continue
			}
			if event.Name != d.configPath {
				continue
			}
			debounce.Reset(500 * time.Millisecond)
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Printf("[schedule] Watcher error: %v", err)
		case <-debounce.C:
			newHash, err := d.hashConfig(d.configPath)
			if err != nil {
				log.Printf("[schedule] Failed to hash config after change: %v", err)
				continue
			}
			d.mu.Lock()
			oldHash := d.cfgHash
			d.mu.Unlock()
			if newHash == oldHash {
				continue
			}
			log.Printf("[schedule] Config change detected (hash changed), reloading schedule...")

			cfg, err := config.LoadConfig(d.configPath)
			if err != nil {
				log.Printf("[schedule] Failed to reload config: %v", err)
				continue
			}

			loc := currentLoc
			if cfg.Schedule.Timezone != "" {
				newLoc, err := time.LoadLocation(cfg.Schedule.Timezone)
				if err != nil {
					log.Printf("[schedule] Failed to load timezone from new config: %v", err)
					continue
				}
				loc = newLoc
			}

			d.mu.Lock()
			if d.cron != nil {
				ctx := d.cron.Stop()
				<-ctx.Done()
			}
			d.cron = cron.New(cron.WithLocation(loc))
			job := &scheduledJob{d: d, cfg: cfg, loc: loc}
			if _, err := d.cron.AddJob(cfg.Schedule.Cron, job); err != nil {
				log.Printf("[schedule] Failed to add cron job after reload: %v", err)
				d.mu.Unlock()
				continue
			}
			d.cron.Start()
			d.mu.Unlock()

			_ = watcher.Remove(d.configPath)
			if err := watcher.Add(d.configPath); err != nil {
				log.Printf("[schedule] Failed to re-watch config file: %v", err)
			}
		}
	}
}

func parseDaemonScheduleTime(v string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t, nil
	}
	return time.Parse("2006-01-02T15:04:05", v)
}

func (d *Daemon) hashConfig(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
