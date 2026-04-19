package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"db-ferry/config"
	"db-ferry/database"
	"db-ferry/processor"

	"github.com/fsnotify/fsnotify"
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

func (d *Daemon) hashConfig(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
