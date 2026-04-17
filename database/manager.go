package database

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"

	"db-ferry/config"
)

type ConnectionManager struct {
	cfg               *config.Config
	mu                sync.Mutex
	connections       map[string]*connectionEntry
	sourceConnections map[string]SourceDB
	nextReplicaIndex  map[string]int
}

type connectionEntry struct {
	source SourceDB
	target TargetDB
	close  func() error
}

func NewConnectionManager(cfg *config.Config) *ConnectionManager {
	return &ConnectionManager{
		cfg:               cfg,
		connections:       make(map[string]*connectionEntry),
		sourceConnections: make(map[string]SourceDB),
		nextReplicaIndex:  make(map[string]int),
	}
}

func (m *ConnectionManager) GetSource(alias string) (SourceDB, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if src, ok := m.sourceConnections[alias]; ok {
		return src, nil
	}

	// If a master connection is already cached and there are no replicas configured,
	// return it directly to preserve backward compatibility.
	if entry, ok := m.connections[alias]; ok {
		dbCfg, hasCfg := m.cfg.GetDatabase(alias)
		if !hasCfg || len(dbCfg.Replicas) == 0 {
			if entry.source == nil {
				return nil, fmt.Errorf("database alias '%s' is not configured as a source", alias)
			}
			return entry.source, nil
		}
	}

	dbCfg, ok := m.cfg.GetDatabase(alias)
	if !ok {
		return nil, fmt.Errorf("database alias '%s' not defined", alias)
	}

	if len(dbCfg.Replicas) > 0 {
		replicas := make([]config.ReplicaConfig, len(dbCfg.Replicas))
		copy(replicas, dbCfg.Replicas)
		sort.SliceStable(replicas, func(i, j int) bool {
			return replicas[i].Priority < replicas[j].Priority
		})

		startIdx := 0
		if idx, ok := m.nextReplicaIndex[alias]; ok {
			startIdx = idx % len(replicas)
		}

		for i := 0; i < len(replicas); i++ {
			idx := (startIdx + i) % len(replicas)
			replicaCfg := dbCfg.ResolveReplicaConfig(replicas[idx])
			var src SourceDB
			var err error
			if testOpenSourceHook != nil {
				src, err = testOpenSourceHook(replicaCfg)
			} else {
				src, err = openSourceConnection(replicaCfg)
			}
			if err == nil {
				m.sourceConnections[alias] = src
				m.nextReplicaIndex[alias] = (idx + 1) % len(replicas)
				return src, nil
			}
			log.Printf("Replica connection failed for %s (host=%s): %v", alias, replicaCfg.Host, err)
		}

		if dbCfg.ReplicaFallback {
			IncReplicaFallbackTotal()
			log.Printf("All replicas failed for %s, falling back to master", alias)
		} else {
			return nil, fmt.Errorf("all replicas failed for %s and replica_fallback is disabled", alias)
		}
	}

	entry, err := m.getOrOpenLocked(alias)
	if err != nil {
		return nil, err
	}
	if entry.source == nil {
		return nil, fmt.Errorf("database alias '%s' is not configured as a source", alias)
	}
	return entry.source, nil
}

func (m *ConnectionManager) GetTarget(alias string) (TargetDB, error) {
	entry, err := m.getOrOpen(alias)
	if err != nil {
		return nil, err
	}
	if entry.target == nil {
		return nil, fmt.Errorf("database alias '%s' is not configured as a target", alias)
	}
	return entry.target, nil
}

func (m *ConnectionManager) CloseAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []string
	for alias, entry := range m.connections {
		if entry.close == nil {
			continue
		}
		if err := entry.close(); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", alias, err))
		}
	}
	for alias, src := range m.sourceConnections {
		if err := src.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("%s(replica): %v", alias, err))
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (m *ConnectionManager) getOrOpen(alias string) (*connectionEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getOrOpenLocked(alias)
}

func (m *ConnectionManager) getOrOpenLocked(alias string) (*connectionEntry, error) {
	if entry, ok := m.connections[alias]; ok {
		return entry, nil
	}

	dbCfg, ok := m.cfg.GetDatabase(alias)
	if !ok {
		return nil, fmt.Errorf("database alias '%s' not defined", alias)
	}

	entry, err := m.openConnection(dbCfg)
	if err != nil {
		return nil, err
	}

	m.connections[alias] = entry
	return entry, nil
}

func (m *ConnectionManager) openConnection(dbCfg config.DatabaseConfig) (*connectionEntry, error) {
	return openConnectionInternal(dbCfg)
}

func openConnectionInternal(dbCfg config.DatabaseConfig) (*connectionEntry, error) {
	switch dbCfg.Type {
	case config.DatabaseTypeOracle:
		conn, err := NewOracleDB(BuildOracleDSN(dbCfg), dbCfg.PoolMaxOpen, dbCfg.PoolMaxIdle)
		if err != nil {
			return nil, err
		}
		return &connectionEntry{source: conn, target: conn, close: conn.Close}, nil
	case config.DatabaseTypeMySQL:
		conn, err := NewMySQLDB(BuildMySQLDSN(dbCfg), dbCfg.PoolMaxOpen, dbCfg.PoolMaxIdle)
		if err != nil {
			return nil, err
		}
		return &connectionEntry{source: conn, target: conn, close: conn.Close}, nil
	case config.DatabaseTypeSQLite:
		conn, err := NewSQLiteDB(dbCfg.Path, dbCfg.PoolMaxOpen, dbCfg.PoolMaxIdle)
		if err != nil {
			return nil, err
		}
		return &connectionEntry{source: conn, target: conn, close: conn.Close}, nil
	case config.DatabaseTypeDuckDB:
		conn, err := NewDuckDB(dbCfg.Path, dbCfg.PoolMaxOpen, dbCfg.PoolMaxIdle)
		if err != nil {
			return nil, err
		}
		return &connectionEntry{source: conn, target: conn, close: conn.Close}, nil
	case config.DatabaseTypePostgreSQL:
		conn, err := NewPostgresDB(BuildPostgresDSN(dbCfg), dbCfg.PoolMaxOpen, dbCfg.PoolMaxIdle)
		if err != nil {
			return nil, err
		}
		return &connectionEntry{source: conn, target: conn, close: conn.Close}, nil
	case config.DatabaseTypeSQLServer:
		conn, err := NewSQLServerDB(BuildSQLServerDSN(dbCfg), dbCfg.PoolMaxOpen, dbCfg.PoolMaxIdle)
		if err != nil {
			return nil, err
		}
		return &connectionEntry{source: conn, target: conn, close: conn.Close}, nil
	default:
		return nil, fmt.Errorf("unsupported database type '%s'", dbCfg.Type)
	}
}

// OpenSource opens a standalone source connection from a database config.
func OpenSource(dbCfg config.DatabaseConfig) (SourceDB, error) {
	entry, err := openConnectionInternal(dbCfg)
	if err != nil {
		return nil, err
	}
	if entry.source == nil {
		return nil, fmt.Errorf("database type '%s' cannot be used as source", dbCfg.Type)
	}
	return entry.source, nil
}

// OpenTarget opens a standalone target connection from a database config.
func OpenTarget(dbCfg config.DatabaseConfig) (TargetDB, error) {
	entry, err := openConnectionInternal(dbCfg)
	if err != nil {
		return nil, err
	}
	if entry.target == nil {
		return nil, fmt.Errorf("database type '%s' cannot be used as target", dbCfg.Type)
	}
	return entry.target, nil
}

func openSourceConnection(dbCfg config.DatabaseConfig) (SourceDB, error) {
	entry, err := openConnectionInternal(dbCfg)
	if err != nil {
		return nil, err
	}
	return entry.source, nil
}

// testOpenSourceHook is used by tests to mock replica connections.
var testOpenSourceHook func(config.DatabaseConfig) (SourceDB, error)

func BuildOracleDSN(dbCfg config.DatabaseConfig) string {
	return fmt.Sprintf("oracle://%s:%s@%s:%s/%s",
		dbCfg.User,
		dbCfg.Password,
		dbCfg.Host,
		dbCfg.Port,
		dbCfg.Service,
	)
}

func BuildMySQLDSN(dbCfg config.DatabaseConfig) string {
	params := "parseTime=true"
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?%s",
		dbCfg.User,
		dbCfg.Password,
		dbCfg.Host,
		dbCfg.Port,
		dbCfg.Database,
		params,
	)
}

func BuildPostgresDSN(dbCfg config.DatabaseConfig) string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s",
		dbCfg.Host,
		dbCfg.Port,
		dbCfg.User,
		dbCfg.Password,
		dbCfg.Database,
	)
}

func BuildSQLServerDSN(dbCfg config.DatabaseConfig) string {
	return fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s",
		dbCfg.User,
		dbCfg.Password,
		dbCfg.Host,
		dbCfg.Port,
		dbCfg.Database,
	)
}
