package database

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"db-ferry/config"
)

type ConnectionManager struct {
	cfg         *config.Config
	mu          sync.Mutex
	connections map[string]*connectionEntry
}

type connectionEntry struct {
	source SourceDB
	target TargetDB
	close  func() error
}

func NewConnectionManager(cfg *config.Config) *ConnectionManager {
	return &ConnectionManager{
		cfg:         cfg,
		connections: make(map[string]*connectionEntry),
	}
}

func (m *ConnectionManager) GetSource(alias string) (SourceDB, error) {
	entry, err := m.getOrOpen(alias)
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

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (m *ConnectionManager) getOrOpen(alias string) (*connectionEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

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
	switch dbCfg.Type {
	case config.DatabaseTypeOracle:
		conn, err := NewOracleDB(buildOracleDSN(dbCfg))
		if err != nil {
			return nil, err
		}
		return &connectionEntry{source: conn, target: conn, close: conn.Close}, nil
	case config.DatabaseTypeMySQL:
		conn, err := NewMySQLDB(buildMySQLDSN(dbCfg))
		if err != nil {
			return nil, err
		}
		return &connectionEntry{source: conn, target: conn, close: conn.Close}, nil
	case config.DatabaseTypeSQLite:
		conn, err := NewSQLiteDB(dbCfg.Path)
		if err != nil {
			return nil, err
		}
		return &connectionEntry{source: conn, target: conn, close: conn.Close}, nil
	case config.DatabaseTypeDuckDB:
		conn, err := NewDuckDB(dbCfg.Path)
		if err != nil {
			return nil, err
		}
		return &connectionEntry{source: conn, target: conn, close: conn.Close}, nil
	default:
		return nil, fmt.Errorf("unsupported database type '%s'", dbCfg.Type)
	}
}

func buildOracleDSN(dbCfg config.DatabaseConfig) string {
	return fmt.Sprintf("oracle://%s:%s@%s:%s/%s",
		dbCfg.User,
		dbCfg.Password,
		dbCfg.Host,
		dbCfg.Port,
		dbCfg.Service,
	)
}

func buildMySQLDSN(dbCfg config.DatabaseConfig) string {
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
