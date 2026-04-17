package database

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"db-ferry/config"
)

// MigrationRecord captures a single migration execution.
type MigrationRecord struct {
	ID               string
	ConfigHash       string
	StartedAt        time.Time
	FinishedAt       *time.Time
	TaskName         string
	SourceDB         string
	TargetDB         string
	Mode             string
	RowsProcessed    int64
	RowsFailed       int64
	ValidationResult string
	ErrorMessage     string
	Version          string
}

// HistoryRecorder writes migration audit records to a target database.
type HistoryRecorder struct {
	dbType    string
	tableName string
	idGen     func() string
}

// NewHistoryRecorder creates a recorder for the given database and table.
func NewHistoryRecorder(dbType, tableName string) *HistoryRecorder {
	return &HistoryRecorder{
		dbType:    dbType,
		tableName: tableName,
		idGen: func() string {
			return fmt.Sprintf("%d", time.Now().UnixNano())
		},
	}
}

// EnsureTable creates the history table if it does not exist.
func (r *HistoryRecorder) EnsureTable(target TargetDB) error {
	sql := r.buildCreateTableSQL()
	return target.Exec(sql)
}

// Start inserts a new migration record and returns its generated ID.
func (r *HistoryRecorder) Start(target TargetDB, rec *MigrationRecord) (string, error) {
	rec.ID = r.idGen()
	rec.StartedAt = time.Now().UTC()
	sql := r.buildInsertSQL(rec)
	if err := target.Exec(sql); err != nil {
		return "", fmt.Errorf("failed to insert history record: %w", err)
	}
	return rec.ID, nil
}

// Finish updates the migration record with results.
func (r *HistoryRecorder) Finish(target TargetDB, id string, processed, failed int64, validationResult, errMsg string) error {
	now := time.Now().UTC()
	sql := r.buildUpdateSQL(id, processed, failed, validationResult, errMsg, now)
	if err := target.Exec(sql); err != nil {
		return fmt.Errorf("failed to update history record: %w", err)
	}
	return nil
}

// List returns the most recent migration records ordered by started_at desc.
func (r *HistoryRecorder) List(target TargetDB, limit int) ([]MigrationRecord, error) {
	if limit <= 0 {
		limit = 10
	}
	q := r.buildListSQL(limit)
	rows, err := target.Query(q)
	if err != nil {
		return nil, fmt.Errorf("failed to query history: %w", err)
	}
	defer rows.Close()

	var out []MigrationRecord
	for rows.Next() {
		var rec MigrationRecord
		var startedAtStr, finishedAtStr sql.NullString
		if err := rows.Scan(
			&rec.ID,
			&rec.ConfigHash,
			&startedAtStr,
			&finishedAtStr,
			&rec.TaskName,
			&rec.SourceDB,
			&rec.TargetDB,
			&rec.Mode,
			&rec.RowsProcessed,
			&rec.RowsFailed,
			&rec.ValidationResult,
			&rec.ErrorMessage,
			&rec.Version,
		); err != nil {
			return nil, fmt.Errorf("failed to scan history row: %w", err)
		}
		if startedAtStr.Valid {
			rec.StartedAt = parseHistoryTime(startedAtStr.String)
		}
		if finishedAtStr.Valid {
			t := parseHistoryTime(finishedAtStr.String)
			rec.FinishedAt = &t
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (r *HistoryRecorder) buildCreateTableSQL() string {
	table := QuoteIdentifier(r.dbType, r.tableName)
	switch strings.ToLower(r.dbType) {
	case config.DatabaseTypePostgreSQL:
		return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id VARCHAR(36) PRIMARY KEY,
			config_hash VARCHAR(64),
			started_at TIMESTAMP,
			finished_at TIMESTAMP,
			task_name VARCHAR(255),
			source_db VARCHAR(255),
			target_db VARCHAR(255),
			mode VARCHAR(50),
			rows_processed BIGINT,
			rows_failed BIGINT,
			validation_result VARCHAR(50),
			error_message TEXT,
			version VARCHAR(50)
		)`, table)
	case config.DatabaseTypeMySQL:
		return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id VARCHAR(36) PRIMARY KEY,
			config_hash VARCHAR(64),
			started_at DATETIME,
			finished_at DATETIME,
			task_name VARCHAR(255),
			source_db VARCHAR(255),
			target_db VARCHAR(255),
			mode VARCHAR(50),
			rows_processed BIGINT,
			rows_failed BIGINT,
			validation_result VARCHAR(50),
			error_message TEXT,
			version VARCHAR(50)
		)`, table)
	case config.DatabaseTypeOracle:
		return fmt.Sprintf(`BEGIN
			EXECUTE IMMEDIATE 'CREATE TABLE %s (
				id VARCHAR2(36) PRIMARY KEY,
				config_hash VARCHAR2(64),
				started_at TIMESTAMP,
				finished_at TIMESTAMP,
				task_name VARCHAR2(255),
				source_db VARCHAR2(255),
				target_db VARCHAR2(255),
				mode VARCHAR2(50),
				rows_processed NUMBER(19,0),
				rows_failed NUMBER(19,0),
				validation_result VARCHAR2(50),
				error_message CLOB,
				version VARCHAR2(50)
			)';
		EXCEPTION
			WHEN OTHERS THEN
				IF SQLCODE != -955 THEN
					RAISE;
				END IF;
		END;`, table)
	case config.DatabaseTypeSQLServer:
		literal := strings.ReplaceAll(table, "'", "''")
		return fmt.Sprintf(`IF OBJECT_ID(N'%s', 'U') IS NULL
		CREATE TABLE %s (
			id NVARCHAR(36) PRIMARY KEY,
			config_hash NVARCHAR(64),
			started_at DATETIME2,
			finished_at DATETIME2,
			task_name NVARCHAR(255),
			source_db NVARCHAR(255),
			target_db NVARCHAR(255),
			mode NVARCHAR(50),
			rows_processed BIGINT,
			rows_failed BIGINT,
			validation_result NVARCHAR(50),
			error_message NVARCHAR(MAX),
			version NVARCHAR(50)
		)`, literal, table)
	case config.DatabaseTypeDuckDB:
		return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id VARCHAR PRIMARY KEY,
			config_hash VARCHAR,
			started_at TIMESTAMP,
			finished_at TIMESTAMP,
			task_name VARCHAR,
			source_db VARCHAR,
			target_db VARCHAR,
			mode VARCHAR,
			rows_processed BIGINT,
			rows_failed BIGINT,
			validation_result VARCHAR,
			error_message VARCHAR,
			version VARCHAR
		)`, table)
	default:
		// SQLite and fallback
		return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id TEXT PRIMARY KEY,
			config_hash TEXT,
			started_at TEXT,
			finished_at TEXT,
			task_name TEXT,
			source_db TEXT,
			target_db TEXT,
			mode TEXT,
			rows_processed INTEGER,
			rows_failed INTEGER,
			validation_result TEXT,
			error_message TEXT,
			version TEXT
		)`, table)
	}
}

func (r *HistoryRecorder) buildInsertSQL(rec *MigrationRecord) string {
	table := QuoteIdentifier(r.dbType, r.tableName)
	started := rec.StartedAt.Format("2006-01-02 15:04:05")
	return fmt.Sprintf(
		"INSERT INTO %s (id, config_hash, started_at, finished_at, task_name, source_db, target_db, mode, rows_processed, rows_failed, validation_result, error_message, version) VALUES (%s, %s, %s, NULL, %s, %s, %s, %s, 0, 0, '', '', %s)",
		table,
		quoteStringLiteral(rec.ID),
		quoteStringLiteral(rec.ConfigHash),
		quoteStringLiteral(started),
		quoteStringLiteral(rec.TaskName),
		quoteStringLiteral(rec.SourceDB),
		quoteStringLiteral(rec.TargetDB),
		quoteStringLiteral(rec.Mode),
		quoteStringLiteral(rec.Version),
	)
}

func (r *HistoryRecorder) buildUpdateSQL(id string, processed, failed int64, validationResult, errMsg string, finished time.Time) string {
	table := QuoteIdentifier(r.dbType, r.tableName)
	finishedStr := finished.Format("2006-01-02 15:04:05")
	return fmt.Sprintf(
		"UPDATE %s SET finished_at = %s, rows_processed = %d, rows_failed = %d, validation_result = %s, error_message = %s WHERE id = %s",
		table,
		quoteStringLiteral(finishedStr),
		processed,
		failed,
		quoteStringLiteral(validationResult),
		quoteStringLiteral(errMsg),
		quoteStringLiteral(id),
	)
}

func (r *HistoryRecorder) buildListSQL(limit int) string {
	table := QuoteIdentifier(r.dbType, r.tableName)
	switch strings.ToLower(r.dbType) {
	case config.DatabaseTypeSQLServer:
		return fmt.Sprintf("SELECT TOP %d id, config_hash, started_at, finished_at, task_name, source_db, target_db, mode, rows_processed, rows_failed, validation_result, error_message, version FROM %s ORDER BY started_at DESC", limit, table)
	case config.DatabaseTypeOracle:
		return fmt.Sprintf("SELECT * FROM (SELECT id, config_hash, started_at, finished_at, task_name, source_db, target_db, mode, rows_processed, rows_failed, validation_result, error_message, version FROM %s ORDER BY started_at DESC) WHERE ROWNUM <= %d", table, limit)
	default:
		return fmt.Sprintf("SELECT id, config_hash, started_at, finished_at, task_name, source_db, target_db, mode, rows_processed, rows_failed, validation_result, error_message, version FROM %s ORDER BY started_at DESC LIMIT %d", table, limit)
	}
}

func parseHistoryTime(s string) time.Time {
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05.999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}
	for _, f := range formats {
		if t, err := time.ParseInLocation(f, s, time.UTC); err == nil {
			return t
		}
	}
	return time.Time{}
}

func quoteStringLiteral(s string) string {
	escaped := strings.ReplaceAll(s, "'", "''")
	return "'" + escaped + "'"
}
