# Configuration (`task.toml`)

All runtime configuration is stored in a single TOML file. Define every database once, then reference the aliases from individual tasks.

## Basic Example

```toml
[[databases]]
name = "oracle_hr"
type = "oracle"
host = "db.example.com"
port = "1521"
service = "ORCLPDB1"
user = "hr"
password = "secret"

[[databases]]
name = "mysql_dw"
type = "mysql"
host = "mysql.internal"
port = "3306"
database = "warehouse"
user = "dw_writer"
password = "secret"

[[databases]]
name = "sqlite_local"
type = "sqlite"
path = "./data/output.db"

[[tasks]]
table_name = "employees"
sql = "SELECT employee_id, first_name, last_name, department_id FROM employees"
source_db = "oracle_hr"
target_db = "sqlite_local"
ignore = false

[[tasks.indexes]]
name = "idx_employees_name"
columns = ["last_name:ASC", "first_name:ASC"]
```

## Database Definitions

- `type`: `oracle`, `mysql`, `postgresql`, `sqlserver`, `sqlite`, or `duckdb`
- Oracle requires `host`, `port`, credentials, and `service`
- MySQL/PostgreSQL/SQL Server require `host`, `port`, credentials, and `database`
- SQLite and DuckDB only require a file `path` (relative or absolute; `:memory:` works for DuckDB)
- Connection pool: `pool_max_open`, `pool_max_idle` tune `sql.DB` settings
- Read replicas: `[[databases.replicas]]` with `host` and `priority`; set `replica_fallback = true` to fall back to the master
- TLS/SSL: `ssl_mode` (`disable`/`require`/`verify-ca`/`verify-full`), plus `ssl_cert`, `ssl_key`, `ssl_root_cert` as needed

## Task Definitions

| Field | Description |
|-------|-------------|
| `sql` | Executed against the `source_db` |
| `source_db` / `target_db` | Aliases declared in the `[[databases]]` section |
| `ignore` | Skip execution without removing the task |
| `mode` | `replace` (default), `append`, or `merge` (`upsert` is accepted) |
| `batch_size` | Number of rows per insert batch (default: 1000) |
| `max_retries` | Retry count for failed batch inserts (default: 0) |
| `validate` | `row_count`, `checksum`, or `sample`; skipped for merge mode |
| `merge_keys` | Columns used to match rows for merge/upsert (requires unique constraint on target) |
| `resume_key` | Column used for incremental/resume filtering |
| `resume_from` | SQL literal for the resume filter (exclusive) |
| `state_file` | JSON file to persist the last resume value per task |
| `allow_same_table` | Allow migrations where `source_db` equals `target_db` |
| `skip_create_table` | Skip dropping/creating the target table |
| `pre_sql` | Custom SQL to execute against target before the task begins |
| `post_sql` | Custom SQL to execute against target after the task completes |
| `dlq_path` | Dead-letter queue file path for failed rows |
| `dlq_format` | DLQ output format: `jsonl` (default) or `csv` |
| `depends_on` | Task dependencies declared by `table_name`; enables DAG-based scheduling |
| `schema_evolution` | In append/merge mode, auto `ALTER TABLE ADD COLUMN` when source introduces new columns |
| `columns` | Column-level mapping with optional transform expressions (`source` → `target`, with `transform`) |
| `masking` | PII masking rules per column (`column`, `rule`, optional `range`/`value`) |
| `adaptive_batch` | Dynamic batch-size tuning (`enabled`, `min_size`, `max_size`, `target_latency_ms`, `memory_limit_mb`) |
| `shard` | Range-based parallel sharding (`enabled`, `shards`); requires `resume_key`, only in append/merge mode |
| `cdc` | Continuous incremental sync via polling (`enabled`, `cursor_column`, `poll_interval`, `initial_cursor`, `delete_detection`); requires `mode = append/merge`, `state_file`, and `resume_key` |
| `validate_sample_size` | Number of rows to sample when `validate = "sample"` |
| `[[tasks.indexes]]` | Optional index creation statements applied after data load |

## History Configuration

Global `[history]` section controls migration audit logging:

```toml
[history]
enabled = true
table_name = "db_ferry_migrations"
```

- `enabled`: Write an audit record per task to the target database
- `table_name`: Override the default audit table name

## Schedule Configuration

Global `[schedule]` section enables cron-based execution in daemon mode:

```toml
[schedule]
cron = "0 2 * * *"
timezone = "Asia/Shanghai"
retry_on_failure = true
max_retry = 3
missed_catchup = true
start_at = "2026-01-01T00:00:00"
end_at = "2026-12-31T23:59:59"
```

- `cron`: Standard 5-field expression or descriptor such as `@every 1h`
- `timezone`: IANA timezone name (e.g., `America/New_York`); defaults to system local time
- `retry_on_failure`: Retries failed rounds up to `max_retry` times with a fixed 1-minute interval
- `max_retry`: Maximum retry attempts (must be >= 0)
- `missed_catchup`: Executes immediately on startup if the last scheduled run was missed
- `start_at` / `end_at`: Optional execution window boundaries
