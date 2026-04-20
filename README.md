# Multi-Database Migration Tool

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

 A Go command-line utility that ferries data between Oracle, MySQL, PostgreSQL, SQL Server, SQLite, and DuckDB databases using declarative tasks. The tool automatically creates target schemas, streams data in batches with progress tracking, and supports flexible routing through named database aliases.

 ## Features

 - Connects to Oracle via `github.com/sijms/go-ora/v2`, MySQL via `github.com/go-sql-driver/mysql`, PostgreSQL via `github.com/lib/pq`, SQL Server via `github.com/denisenkom/go-mssqldb`, SQLite via `github.com/mattn/go-sqlite3`, and DuckDB via `github.com/duckdb/duckdb-go/v2`
- Declarative `task.toml` with alias-based source/target selection and optional index creation
- Automatic table DDL generation based on source column metadata
- Batch inserts with transactional guarantees and efficient memory usage
- Incremental/resumable migrations via resume key and state file
- Task-level write modes (append/replace/merge), batch size, retries, and row-level validation (row_count / checksum / sample)
- Dead-letter queue (DLQ) for persisting failed rows after all retry attempts
- DAG-based scheduling for parallel execution of independent tasks
- Task-level `pre_sql` / `post_sql` hooks for running custom SQL before and after execution
- Interactive configuration wizard (`db-ferry config init`) with step-by-step prompts
- PII masking and anonymization rules with 8 built-in rule types
- Schema evolution (auto `ALTER TABLE ADD COLUMN`) in append/merge mode
- Migration audit table written to target databases for traceability
- Adaptive batch size dynamic tuning based on latency and memory
- Column-level mapping and transform expressions for ETL-style pipelines
- Unified TLS/SSL support across all database adapters
- `diff` command for source-target data comparison
- MCP server with 5 agent-native tools for AI integration
- Range-based sharding for single-table parallel reads (append/merge mode)
- CDC polling mode for continuous incremental synchronization with cursor-based filtering
- Read replica and connection pool configuration
- Progress bars for each task, including row counts when available

 ## Installation

 ### Install from npm

 `db-ferry` can be installed globally or executed directly with `npx`:

 ```bash
 npm install -g db-ferry
 db-ferry -version

 npx db-ferry -version
 ```

 Supported npm binary packages:

 | Platform | Arch | npm package | Notes |
 |----------|------|-------------|-------|
 | Linux | x64 | `db-ferry-linux-x64` | Included via main `db-ferry` package |
 | Linux | arm64 | `db-ferry-linux-arm64` | Included via main `db-ferry` package |
 | macOS | x64 | `db-ferry-darwin-x64` | Included via main `db-ferry` package |
 | macOS | arm64 | `db-ferry-darwin-arm64` | Included via main `db-ferry` package |
| Windows | x64 | `db-ferry-windows-x64` | Included via main `db-ferry` package; renamed from `db-ferry-win32-x64` to avoid npm spam detection |

 > Windows arm64 npm binaries are not published yet. DuckDB remains unsupported on Windows builds.

 ### Build from source

 1. Clone the repository:

    ```bash
    git clone <repository-url>
    cd db-ferry
    ```

 2. Install dependencies:

    ```bash
    go mod tidy
    ```

 3. Build the application:

    ```bash
    go build -o db-ferry
    ```

    > DuckDB support relies on CGO. Ensure `CGO_ENABLED=1` and the default C toolchain (clang on macOS, gcc/clang on Linux) are available when building binaries that include DuckDB aliases.

 ## Configuration (`task.toml`)

 All runtime configuration is stored in a single TOML file. Define every database once, then reference the aliases from individual tasks.

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

 [[databases]]
 name = "duckdb_local"
 type = "duckdb"
 path = "./data/local.duckdb"

 [[tasks]]
 table_name = "employees"
 sql = "SELECT employee_id, first_name, last_name, department_id FROM employees"
 source_db = "oracle_hr"
 target_db = "sqlite_local"
 ignore = false

 [[tasks.indexes]]
 name = "idx_employees_name"
 columns = ["last_name:ASC", "first_name:ASC"]

 [[tasks]]
 table_name = "department_snapshot"
 sql = "SELECT * FROM departments"
 source_db = "oracle_hr"
 target_db = "mysql_dw"
 ignore = false
 ```

 ### Database definitions

 - `type`: `oracle`, `mysql`, `postgresql`, `sqlserver`, `sqlite`, or `duckdb`
 - Oracle requires host, port, credentials, and service; MySQL/PostgreSQL/SQL Server require host, port, credentials, and database
 - SQLite and DuckDB only require a file `path` (relative or absolute, `:memory:` works for DuckDB)
 - Connection pool: `pool_max_open`, `pool_max_idle` tune `sql.DB` settings
 - Read replicas: `[[databases.replicas]]` with `host` and `priority`; set `replica_fallback = true` to fall back to the master
 - TLS/SSL: `ssl_mode` (`disable`/`require`/`verify-ca`/`verify-full`), plus `ssl_cert`, `ssl_key`, `ssl_root_cert` as needed

 ### Task definitions

 - `sql`: executed against the `source_db`
 - `source_db` / `target_db`: aliases declared in the `[[databases]]` section
 - `ignore`: skip execution without removing the task
- `mode`: `replace` (default), `append`, or `merge` (`upsert` is accepted)
- `batch_size`: number of rows per insert batch (default: 1000)
- `max_retries`: retry count for failed batch inserts (default: 0)
- `validate`: `row_count` (compare inserted rows vs target table count), `checksum` (hash-based row comparison), or `sample` (random sampling validation); skipped for merge mode
- `merge_keys`: columns used to match rows for merge/upsert (requires unique constraint on target)
- `resume_key`: column used for incremental/resume filtering
 - `resume_from`: SQL literal for the resume filter (exclusive)
 - `state_file`: JSON file to persist the last resume value per task
 - `allow_same_table`: allow migrations where `source_db` equals `target_db` (acknowledges table drop risk)
 - `skip_create_table`: skip dropping/creating the target table (use when the table already exists)
 - `pre_sql`: custom SQL to execute against the target database before the task begins
 - `post_sql`: custom SQL to execute against the target database after the task completes
 - `dlq_path`: dead-letter queue file path; failed rows are written here instead of failing the entire task
 - `dlq_format`: DLQ output format, `jsonl` (default) or `csv`
 - `depends_on`: task dependencies declared by `table_name`; enables DAG-based scheduling
 - `schema_evolution`: in append/merge mode, automatically run `ALTER TABLE ADD COLUMN` when the source introduces new columns
 - `columns`: column-level mapping with optional transform expressions (`source` -> `target`, with `transform`)
 - `masking`: PII masking rules per column (`column`, `rule`, optional `range`/`value`)
 - `adaptive_batch`: dynamic batch-size tuning (`enabled`, `min_size`, `max_size`, `target_latency_ms`, `memory_limit_mb`)
 - `shard`: range-based parallel sharding for single-table reads (`enabled`, `shards`); requires `resume_key`, only in append/merge mode
 - `cdc`: continuous incremental sync via polling (`enabled`, `cursor_column`, `poll_interval`, `initial_cursor`, `delete_detection`); requires `mode = append/merge`, `state_file`, and `resume_key` (auto-set to `cursor_column`); not supported with federated or shard tasks
 - `validate_sample_size`: number of rows to sample when `validate = "sample"`
 - `[[tasks.indexes]]`: optional index creation statements applied after data load (partial indexes via `where` are supported on SQLite targets)

 ### History configuration

 Global `[history]` section controls migration audit logging to the target database:

 ```toml
 [history]
 enabled = true
 table_name = "db_ferry_migrations"
 ```

 - `enabled`: write an audit record per task to the target database (table auto-created if missing)
 - `table_name`: override the default audit table name

 ## Usage

 ```bash
 # Generate task.toml interactively (wizard) or from the built-in sample
 db-ferry config init

 # Run with default task.toml
 db-ferry

 # Specify an alternate configuration file
 db-ferry -config ./configs/task.toml

 # Enable verbose logging
 db-ferry -v

 # Show version information
 db-ferry -version

 # Compare source and target data for a task
 db-ferry diff -task employees

 # Start MCP server for AI agent integration
 db-ferry mcp serve
 ```

 ### Command line options

 - `config init`: Interactive configuration wizard that creates `task.toml` in the current directory; walks through engine selection, connection details, and table choices. Falls back to the built-in sample if non-interactive. Fails if the file already exists
 - `diff`: Compare source and target data for a given task. Flags: `-task` (required), `-keys`, `-where`, `-limit`, `-output`, `-format` (json/csv/html)
 - `mcp serve`: Start an MCP server with 5 agent-native tools for AI integration
 - `-config`: Path to the TOML configuration file (default: `task.toml`)
 - `-v`: Enable verbose logging with file/line prefixes
 - `-version`: Print build version and exit

 ## Development Commands

 `db-ferry` provides a `justfile` with common Go quality checks:

 ```bash
 # list all recipes
 just

 # format all go files
 just fmt

 # check formatting
 just fmt-check

 # run lint checks (golangci-lint)
 just lint

 # run tests
 just test

 # run coverage gate (global >=80%, each package >=70%)
 just test-cover

 # build all packages
 just build

 # run full local quality gate: fmt-check + lint + test-cover
 just check
 ```

 ## Release

- Tag pushes matching `v*` trigger multi-platform binary builds and npm publishing
- npm publishing expects a repository secret named `NPM_TOKEN`
- Published package layout uses one public main package (`db-ferry`) plus per-platform binary packages via `optionalDependencies`
- npm publish steps skip package versions that already exist, so a fixed workflow can rerun the same tag release to backfill only the missing packages

 Coverage rules:
 - Global coverage must be `>= 80%`
 - Each package coverage must be `>= 70%`
 - Any package without test files is treated as failure in `test-cover`

 ## Data type mapping (high level)

 | Source Type (Oracle/MySQL/PostgreSQL/SQL Server) | Target Mapping |
 |---------------------------|----------------|
 | NUMBER / DECIMAL          | INTEGER or REAL (precision-aware) |
 | VARCHAR / CHAR / TEXT     | TEXT |
 | DATE / DATETIME / TIMESTAMP | TEXT (ISO 8601) |
 | BLOB / RAW / BINARY       | BLOB |

 The processor inspects driver metadata to determine precision, scale, length, and nullability before creating the target table.

 ## Project structure

 ```
 db-ferry/
 ├── main.go                 # CLI entry point
 ├── go.mod                  # Go module definition
 ├── task.toml.sample        # Sample configuration with database/task templates
 ├── config/
 │   └── config.go           # Configuration loading & validation
 ├── database/
 │   ├── interface.go        # Shared interfaces & metadata structs
 │   ├── manager.go          # Connection registry for aliased DBs
 │   ├── mysql.go            # MySQL source/target implementation
 │   ├── oracle.go           # Oracle source/target implementation
 │   ├── duckdb.go           # DuckDB source/target implementation
 │   ├── postgres.go         # PostgreSQL source/target implementation
 │   ├── sqlserver.go        # SQL Server source/target implementation
 │   └── sqlite.go           # SQLite source/target implementation
 ├── processor/
 │   └── processor.go        # Task execution engine
 └── utils/
     └── progress.go         # Progress bar utilities
 ```
