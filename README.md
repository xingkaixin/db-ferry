# Multi-Database Migration Tool

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

 A Go command-line utility that ferries data between Oracle, MySQL, SQLite, and DuckDB databases using declarative tasks. The tool automatically creates target schemas, streams data in batches with progress tracking, and supports flexible routing through named database aliases.

 ## Features

 - Connects to Oracle via `github.com/sijms/go-ora/v2`, MySQL via `github.com/go-sql-driver/mysql`, SQLite via `github.com/mattn/go-sqlite3`, and DuckDB via `github.com/duckdb/duckdb-go/v2`
 - Declarative `task.toml` with alias-based source/target selection and optional index creation
 - Automatic table DDL generation based on source column metadata
 - Batch inserts with transactional guarantees and efficient memory usage
 - Incremental/resumable migrations via resume key and state file
 - Task-level write modes (append/replace), batch size, retries, and row-count validation
 - Progress bars for each task, including row counts when available

 ## Installation

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

 - `type`: `oracle`, `mysql`, `sqlite`, or `duckdb`
 - Oracle/MySQL require host, port, credentials, and service/database identifiers
 - SQLite and DuckDB only require a file `path` (relative or absolute, `:memory:` works for DuckDB)

 ### Task definitions

 - `sql`: executed against the `source_db`
 - `source_db` / `target_db`: aliases declared in the `[[databases]]` section
 - `ignore`: skip execution without removing the task
 - `mode`: `replace` (default) or `append`
 - `batch_size`: number of rows per insert batch (default: 1000)
 - `max_retries`: retry count for failed batch inserts (default: 0)
 - `validate`: `row_count` to compare inserted rows vs target table count
 - `resume_key`: column used for incremental/resume filtering
 - `resume_from`: SQL literal for the resume filter (exclusive)
 - `state_file`: JSON file to persist the last resume value per task
 - `allow_same_table`: allow migrations where `source_db` equals `target_db` (acknowledges table drop risk)
 - `skip_create_table`: skip dropping/creating the target table (use when the table already exists)
 - `[[tasks.indexes]]`: optional index creation statements applied after data load (partial indexes via `where` are supported on SQLite targets)

 ## Usage

 ```bash
 # Run with default task.toml
 ./db-ferry

 # Specify an alternate configuration file
 ./db-ferry -config ./configs/task.toml

 # Enable verbose logging
 ./db-ferry -v

 # Show version information
 ./db-ferry -version
 ```

 ### Command line options

 - `-config`: Path to the TOML configuration file (default: `task.toml`)
 - `-v`: Enable verbose logging with file/line prefixes
 - `-version`: Print build version and exit

 ## Data type mapping (high level)

 | Source Type (Oracle/MySQL) | Target Mapping |
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
 │   └── sqlite.go           # SQLite source/target implementation
 ├── processor/
 │   └── processor.go        # Task execution engine
 └── utils/
     └── progress.go         # Progress bar utilities
 ```
