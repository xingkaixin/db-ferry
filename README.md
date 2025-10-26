# Oracle to SQLite Migration Tool

A Go command-line tool that transfers data from Oracle 11g databases to SQLite, with progress tracking and batch processing capabilities.

## Features

- Connects to Oracle 11g using `github.com/sijms/go-ora/v2`
- Creates SQLite databases using `github.com/mattn/go-sqlite3`
- Configurable tasks via TOML files
- Environment variable based configuration
- Real-time progress bars for data transfer
- Automatic table creation based on query results
- Batch processing for memory efficiency
- Transaction-based data integrity

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd cbd_data_go
```

2. Install dependencies:
```bash
go mod tidy
```

3. Build the application:
```bash
go build -o oracle-to-sqlite
```

## Configuration

### 1. Environment Variables (.env)

Copy `.env.example` to `.env` and configure your Oracle connection:

```env
# Oracle Database Configuration
ORACLE_HOST=localhost
ORACLE_PORT=1521
ORACLE_SERVICE=ORCL
ORACLE_USER=your_username
ORACLE_PASSWORD=your_password

# SQLite Database Configuration
SQLITE_DB_PATH=./data.db
```

### 2. Task Configuration (task.toml)

Copy `task.toml.example` to `task.toml` and define your migration tasks:

```toml
[[tasks]]
table_name = "employees"
sql = "SELECT employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, department_id FROM employees WHERE department_id = 10"
ignore = false

[[tasks]]
table_name = "departments"
sql = "SELECT department_id, department_name, manager_id, location_id FROM departments"
ignore = false
```

## Usage

### Basic Usage

```bash
# Run with default configuration files (.env and task.toml)
./oracle-to-sqlite

# Specify custom configuration files
./oracle-to-sqlite -env custom.env -config custom.toml

# Enable verbose logging
./oracle-to-sqlite -v

# Show version information
./oracle-to-sqlite -version
```

### Command Line Options

- `-env`: Path to environment file (default: `.env`)
- `-config`: Path to TOML configuration file (default: `task.toml`)
- `-v`: Enable verbose logging
- `-version`: Show version information

## Data Type Mapping

The tool automatically maps Oracle data types to SQLite:

| Oracle Type | SQLite Type |
|-------------|-------------|
| VARCHAR2, CHAR, CLOB | TEXT |
| NUMBER | INTEGER or REAL (based on precision) |
| DATE, TIMESTAMP | TEXT (ISO format) |
| BLOB | BLOB |

## Features in Detail

### Progress Tracking

Each task shows a real-time progress bar with:
- Row count and processing speed
- Elapsed and remaining time
- Task completion percentage

### Error Handling

- Comprehensive error reporting with context
- Transaction rollback on failures
- Graceful handling of NULL values
- Type conversion safety

### Performance

- Batch processing (1000 rows per batch)
- Memory-efficient row handling
- Connection pooling for Oracle
- Transaction-based SQLite operations

## Project Structure

```
cbd_data_go/
├── main.go                 # CLI entry point
├── go.mod                  # Go module file
├── .env.example           # Example environment configuration
├── task.toml.example      # Example task configuration
├── config/
│   └── config.go          # Configuration management
├── database/
│   ├── oracle.go          # Oracle connection
│   └── sqlite.go          # SQLite operations
├── processor/
│   └── processor.go       # Main data processing logic
└── utils/
    └── progress.go        # Progress bar utilities
```

## Dependencies

- `github.com/sijms/go-ora/v2` - Oracle database driver
- `github.com/mattn/go-sqlite3` - SQLite database driver
- `github.com/joho/godotenv` - Environment variable loading
- `github.com/BurntSushi/toml` - TOML configuration parsing
- `github.com/schollz/progressbar/v3` - Progress bar display

## License

[Add your license information here]