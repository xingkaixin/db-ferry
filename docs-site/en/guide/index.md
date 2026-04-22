# Introduction

db-ferry is a Go command-line utility that ferries data between Oracle, MySQL, PostgreSQL, SQL Server, SQLite, and DuckDB databases using declarative tasks.

The tool automatically creates target schemas, streams data in batches with progress tracking, and supports flexible routing through named database aliases.

## Features

- **Six database engines**: Oracle, MySQL, PostgreSQL, SQL Server, SQLite, and DuckDB
- **Declarative `task.toml`**: Alias-based source/target selection with optional index creation
- **Automatic DDL generation**: Creates target tables based on source column metadata
- **Batch inserts**: Transactional guarantees with efficient memory usage
- **Incremental/resumable migrations**: Via `resume_key` and state file
- **Flexible write modes**: `append` / `replace` / `merge` with row-level validation
- **Dead-letter queue (DLQ)**: Persists failed rows after all retry attempts
- **DAG-based scheduling**: Parallel execution of independent tasks
- **Pre/post SQL hooks**: Run custom SQL before and after task execution
- **Interactive wizard**: `db-ferry config init` with step-by-step prompts
- **PII masking**: 8 built-in anonymization rule types
- **Schema evolution**: Auto `ALTER TABLE ADD COLUMN` in append/merge mode
- **Migration audit table**: Written to target databases for traceability
- **Adaptive batch size**: Dynamic tuning based on latency and memory
- **Column-level mapping**: Transform expressions for ETL-style pipelines
- **Unified TLS/SSL**: Across all database adapters
- **Data comparison**: `diff` command for source-target comparison
- **MCP server**: 5 agent-native tools for AI integration
- **Range-based sharding**: Parallel reads for single-table workloads
- **CDC polling**: Continuous incremental synchronization
- **Built-in cron scheduling**: For daemon mode with timezone support
- **Read replicas & connection pooling**
- **Progress bars**: Per-task with row counts when available

## How It Works

1. Define your databases and tasks in `task.toml`
2. Run `db-ferry`
3. The tool connects to source databases, executes SQL queries, and streams results to targets
4. Target schemas are created automatically
5. Data is inserted in configurable batches with progress tracking
6. Optional indexes are created after data load completes
