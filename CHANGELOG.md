# Changelog

## [0.8.0] - 2026-04-20
- Added built-in cron scheduling for daemon mode with timezone, retry, missed-catchup, and execution window support
- Added CDC polling mode for continuous incremental synchronization with cursor-based filtering and graceful shutdown
- Added PII masking and anonymization rules with 8 built-in rule types
- Added schema evolution (auto `ALTER TABLE ADD COLUMN`) in append/merge mode
- Added migration audit table to target databases for traceability
- Added adaptive batch size dynamic tuning based on latency and memory
- Added read replica and connection pool configuration for databases
- Added column-level mapping and transform expressions for ETL-style pipelines
- Added unified TLS/SSL support across all database adapters
- Added `diff` command for source-target data comparison
- Added MCP server with 5 agent-native tools for AI integration
- Added range-based sharding for single-table parallel reads (append/merge mode)
- Fixed ineffectual `batchSize` assignment in processor final batch
- Fixed ineffectual `resumeIndex` assignment in `processShardedTask`

## [0.7.0] - 2026-04-17
- Added DAG-based scheduling for parallel execution of independent tasks
- Added dead-letter queue (DLQ) for failed batch inserts, capturing rows that fail after all retry attempts
- Added task-level `pre_sql`/`post_sql` hooks to run custom SQL before and after a task executes
- Added interactive configuration wizard (`db-ferry config init`) with step-by-step prompts for engine, connection, and table setup
- Added SQLite as a source database engine
- Added SQL Server as a source database engine
- Added row-level validation: `checksum` and `sample` validation modes alongside the existing `row_count` validator
- Added a project landing page (dark theme, i18n, Cloudflare Web Analytics, AI Skills integration)
- Fixed processor regressions introduced during rebase operations
- Fixed `MapToDuckDBType` visibility on Windows builds
- Fixed dry-run output to include index DDL when `skip_create_table` is enabled

## [0.6.0] - 2026-03-11
- Added npm binary distribution scaffolding: a main `db-ferry` package plus platform packages named `db-ferry-{os}-{arch}`, supporting both `npm install -g db-ferry` and `npx db-ferry`
- Updated the build workflow to produce per-platform binaries and publish npm packages automatically on tag releases
- Renamed the Windows npm binary package to `db-ferry-windows-x64` to avoid npm spam detection, and made npm publishing resumable for reruns of the same tag
- Fixed npm platform package publishing to pass explicit local directory targets such as `./npm/db-ferry-windows-x64`, avoiding npm misparsing them as Git package specs like `github.com/npm/...`
- Changed `-version` to print an injected build version so Git tags, Go binaries, and npm package versions share one source of truth
- Added a standalone GitHub Actions test workflow `test.yml` for PRs and non-tag pushes, running `just fmt-check`, `golangci-lint-action`, and `scripts/coverage-check.sh`
- Added `db-ferry config init` to generate the built-in `task.toml` sample in the current directory and fail if the target file already exists
- Added a `justfile` with shared development commands: `fmt`, `fmt-check`, `lint`, `test`, `build`, and `check`
- Added a CI quality stage covering `gofmt`, `golangci-lint`, and `go test`
- Added the coverage gate script `scripts/coverage-check.sh` and wired it into `just test-cover` and the CI quality stage
- Added core module tests for `main`, `config`, `database`, `processor`, and `utils`, with fixed coverage thresholds of `>=80%` globally and `>=70%` per package

## [0.5.0] - 2025-12-19
- Added an explicit opt-in flag for same-database migrations and a flag to skip table creation
- Filtered ignored tasks out of progress totals and added an overall task progress bar
- Fixed nil pointer column type scanning and the Oracle row count alias issue
- Unified Oracle identifier quoting to reduce reserved-word and special-character risks
- Updated the usage documentation and sample configuration

## [0.4.0] - 2025-11-16
- Added DuckDB support
- Improved the cross-platform build workflow

## [0.3.0] - 2025-11-14
- Added a complete user guide for non-technical users

## [0.2.0] - 2025-10-26
- Introduced alias-based configuration and multi-database routing
- Improved cross-platform build support

## [0.1.0] - 2025-10-26
- Initial data migration tool release
