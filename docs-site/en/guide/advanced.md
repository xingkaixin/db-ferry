# Advanced Features

## Incremental / Resumable Migrations

Use `resume_key` with `state_file` for incremental migrations:

```toml
[[tasks]]
table_name = "orders"
sql = "SELECT order_id, order_date, total_amount FROM orders"
source_db = "production"
target_db = "warehouse"
mode = "append"
resume_key = "order_id"
state_file = "./state/orders.json"
validate = "row_count"
```

- First run performs a full migration and writes the `state_file`
- Subsequent runs automatically resume from the last `resume_key` value
- Ensure the SQL orders by `resume_key` monotonically

## Merge / Upsert

When you need "update if exists, insert if not":

```toml
[[tasks]]
table_name = "customers"
sql = "SELECT customer_id, customer_name, email FROM customers"
source_db = "production"
target_db = "warehouse"
mode = "merge"
merge_keys = ["customer_id"]
```

- Target table must have a unique constraint on `merge_keys`
- Merge avoids dropping and recreating the table, preserving existing data

## CDC Polling Mode

For continuous incremental synchronization:

```toml
[[tasks]]
table_name = "events"
sql = "SELECT id, event_type, payload, created_at FROM events WHERE created_at > \{\{.LastValue\}\}"
source_db = "production"
target_db = "analytics"
mode = "append"
state_file = "./state/events.json"

[tasks.cdc]
enabled = true
cursor_column = "created_at"
poll_interval = "5m"
initial_cursor = "2024-01-01"
```

- `mode` must be `append` or `merge`
- `cursor_column` is automatically used as `resume_key`
- SQL uses `\{\{.LastValue\}\}` template variable for cursor filtering
- First run performs a full sync, then enters polling loop
- Send SIGINT/SIGTERM for graceful shutdown
- Not supported with federated queries or shard tasks

## Range-Based Sharding

Parallel reads for single-table workloads:

```toml
[[tasks]]
table_name = "large_table"
sql = "SELECT * FROM large_table"
source_db = "production"
target_db = "warehouse"
mode = "append"
resume_key = "id"

[tasks.shard]
enabled = true
shards = 4
```

- Requires `resume_key` and only works in `append` or `merge` mode
- Splits the table into ranges and processes them in parallel

## Cron Scheduling

Automated execution in daemon mode:

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

- `cron` supports standard 5-field expressions and descriptors like `@daily`
- `timezone` uses IANA names
- Schedule only active in daemon mode (e.g., via `-watch`)
- Config file changes trigger automatic schedule reload in watch mode

## PII Masking

Anonymize sensitive data during migration:

```toml
[[tasks]]
table_name = "users"
sql = "SELECT * FROM users"
source_db = "production"
target_db = "analytics"

[[tasks.masking]]
column = "email"
rule = "email_hash"

[[tasks.masking]]
column = "phone"
rule = "phone_mask"
range = [0, 3]
```

## Column-Level Mapping

ETL-style column transforms:

```toml
[[tasks]]
table_name = "orders"
sql = "SELECT * FROM orders"
source_db = "production"
target_db = "warehouse"

[[tasks.columns]]
source = "unit_price"
target = "price_cents"
transform = "unit_price * 100"
```

## Tips

- **Test with a small dataset first**: Use `LIMIT` in your SQL to validate the configuration before running a full migration
- **Split large tables**: For tables with millions of rows, split by date or ID ranges into multiple tasks
- **Use views for complex queries**: Create a view in the source database and migrate from the view
- **Monitor progress**: Watch the progress bars and log output during migration
- **Verify results**: Use `validate` options or the `diff` command after migration
- **Backup target databases**: Always back up important data before running migrations
