---
name: db-ferry
description: 数据库迁移配置生成与执行指导。当用户提到数据库迁移、数据搬运、db-ferry、或需要在不同数据库之间移动数据时触发。帮助创建 task.toml 配置文件、生成正确的迁移命令、提示数据丢失风险。
---

# db-ferry 数据迁移 Skill

## 触发场景

- 用户提及 "数据库迁移"、"数据搬运"、"db-ferry"
- 需要在不同数据库之间移动/同步数据
- 用户提供了一个 `task.toml` 文件需要修改或排查
- 询问如何将某个数据库的数据迁移到另一个数据库

## 快速概览

db-ferry 是一个声明式 CLI 工具（Go 编写），通过 TOML 配置文件定义迁移任务，支持 Oracle/MySQL/PostgreSQL/SQL Server/SQLite/DuckDB 之间的数据搬运。工具自动创建目标表结构、批量插入、可选创建索引、支持进度条显示。独立任务按 DAG 自动并行执行。0.9.0 新增 Prometheus/OTLP 指标导出、数据质量断言、守护进程模式、Lua/JS 行级插件、S3/GCS DLQ、跨库内存 JOIN、Webhook 通知、SSE 实时进度流等能力。

## 配置文件生成流程

### Step 1: 理解用户需求

确认以下信息：
- **源数据库**：类型、连接信息、需要迁移的表/SQL 查询
- **目标数据库**：类型、连接信息、目标表名
- **迁移范围**：全表/部分字段/条件过滤
- **写入模式**：替换(默认)/追加/合并(upsert)
- **是否有索引需求**

### Step 2: 生成 `[[databases]]` 配置

每个数据库需要一个唯一 `name` 作为别名。各类型必填字段如下：

| 类型 | 必填字段 | 选填字段 |
|------|----------|----------|
| `oracle` | name, type, host, service, user, password | port (默认1521) |
| `mysql` | name, type, host, database, user, password | port (默认3306) |
| `postgresql` | name, type, host, database, user, password | port (默认5432) |
| `sqlserver` | name, type, host, database, user, password | port (默认1433) |
| `sqlite` | name, type, path | — |
| `duckdb` | name, type, path | 支持 `:memory:` |

注意：SQLite 和 DuckDB 只需要 `path`，不需要 host/port/user/password。

连接池与副本（可选）：

| 字段 | 说明 |
|------|------|
| `pool_max_open` | 连接池最大打开连接数 |
| `pool_max_idle` | 连接池最大空闲连接数 |
| `replica_fallback` | 副本不可用时是否回退到主库 |
| `[[databases.replicas]]` | 只读副本，含 `host`（可选 `port`）和 `priority` |

TLS/SSL（可选）：

| 字段 | 说明 |
|------|------|
| `ssl_mode` | `disable` / `require` / `verify-ca` / `verify-full` |
| `ssl_cert` | 客户端证书路径 |
| `ssl_key` | 客户端私钥路径 |
| `ssl_root_cert` | CA 根证书路径 |

### Step 3: 生成 `[[tasks]]` 配置

每个任务必填字段：

| 字段 | 说明 |
|------|------|
| `table_name` | 目标表名（迁移前会自动 drop/create） |
| `sql` | 源库 SQL 查询语句 |
| `source_db` | 引用 `[[databases]]` 中的别名 |
| `target_db` | 引用 `[[databases]]` 中的别名 |

可选字段：

| 字段 | 说明 | 默认值 |
|------|------|--------|
| `ignore` | 设为 true 跳过此任务 | false |
| `mode` | 写入模式: replace/append/merge/upsert | replace |
| `batch_size` | 每批插入行数 | 1000 |
| `max_retries` | 批量插入失败重试次数 | 0 |
| `validate` | 迁移后校验: none/row_count/checksum/sample | none |
| `merge_keys` | merge/upsert 模式的匹配键（必填） | — |
| `resume_key` | 增量续传的字段名 | — |
| `resume_from` | 增量起点 SQL 字面量 | — |
| `state_file` | 断点状态文件路径（JSON） | — |
| `allow_same_table` | 允许源库=目标库时执行（⚠️ 风险） | false |
| `skip_create_table` | 跳过目标表 drop/create | false |
| `pre_sql` | 任务执行前在目标库执行的自定义 SQL | — |
| `post_sql` | 任务执行后在目标库执行的自定义 SQL | — |
| `dlq_path` | 死信队列输出路径，隔离失败行；支持本地路径、S3（s3://）、GCS（gs://） | — |
| `dlq_format` | 死信队列格式: jsonl/csv | jsonl |
| `depends_on` | 任务依赖（按 `table_name` 声明），支持 DAG 调度 | — |
| `schema_evolution` | append/merge 模式下自动 ALTER TABLE ADD COLUMN | false |
| `columns` | 列级映射与转换（`source`→`target`，可选 `transform`） | — |
| `masking` | PII 脱敏规则（`column`、`rule`，可选 `range`/`value`） | — |
| `adaptive_batch` | 自适应批量调优（`enabled`、`min_size`、`max_size`、`target_latency_ms`、`memory_limit_mb`） | — |
| `shard` | 范围分片并行读取（`enabled`、`shards`），需 `resume_key`，仅 append/merge | — |
| `validate_sample_size` | `validate=sample` 时的采样行数 | — |
| `[[tasks.sources]]` / `[tasks.join]` | 跨库内存 JOIN；每个 source 需 `alias`、`db`、`sql`；join 需 `keys` 和 `type`（inner/left/right） | — |
| `[[tasks.assertions]]` | 数据质量断言（`column`/`columns`、`rule`、`on_fail`）；规则：not_null/range/in_set/unique/regex/min_length/max_length | — |
| `[tasks.plugin]` | 行级转换插件（`engine`: lua/javascript、`script`、可选 `timeout_ms`） | — |

### Step 4: 配置索引（可选）

在 `[[tasks]]` 下添加 `[[tasks.indexes]]`：

| 字段 | 说明 |
|------|------|
| `name` | 索引名称（全局唯一） |
| `columns` | 列列表，支持排序: `["col:ASC", "col2:DESC"]` |
| `unique` | 是否唯一索引 |
| `where` | 部分索引条件（仅 SQLite 目标支持） |

### Step 5: 全局配置（Metrics / Notify / Schedule）

在根级别添加可选全局段：

**Metrics 导出** `[metrics]`：
- `enabled`: 是否启用指标收集
- `listen_addr`: Prometheus pull 端点地址，如 `:9090`
- `endpoint`: OTLP HTTP push 端点，如 `http://localhost:4318/v1/metrics`
- `interval`: push 间隔，默认 `30s`

**Webhook 通知** `[notify]`：
- `on_success`: 迁移成功时调用的 Webhook URL 列表
- `on_failure`: 迁移失败时调用的 Webhook URL 列表
- `timeout`: 单次请求超时
- `retry`: 失败重试次数

**Schedule 定时调度** `[schedule]`（daemon 模式生效）：
- `cron`: cron 表达式或描述符，如 `@every 1h`
- `timezone`: IANA 时区名
- `retry_on_failure` / `max_retry`: 失败重试
- `missed_catchup`: 错过调度时立即补执行
- `start_at` / `end_at`: 执行窗口边界

### Step 6: 风险检查清单

生成配置后，务必确认以下风险点：

- **同库迁移**：`source_db` == `target_db` 时必须设置 `allow_same_table = true`，否则配置验证会报错
- **replace 模式**：会先 DROP 目标表再重建，**会丢失目标表原有数据**
- **密码安全**：`task.toml` 明文存储密码，建议 `chmod 600 task.toml`，不要提交到版本控制
- **merge_keys 一致性**：mode=merge 时 `merge_keys` 必填，且应对应目标表的唯一约束
- **增量续传**：`state_file` 需配合 `resume_key`，`resume_key` 需配合 `state_file` 或 `resume_from`
- **校验模式**：`validate` 支持 `row_count`（行数对比）、`checksum`（哈希行级对比）、`sample`（随机采样校验）；`merge` 模式下校验自动跳过
- **SQL 钩子**：`pre_sql` 在目标表创建后、数据插入前执行；`post_sql` 在数据插入完成后执行，可用于创建物化视图、刷新统计信息等
- **PII 脱敏**：`masking` 在数据写入目标前对列值进行脱敏，支持 phone_cn、email、id_card_cn、hash 等 8 种规则
- **列映射转换**：`columns` 可重命名列并应用 transform 表达式（如 `UPPER(source_col)`），注意 transform 由目标库执行
- **自适应批量**：`adaptive_batch` 根据延迟和内存动态调整 batch_size，启用后 task 的 `batch_size` 作为初始值
- **分片并行**：`shard` 将单表按 resume_key 范围拆分为多片并行读取，仅支持 append/merge 模式，不支持 state_file
- **Schema 演进**：`schema_evolution` 在 append/merge 模式下检测到源端新增列时自动 ALTER TABLE ADD COLUMN
- **迁移审计**：`history.enabled` 会在目标库自动创建审计表记录每次迁移
- **Diff 对比**：`db-ferry diff` 需任务已执行过且目标表存在，默认输出 JSON 格式差异
- **数据质量断言**：`assertions` 在数据写入目标前对行/列进行规则校验，`on_fail=abort` 会终止任务，`warn` 仅记录，`dlq` 将失败行写入死信队列
- **行级插件**：`plugin` 在每行数据写入目标前执行 Lua/JavaScript 脚本转换，注意脚本执行性能开销
- **跨库 JOIN**：`sources` + `join` 将多个数据库数据在内存中 JOIN 后写入目标，不支持断点续传、分片或 CDC
- **SSE 实时进度**：启动时传入 `-sse-port :8080` 可在 `/events` 订阅实时任务进度，`/status` 查看当前状态

## CLI 命令参考

| 命令 | 说明 |
|------|------|
| `db-ferry` | 使用当前目录 `task.toml` 执行迁移 |
| `db-ferry -config <path>` | 指定配置文件路径 |
| `db-ferry -v` | 详细日志输出（调试用） |
| `db-ferry config init` | 交互式配置向导，引导选择引擎、连接、表后生成 `task.toml`（非交互环境回退到内置样例；文件已存在则报错） |
| `db-ferry diff -task <name>` | 对比指定任务的源库与目标库数据，支持 `-keys`、`-where`、`-limit`、`-output`、`-format` |
| `db-ferry mcp serve` | 启动 MCP 服务器，提供 5 个 AI 原生工具 |
| `db-ferry -version` | 查看版本号 |
| `db-ferry -sse-port :8080` | 启动 SSE 服务器，实时推送任务进度到 `/events`，状态查询 `/status` |

## 写入模式说明

| 模式 | 行为 | 风险 |
|------|------|------|
| `replace`（默认） | DROP 目标表 → 重建 → 插入 | 目标表数据完全丢失 |
| `append` | 直接 INSERT 追加数据 | 目标表已存在则报错（除非 `skip_create_table=true`） |
| `merge` / `upsert` | 按 `merge_keys` 匹配，存在则更新，不存在则插入 | 目标表需有 `merge_keys` 对应的唯一约束 |

注意：`upsert` 是 `merge` 的别名，内部会自动统一为 `merge`。

## 增量续传配置

使用 `resume_key` + `state_file` 实现断点续传：

```toml
[[tasks]]
table_name = "orders"
sql = "SELECT order_id, order_date, total_amount FROM orders"
source_db = "prod"
target_db = "analysis"
mode = "append"
resume_key = "order_id"
state_file = "./state/orders.json"
validate = "row_count"
```

规则：
- `state_file` 必须同时搭配 `resume_key`
- `resume_key` 必须搭配 `state_file` 或 `resume_from`
- SQL 中应保证 `resume_key` 字段单调递增
- `resume_from` 为 SQL 字面量，表示从哪个值之后开始迁移（不包含该值）

## 常见错误与规避

| 错误 | 原因 | 解决 |
|------|------|------|
| `database definition: name is required` | `[[databases]]` 缺少 name 字段 | 为每个数据库添加唯一 name |
| `duplicate database name` | 数据库别名重复 | 修改为不同的 name |
| `source_db and target_db are both 'xxx'; set allow_same_table = true` | 同库迁移未授权 | 添加 `allow_same_table = true` |
| `mode must be "replace", "append", "merge", or "upsert"` | mode 值拼写错误 | 检查拼写，确认为四种之一 |
| `merge_keys is required when mode is "merge"` | merge 模式缺少匹配键 | 添加 `merge_keys = ["primary_key"]` |
| `state_file requires resume_key` | 配了 state_file 但没配 resume_key | 添加对应的 resume_key |
| `resume_key requires resume_from or state_file` | 配了 resume_key 但没配 state_file 或 resume_from | 添加 state_file 或 resume_from |
| `partial indexes (where clause) are only supported for SQLite targets` | 非 SQLite 目标使用了 where | 仅 SQLite 目标可使用部分索引 |
| `at least one database must be defined` | 配置文件没有 `[[databases]]` 段 | 添加数据库定义 |
| `index name 'xxx' already defined` | 索引名重复 | 修改为唯一的索引名 |
| `unsupported rule 'xxx'` | 脱敏规则不存在 | 检查 rule 是否为内置 8 种之一 |
| `shard requires resume_key` | 分片未配置 resume_key | 添加 resume_key 并确保 mode 为 append/merge |
| `adaptive_batch.min_size must be > 0` | 自适应批量参数缺失 | 补全 min_size、max_size、target_latency_ms、memory_limit_mb |
| `duplicate masking column` / `duplicate target column` | masking 或 columns 中列重复 | 确保每列只出现一次 |
| `plugin.engine must be "lua" or "javascript"` | 插件引擎不支持 | 检查 engine 是否为 lua 或 javascript |
| `plugin.script is required` | 启用插件但未提供脚本 | 添加 script 字段 |
| `unsupported rule 'xxx'` | 断言规则不存在 | 检查 rule 是否为内置 7 种之一 |
| `federated task requires at least 2 sources` | 跨库 JOIN 源表不足 | 确保 sources 至少定义 2 个 |
| `join.keys is required for federated tasks` | 跨库 JOIN 未指定连接键 | 添加 join.keys |
| `resume_key is not supported in federated mode` | 联邦任务不支持断点续传 | 移除 resume_key 和 state_file |
| `invalid URL` | notify 中的 Webhook URL 格式错误 | 检查 on_success/on_failure URL 格式 |

## 参考文件

详细的字段说明、验证规则和数据类型映射请参考：`skills/db-ferry/references/config_reference.md`

更多示例请参考：`task.toml.sample`、`docs/user_guide.md`
