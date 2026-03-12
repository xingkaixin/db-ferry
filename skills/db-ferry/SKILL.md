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

db-ferry 是一个声明式 CLI 工具（Go 编写），通过 TOML 配置文件定义迁移任务，支持 Oracle/MySQL/PostgreSQL/SQL Server/SQLite/DuckDB 之间的数据搬运。工具自动创建目标表结构、批量插入、可选创建索引、支持进度条显示。

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
| `validate` | 迁移后校验: none/row_count | none |
| `merge_keys` | merge/upsert 模式的匹配键（必填） | — |
| `resume_key` | 增量续传的字段名 | — |
| `resume_from` | 增量起点 SQL 字面量 | — |
| `state_file` | 断点状态文件路径（JSON） | — |
| `allow_same_table` | 允许源库=目标库时执行（⚠️ 风险） | false |
| `skip_create_table` | 跳过目标表 drop/create | false |

### Step 4: 配置索引（可选）

在 `[[tasks]]` 下添加 `[[tasks.indexes]]`：

| 字段 | 说明 |
|------|------|
| `name` | 索引名称（全局唯一） |
| `columns` | 列列表，支持排序: `["col:ASC", "col2:DESC"]` |
| `unique` | 是否唯一索引 |
| `where` | 部分索引条件（仅 SQLite 目标支持） |

### Step 5: 风险检查清单

生成配置后，务必确认以下风险点：

- **同库迁移**：`source_db` == `target_db` 时必须设置 `allow_same_table = true`，否则配置验证会报错
- **replace 模式**：会先 DROP 目标表再重建，**会丢失目标表原有数据**
- **密码安全**：`task.toml` 明文存储密码，建议 `chmod 600 task.toml`，不要提交到版本控制
- **merge_keys 一致性**：mode=merge 时 `merge_keys` 必填，且应对应目标表的唯一约束
- **增量续传**：`state_file` 需配合 `resume_key`，`resume_key` 需配合 `state_file` 或 `resume_from`

## CLI 命令参考

| 命令 | 说明 |
|------|------|
| `db-ferry` | 使用当前目录 `task.toml` 执行迁移 |
| `db-ferry -config <path>` | 指定配置文件路径 |
| `db-ferry -v` | 详细日志输出（调试用） |
| `db-ferry config init` | 在当前目录生成示例 `task.toml`（文件已存在则报错） |
| `db-ferry -version` | 查看版本号 |

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

## 参考文件

详细的字段说明、验证规则和数据类型映射请参考：`skills/db-ferry/references/config_reference.md`

更多示例请参考：`task.toml.sample`、`docs/user_guide.md`
