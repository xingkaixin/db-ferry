# db-ferry 配置文件格式详细参考

## 数据库类型字段表

### Oracle

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| name | string | 是 | — | 数据库别名（唯一） |
| type | string | 是 | — | `"oracle"` |
| host | string | 是 | — | 数据库服务器地址 |
| port | string | 否 | `"1521"` | 监听端口 |
| service | string | 是 | — | Oracle 服务名（如 ORCLPDB1） |
| user | string | 是 | — | 用户名 |
| password | string | 是 | — | 密码 |

### MySQL

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| name | string | 是 | — | 数据库别名（唯一） |
| type | string | 是 | — | `"mysql"` |
| host | string | 是 | — | 数据库服务器地址 |
| port | string | 否 | `"3306"` | 监听端口 |
| database | string | 是 | — | 数据库名称 |
| user | string | 是 | — | 用户名 |
| password | string | 是 | — | 密码 |

### PostgreSQL

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| name | string | 是 | — | 数据库别名（唯一） |
| type | string | 是 | — | `"postgresql"` |
| host | string | 是 | — | 数据库服务器地址 |
| port | string | 否 | `"5432"` | 监听端口 |
| database | string | 是 | — | 数据库名称 |
| user | string | 是 | — | 用户名 |
| password | string | 是 | — | 密码 |

### SQL Server

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| name | string | 是 | — | 数据库别名（唯一） |
| type | string | 是 | — | `"sqlserver"` |
| host | string | 是 | — | 数据库服务器地址 |
| port | string | 否 | `"1433"` | 监听端口 |
| database | string | 是 | — | 数据库名称 |
| user | string | 是 | — | 用户名 |
| password | string | 是 | — | 密码 |

### SQLite

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| name | string | 是 | — | 数据库别名（唯一） |
| type | string | 是 | — | `"sqlite"` |
| path | string | 是 | — | 数据库文件路径（相对/绝对） |

### DuckDB

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| name | string | 是 | — | 数据库别名（唯一） |
| type | string | 是 | — | `"duckdb"` |
| path | string | 是 | — | 数据库文件路径，支持 `:memory:` |

> 注意：DuckDB 依赖 CGO，构建时需 `CGO_ENABLED=1`。

### 数据库通用可选字段

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| pool_max_open | int | 否 | — | 连接池最大打开连接数 |
| pool_max_idle | int | 否 | — | 连接池最大空闲连接数 |
| replica_fallback | bool | 否 | false | 副本不可用时回退到主库 |
| ssl_mode | string | 否 | `"disable"` | TLS 模式: disable/require/verify-ca/verify-full |
| ssl_cert | string | 否 | — | 客户端证书文件路径 |
| ssl_key | string | 否 | — | 客户端私钥文件路径 |
| ssl_root_cert | string | 否 | — | CA 根证书文件路径 |
| encryption_key | string | 否 | — | 文件型数据库加密密钥（如 SQLCipher） |

#### 读副本配置

`[[databases.replicas]]` 紧跟在 `[[databases]]` 之后：

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| host | string | 是 | — | 副本服务器地址 |
| port | string | 否 | 继承主库 | 副本端口 |
| priority | int | 否 | 0 | 优先级，数值越小越优先 |

## 任务配置字段

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| table_name | string | 是 | — | 目标表名 |
| sql | string | 是 | — | 源库 SQL 查询语句 |
| source_db | string | 是 | — | 引用 `[[databases]]` 中的 name |
| target_db | string | 是 | — | 引用 `[[databases]]` 中的 name |
| ignore | bool | 否 | false | true 则跳过此任务 |
| mode | string | 否 | `"replace"` | 写入模式: replace/append/merge/upsert |
| batch_size | int | 否 | 1000 | 每批插入行数，0 表示无限制 |
| max_retries | int | 否 | 0 | 批量插入失败重试次数 |
| validate | string | 否 | `"none"` | 迁移后校验: none/row_count/checksum/sample |
| merge_keys | []string | 条件必填 | — | merge/upsert 的匹配键（mode=merge 时必填） |
| resume_key | string | 否 | — | 增量续传的字段名 |
| resume_from | string | 否 | — | 增量起点 SQL 字面量（不含该值） |
| state_file | string | 否 | — | 断点状态文件路径（JSON 格式） |
| allow_same_table | bool | 否 | false | 允许 source_db == target_db |
| skip_create_table | bool | 否 | false | 跳过目标表 DROP/CREATE |
| pre_sql | string | 否 | — | 任务执行前在目标库执行的自定义 SQL |
| post_sql | string | 否 | — | 任务执行后在目标库执行的自定义 SQL |
| dlq_path | string | 否 | — | 死信队列文件路径，用于隔离插入失败的行 |
| dlq_format | string | 否 | `"jsonl"` | 死信队列格式: jsonl/csv |
| depends_on | []string | 否 | — | 任务依赖，按 table_name 声明，支持 DAG 调度 |
| schema_evolution | bool | 否 | false | append/merge 模式下自动为目标表添加源端新增列 |
| validate_sample_size | int | 否 | — | validate=sample 时的采样行数（必须 >0） |

## 索引配置字段

`[[tasks.indexes]]` 紧跟在 `[[tasks]]` 之后，关联到当前任务的 table_name。

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| name | string | 是 | 索引名称（全局唯一，不能重复） |
| columns | []string | 是 | 索引列列表 |
| unique | bool | 否 | 是否唯一索引，默认 false |
| where | string | 否 | 部分索引 WHERE 条件（仅 SQLite 目标支持） |

columns 格式支持排序指定：
- `"column_name"` 或 `"column_name:ASC"` / `"column_name:1"` → 升序（默认）
- `"column_name:DESC"` / `"-1"` → 降序

## 列映射配置字段

`[[tasks.columns]]` 紧跟在 `[[tasks]]` 之后：

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| source | string | 是 | 源列名 |
| target | string | 是 | 目标列名 |
| transform | string | 否 | 转换表达式（如 `UPPER(source_col)`），由目标库执行 |

## 脱敏规则配置字段

`[[tasks.masking]]` 紧跟在 `[[tasks]]` 之后：

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| column | string | 是 | 要脱敏的列名 |
| rule | string | 是 | 规则类型: phone_cn / phone_us / email / id_card_cn / name_cn / random_numeric / random_date / fixed_value / hash |
| range | []float64 | 条件必填 | random_numeric 规则需填 [min, max] |
| value | string | 条件必填 | fixed_value 规则需填固定值 |

## 自适应批量配置字段

`[tasks.adaptive_batch]` 配置动态 batch-size 调优：

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| enabled | bool | 是 | 是否启用 |
| min_size | int | 是 | 最小 batch 大小（必须 >0） |
| max_size | int | 是 | 最大 batch 大小（必须 >= min_size） |
| target_latency_ms | int | 是 | 目标延迟毫秒（必须 >0） |
| memory_limit_mb | int | 是 | 内存限制 MB（必须 >0） |

## 分片配置字段

`[tasks.shard]` 配置单表范围分片并行读取：

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| enabled | bool | 是 | 是否启用分片 |
| shards | int | 是 | 分片数量（必须 >1） |

注意：分片需配合 `resume_key` 使用，仅支持 append/merge 模式，不支持 `state_file`。

## 迁移审计配置字段

全局 `[history]` 控制目标库迁移审计：

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| enabled | bool | 否 | false | 是否启用审计 |
| table_name | string | 否 | `"db_ferry_migrations"` | 审计表名 |

## 配置验证规则速查

| 规则 | 说明 |
|------|------|
| 至少一个 `[[databases]]` | 不能没有数据库定义 |
| 至少一个 `[[tasks]]` | 不能没有迁移任务 |
| 数据库 name 唯一 | 不同数据库不能同名 |
| 数据库 name 非空 | 不能空字符串 |
| 数据库 type 必须是六种之一 | oracle/mysql/postgresql/sqlserver/sqlite/duckdb |
| 各类型必填字段必须存在 | 见上方字段表 |
| task.table_name 非空 | 每个任务必须有目标表名 |
| task.sql 非空 | 每个任务必须有查询 SQL |
| task.source_db 存在 | 必须引用已定义的数据库 |
| task.target_db 存在 | 必须引用已定义的数据库 |
| 同库需 allow_same_table=true | source == target 时的防护 |
| mode 只能是四种之一 | replace/append/merge/upsert |
| merge 模式必须有 merge_keys | mode=merge 时 merge_keys 不能为空 |
| 非 merge 模式不能有 merge_keys | merge_keys 仅对 merge 模式有效 |
| state_file 需搭配 resume_key | 有 state_file 就必须有 resume_key |
| resume_key 需搭配 state_file 或 resume_from | 有 resume_key 就必须有其中之一 |
| batch_size >= 0 | 不能为负数 |
| max_retries >= 0 | 不能为负数 |
| 验证模式只能是 none/row_count/checksum/sample | validate 字段值有限制 |
| dlq_format 只能是 jsonl 或 csv | 默认为 jsonl |
| 索引 name 全局唯一 | 跨表也不能重复 |
| 索引 columns 不能空 | 至少一列 |
| 索引列格式正确 | ASC/DESC 排序器必须合法 |
| 部分索引仅限 SQLite 目标 | where 字段仅在 target_db 为 sqlite 时可用 |
| adaptive_batch 参数必须完整 | enabled 时 min_size/max_size/target_latency_ms/memory_limit_mb 均需 >0 |
| shard 需 resume_key 且 shards>1 | 分片必须配置 resume_key，且 shards 必须大于 1 |
| shard 不支持 replace 模式 | 分片仅支持 append/merge 模式 |
| shard 不支持 state_file | 分片与断点续传互斥 |
| masking rule 必须是内置类型 | rule 只能是 9 种内置规则之一 |
| random_numeric 需 2 个 range 值 | rule=random_numeric 时 range 必须恰好为 [min, max] |
| fixed_value 需 value 字段 | rule=fixed_value 时 value 不能为空 |
| columns source/target 必填 | 列映射必须提供源列和目标列 |
| columns target 不能重复 | 同一任务的列映射目标列不能重复 |
| ssl_mode 必须是四种之一 | disable / require / verify-ca / verify-full |

## 数据类型映射

工具在运行时自动分析源查询结果的 Go scan type，并在目标数据库创建对应类型。以下是主要映射关系：

| 源数据类别 | SQLite 目标类型 | 说明 |
|-----------|---------------|------|
| 整数 (int64, int32 等) | INTEGER | 64 位整数存储 |
| 浮点数 (float64, float32) | REAL | 双精度浮点 |
| 字符串 (string) | TEXT | 文本存储 |
| 时间 (time.Time) | TEXT | ISO 格式文本 |
| 二进制 ([]byte) | BLOB | 二进制大对象 |
| 布尔 (bool) | INTEGER | 0/1 存储 |
| NULL | TEXT | 允许空值 |

> DuckDB 的类型映射与 SQLite 类似。对于 MySQL/PostgreSQL/SQL Server/Oracle 作为目标，工具会根据列元数据自动推断合适的类型。
