# 配置参考

## 数据库定义

### 示例1：连接 MySQL 数据库

```toml
[[databases]]
name = "我的MySQL数据库"
host = "192.168.1.100"
type = "mysql"
port = "3306"
database = "business_db"
user = "dbuser"
password = "your_password"
```

### 示例2：连接 Oracle 数据库

```toml
[[databases]]
name = "公司Oracle数据库"
type = "oracle"
host = "oracle.company.com"
port = "1521"
service = "ORCLPDB1"
user = "hr"
password = "hr_password"
```

### 示例3：SQLite 数据库（本地文件）

```toml
[[databases]]
name = "本地数据文件"
type = "sqlite"
path = "./mydata.db"
```

### 示例4：同时定义多个数据库

```toml
[[databases]]
name = "生产数据库"
type = "mysql"
host = "prod.company.com"
port = "3306"
database = "production"
user = "readonly"
password = "readonly_pass"

[[databases]]
name = "本地分析库"
type = "sqlite"
path = "./analysis_data.db"

[[databases]]
name = "测试数据库"
type = "mysql"
host = "test.company.com"
port = "3306"
database = "test_db"
user = "test_user"
password = "test_pass"
```

## 任务定义

### 简单全表迁移

```toml
[[tasks]]
table_name = "员工表"
sql = "SELECT * FROM employees"
source_db = "生产数据库"
target_db = "本地分析库"
ignore = false
```

### 只迁移部分字段

```toml
[[tasks]]
table_name = "员工基本信息"
sql = """
SELECT
    employee_id,
    first_name,
    last_name,
    email,
    department_id
FROM employees
"""
source_db = "生产数据库"
target_db = "本地分析库"
ignore = false
```

### 带条件过滤

```toml
[[tasks]]
table_name = "销售数据_2024"
sql = """
SELECT
    order_id,
    customer_name,
    order_date,
    total_amount
FROM sales_orders
WHERE order_date >= '2024-01-01'
    AND total_amount > 1000
"""
source_db = "生产数据库"
target_db = "本地分析库"
ignore = false
```

### 复杂业务查询

```toml
[[tasks]]
table_name = "完整订单信息"
sql = """
SELECT
    o.order_id,
    o.order_date,
    c.customer_name,
    p.product_name,
    o.quantity,
    o.unit_price,
    (o.quantity * o.unit_price) as total_price
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
WHERE o.order_status = '已完成'
"""
source_db = "生产数据库"
target_db = "本地分析库"
ignore = false
```

## 索引配置

### 单列索引

```toml
[[tasks]]
table_name = "员工表"
sql = "SELECT * FROM employees"
source_db = "生产数据库"
target_db = "本地分析库"
ignore = false

[[tasks.indexes]]
name = "idx_员工编号"
columns = ["employee_id"]
unique = false
```

### 多列组合索引

```toml
[[tasks.indexes]]
name = "idx_部门员工姓名"
columns = ["department_id", "last_name", "first_name"]
unique = false
```

### 带排序的索引

```toml
[[tasks.indexes]]
name = "idx_订单日期"
columns = ["order_date:DESC"]
unique = false
```

### 唯一索引

```toml
[[tasks.indexes]]
name = "idx_邮箱唯一"
columns = ["email"]
unique = true
```

### 部分索引（仅 SQLite）

```toml
[[tasks.indexes]]
name = "idx_大额订单"
columns = ["order_id"]
unique = false
where = "total_amount > 10000"
```

## 高级选项

| 选项 | 说明 |
|------|------|
| `mode` | 写入模式：`replace`（默认，会重建表）、`append`（追加）或 `merge`/`upsert`（按键更新或插入） |
| `batch_size` | 每批插入的行数（默认 1000） |
| `max_retries` | 批量插入失败时的重试次数（默认 0） |
| `validate` | 迁移后校验：`row_count`（merge 模式会跳过） |
| `merge_keys` | merge/upsert 的匹配键（需要目标表对应唯一约束） |
| `resume_key` | 用于增量/断点续传的字段名 |
| `resume_from` | 增量起点的 SQL 字面量（排除该值） |
| `state_file` | 断点状态文件（JSON），自动记录上次迁移的 `resume_key` 值 |
| `dlq_path` | 死信队列文件路径。当批量插入最终失败时，将失败的单行写入该文件 |
| `dlq_format` | 死信队列格式，支持 `jsonl`（默认）和 `csv` |
| `allow_same_table` | 允许同库迁移（需显式开启，避免误删源表） |
| `skip_create_table` | 跳过目标表的 drop/create 操作 |
| `pre_sql` | 任务开始前在目标库执行的自定义 SQL |
| `post_sql` | 任务完成后在目标库执行的自定义 SQL |
| `depends_on` | 任务依赖，按 `table_name` 声明，启用 DAG 调度 |
| `schema_evolution` | append/merge 模式下，自动 ALTER TABLE ADD COLUMN |
| `columns` | 列级映射与转换表达式 |
| `masking` | PII 脱敏规则 |
| `adaptive_batch` | 自适应批量大小动态调优 |
| `shard` | 范围分片并行读取 |
| `cdc` | CDC 轮询模式，持续增量同步 |
| `validate_sample_size` | `validate = "sample"` 时的抽样行数 |

## 全局配置

### 迁移审计

```toml
[history]
enabled = true
table_name = "db_ferry_migrations"
```

### 定时调度

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
