# 高级功能与使用技巧

## 使用技巧

### 技巧1：先测试小数据量

首次使用时，建议先用少量数据测试：

```toml
[[tasks]]
table_name = "测试表"
sql = "SELECT * FROM large_table LIMIT 1000"
source_db = "生产数据库"
target_db = "本地分析库"
ignore = false
```

确认无误后，再移除 `LIMIT` 进行完整迁移。

### 技巧2：分批迁移大表

对于几百万行的大表，建议按时间或 ID 范围分批：

```toml
[[tasks]]
table_name = "用户数据_第一批"
sql = "SELECT * FROM users WHERE id <= 1000000"
source_db = "生产数据库"
target_db = "本地分析库"
ignore = false

[[tasks]]
table_name = "用户数据_第二批"
sql = "SELECT * FROM users WHERE id > 1000000 AND id <= 2000000"
source_db = "生产数据库"
target_db = "本地分析库"
ignore = false
```

### 技巧3：使用视图简化复杂查询

如果 SQL 很复杂，可以在源数据库创建视图，然后迁移视图：

```toml
[[tasks]]
table_name = "汇总数据"
sql = "SELECT * FROM summary_view"
source_db = "生产数据库"
target_db = "本地分析库"
ignore = false
```

### 技巧4：合理安排任务顺序

有依赖关系的表按顺序迁移：

```toml
[[tasks]]
table_name = "客户表"
sql = "SELECT * FROM customers"
source_db = "生产数据库"
target_db = "本地分析库"
ignore = false

[[tasks]]
table_name = "订单表"
sql = """
SELECT o.* FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
"""
source_db = "生产数据库"
target_db = "本地分析库"
ignore = false
```

### 技巧5：增量/断点续传

使用 `resume_key` 配合 `state_file` 可以实现增量迁移和中断后续传：

```toml
[[tasks]]
table_name = "订单增量"
sql = "SELECT order_id, order_date, total_amount FROM orders"
source_db = "生产数据库"
target_db = "本地分析库"
mode = "append"
resume_key = "order_id"
state_file = "./state/orders.json"
validate = "row_count"
```

说明：
- 首次运行会全量迁移并写入 `state_file`
- 后续运行会自动从上次最大的 `order_id` 继续
- 建议在 SQL 中保证 `resume_key` 单调递增（如按主键或时间）

### 技巧6：merge/upsert 合并写入

当目标表需要"存在则更新，不存在则插入"时使用 `merge`：

```toml
[[tasks]]
table_name = "客户表"
sql = "SELECT customer_id, customer_name, email FROM customers"
source_db = "生产数据库"
target_db = "本地分析库"
mode = "merge"
merge_keys = ["customer_id"]
```

说明：
- 目标表需要在 `merge_keys` 上有唯一约束
- merge 会避免重建表，保留已有数据

### 技巧7：CDC 轮询模式实现持续增量同步

当需要持续不断地同步增量数据时，启用 CDC 轮询模式：

```toml
[[tasks]]
table_name = "events"
sql = "SELECT id, event_type, payload, created_at FROM events WHERE created_at > \{\{.LastValue\}\}"
source_db = "生产数据库"
target_db = "分析库"
mode = "append"
state_file = "./state/events.json"

[tasks.cdc]
enabled = true
cursor_column = "created_at"
poll_interval = "5m"
initial_cursor = "2024-01-01"
```

说明：
- `mode` 必须是 `append` 或 `merge`
- `state_file` 用于持久化游标位置
- `cursor_column` 自动作为 `resume_key` 使用
- `poll_interval` 控制轮询间隔（如 `5m`、`30s`、`1h`）
- `initial_cursor` 可选，首次同步的起始值；未设置时默认按 `0` 处理
- SQL 中使用 `\{\{.LastValue\}\}` 模板变量，会被替换为当前游标值
- 首次运行会先执行一次全量同步，随后进入轮询循环
- 发送 SIGINT/SIGTERM 信号可优雅停止轮询
- CDC 不支持 federated 查询和 shard 分片

### 技巧8：定时调度（Cron）实现自动化数据同步

当需要按固定时间周期自动执行迁移时，在全局配置中添加 `[schedule]` 段，并配合 daemon 模式运行：

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

说明：
- `cron` 支持标准 5 字段表达式，也支持描述符如 `@every 1h`、`@daily`
- `timezone` 使用 IANA 时区名（如 `Asia/Shanghai`、`America/New_York`），默认使用系统本地时区
- `retry_on_failure` 开启后，失败会等待 1 分钟后重试，最多 `max_retry` 次
- `missed_catchup` 开启后，如果 daemon 在计划时间未运行，下次启动时会立即补跑一次
- `start_at`/`end_at` 限定执行窗口，窗口外的时间跳过执行
- schedule 仅在 daemon 模式下生效，单次运行 `db-ferry` 命令不受 schedule 控制
- 配置文件修改后会自动重载 schedule（需启用 watch 模式）
- 每次调度的日志会写入 `logs/YYYY-MM-DD.log` 文件，方便排查问题

## 常见问题

### Q1：密码在配置文件中明文保存，安全吗？

A：配置文件只在你的电脑上使用，不会被上传到任何地方。确保配置文件不被他人访问即可。为了更安全，可以：
- 设置配置文件权限：`chmod 600 task.toml`
- 将配置文件放在安全的目录中

### Q2：迁移中断后，可以重新开始吗？

A：可以。db-ferry 会自动创建新表，如果表已存在会报错。如果想重新迁移：
1. 删除目标数据库中的表
2. 重新运行 db-ferry

### Q3：如何只迁移部分数据？

A：在 SQL 中使用 WHERE 条件筛选：
```sql
SELECT * FROM orders WHERE order_date >= '2024-01-01'
```

### Q4：迁移速度如何？

A：db-ferry 使用批量插入，默认每批 1000 条数据（可通过 `batch_size` 调整）。速度取决于：
- 网络速度（远程数据库）
- 数据量大小
- 目标数据库性能

### Q5：如何临时跳过某个任务？

A：将该任务的 `ignore` 设置为 `true`：
```toml
[[tasks]]
ignore = true
```

## 故障排查

### 问题1：无法连接数据库

**现象**：提示连接错误

**检查**：
1. 确认数据库地址、端口、用户名、密码正确
2. 确认数据库服务正在运行
3. 确认网络可达（可以尝试 ping）
4. 检查防火墙设置

### 问题2：权限不足

**现象**：提示权限错误

**解决**：
- 源数据库：需要 SELECT 权限
- 目标数据库：需要 CREATE TABLE、INSERT、CREATE INDEX 权限

### 问题3：表已存在

**现象**：提示表已存在错误

**解决**：
1. 删除目标数据库中的表
2. 或修改 `table_name` 使用新表名

### 问题4：数据类型不支持

**现象**：某些字段迁移后数据异常

**解决**：
在 SQL 中转换数据类型：
```sql
SELECT
    CAST(字段 AS CHAR) as 新字段名
FROM table
```

### 问题5：迁移时间过长

**解决**：
1. 检查网络速度
2. 分批迁移
3. 关闭不必要的索引（迁移完成后再创建）
4. 在目标数据库所在服务器上运行 db-ferry
