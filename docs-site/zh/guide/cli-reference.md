# 命令行用法

## 基本命令

```bash
# 生成 task.toml 示例文件
db-ferry config init

# 基本用法（使用当前目录下的 task.toml）
db-ferry

# 如果配置文件在其他位置
db-ferry -config /path/to/your/task.toml

# 查看详细日志（调试时使用）
db-ferry -v

# 查看版本
db-ferry -version
```

## 子命令

### `config init`

交互式配置向导，在当前目录创建 `task.toml`。会逐步引导选择引擎、填写连接信息、选择迁移表。如果目标文件已存在则报错退出。

### `diff`

对比源库和目标库的数据差异：

```bash
db-ferry diff -task 员工表
```

参数：
- `-task`（必填）：任务表名
- `-keys`：对比键
- `-where`：WHERE 条件过滤
- `-limit`：行数限制
- `-output`：输出文件路径
- `-format`：输出格式（`json`/`csv`/`html`）

### `mcp serve`

启动 MCP 服务器，提供 5 个原生工具用于 AI 集成：

```bash
db-ferry mcp serve
```

## 全局参数

| 参数 | 说明 |
|------|------|
| `-config` | TOML 配置文件路径（默认：`task.toml`） |
| `-v` | 启用详细日志，显示文件/行号前缀 |
| `-version` | 打印构建版本并退出 |
| `-sse-port` | 启动 SSE 服务器（如 `:8080`），通过 `/events` 实时推送任务进度，通过 `/status` 暴露当前状态；支持 CORS |
