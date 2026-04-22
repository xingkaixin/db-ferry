# 快速开始

## 安装

通过 npm 全局安装：

```bash
npm install -g db-ferry
db-ferry -version

# 或者免安装直接运行
npx db-ferry -version
```

当前 npm 预编译二进制支持 Linux/macOS 的 x64 与 arm64，以及 Windows x64。Windows arm64 暂未提供 npm 二进制包；Windows 上若配置 DuckDB 仍会报"不支持"。

## 生成配置文件

在当前目录执行：

```bash
db-ferry config init
```

命令会创建一个 `task.toml` 文件。如果当前目录已经有同名文件，命令会直接报错退出，避免覆盖你现有的配置。

## 配置数据库连接

在 `task.toml` 中定义数据库连接：

```toml
[[databases]]
name = "我的MySQL数据库"
host = "192.168.1.100"
type = "mysql"
port = "3306"
database = "business_db"
user = "dbuser"
password = "your_password"

[[databases]]
name = "本地数据文件"
type = "sqlite"
path = "./mydata.db"
```

## 定义迁移任务

```toml
[[tasks]]
table_name = "员工表"
sql = "SELECT * FROM employees"
source_db = "我的MySQL数据库"
target_db = "本地数据文件"
ignore = false
```

## 执行迁移

```bash
# 基本用法（使用当前目录下的 task.toml）
db-ferry

# 如果配置文件在其他位置
db-ferry -config /path/to/your/task.toml

# 查看详细日志（调试时使用）
db-ferry -v

# 查看版本
db-ferry -version
```

## 从源码构建

```bash
git clone <repository-url>
cd db-ferry
go mod tidy
go build -o db-ferry
```

> DuckDB 支持依赖 CGO。确保 `CGO_ENABLED=1` 和默认 C 工具链可用。
