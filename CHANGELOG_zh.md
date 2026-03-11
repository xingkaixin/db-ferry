# Changelog

## [0.6.0] - 2026-03-11
- 新增 npm 二进制分发骨架：主包 `db-ferry` + 平台包 `db-ferry-{os}-{arch}`，支持 `npm install -g db-ferry` 与 `npx db-ferry`
- 构建工作流改为按平台产出独立二进制，并在 tag 发布时自动发布 npm 包
- 将 Windows npm 二进制包名调整为 `db-ferry-windows-x64` 以避开 npm spam detection，并让 npm 发布支持同一 tag 的失败重跑补发
- `-version` 改为输出可注入的构建版本，统一 Git tag、Go 二进制与 npm 包版本真源
- 新增独立 GitHub Actions 测试工作流 `test.yml`，对 PR 和非 tag push 执行 `just fmt-check`、`golangci-lint-action` 与 `scripts/coverage-check.sh`
- 新增 `db-ferry config init`，可在当前目录生成内置 `task.toml` 示例文件，若目标文件已存在则报错退出
- 新增 `justfile`，统一提供 `fmt`/`fmt-check`/`lint`/`test`/`build`/`check` 开发命令
- CI 新增质量检查阶段：`gofmt` 格式检查、`golangci-lint`、`go test`
- 新增覆盖率门禁脚本 `scripts/coverage-check.sh`，并接入 `just test-cover` 与 CI 质量阶段
- 新增核心模块测试（`main`、`config`、`database`、`processor`、`utils`），覆盖率门槛固定为全局 `>=80%`、分包 `>=70%`

## [0.5.0] - 2025-12-19
- 增加同库迁移显式开关与跳过建表开关
- 任务进度统计过滤忽略任务，并新增整体任务进度条
- 修复列类型扫描空指针与 Oracle 行数统计别名问题
- Oracle 标识符统一引用，降低保留字/特殊字符风险
- 更新使用文档与示例配置

## [0.4.0] - 2025-11-16
- 新增 DuckDB 支持
- 改进跨平台构建流程

## [0.3.0] - 2025-11-14
- 增加面向非技术用户的完整使用手册

## [0.2.0] - 2025-10-26
- 引入别名配置与多数据库路由能力
- 增强跨平台构建支持

## [0.1.0] - 2025-10-26
- 初始数据迁移工具搭建
