# db-ferry AGENTS 指南

## 项目定位
- `db-ferry` 是一个声明式任务驱动的多数据库迁移 CLI。
- 核心执行链路：`task.toml -> config.Validate() -> processor -> database adapters`。
- 目标是用最小配置在 Oracle/MySQL/PostgreSQL/SQL Server/SQLite/DuckDB 之间完成可追踪的数据搬运。

## 技术栈与环境
- 语言与版本：Go `1.25.2`。
- 核心依赖：`github.com/BurntSushi/toml`、6 类数据库驱动、`github.com/schollz/progressbar/v3`。
- 平台要点：
  - DuckDB 依赖 CGO（非 Windows 构建）。
  - Windows 下 DuckDB 明确不支持，见 [duckdb_windows.go](database/duckdb_windows.go)。
- 建议固定工具链与缓存目录后执行：
  - `mkdir -p .cache/go-build`
  - `GOCACHE=$(pwd)/.cache/go-build mise x go -- go test ./...`
  - `GOCACHE=$(pwd)/.cache/go-build mise x go -- go build ./...`

## 代码与改动规范
- 最小改动优先：优先改单模块，不做跨层重构。
- 新增任务配置字段时，必须同步：
  - [config.go](config/config.go) 的 `TaskConfig` 与 `Validate()`
  - [task.toml.sample](task.toml.sample)
  - [README.md](README.md) 与 [user_guide.md](docs/user_guide.md)
- 维护 changelog 时，必须同步：
  - [CHANGELOG.md](CHANGELOG.md) 英文版
  - [CHANGELOG_zh.md](CHANGELOG_zh.md) 中文版
- 新增数据库类型时，必须同步：
  - [config.go](config/config.go) 类型常量与 `validateDatabaseConfig`
  - [manager.go](database/manager.go) 的 `openConnection`
  - 对应 `SourceDB`/`TargetDB` 实现（见 [interface.go](database/interface.go)）
- 现有行为约束保持不变：
  - 默认 `mode=replace`
  - `upsert` 在校验阶段归一为 `merge`
  - `validate=row_count` 在 `merge` 模式下会被跳过
- 安全红线：
  - 禁止提交真实凭据（尤其是 `task.toml` 明文连接信息）
  - 示例配置使用 `task.toml.sample`
  - 严格遵守 [.gitignore](.gitignore)（`task.toml`、`*.db`、`dist`、`*.zip`）

## 开发流程（PR 前）
1. 确认工具链：`mise x go -- go version`
2. 跑格式化检查：`just fmt-check`（或 `gofmt -l .`）
3. 跑静态检查：`just lint`（或 `golangci-lint run`）
4. 跑测试与覆盖率门禁：`just test-cover`（等价于 `go test ./...` + `scripts/coverage-check.sh`）
5. 跑构建检查：`GOCACHE=$(pwd)/.cache/go-build mise x go -- go build ./...`
6. 若改动配置/行为：同步更新 `README.md`、`docs/user_guide.md`、`CHANGELOG.md`、`CHANGELOG_zh.md`
7. 提交信息建议沿用既有风格：`feat: ...`、`fix: ...`、`ci: ...`
8. PR / 分支 CI 由 [test.yml](.github/workflows/test.yml) 执行质量门禁（`just fmt-check`、`golangci-lint-action`、`scripts/coverage-check.sh`）

## 发布流程（简版）
- GitHub Actions 触发条件：`v*` tag push，见 [build.yml](.github/workflows/build.yml)。
- PR / 分支测试与发布拆分：测试见 [test.yml](.github/workflows/test.yml)，构建发布见 [build.yml](.github/workflows/build.yml)。
- 构建矩阵：Linux `amd64/arm64`、macOS `amd64/arm64`、Windows `amd64`。
- npm 发布采用主包 `db-ferry` + 平台包 `db-ferry-{os}-{arch}` 的结构；tag 发布时先发平台包，再发主包。
- 产物命名：`db-ferry-<version>-<platform>.tgz`（上传为 artifact）。

## 常见坑
- `go` 二进制版本与 `GOROOT` 不一致会导致编译失败。
- `resume_key` 必须出现在查询结果列中，否则任务失败。
- 同库迁移必须显式设置 `allow_same_table=true`。
- `merge_keys` 仅在 `mode=merge` 时有效，其他模式配置会校验失败。
