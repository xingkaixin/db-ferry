---
layout: home

head:
  - - meta
    - property: og:url
      content: https://db-ferry.xingkaixin.me/zh/
  - - meta
    - property: og:title
      content: 'db-ferry — 跨数据库迁移 CLI 工具'
  - - meta
    - property: og:description
      content: '开源跨数据库迁移 CLI。用一份 task.toml 在 Oracle、MySQL、PostgreSQL、SQL Server、SQLite 和 DuckDB 之间搬运数据，DAG 并行执行、断点续传、Upsert、PII 脱敏，无需编写代码。'
  - - meta
    - name: description
      content: '开源跨数据库迁移 CLI。用一份 task.toml 在 Oracle、MySQL、PostgreSQL、SQL Server、SQLite 和 DuckDB 之间迁移数据，无需编写代码。'

hero:
  name: db-ferry
  text: 跨数据库迁移工具
  tagline: 声明源、目标、写入模式。DAG 并行执行、跨库内存 JOIN、分片并行读取、断点续传、死信队列、PII 脱敏、数据质量断言、diff 对比、MCP 集成与 SSE 实时进度，把跨数据库迁移收成一条团队可复用的工作流。
  image:
    src: /logo.svg
    alt: db-ferry logo
  actions:
    - theme: brand
      text: 快速开始
      link: /zh/guide/getting-started
    - theme: alt
      text: 在线演示
      link: /zh/demo
    - theme: alt
      text: npm install db-ferry
      link: https://www.npmjs.com/package/db-ferry

features:
  - icon: 🔄
    title: 多数据库支持
    details: 支持 Oracle、MySQL、PostgreSQL、SQL Server、SQLite 和 DuckDB，任意两库之间迁移数据，无需针对每个数据库编写专属脚本。
  - icon: 📝
    title: 声明式配置，零代码
    details: 在一个 task.toml 中声明源库、目标库、SQL 查询和写入模式，无需编写应用代码、无需 ORM，无模板代码。
  - icon: ⚡
    title: DAG 并行执行
    details: 无依赖任务自动并行运行，有依赖关系的任务通过内置 DAG 引擎按拓扑顺序调度，最大化迁移吞吐。
  - icon: 🛡️
    title: 断点续传与容错
    details: 通过 resume_key 支持断点续传。状态文件持久化、可配置批量重试、死信队列确保每一行失败数据都被捕获。
  - icon: 🔍
    title: 数据校验
    details: 行数校验、哈希校验、抽样校验三种模式，迁移后自动验证数据完整性。diff 命令精确对比源目标差异。
  - icon: 🔒
    title: PII 脱敏与 TLS 加密
    details: 内置 8 种脱敏规则，在数据写入目标库前完成敏感字段匿名化。全数据库适配器统一 TLS/SSL 加密传输支持。
---

<LandingSections locale="zh" />
