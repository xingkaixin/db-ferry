---
layout: home

hero:
  name: db-ferry
  text: 跨数据库迁移工具
  tagline: 用一份 task.toml，把跨数据库迁移收成一条可复用的工作流。
  image:
    src: /logo.svg
    alt: db-ferry logo
  actions:
    - theme: brand
      text: 快速开始
      link: /zh/guide/getting-started
    - theme: alt
      text: GitHub
      link: https://github.com/xingkaixin/db-ferry

features:
  - icon: 🔄
    title: 多数据库支持
    details: 支持 Oracle、MySQL、PostgreSQL、SQL Server、SQLite 和 DuckDB，任意两库之间迁移数据。
  - icon: 📝
    title: 声明式任务
    details: 在一个 task.toml 中定义源库、目标库、SQL 查询和写入模式，无需编写代码。
  - icon: ⚡
    title: DAG 并行执行
    details: 无依赖任务自动并行执行，有依赖关系的任务按 DAG 顺序调度运行。
  - icon: 🛡️
    title: 断点续传与可靠
    details: 支持 resume_key 断点续传、状态文件持久化、批量重试和死信队列捕获失败行。
  - icon: 🔍
    title: 数据校验
    details: 行数校验、哈希校验、抽样校验三种方式，diff 命令精确对比源目标差异。
  - icon: 🔒
    title: 脱敏与 TLS
    details: 内置 PII 脱敏规则，全数据库适配器统一 TLS/SSL 加密传输支持。
---
