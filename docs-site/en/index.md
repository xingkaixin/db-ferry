---
layout: home

head:
  - - meta
    - property: og:url
      content: https://db-ferry.xingkaixin.me/
  - - meta
    - property: og:title
      content: 'db-ferry — Cross-Database Migration CLI'
  - - meta
    - property: og:description
      content: 'Open-source CLI for cross-database migration. Move data between Oracle, MySQL, PostgreSQL, SQL Server, SQLite, and DuckDB with a single task.toml. DAG parallel execution, incremental sync, upsert, PII masking, and row-level validation — no code required.'
  - - meta
    - name: description
      content: 'Open-source CLI for cross-database migration. Move data between Oracle, MySQL, PostgreSQL, SQL Server, SQLite, and DuckDB with a single task.toml. No code required.'

hero:
  name: db-ferry
  text: Cross-Database Migration
  tagline: One task.toml moves data between Oracle, MySQL, PostgreSQL, SQL Server, SQLite, and DuckDB — no code, no boilerplate, fully resumable.
  image:
    src: /logo.svg
    alt: db-ferry logo
  actions:
    - theme: brand
      text: Get Started
      link: /guide/getting-started
    - theme: alt
      text: View on GitHub
      link: https://github.com/xingkaixin/db-ferry

features:
  - icon: 🔄
    title: Multi-Database Support
    details: Oracle, MySQL, PostgreSQL, SQL Server, SQLite, and DuckDB — migrate between any combination with one unified tool. No per-database scripts needed.
  - icon: 📝
    title: Declarative Config, Zero Code
    details: Define source database, target database, SQL query, and write mode in a single task.toml. No application code, no ORM, no boilerplate.
  - icon: ⚡
    title: DAG Parallel Execution
    details: Independent tasks run in parallel automatically. Tasks with dependencies are scheduled in topological order via a built-in DAG engine.
  - icon: 🛡️
    title: Resumable & Fault-Tolerant
    details: Resume interrupted migrations with resume_key. State files, configurable batch retries, and a dead-letter queue capture every failed row.
  - icon: 🔍
    title: Data Validation
    details: Row count, checksum, and sample validation after each migration. Use the diff command for fine-grained source-to-target comparison.
  - icon: 🔒
    title: PII Masking & TLS
    details: Eight built-in anonymization rule types mask sensitive data before it reaches the target. Unified TLS/SSL support across all database adapters.
---
