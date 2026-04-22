---
layout: home

hero:
  name: db-ferry
  text: Cross-Database Migration
  tagline: Turn cross-database migration into a single reusable task.
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
    details: Oracle, MySQL, PostgreSQL, SQL Server, SQLite, and DuckDB — migrate between any of them with one tool.
  - icon: 📝
    title: Declarative Tasks
    details: Define source, target, SQL query, and write mode in a single task.toml. No code required.
  - icon: ⚡
    title: DAG Parallel Execution
    details: Independent tasks run in parallel. Dependent tasks wait for their prerequisites automatically.
  - icon: 🛡️
    title: Resumable & Reliable
    details: Resume key, state files, batch retries, and dead-letter queue for failed rows.
  - icon: 🔍
    title: Data Validation
    details: Row count, checksum, and sample validation. diff command for fine-grained comparison.
  - icon: 🔒
    title: PII Masking & TLS
    details: Built-in anonymization rules and unified TLS/SSL support across all adapters.
---
