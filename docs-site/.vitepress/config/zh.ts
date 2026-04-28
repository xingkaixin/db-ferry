import { defineConfig } from 'vitepress'

export const zh = defineConfig({
  lang: 'zh-CN',
  description: 'db-ferry 是声明式、任务驱动的跨数据库迁移 CLI。用一份 task.toml 在 Oracle、MySQL、PostgreSQL、SQL Server、SQLite、DuckDB 之间搬运数据，无需编写代码。',

  head: [
    ['meta', { property: 'og:locale', content: 'zh_CN' }],
    ['meta', { property: 'og:title', content: 'db-ferry — 跨数据库迁移 CLI 工具' }],
    ['meta', { property: 'og:description', content: '声明式、任务驱动的跨数据库迁移 CLI。支持 Oracle、MySQL、PostgreSQL、SQL Server、SQLite 和 DuckDB。DAG 并行执行、断点续传、Upsert、PII 脱敏、数据校验，一份 task.toml 搞定全部。' }],
    ['meta', { name: 'twitter:title', content: 'db-ferry — 跨数据库迁移 CLI 工具' }],
    ['meta', { name: 'twitter:description', content: '用一份 task.toml 在 Oracle、MySQL、PostgreSQL、SQL Server、SQLite、DuckDB 之间迁移数据，无需编写代码。' }],
    ['meta', { name: 'keywords', content: '数据库迁移工具, 跨数据库迁移, ETL工具, Oracle迁移, MySQL迁移, PostgreSQL迁移, SQL Server迁移, DuckDB, SQLite, 数据管道, CLI, Go, 开源' }],
  ],

  themeConfig: {
    nav: [
      { text: '指南', link: '/zh/guide/' },
      { text: '示例', link: '/zh/examples/full-sync' },
      { text: '演示', link: '/zh/demo' },
      { text: '更新日志', link: '/zh/guide/changelog' },
    ],

    sidebar: {
      '/zh/guide/': {
        base: '/zh/guide/',
        items: [
          { text: '介绍', link: '/' },
          { text: '快速开始', link: 'getting-started' },
          { text: '配置参考', link: 'configuration' },
          { text: '命令行用法', link: 'cli-reference' },
          { text: '数据类型映射', link: 'data-types' },
          { text: '高级功能', link: 'advanced' },
          { text: '更新日志', link: 'changelog' },
        ],
      },
      '/zh/examples/': {
        base: '/zh/examples/',
        items: [
          { text: '生产库同步', link: 'full-sync' },
        ],
      },
    },

    footer: {
      message: '基于 MIT 许可证发布。',
      copyright: 'Copyright © 2025–2026 db-ferry contributors',
    },
  },
})
