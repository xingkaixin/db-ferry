import { defineConfig } from 'vitepress'

export const en = defineConfig({
  lang: 'en-US',
  description: 'db-ferry is a declarative, task-driven CLI for cross-database migration. Move data between Oracle, MySQL, PostgreSQL, SQL Server, SQLite, and DuckDB with a single config file — no code, no boilerplate.',

  head: [
    ['meta', { property: 'og:locale', content: 'en_US' }],
    ['meta', { property: 'og:title', content: 'db-ferry — Cross-Database Migration CLI' }],
    ['meta', { property: 'og:description', content: 'Declarative, task-driven CLI for cross-database migration. Supports Oracle, MySQL, PostgreSQL, SQL Server, SQLite, and DuckDB. DAG parallel execution, incremental sync, upsert, PII masking, and data validation — all from one task.toml.' }],
    ['meta', { name: 'twitter:title', content: 'db-ferry — Cross-Database Migration CLI' }],
    ['meta', { name: 'twitter:description', content: 'Move data between any two databases with a single task.toml. Oracle, MySQL, PostgreSQL, SQL Server, SQLite, DuckDB. No code required.' }],
    ['meta', { name: 'keywords', content: 'database migration, cross-database migration, ETL tool, Oracle migration, MySQL migration, PostgreSQL migration, SQL Server migration, DuckDB, SQLite, data pipeline, CLI, Go, open source' }],
  ],

  themeConfig: {
    nav: [
      { text: 'Guide', link: '/en/guide/' },
      { text: 'Examples', link: '/en/examples/full-sync' },
      { text: 'Demo', link: '/demo' },
      { text: 'Changelog', link: '/en/guide/changelog' },
    ],

    sidebar: {
      '/en/guide/': {
        base: '/en/guide/',
        items: [
          { text: 'Introduction', link: '/en/' },
          { text: 'Getting Started', link: 'getting-started' },
          { text: 'Configuration', link: 'configuration' },
          { text: 'CLI Reference', link: 'cli-reference' },
          { text: 'Data Types', link: 'data-types' },
          { text: 'Advanced', link: 'advanced' },
          { text: 'Changelog', link: 'changelog' },
        ],
      },
      '/en/examples/': {
        base: '/en/examples/',
        items: [
          { text: 'Full Sync', link: 'full-sync' },
        ],
      },
    },

    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright © 2025–2026 db-ferry contributors',
    },
  },
})
