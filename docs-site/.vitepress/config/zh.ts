import { defineConfig } from 'vitepress'

export const zh = defineConfig({
  lang: 'zh-CN',
  description: '声明式任务驱动的跨数据库迁移 CLI',

  themeConfig: {
    nav: [
      { text: '指南', link: '/zh/guide/' },
      { text: '示例', link: '/zh/examples/full-sync' },
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
