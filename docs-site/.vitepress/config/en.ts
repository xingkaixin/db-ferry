import { defineConfig } from 'vitepress'

export const en = defineConfig({
  lang: 'en-US',
  description: 'Declarative task-driven cross-database migration CLI',

  themeConfig: {
    nav: [
      { text: 'Guide', link: '/guide/' },
      { text: 'Examples', link: '/examples/full-sync' },
      { text: 'Demo', link: '/demo' },
      { text: 'Changelog', link: '/guide/changelog' },
    ],

    sidebar: {
      '/guide/': {
        base: '/guide/',
        items: [
          { text: 'Introduction', link: '/' },
          { text: 'Getting Started', link: 'getting-started' },
          { text: 'Configuration', link: 'configuration' },
          { text: 'CLI Reference', link: 'cli-reference' },
          { text: 'Data Types', link: 'data-types' },
          { text: 'Advanced', link: 'advanced' },
          { text: 'Changelog', link: 'changelog' },
        ],
      },
      '/examples/': {
        base: '/examples/',
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
