import { defineConfig } from 'vitepress'
import react from '@vitejs/plugin-react'

const SITE_URL = 'https://db-ferry.xingkaixin.me'
const OG_IMAGE = `${SITE_URL}/og-image.png`

export const shared = defineConfig({
  title: 'db-ferry',
  description: 'Open-source CLI for cross-database migration. Sync Oracle, MySQL, PostgreSQL, SQL Server, SQLite, and DuckDB with a single task.toml — no code required.',

  base: '/',
  lastUpdated: true,
  cleanUrls: true,

  sitemap: {
    hostname: SITE_URL,
  },

  head: [
    ['link', { rel: 'icon', type: 'image/png', sizes: '32x32', href: '/favicon-32.png' }],
    ['link', { rel: 'apple-touch-icon', sizes: '180x180', href: '/apple-touch-icon.png' }],
    ['link', { rel: 'preconnect', href: 'https://fonts.googleapis.com' }],
    ['link', { rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: '' }],
    [
      'link',
      {
        rel: 'stylesheet',
        href: 'https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&family=Inter:wght@400;500;600&display=swap',
      },
    ],
    ['meta', { name: 'theme-color', content: '#0e141a' }],
    ['meta', { name: 'robots', content: 'index, follow' }],
    // Open Graph
    ['meta', { property: 'og:type', content: 'website' }],
    ['meta', { property: 'og:site_name', content: 'db-ferry' }],
    ['meta', { property: 'og:image', content: OG_IMAGE }],
    ['meta', { property: 'og:image:width', content: '1200' }],
    ['meta', { property: 'og:image:height', content: '630' }],
    // Twitter Card
    ['meta', { name: 'twitter:card', content: 'summary_large_image' }],
    ['meta', { name: 'twitter:site', content: '@dbferry' }],
    ['meta', { name: 'twitter:image', content: OG_IMAGE }],
    ['script', { defer: '', src: 'https://static.cloudflareinsights.com/beacon.min.js', 'data-cf-beacon': '{"token": "da8e71b95633452388bd295d5641c627"}' }],
  ],

  appearance: 'dark',

  themeConfig: {
    logo: '/logo.svg',

    socialLinks: [
      { icon: 'github', link: 'https://github.com/xingkaixin/db-ferry' },
    ],

    search: {
      provider: 'local',
    },
  },

  vite: {
    plugins: [react()],
  },
})
