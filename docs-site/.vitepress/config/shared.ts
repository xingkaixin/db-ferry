import { defineConfig } from 'vitepress'
import react from '@vitejs/plugin-react'

export const shared = defineConfig({
  title: 'db-ferry',
  description: 'Task-driven cross-database migration tool',

  base: '/',
  lastUpdated: true,
  cleanUrls: true,

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
