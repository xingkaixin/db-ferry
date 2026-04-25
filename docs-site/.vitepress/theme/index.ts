import DefaultTheme from 'vitepress/theme'
import type { Theme } from 'vitepress'
import DemoConsole from './components/DemoConsole.vue'
import './custom.css'

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component('DemoConsole', DemoConsole)
  },
} satisfies Theme
