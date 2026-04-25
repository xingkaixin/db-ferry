import DefaultTheme from 'vitepress/theme'
import type { Theme } from 'vitepress'
import DemoMount from './components/DemoMount.vue'
import './custom.css'

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component('DemoConsole', DemoMount)
  },
} satisfies Theme
