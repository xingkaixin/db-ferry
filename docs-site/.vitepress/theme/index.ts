import DefaultTheme from 'vitepress/theme'
import type { Theme } from 'vitepress'
import DemoMount from './components/DemoMount.vue'
import LandingSections from './components/LandingSections.vue'
import './custom.css'

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component('DemoConsole', DemoMount)
    app.component('LandingSections', LandingSections)
  },
} satisfies Theme
