<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue'
import { createRoot, type Root } from 'react-dom/client'
import DemoConsole from './DemoConsole'

const containerRef = ref<HTMLDivElement | null>(null)
let root: Root | null = null

onMounted(() => {
  if (containerRef.value) {
    root = createRoot(containerRef.value)
    root.render(DemoConsole({}))
  }
})

onUnmounted(() => {
  if (root) {
    root.unmount()
    root = null
  }
})
</script>

<template>
  <div ref="containerRef" />
</template>
