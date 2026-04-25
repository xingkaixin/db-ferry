<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'

// ==================== 类型 ====================
interface DemoTask {
  id: string
  name: string
  source: string
  target: string
  status: 'success' | 'running' | 'failed' | 'pending'
  rowsMigrated: number
  rowsTotal: number
  duration: string
  rps: number
}

interface HistoryRecord {
  id: string
  taskName: string
  status: 'success' | 'failed' | 'partial'
  timestamp: string
  rowsMigrated: number
  duration: string
  message: string
}

interface DLQRecord {
  id: string
  taskName: string
  rowData: string
  error: string
  timestamp: string
}

interface ProgressEvent {
  taskId: string
  percent: number
  rowsDone: number
  rowsTotal: number
  rps: number
  eta: string
}

// ==================== 模拟数据 ====================
const allTasks = ref<DemoTask[]>([
  {
    id: 't1',
    name: 'users_full_sync',
    source: 'Oracle',
    target: 'PostgreSQL',
    status: 'success',
    rowsMigrated: 1248320,
    rowsTotal: 1248320,
    duration: '45s',
    rps: 27740,
  },
  {
    id: 't2',
    name: 'orders_incremental',
    source: 'MySQL',
    target: 'PostgreSQL',
    status: 'running',
    rowsMigrated: 890420,
    rowsTotal: 2000000,
    duration: '23s',
    rps: 38714,
  },
  {
    id: 't3',
    name: 'products_catalog',
    source: 'SQL Server',
    target: 'MySQL',
    status: 'failed',
    rowsMigrated: 0,
    rowsTotal: 156000,
    duration: '3s',
    rps: 0,
  },
  {
    id: 't4',
    name: 'audit_logs_archive',
    source: 'PostgreSQL',
    target: 'DuckDB',
    status: 'pending',
    rowsMigrated: 0,
    rowsTotal: 0,
    duration: '-',
    rps: 0,
  },
  {
    id: 't5',
    name: 'inventory_sync',
    source: 'MySQL',
    target: 'Oracle',
    status: 'success',
    rowsMigrated: 452100,
    rowsTotal: 452100,
    duration: '28s',
    rps: 16146,
  },
  {
    id: 't6',
    name: 'analytics_warehouse',
    source: 'PostgreSQL',
    target: 'DuckDB',
    status: 'running',
    rowsMigrated: 1520000,
    rowsTotal: 5000000,
    duration: '67s',
    rps: 22686,
  },
])

const historyRecords = ref<HistoryRecord[]>([
  {
    id: 'h1',
    taskName: 'users_full_sync',
    status: 'success',
    timestamp: '2026-04-25 09:12:33',
    rowsMigrated: 1248320,
    duration: '45s',
    message: '全部 1,248,320 行迁移成功',
  },
  {
    id: 'h2',
    taskName: 'inventory_sync',
    status: 'success',
    timestamp: '2026-04-25 08:45:10',
    rowsMigrated: 452100,
    duration: '28s',
    message: '全部 452,100 行迁移成功',
  },
  {
    id: 'h3',
    taskName: 'products_catalog',
    status: 'failed',
    timestamp: '2026-04-25 08:30:05',
    rowsMigrated: 0,
    duration: '3s',
    message: '连接超时：SQL Server 192.168.1.45:1433 不可达',
  },
  {
    id: 'h4',
    taskName: 'orders_incremental',
    status: 'partial',
    timestamp: '2026-04-25 07:55:22',
    rowsMigrated: 1987450,
    rowsTotal: 2000000,
    duration: '52s',
    message: '完成 1,987,450 / 2,000,000 行，5,320 行进入 DLQ',
  },
  {
    id: 'h5',
    taskName: 'session_cleanup',
    status: 'success',
    timestamp: '2026-04-25 06:10:00',
    rowsMigrated: 89200,
    duration: '8s',
    message: '全部 89,200 行迁移成功',
  },
])

const dlqRecords = ref<DLQRecord[]>([
  {
    id: 'd1',
    taskName: 'orders_incremental',
    rowData: '{"order_id": 1048291, "amount": 999999.99, "currency": null}',
    error: '列 currency 不允许 NULL 值',
    timestamp: '2026-04-25 07:56:01',
  },
  {
    id: 'd2',
    taskName: 'orders_incremental',
    rowData: '{"order_id": 1048305, "amount": -12.50, "currency": "CNY"}',
    error: 'CHECK 约束失败：amount >= 0',
    timestamp: '2026-04-25 07:56:03',
  },
  {
    id: 'd3',
    taskName: 'users_full_sync',
    rowData: '{"user_id": 55231, "email": "invalid-email", "phone": "13800138000"}',
    error: '邮箱格式校验失败',
    timestamp: '2026-04-25 09:12:45',
  },
  {
    id: 'd4',
    taskName: 'orders_incremental',
    rowData: '{"order_id": 1048312, "amount": 128.00, "currency": "US Dollar"}',
    error: '外键约束失败：currency 值不在引用表中',
    timestamp: '2026-04-25 07:56:08',
  },
])

// ==================== 进度模拟 ====================
const activeProgress = ref<ProgressEvent>({
  taskId: 't2',
  percent: 44,
  rowsDone: 890420,
  rowsTotal: 2000000,
  rps: 38714,
  eta: '28s',
})

const logLines = ref<string[]>([
  '[09:15:01] INFO  开始迁移任务 orders_incremental',
  '[09:15:01] INFO  源库: MySQL (192.168.1.10:3306) / 目标库: PostgreSQL (192.168.1.20:5432)',
  '[09:15:02] INFO  预估总行数: 2,000,000',
  '[09:15:03] INFO  批次 1/200 完成: 10,000 行, RPS 38,714',
  '[09:15:04] INFO  批次 2/200 完成: 20,000 行, RPS 39,102',
])

let progressTimer: ReturnType<typeof setInterval> | null = null
let logTimer: ReturnType<typeof setInterval> | null = null

const logTemplates = [
  '批次 {n}/200 完成: {rows} 行, RPS {rps}',
  '写入目标表成功, 累计 {rows} 行',
  '进度更新: {pct}% ({rows}/{total})',
  '内存使用: {mem} MB, 连接池活跃: {conn}',
  '索引重建中... 预计剩余 {eta}',
]

function simulateProgress() {
  const t = activeProgress.value
  if (t.percent >= 99) {
    t.percent = 44
    t.rowsDone = 890420
    t.rps = 38000 + Math.floor(Math.random() * 8000)
    logLines.value = logLines.value.slice(0, 4)
    return
  }
  const increment = Math.floor(Math.random() * 3) + 1
  t.percent = Math.min(99, t.percent + increment)
  t.rowsDone = Math.min(t.rowsTotal, Math.floor(t.rowsTotal * (t.percent / 100)))
  t.rps = 35000 + Math.floor(Math.random() * 12000)
  const remaining = Math.ceil((t.rowsTotal - t.rowsDone) / t.rps)
  t.eta = remaining + 's'
}

function simulateLog() {
  const t = activeProgress.value
  const tpl = logTemplates[Math.floor(Math.random() * logTemplates.length)]
  const now = new Date().toTimeString().slice(0, 8)
  let msg = tpl
    .replace('{n}', String(Math.floor(t.percent * 2)))
    .replace('{rows}', t.rowsDone.toLocaleString())
    .replace('{rps}', t.rps.toLocaleString())
    .replace('{pct}', String(t.percent))
    .replace('{total}', t.rowsTotal.toLocaleString())
    .replace('{mem}', String(Math.floor(120 + Math.random() * 80)))
    .replace('{conn}', String(Math.floor(4 + Math.random() * 8)))
    .replace('{eta}', t.eta)
  const level = Math.random() > 0.9 ? 'WARN ' : 'INFO '
  logLines.value.push(`[${now}] ${level}${msg}`)
  if (logLines.value.length > 50) {
    logLines.value = logLines.value.slice(-50)
  }
}

onMounted(() => {
  progressTimer = setInterval(simulateProgress, 1200)
  logTimer = setInterval(simulateLog, 1800)
})

onUnmounted(() => {
  if (progressTimer) clearInterval(progressTimer)
  if (logTimer) clearInterval(logTimer)
})

// ==================== 计算属性 ====================
const statusMeta = computed(() => {
  const map: Record<string, { label: string; color: string; dot: string }> = {
    success: { label: '成功', color: 'var(--status-success)', dot: 'dot-success' },
    running: { label: '运行中', color: 'var(--status-running)', dot: 'dot-running' },
    failed: { label: '失败', color: 'var(--status-failed)', dot: 'dot-failed' },
    pending: { label: '待运行', color: 'var(--status-pending)', dot: 'dot-pending' },
  }
  return map
})

const activeTask = computed(() => allTasks.value.find(t => t.id === activeProgress.value.taskId))

// ==================== 交互状态 ====================
const hoveredTask = ref<string | null>(null)
const expandedHistory = ref<string | null>(null)
const expandedDLQ = ref<string | null>(null)

function formatNumber(n: number): string {
  return n.toLocaleString('en-US')
}

function formatRows(migrated: number, total: number): string {
  if (total === 0) return '-'
  return `${formatNumber(migrated)} / ${formatNumber(total)}`
}
</script>

<template>
  <div class="demo-console">
    <!-- 顶部提示栏 -->
    <div class="demo-banner">
      <span class="demo-banner-icon">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <circle cx="12" cy="12" r="10"/>
          <path d="M12 16v-4M12 8h.01"/>
        </svg>
      </span>
      <span class="demo-banner-text">
        这是基于模拟数据的演示环境，真实能力请
        <a href="/zh/guide/getting-started.html" class="demo-banner-link">下载 db-ferry</a>
        体验
      </span>
    </div>

    <!-- 任务看板 -->
    <section class="demo-section">
      <h2 class="demo-section-title">
        <span class="demo-icon">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <rect x="3" y="3" width="7" height="7" rx="1"/>
            <rect x="14" y="3" width="7" height="7" rx="1"/>
            <rect x="3" y="14" width="7" height="7" rx="1"/>
            <rect x="14" y="14" width="7" height="7" rx="1"/>
          </svg>
        </span>
        任务看板
      </h2>
      <div class="task-grid">
        <div
          v-for="task in allTasks"
          :key="task.id"
          class="task-card"
          :class="`task-card--${task.status}`"
          @mouseenter="hoveredTask = task.id"
          @mouseleave="hoveredTask = null"
        >
          <div class="task-card-header">
            <span class="task-card-name">{{ task.name }}</span>
            <span class="task-status-dot" :class="statusMeta[task.status].dot" />
          </div>
          <div class="task-card-route">
            <span class="db-badge db-badge--source">{{ task.source }}</span>
            <span class="route-arrow">→</span>
            <span class="db-badge db-badge--target">{{ task.target }}</span>
          </div>
          <div class="task-card-meta">
            <span class="task-status-label" :style="{ color: statusMeta[task.status].color }">
              {{ statusMeta[task.status].label }}
            </span>
            <span v-if="task.status === 'running'" class="task-spinner" />
          </div>

          <!-- Hover 详情 -->
          <div class="task-card-detail" :class="{ 'is-visible': hoveredTask === task.id }">
            <div class="detail-row">
              <span class="detail-label">迁移行数</span>
              <span class="detail-value">{{ formatRows(task.rowsMigrated, task.rowsTotal) }}</span>
            </div>
            <div class="detail-row">
              <span class="detail-label">耗时</span>
              <span class="detail-value">{{ task.duration }}</span>
            </div>
            <div v-if="task.rps > 0" class="detail-row">
              <span class="detail-label">RPS</span>
              <span class="detail-value">{{ formatNumber(task.rps) }}</span>
            </div>
          </div>
        </div>
      </div>
    </section>

    <!-- 实时进度 -->
    <section class="demo-section">
      <h2 class="demo-section-title">
        <span class="demo-icon">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>
          </svg>
        </span>
        实时进度
      </h2>
      <div class="progress-panel">
        <div class="progress-header">
          <div class="progress-task-info">
            <span class="progress-task-name">{{ activeTask?.name }}</span>
            <span class="progress-task-route">{{ activeTask?.source }} → {{ activeTask?.target }}</span>
          </div>
          <div class="progress-percent">{{ activeProgress.percent }}%</div>
        </div>
        <div class="progress-bar-bg">
          <div class="progress-bar-fill" :style="{ width: activeProgress.percent + '%' }" />
        </div>
        <div class="progress-stats">
          <div class="progress-stat">
            <span class="progress-stat-label">已迁移</span>
            <span class="progress-stat-value">{{ formatNumber(activeProgress.rowsDone) }}</span>
          </div>
          <div class="progress-stat">
            <span class="progress-stat-label">总行数</span>
            <span class="progress-stat-value">{{ formatNumber(activeProgress.rowsTotal) }}</span>
          </div>
          <div class="progress-stat">
            <span class="progress-stat-label">RPS</span>
            <span class="progress-stat-value">{{ formatNumber(activeProgress.rps) }}</span>
          </div>
          <div class="progress-stat">
            <span class="progress-stat-label">预计剩余</span>
            <span class="progress-stat-value">{{ activeProgress.eta }}</span>
          </div>
        </div>
        <div class="progress-log">
          <div
            v-for="(line, idx) in logLines"
            :key="idx"
            class="progress-log-line"
          >
            <span
              class="log-level"
              :class="line.includes('WARN') ? 'log-warn' : 'log-info'"
            >
              {{ line.includes('WARN') ? 'WARN' : 'INFO' }}
            </span>
            <span class="log-message">{{ line.replace(/^\[.*?\]\s*(INFO|WARN)\s*/, '') }}</span>
          </div>
        </div>
      </div>
    </section>

    <!-- 配置预览 -->
    <section class="demo-section">
      <h2 class="demo-section-title">
        <span class="demo-icon">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
            <polyline points="14 2 14 8 20 8"/>
            <line x1="16" y1="13" x2="8" y2="13"/>
            <line x1="16" y1="17" x2="8" y2="17"/>
          </svg>
        </span>
        配置预览
      </h2>
      <div class="code-block">
        <div class="code-header">
          <span class="code-lang">task.toml</span>
          <span class="code-dots">
            <span />
            <span />
            <span />
          </span>
        </div>
        <pre class="code-body"><code><span class="tok-comment"># db-ferry 迁移任务配置</span>
<span class="tok-section">[source]</span>
<span class="tok-key">type</span> <span class="tok-punct">=</span> <span class="tok-string">"mysql"</span>
<span class="tok-key">host</span> <span class="tok-punct">=</span> <span class="tok-string">"192.168.1.10"</span>
<span class="tok-key">port</span> <span class="tok-punct">=</span> <span class="tok-num">3306</span>
<span class="tok-key">database</span> <span class="tok-punct">=</span> <span class="tok-string">"production"</span>
<span class="tok-key">user</span> <span class="tok-punct">=</span> <span class="tok-string">"db_ferry"</span>

<span class="tok-section">[target]</span>
<span class="tok-key">type</span> <span class="tok-punct">=</span> <span class="tok-string">"postgresql"</span>
<span class="tok-key">host</span> <span class="tok-punct">=</span> <span class="tok-string">"192.168.1.20"</span>
<span class="tok-key">port</span> <span class="tok-punct">=</span> <span class="tok-num">5432</span>
<span class="tok-key">database</span> <span class="tok-punct">=</span> <span class="tok-string">"warehouse"</span>
<span class="tok-key">schema</span> <span class="tok-punct">=</span> <span class="tok-string">"public"</span>

<span class="tok-section">[[task]]</span>
<span class="tok-key">name</span> <span class="tok-punct">=</span> <span class="tok-string">"orders_incremental"</span>
<span class="tok-key">source</span> <span class="tok-punct">=</span> <span class="tok-string">"orders"</span>
<span class="tok-key">target</span> <span class="tok-punct">=</span> <span class="tok-string">"orders_new"</span>
<span class="tok-key">mode</span> <span class="tok-punct">=</span> <span class="tok-string">"merge"</span>
<span class="tok-key">merge_keys</span> <span class="tok-punct">=</span> [<span class="tok-string">"order_id"</span>]
<span class="tok-key">resume_key</span> <span class="tok-punct">=</span> <span class="tok-string">"updated_at"</span>
<span class="tok-key">batch_size</span> <span class="tok-punct">=</span> <span class="tok-num">10000</span>

<span class="tok-section">[[task]]</span>
<span class="tok-key">name</span> <span class="tok-punct">=</span> <span class="tok-string">"users_full_sync"</span>
<span class="tok-key">source</span> <span class="tok-punct">=</span> <span class="tok-string">"users"</span>
<span class="tok-key">target</span> <span class="tok-punct">=</span> <span class="tok-string">"users"</span>
<span class="tok-key">mode</span> <span class="tok-punct">=</span> <span class="tok-string">"replace"</span></code></pre>
      </div>
    </section>

    <!-- 迁移历史 -->
    <section class="demo-section">
      <h2 class="demo-section-title">
        <span class="demo-icon">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <circle cx="12" cy="12" r="10"/>
            <polyline points="12 6 12 12 16 14"/>
          </svg>
        </span>
        迁移历史
      </h2>
      <div class="timeline">
        <div
          v-for="record in historyRecords"
          :key="record.id"
          class="timeline-item"
          :class="`timeline-item--${record.status}`"
          @click="expandedHistory = expandedHistory === record.id ? null : record.id"
        >
          <div class="timeline-dot" />
          <div class="timeline-content">
            <div class="timeline-header">
              <span class="timeline-task">{{ record.taskName }}</span>
              <span
                class="timeline-status"
                :class="`status-${record.status}`"
              >
                {{ record.status === 'success' ? '成功' : record.status === 'failed' ? '失败' : '部分成功' }}
              </span>
              <span class="timeline-time">{{ record.timestamp }}</span>
            </div>
            <div class="timeline-meta">
              <span>{{ formatNumber(record.rowsMigrated) }} 行</span>
              <span class="timeline-sep">·</span>
              <span>{{ record.duration }}</span>
            </div>
            <div
              class="timeline-detail"
              :class="{ 'is-expanded': expandedHistory === record.id }"
            >
              {{ record.message }}
            </div>
          </div>
        </div>
      </div>
    </section>

    <!-- DLQ 样本 -->
    <section class="demo-section">
      <h2 class="demo-section-title">
        <span class="demo-icon">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/>
            <line x1="12" y1="9" x2="12" y2="13"/>
            <line x1="12" y1="17" x2="12.01" y2="17"/>
          </svg>
        </span>
        DLQ 样本
      </h2>
      <div class="dlq-table-wrap">
        <table class="dlq-table">
          <thead>
            <tr>
              <th>任务</th>
              <th>行数据</th>
              <th>错误原因</th>
              <th>时间</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="record in dlqRecords"
              :key="record.id"
              @click="expandedDLQ = expandedDLQ === record.id ? null : record.id"
              class="dlq-row"
            >
              <td class="dlq-cell dlq-cell--task">{{ record.taskName }}</td>
              <td class="dlq-cell dlq-cell--data">
                <code class="dlq-data-code">{{ record.rowData }}</code>
              </td>
              <td class="dlq-cell dlq-cell--error">{{ record.error }}</td>
              <td class="dlq-cell dlq-cell--time">{{ record.timestamp }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <p class="dlq-note">
        DLQ（Dead Letter Queue）自动收集迁移失败的行数据及错误原因，便于事后排查与修复重试。
      </p>
    </section>

    <!-- CTA -->
    <div class="demo-cta">
      <p class="demo-cta-text">准备好迁移你的第一组数据了吗？</p>
      <a href="/zh/guide/getting-started.html" class="demo-cta-btn">开始使用 db-ferry →</a>
    </div>
  </div>
</template>

<style scoped>
/* ==================== CSS 变量 ==================== */
.demo-console {
  --status-success: #22c55e;
  --status-running: #38bdf8;
  --status-failed: #ef4444;
  --status-pending: #94a3b8;
  --status-partial: #f59e0b;
  --demo-bg: var(--vp-c-bg, #0e141a);
  --demo-bg-soft: var(--vp-c-bg-soft, #1c2229);
  --demo-bg-elv: var(--vp-c-bg-elv, #161c23);
  --demo-border: var(--vp-c-border, rgba(62, 73, 75, 0.3));
  --demo-text-1: var(--vp-c-text-1, #dde3ec);
  --demo-text-2: var(--vp-c-text-2, #b4c9df);
  --demo-text-3: var(--vp-c-text-3, #93a7bd);
  --demo-brand: var(--vp-c-brand-1, #81e6fa);
  --demo-brand-soft: var(--vp-c-brand-soft, rgba(129, 230, 250, 0.12));
}

/* ==================== 顶部提示栏 ==================== */
.demo-banner {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 12px 16px;
  margin-bottom: 24px;
  border-radius: 10px;
  background: rgba(245, 158, 11, 0.1);
  border: 1px solid rgba(245, 158, 11, 0.25);
  color: #fbbf24;
  font-size: 14px;
}

.demo-banner-icon {
  display: flex;
  flex-shrink: 0;
}

.demo-banner-link {
  color: var(--demo-brand);
  text-decoration: underline;
  text-underline-offset: 2px;
}

.demo-banner-link:hover {
  color: var(--vp-c-brand-2, #63cadd);
}

/* ==================== 区块标题 ==================== */
.demo-section {
  margin-bottom: 40px;
}

.demo-section-title {
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 18px;
  font-weight: 600;
  color: var(--demo-text-1);
  margin-bottom: 16px;
  padding-bottom: 8px;
  border-bottom: 1px solid var(--demo-border);
}

.demo-icon {
  display: flex;
  color: var(--demo-brand);
}

/* ==================== 任务看板 ==================== */
.task-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
  gap: 14px;
}

.task-card {
  position: relative;
  padding: 16px;
  border-radius: 10px;
  background: var(--demo-bg-soft);
  border: 1px solid var(--demo-border);
  transition: border-color 0.2s, transform 0.2s, box-shadow 0.2s;
  cursor: default;
  overflow: hidden;
}

.task-card:hover {
  border-color: var(--demo-brand);
  transform: translateY(-2px);
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.25);
}

.task-card--success { border-left: 3px solid var(--status-success); }
.task-card--running { border-left: 3px solid var(--status-running); }
.task-card--failed  { border-left: 3px solid var(--status-failed); }
.task-card--pending { border-left: 3px solid var(--status-pending); }

.task-card-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 10px;
}

.task-card-name {
  font-size: 14px;
  font-weight: 600;
  color: var(--demo-text-1);
  font-family: var(--vp-font-family-mono, monospace);
}

.task-status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
}

.dot-success { background: var(--status-success); }
.dot-running { background: var(--status-running); animation: pulse 1.5s ease-in-out infinite; }
.dot-failed  { background: var(--status-failed); }
.dot-pending { background: var(--status-pending); }

@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.35; }
}

.task-card-route {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-bottom: 10px;
  font-size: 12px;
}

.db-badge {
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 11px;
  font-weight: 500;
}

.db-badge--source {
  background: rgba(56, 189, 248, 0.12);
  color: #7dd3fc;
}

.db-badge--target {
  background: rgba(129, 230, 250, 0.12);
  color: var(--demo-brand);
}

.route-arrow {
  color: var(--demo-text-3);
}

.task-card-meta {
  display: flex;
  align-items: center;
  gap: 8px;
}

.task-status-label {
  font-size: 12px;
  font-weight: 500;
}

.task-spinner {
  width: 12px;
  height: 12px;
  border: 2px solid var(--demo-border);
  border-top-color: var(--status-running);
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

/* Hover 详情 */
.task-card-detail {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: var(--demo-bg-elv);
  padding: 16px;
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 8px;
  opacity: 0;
  pointer-events: none;
  transition: opacity 0.25s ease;
  border-radius: 10px;
}

.task-card-detail.is-visible {
  opacity: 1;
}

.detail-row {
  display: flex;
  justify-content: space-between;
  font-size: 13px;
}

.detail-label {
  color: var(--demo-text-3);
}

.detail-value {
  color: var(--demo-text-1);
  font-weight: 500;
  font-family: var(--vp-font-family-mono, monospace);
}

/* ==================== 实时进度 ==================== */
.progress-panel {
  background: var(--demo-bg-soft);
  border: 1px solid var(--demo-border);
  border-radius: 10px;
  padding: 20px;
}

.progress-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 12px;
}

.progress-task-info {
  display: flex;
  align-items: baseline;
  gap: 10px;
}

.progress-task-name {
  font-size: 15px;
  font-weight: 600;
  color: var(--demo-text-1);
  font-family: var(--vp-font-family-mono, monospace);
}

.progress-task-route {
  font-size: 12px;
  color: var(--demo-text-3);
}

.progress-percent {
  font-size: 22px;
  font-weight: 700;
  color: var(--demo-brand);
  font-family: var(--vp-font-family-mono, monospace);
}

.progress-bar-bg {
  height: 8px;
  background: var(--demo-bg-elv);
  border-radius: 4px;
  overflow: hidden;
  margin-bottom: 16px;
}

.progress-bar-fill {
  height: 100%;
  background: linear-gradient(90deg, var(--vp-c-brand-3, #4ba7cc), var(--demo-brand));
  border-radius: 4px;
  transition: width 0.6s ease;
}

.progress-stats {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;
  margin-bottom: 16px;
}

.progress-stat {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.progress-stat-label {
  font-size: 11px;
  color: var(--demo-text-3);
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.progress-stat-value {
  font-size: 15px;
  font-weight: 600;
  color: var(--demo-text-1);
  font-family: var(--vp-font-family-mono, monospace);
}

.progress-log {
  background: var(--demo-bg);
  border: 1px solid var(--demo-border);
  border-radius: 8px;
  padding: 12px;
  max-height: 200px;
  overflow-y: auto;
  font-family: var(--vp-font-family-mono, monospace);
  font-size: 12px;
  line-height: 1.7;
}

.progress-log-line {
  display: flex;
  gap: 8px;
  align-items: baseline;
}

.log-level {
  font-size: 10px;
  font-weight: 600;
  padding: 1px 5px;
  border-radius: 3px;
  flex-shrink: 0;
}

.log-info {
  background: rgba(34, 197, 94, 0.12);
  color: #4ade80;
}

.log-warn {
  background: rgba(245, 158, 11, 0.12);
  color: #fbbf24;
}

.log-message {
  color: var(--demo-text-2);
}

/* ==================== 配置预览 ==================== */
.code-block {
  background: var(--vp-code-block-bg, #111820);
  border: 1px solid var(--demo-border);
  border-radius: 10px;
  overflow: hidden;
}

.code-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 10px 16px;
  background: var(--demo-bg-elv);
  border-bottom: 1px solid var(--demo-border);
}

.code-lang {
  font-size: 12px;
  font-weight: 500;
  color: var(--demo-text-3);
  font-family: var(--vp-font-family-mono, monospace);
}

.code-dots {
  display: flex;
  gap: 6px;
}

.code-dots span {
  width: 10px;
  height: 10px;
  border-radius: 50%;
}

.code-dots span:nth-child(1) { background: #ef4444; }
.code-dots span:nth-child(2) { background: #f59e0b; }
.code-dots span:nth-child(3) { background: #22c55e; }

.code-body {
  margin: 0;
  padding: 16px 20px;
  overflow-x: auto;
  font-size: 13px;
  line-height: 1.8;
  font-family: var(--vp-font-family-mono, monospace);
}

/* TOML 语法高亮 */
.tok-comment { color: #6b7a8f; font-style: italic; }
.tok-section { color: #f472b6; font-weight: 600; }
.tok-key     { color: #93c5fd; }
.tok-punct   { color: #94a3b8; }
.tok-string  { color: #86efac; }
.tok-num     { color: #fdba74; }

/* ==================== 迁移历史 ==================== */
.timeline {
  position: relative;
  padding-left: 20px;
}

.timeline::before {
  content: '';
  position: absolute;
  left: 5px;
  top: 8px;
  bottom: 8px;
  width: 2px;
  background: var(--demo-border);
}

.timeline-item {
  position: relative;
  padding-bottom: 20px;
  cursor: pointer;
}

.timeline-item:last-child {
  padding-bottom: 0;
}

.timeline-dot {
  position: absolute;
  left: -17px;
  top: 5px;
  width: 10px;
  height: 10px;
  border-radius: 50%;
  border: 2px solid var(--demo-bg);
}

.timeline-item--success .timeline-dot { background: var(--status-success); }
.timeline-item--failed  .timeline-dot { background: var(--status-failed); }
.timeline-item--partial .timeline-dot { background: var(--status-partial); }

.timeline-content {
  background: var(--demo-bg-soft);
  border: 1px solid var(--demo-border);
  border-radius: 8px;
  padding: 12px 14px;
  transition: border-color 0.2s;
}

.timeline-item:hover .timeline-content {
  border-color: var(--demo-brand);
}

.timeline-header {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  margin-bottom: 4px;
}

.timeline-task {
  font-size: 14px;
  font-weight: 600;
  color: var(--demo-text-1);
  font-family: var(--vp-font-family-mono, monospace);
}

.timeline-status {
  font-size: 11px;
  font-weight: 500;
  padding: 2px 8px;
  border-radius: 4px;
}

.status-success {
  background: rgba(34, 197, 94, 0.12);
  color: var(--status-success);
}

.status-failed {
  background: rgba(239, 68, 68, 0.12);
  color: var(--status-failed);
}

.status-partial {
  background: rgba(245, 158, 11, 0.12);
  color: var(--status-partial);
}

.timeline-time {
  font-size: 12px;
  color: var(--demo-text-3);
  margin-left: auto;
}

.timeline-meta {
  font-size: 12px;
  color: var(--demo-text-2);
  display: flex;
  align-items: center;
  gap: 6px;
}

.timeline-sep {
  color: var(--demo-text-3);
}

.timeline-detail {
  font-size: 12px;
  color: var(--demo-text-3);
  margin-top: 6px;
  padding-top: 6px;
  border-top: 1px solid var(--demo-border);
  max-height: 0;
  overflow: hidden;
  opacity: 0;
  transition: max-height 0.3s ease, opacity 0.3s ease, margin-top 0.3s ease;
}

.timeline-detail.is-expanded {
  max-height: 200px;
  opacity: 1;
}

/* ==================== DLQ ==================== */
.dlq-table-wrap {
  overflow-x: auto;
  border: 1px solid var(--demo-border);
  border-radius: 10px;
}

.dlq-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}

.dlq-table thead {
  background: var(--demo-bg-elv);
}

.dlq-table th {
  text-align: left;
  padding: 10px 14px;
  font-weight: 600;
  color: var(--demo-text-2);
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  border-bottom: 1px solid var(--demo-border);
}

.dlq-table td {
  padding: 12px 14px;
  border-bottom: 1px solid var(--demo-border);
  color: var(--demo-text-1);
}

.dlq-table tbody tr:last-child td {
  border-bottom: none;
}

.dlq-row {
  cursor: pointer;
  transition: background 0.15s;
}

.dlq-row:hover {
  background: var(--demo-bg-elv);
}

.dlq-cell--task {
  font-family: var(--vp-font-family-mono, monospace);
  font-size: 12px;
  white-space: nowrap;
}

.dlq-cell--data {
  max-width: 320px;
}

.dlq-data-code {
  font-size: 11px;
  color: var(--demo-brand);
  background: var(--demo-bg);
  padding: 3px 6px;
  border-radius: 4px;
  word-break: break-all;
}

.dlq-cell--error {
  color: var(--status-failed);
  font-size: 12px;
}

.dlq-cell--time {
  font-size: 12px;
  color: var(--demo-text-3);
  white-space: nowrap;
}

.dlq-note {
  font-size: 12px;
  color: var(--demo-text-3);
  margin-top: 10px;
  font-style: italic;
}

/* ==================== CTA ==================== */
.demo-cta {
  text-align: center;
  padding: 40px 20px;
  margin-top: 20px;
  border-top: 1px solid var(--demo-border);
}

.demo-cta-text {
  font-size: 16px;
  color: var(--demo-text-2);
  margin-bottom: 16px;
}

.demo-cta-btn {
  display: inline-block;
  padding: 10px 24px;
  background: linear-gradient(135deg, #8be2ef, #63cadd 50%, #4ba7cc);
  color: #00363e;
  font-weight: 600;
  font-size: 14px;
  border-radius: 8px;
  text-decoration: none;
  transition: box-shadow 0.2s, transform 0.2s;
}

.demo-cta-btn:hover {
  box-shadow: 0 0 30px rgba(99, 202, 221, 0.25);
  transform: translateY(-1px);
}

/* ==================== 响应式 ==================== */
@media (max-width: 640px) {
  .task-grid {
    grid-template-columns: 1fr;
  }

  .progress-stats {
    grid-template-columns: repeat(2, 1fr);
  }

  .timeline-time {
    margin-left: 0;
    width: 100%;
  }

  .dlq-cell--data {
    max-width: 200px;
  }
}
</style>
