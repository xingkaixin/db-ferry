import { useState, useEffect, useMemo, useRef, useCallback } from 'react'
import './DemoConsole.css'

// ==================== Types ====================
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

// ==================== Mock Data ====================
const ALL_TASKS: DemoTask[] = [
  {
    id: 't1', name: 'users_full_sync', source: 'Oracle', target: 'PostgreSQL',
    status: 'success', rowsMigrated: 1248320, rowsTotal: 1248320, duration: '45s', rps: 27740,
  },
  {
    id: 't2', name: 'orders_incremental', source: 'MySQL', target: 'PostgreSQL',
    status: 'running', rowsMigrated: 890420, rowsTotal: 2000000, duration: '23s', rps: 38714,
  },
  {
    id: 't3', name: 'products_catalog', source: 'SQL Server', target: 'MySQL',
    status: 'failed', rowsMigrated: 0, rowsTotal: 156000, duration: '3s', rps: 0,
  },
  {
    id: 't4', name: 'audit_logs_archive', source: 'PostgreSQL', target: 'DuckDB',
    status: 'pending', rowsMigrated: 0, rowsTotal: 0, duration: '-', rps: 0,
  },
  {
    id: 't5', name: 'inventory_sync', source: 'MySQL', target: 'Oracle',
    status: 'success', rowsMigrated: 452100, rowsTotal: 452100, duration: '28s', rps: 16146,
  },
  {
    id: 't6', name: 'analytics_warehouse', source: 'PostgreSQL', target: 'DuckDB',
    status: 'running', rowsMigrated: 1520000, rowsTotal: 5000000, duration: '67s', rps: 22686,
  },
]

const HISTORY_RECORDS: HistoryRecord[] = [
  {
    id: 'h1', taskName: 'users_full_sync', status: 'success',
    timestamp: '2026-04-25 09:12:33', rowsMigrated: 1248320, duration: '45s',
    message: '全部 1,248,320 行迁移成功',
  },
  {
    id: 'h2', taskName: 'inventory_sync', status: 'success',
    timestamp: '2026-04-25 08:45:10', rowsMigrated: 452100, duration: '28s',
    message: '全部 452,100 行迁移成功',
  },
  {
    id: 'h3', taskName: 'products_catalog', status: 'failed',
    timestamp: '2026-04-25 08:30:05', rowsMigrated: 0, duration: '3s',
    message: '连接超时：SQL Server 192.168.1.45:1433 不可达',
  },
  {
    id: 'h4', taskName: 'orders_incremental', status: 'partial',
    timestamp: '2026-04-25 07:55:22', rowsMigrated: 1987450, duration: '52s',
    message: '完成 1,987,450 / 2,000,000 行，5,320 行进入 DLQ',
  },
  {
    id: 'h5', taskName: 'session_cleanup', status: 'success',
    timestamp: '2026-04-25 06:10:00', rowsMigrated: 89200, duration: '8s',
    message: '全部 89,200 行迁移成功',
  },
]

const DLQ_DATA: DLQRecord[] = [
  {
    id: 'd1', taskName: 'orders_incremental',
    rowData: '{"order_id": 1048291, "amount": 999999.99, "currency": null}',
    error: '列 currency 不允许 NULL 值',
    timestamp: '2026-04-25 07:56:01',
  },
  {
    id: 'd2', taskName: 'orders_incremental',
    rowData: '{"order_id": 1048305, "amount": -12.50, "currency": "CNY"}',
    error: 'CHECK 约束失败：amount >= 0',
    timestamp: '2026-04-25 07:56:03',
  },
  {
    id: 'd3', taskName: 'users_full_sync',
    rowData: '{"user_id": 55231, "email": "invalid-email", "phone": "13800138000"}',
    error: '邮箱格式校验失败',
    timestamp: '2026-04-25 09:12:45',
  },
  {
    id: 'd4', taskName: 'orders_incremental',
    rowData: '{"order_id": 1048312, "amount": 128.00, "currency": "US Dollar"}',
    error: '外键约束失败：currency 值不在引用表中',
    timestamp: '2026-04-25 07:56:08',
  },
]

const INITIAL_LOGS = [
  '[09:15:01] INFO  开始迁移任务 orders_incremental',
  '[09:15:01] INFO  源库: MySQL (192.168.1.10:3306) / 目标库: PostgreSQL (192.168.1.20:5432)',
  '[09:15:02] INFO  预估总行数: 2,000,000',
  '[09:15:03] INFO  批次 1/200 完成: 10,000 行, RPS 38,714',
  '[09:15:04] INFO  批次 2/200 完成: 20,000 行, RPS 39,102',
]

const LOG_TEMPLATES = [
  '批次 {n}/200 完成: {rows} 行, RPS {rps}',
  '写入目标表成功, 累计 {rows} 行',
  '进度更新: {pct}% ({rows}/{total})',
  '内存使用: {mem} MB, 连接池活跃: {conn}',
  '索引重建中... 预计剩余 {eta}',
]

// ==================== Helpers ====================
function formatNumber(n: number): string {
  return n.toLocaleString('en-US')
}

function formatRows(migrated: number, total: number): string {
  if (total === 0) return '-'
  return `${formatNumber(migrated)} / ${formatNumber(total)}`
}

// ==================== Components ====================
function DemoBanner() {
  return (
    <div className="demo-banner">
      <span className="demo-banner-icon">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="12" cy="12" r="10" />
          <path d="M12 16v-4M12 8h.01" />
        </svg>
      </span>
      <span className="demo-banner-text">
        这是基于模拟数据的演示环境，真实能力请
        <a href="/zh/guide/getting-started.html" className="demo-banner-link">下载 db-ferry</a>
        体验
      </span>
    </div>
  )
}

function TaskBoard() {
  const [hoveredTask, setHoveredTask] = useState<string | null>(null)

  const statusMeta: Record<string, { label: string; dot: string }> = {
    success: { label: '成功', dot: 'dot-success' },
    running: { label: '运行中', dot: 'dot-running' },
    failed: { label: '失败', dot: 'dot-failed' },
    pending: { label: '待运行', dot: 'dot-pending' },
  }

  return (
    <section className="demo-section">
      <h2 className="demo-section-title">
        <span className="demo-icon">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <rect x="3" y="3" width="7" height="7" rx="1" />
            <rect x="14" y="3" width="7" height="7" rx="1" />
            <rect x="3" y="14" width="7" height="7" rx="1" />
            <rect x="14" y="14" width="7" height="7" rx="1" />
          </svg>
        </span>
        任务看板
      </h2>
      <div className="task-grid">
        {ALL_TASKS.map((task) => (
          <div
            key={task.id}
            className={`task-card task-card--${task.status}`}
            onMouseEnter={() => setHoveredTask(task.id)}
            onMouseLeave={() => setHoveredTask(null)}
          >
            <div className="task-card-header">
              <span className="task-card-name">{task.name}</span>
              <span className={`task-status-dot ${statusMeta[task.status].dot}`} />
            </div>
            <div className="task-card-route">
              <span className="db-badge db-badge--source">{task.source}</span>
              <span className="route-arrow">→</span>
              <span className="db-badge db-badge--target">{task.target}</span>
            </div>
            <div className="task-card-meta">
              <span className={`task-status-label status-${task.status}`}>
                {statusMeta[task.status].label}
              </span>
              {task.status === 'running' && <span className="task-spinner" />}
            </div>
            <div className={`task-card-detail ${hoveredTask === task.id ? 'is-visible' : ''}`}>
              <div className="detail-row">
                <span className="detail-label">迁移行数</span>
                <span className="detail-value">{formatRows(task.rowsMigrated, task.rowsTotal)}</span>
              </div>
              <div className="detail-row">
                <span className="detail-label">耗时</span>
                <span className="detail-value">{task.duration}</span>
              </div>
              {task.rps > 0 && (
                <div className="detail-row">
                  <span className="detail-label">RPS</span>
                  <span className="detail-value">{formatNumber(task.rps)}</span>
                </div>
              )}
            </div>
          </div>
        ))}
      </div>
    </section>
  )
}

function LiveProgress() {
  const [progress, setProgress] = useState<ProgressEvent>({
    taskId: 't2', percent: 44, rowsDone: 890420, rowsTotal: 2000000, rps: 38714, eta: '28s',
  })
  const [logs, setLogs] = useState<string[]>(INITIAL_LOGS)
  const logRef = useRef<HTMLDivElement>(null)

  const activeTask = useMemo(() => ALL_TASKS.find((t) => t.id === progress.taskId), [progress.taskId])

  useEffect(() => {
    const progressTimer = setInterval(() => {
      setProgress((prev) => {
        if (prev.percent >= 99) {
          return { ...prev, percent: 44, rowsDone: 890420, rps: 38000 + Math.floor(Math.random() * 8000), eta: '28s' }
        }
        const increment = Math.floor(Math.random() * 3) + 1
        const newPercent = Math.min(99, prev.percent + increment)
        const newRowsDone = Math.min(prev.rowsTotal, Math.floor(prev.rowsTotal * (newPercent / 100)))
        const newRps = 35000 + Math.floor(Math.random() * 12000)
        const remaining = Math.ceil((prev.rowsTotal - newRowsDone) / newRps)
        return {
          ...prev,
          percent: newPercent,
          rowsDone: newRowsDone,
          rps: newRps,
          eta: remaining + 's',
        }
      })
    }, 1200)

    return () => clearInterval(progressTimer)
  }, [])

  useEffect(() => {
    const logTimer = setInterval(() => {
      setProgress((current) => {
        const tpl = LOG_TEMPLATES[Math.floor(Math.random() * LOG_TEMPLATES.length)]
        const now = new Date().toTimeString().slice(0, 8)
        const msg = tpl
          .replace('{n}', String(Math.floor(current.percent * 2)))
          .replace('{rows}', current.rowsDone.toLocaleString())
          .replace('{rps}', current.rps.toLocaleString())
          .replace('{pct}', String(current.percent))
          .replace('{total}', current.rowsTotal.toLocaleString())
          .replace('{mem}', String(Math.floor(120 + Math.random() * 80)))
          .replace('{conn}', String(Math.floor(4 + Math.random() * 8)))
          .replace('{eta}', current.eta)
        const level = Math.random() > 0.9 ? 'WARN ' : 'INFO '
        setLogs((prev) => {
          const next = [...prev, `[${now}] ${level}${msg}`]
          return next.length > 50 ? next.slice(-50) : next
        })
        return current
      })
    }, 1800)

    return () => clearInterval(logTimer)
  }, [])

  useEffect(() => {
    if (logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight
    }
  }, [logs])

  return (
    <section className="demo-section">
      <h2 className="demo-section-title">
        <span className="demo-icon">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
          </svg>
        </span>
        实时进度
      </h2>
      <div className="progress-panel">
        <div className="progress-header">
          <div className="progress-task-info">
            <span className="progress-task-name">{activeTask?.name}</span>
            <span className="progress-task-route">{activeTask?.source} → {activeTask?.target}</span>
          </div>
          <div className="progress-percent">{progress.percent}%</div>
        </div>
        <div className="progress-bar-bg">
          <div className="progress-bar-fill" style={{ width: `${progress.percent}%` }} />
        </div>
        <div className="progress-stats">
          <div className="progress-stat">
            <span className="progress-stat-label">已迁移</span>
            <span className="progress-stat-value">{formatNumber(progress.rowsDone)}</span>
          </div>
          <div className="progress-stat">
            <span className="progress-stat-label">总行数</span>
            <span className="progress-stat-value">{formatNumber(progress.rowsTotal)}</span>
          </div>
          <div className="progress-stat">
            <span className="progress-stat-label">RPS</span>
            <span className="progress-stat-value">{formatNumber(progress.rps)}</span>
          </div>
          <div className="progress-stat">
            <span className="progress-stat-label">预计剩余</span>
            <span className="progress-stat-value">{progress.eta}</span>
          </div>
        </div>
        <div className="progress-log" ref={logRef}>
          {logs.map((line, idx) => (
            <div key={idx} className="progress-log-line">
              <span className={`log-level ${line.includes('WARN') ? 'log-warn' : 'log-info'}`}>
                {line.includes('WARN') ? 'WARN' : 'INFO'}
              </span>
              <span className="log-message">{line.replace(/^\[.*?\]\s*(INFO|WARN)\s*/, '')}</span>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}

function ConfigPreview() {
  const tomlCode = `# db-ferry 迁移任务配置
[source]
type = "mysql"
host = "192.168.1.10"
port = 3306
database = "production"
user = "db_ferry"

[target]
type = "postgresql"
host = "192.168.1.20"
port = 5432
database = "warehouse"
schema = "public"

[[task]]
name = "orders_incremental"
source = "orders"
target = "orders_new"
mode = "merge"
merge_keys = ["order_id"]
resume_key = "updated_at"
batch_size = 10000

[[task]]
name = "users_full_sync"
source = "users"
target = "users"
mode = "replace"`

  const highlighted = tomlCode.split('\n').map((line, idx) => {
    let content: React.ReactNode
    if (line.startsWith('#')) {
      content = <span className="tok-comment">{line}</span>
    } else if (line.startsWith('[')) {
      content = <span className="tok-section">{line}</span>
    } else if (line.includes('=')) {
      const [key, ...rest] = line.split('=')
      const value = rest.join('=')
      content = (
        <>
          <span className="tok-key">{key}</span>
          <span className="tok-punct">=</span>
          {value.trim().startsWith('"') ? (
            <span className="tok-string">{value.trim()}</span>
          ) : /^\d+$/.test(value.trim()) ? (
            <span className="tok-num">{value.trim()}</span>
          ) : (
            <span className="tok-string">{value.trim()}</span>
          )}
        </>
      )
    } else {
      content = line
    }
    return <div key={idx}>{content}</div>
  })

  return (
    <section className="demo-section">
      <h2 className="demo-section-title">
        <span className="demo-icon">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
            <polyline points="14 2 14 8 20 8" />
            <line x1="16" y1="13" x2="8" y2="13" />
            <line x1="16" y1="17" x2="8" y2="17" />
          </svg>
        </span>
        配置预览
      </h2>
      <div className="code-block">
        <div className="code-header">
          <span className="code-lang">task.toml</span>
          <span className="code-dots">
            <span /><span /><span />
          </span>
        </div>
        <pre className="code-body"><code>{highlighted}</code></pre>
      </div>
    </section>
  )
}

function MigrationHistory() {
  const [expanded, setExpanded] = useState<string | null>(null)

  const statusLabel = (s: string) => {
    if (s === 'success') return '成功'
    if (s === 'failed') return '失败'
    return '部分成功'
  }

  return (
    <section className="demo-section">
      <h2 className="demo-section-title">
        <span className="demo-icon">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <circle cx="12" cy="12" r="10" />
            <polyline points="12 6 12 12 16 14" />
          </svg>
        </span>
        迁移历史
      </h2>
      <div className="timeline">
        {HISTORY_RECORDS.map((record) => (
          <div
            key={record.id}
            className={`timeline-item timeline-item--${record.status}`}
            onClick={() => setExpanded(expanded === record.id ? null : record.id)}
          >
            <div className="timeline-dot" />
            <div className="timeline-content">
              <div className="timeline-header">
                <span className="timeline-task">{record.taskName}</span>
                <span className={`timeline-status status-${record.status}`}>
                  {statusLabel(record.status)}
                </span>
                <span className="timeline-time">{record.timestamp}</span>
              </div>
              <div className="timeline-meta">
                <span>{formatNumber(record.rowsMigrated)} 行</span>
                <span className="timeline-sep">·</span>
                <span>{record.duration}</span>
              </div>
              <div className={`timeline-detail ${expanded === record.id ? 'is-expanded' : ''}`}>
                {record.message}
              </div>
            </div>
          </div>
        ))}
      </div>
    </section>
  )
}

function DLQSample() {
  return (
    <section className="demo-section">
      <h2 className="demo-section-title">
        <span className="demo-icon">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" />
            <line x1="12" y1="9" x2="12" y2="13" />
            <line x1="12" y1="17" x2="12.01" y2="17" />
          </svg>
        </span>
        DLQ 样本
      </h2>
      <div className="dlq-table-wrap">
        <table className="dlq-table">
          <thead>
            <tr>
              <th>任务</th>
              <th>行数据</th>
              <th>错误原因</th>
              <th>时间</th>
            </tr>
          </thead>
          <tbody>
            {DLQ_DATA.map((record) => (
              <tr key={record.id} className="dlq-row">
                <td className="dlq-cell dlq-cell--task">{record.taskName}</td>
                <td className="dlq-cell dlq-cell--data">
                  <code className="dlq-data-code">{record.rowData}</code>
                </td>
                <td className="dlq-cell dlq-cell--error">{record.error}</td>
                <td className="dlq-cell dlq-cell--time">{record.timestamp}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="dlq-note">
        DLQ（Dead Letter Queue）自动收集迁移失败的行数据及错误原因，便于事后排查与修复重试。
      </p>
    </section>
  )
}

function DemoCTA() {
  return (
    <div className="demo-cta">
      <p className="demo-cta-text">准备好迁移你的第一组数据了吗？</p>
      <a href="/zh/guide/getting-started.html" className="demo-cta-btn">开始使用 db-ferry →</a>
    </div>
  )
}

// ==================== Main Export ====================
export default function DemoConsole() {
  return (
    <div className="demo-console">
      <DemoBanner />
      <TaskBoard />
      <LiveProgress />
      <ConfigPreview />
      <MigrationHistory />
      <DLQSample />
      <DemoCTA />
    </div>
  )
}
