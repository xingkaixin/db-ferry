<script setup lang="ts">
const props = defineProps<{
  locale: 'zh' | 'en'
}>()

const copy = {
  zh: {
    workflowLabel: 'Workflow',
    workflowTitle: '三步上手，命令就是界面。',
    workflowSub: '安装、声明、执行。每一步都是一个确定性的命令。',
    steps: [
      ['01. Install', 'npm install -g db-ferry', '全局安装 CLI，或用 npx 临时运行。'],
      ['02. Declare', 'db-ferry config init', '生成 task.toml 模板，声明源库、目标库、写入模式。'],
      ['03. Execute', 'db-ferry', '执行迁移，DAG 调度独立任务并行执行，支持范围分片、断点续传与批量进度显示。'],
    ],
    enginesLabel: 'Engines',
    enginesTitle: '覆盖主流业务库与本地分析库。',
    enginesSub: '同一套 task.toml 语义，跨引擎迁移保持一致。',
    engineDesc: 'db-ferry 处理协议转换、类型映射和批量流式传输，让你专注在数据路由上。',
    engineMeta: [
      ['write modes', 'replace / append / merge 覆盖全量与增量场景。支持 schema 演进、列映射转换、PII 脱敏、Lua/JS 插件与数据质量断言。'],
      ['validation', 'row_count / checksum / sample 三级校验，在任务链里完成基础到精细的验收。'],
      ['parallel & resume', 'DAG 并行调度、跨库内存 JOIN、范围分片并行读取、断点续传，大表迁移保持可控。'],
    ],
    docsTitle: '文档直接进入主站路径。',
    docsSub: '从落地页进入配置、命令、示例和高级能力，避免在 GitHub README 与站点之间来回跳转。',
    docsLinks: [
      ['快速开始', '/zh/guide/getting-started', '安装、初始化 task.toml、执行第一条迁移。'],
      ['配置参考', '/zh/guide/configuration', '数据库连接、任务字段、模式、校验与断点续传。'],
      ['命令行用法', '/zh/guide/cli-reference', 'config init、doctor、diff、daemon 等命令入口。'],
      ['完整示例', '/zh/examples/full-sync', '从生产 MySQL 同步到本地 SQLite 的端到端配置。'],
    ],
    demoTitle: 'Demo 演示也随文档一起部署。',
    demoSub: '在线演示展示任务看板、实时进度、配置预览、迁移历史与 DLQ 样本。',
    demoCta: '打开在线演示',
    demoHref: '/zh/demo',
    skillText: '把 db-ferry 的真实语义交给 AI，让它理解字段、命令和迁移边界，直接生成可用的任务片段。',
  },
  en: {
    workflowLabel: 'Workflow',
    workflowTitle: 'Three steps. Commands as the interface.',
    workflowSub: 'Install, declare, execute. Each step is a deterministic command.',
    steps: [
      ['01. Install', 'npm install -g db-ferry', 'Install the CLI globally, or run ad-hoc with npx.'],
      ['02. Declare', 'db-ferry config init', 'Generate a task.toml template. Declare source, target, and write mode.'],
      ['03. Execute', 'db-ferry', 'Run DAG-scheduled parallel tasks with sharded reads, resume support, and batch progress.'],
    ],
    enginesLabel: 'Engines',
    enginesTitle: 'From production databases to local analytics.',
    enginesSub: 'One task.toml syntax across engines.',
    engineDesc: 'db-ferry handles protocol translation, type mapping, and bulk streaming so you can focus on the data route.',
    engineMeta: [
      ['write modes', 'replace / append / merge cover full refreshes and incremental loads. Schema evolution, column mapping, PII masking, Lua/JS plugins, and data quality assertions included.'],
      ['validation', 'row_count / checksum / sample validation completes acceptance checks inside the migration flow.'],
      ['parallel & resume', 'DAG scheduling, federated in-memory JOIN, range-sharded reads, and resume support keep large transfers controlled.'],
    ],
    docsTitle: 'Documentation lives inside the product site.',
    docsSub: 'Move from the landing page into configuration, CLI, examples, and advanced guides without jumping back to GitHub README.',
    docsLinks: [
      ['Getting Started', '/en/guide/getting-started', 'Install, initialize task.toml, and run the first migration.'],
      ['Configuration', '/en/guide/configuration', 'Database connections, task fields, modes, validation, and resume.'],
      ['CLI Reference', '/en/guide/cli-reference', 'config init, doctor, diff, daemon, and command flags.'],
      ['Full Example', '/en/examples/full-sync', 'End-to-end config from production MySQL to local SQLite.'],
    ],
    demoTitle: 'Demo ships with the docs deployment.',
    demoSub: 'The online demo shows the task board, live progress, config preview, migration history, and DLQ samples.',
    demoCta: 'Open online demo',
    demoHref: '/demo',
    skillText: 'Give AI the actual db-ferry contract so it understands real fields, commands, and migration boundaries.',
  },
}[props.locale]

const engines = ['ORACLE', 'POSTGRES', 'MYSQL', 'SQL SERVER', 'SQLITE', 'DUCKDB']
</script>

<template>
  <div class="landing-sections">
    <section class="landing-section">
      <div class="landing-section-head">
        <span>{{ copy.workflowLabel }}</span>
        <h2>{{ copy.workflowTitle }}</h2>
        <p>{{ copy.workflowSub }}</p>
      </div>
      <div class="landing-workflow">
        <article v-for="step in copy.steps" :key="step[0]" class="landing-card">
          <div class="landing-step">{{ step[0] }}</div>
          <pre>{{ step[1] }}</pre>
          <p>{{ step[2] }}</p>
        </article>
      </div>
    </section>

    <section class="landing-section landing-engines">
      <div class="landing-section-head">
        <span>{{ copy.enginesLabel }}</span>
        <h2>{{ copy.enginesTitle }}</h2>
        <p>{{ copy.enginesSub }}</p>
      </div>
      <div class="landing-engine-layout">
        <div>
          <p class="landing-engine-desc">{{ copy.engineDesc }}</p>
          <div class="landing-engine-meta">
            <div v-for="item in copy.engineMeta" :key="item[0]">
              <strong>{{ item[0] }}</strong>
              <p>{{ item[1] }}</p>
            </div>
          </div>
        </div>
        <div class="landing-engine-list" aria-label="Supported databases">
          <span v-for="engine in engines" :key="engine">{{ engine }}</span>
        </div>
      </div>
    </section>

    <section class="landing-section landing-docs-demo">
      <div class="landing-docs">
        <div class="landing-section-head landing-section-head-left">
          <h2>{{ copy.docsTitle }}</h2>
          <p>{{ copy.docsSub }}</p>
        </div>
        <div class="landing-doc-list">
          <a v-for="link in copy.docsLinks" :key="link[1]" :href="link[1]">
            <strong>{{ link[0] }}</strong>
            <span>{{ link[2] }}</span>
          </a>
        </div>
      </div>

      <div class="landing-demo">
        <div class="landing-demo-panel">
          <div class="landing-demo-row">
            <span class="status-dot status-dot-running"></span>
            <strong>orders_incremental</strong>
            <span>44%</span>
          </div>
          <div class="landing-demo-bar"><span></span></div>
          <div class="landing-demo-stats">
            <span>890,420 rows</span>
            <span>38,714 rps</span>
            <span>28s eta</span>
          </div>
          <div class="landing-demo-log">
            <span>INFO 批次 89/200 完成</span>
            <span>INFO 写入目标表成功</span>
            <span>WARN 5,320 行进入 DLQ</span>
          </div>
        </div>
        <h2>{{ copy.demoTitle }}</h2>
        <p>{{ copy.demoSub }}</p>
        <a class="landing-primary-link" :href="copy.demoHref">{{ copy.demoCta }}</a>
      </div>
    </section>

    <section class="landing-skill">
      <div>
        <strong>AI Skills</strong>
        <p>{{ copy.skillText }}</p>
      </div>
      <pre>npx skills add xingkaixin/db-ferry</pre>
    </section>
  </div>
</template>
