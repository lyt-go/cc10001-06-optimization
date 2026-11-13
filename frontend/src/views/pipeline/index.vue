<template>
  <section class="page" data-module="pipeline">
    <header class="page-head">
      <div>
        <h2>预处理管道管理</h2>
        <p class="page-desc">定义采集数据的后处理管道，对原始采集结果做去重、清洗、格式化和异常行隔离。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记处理管道</button>
        <button class="btn" type="button" @click="exportRows">导出预处理管道清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button class="link" type="button" @click="runQualityChain(row)">运行质量链路</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无预处理管道数据，可先登记处理管道</td>
        </tr>
      </tbody>
    </table>

    <section v-if="qualityReport" class="quality-panel">
      <h3>质量链路运行报告（{{ qualityReport.运行编号 }} · {{ qualityReport.任务编号 }}）</h3>
      <div class="stat-row">
        <article v-for="item in reportStats" :key="item.label" class="stat-card">
          <span class="stat-label">{{ item.label }}</span>
          <strong class="stat-value">{{ item.value }}</strong>
        </article>
      </div>
      <p v-if="qualityReport.是否超阈值" class="error-text">
        异常比例 {{ qualityReport.异常比例 }} 超过阈值 {{ qualityReport.异常阈值 }}，失败原因：{{ qualityReport.失败原因 }}（已同步写入结果校验台账与采集日志）
      </p>
      <p v-else>失败原因：{{ qualityReport.失败原因 }}；输出文件：{{ qualityReport.输出文件 }}</p>
      <p v-for="warning in qualityReport.配置警告" :key="warning" class="warn-text">配置警告：{{ warning }}</p>
    </section>

    <section v-if="quarantineRows.length" class="quality-panel">
      <h3>异常隔离区（{{ quarantineTotal }} 条失败行，保留原始 JSON、字段名与原因）</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th v-for="column in quarantineColumns" :key="column">{{ column }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in quarantineRows" :key="String(row.隔离编号)">
            <td>{{ row.隔离编号 }}</td>
            <td>{{ row.任务编号 }}</td>
            <td>{{ row.失败字段 }}</td>
            <td>{{ row.失败原因 }}</td>
            <td><code>{{ JSON.stringify(row.原始JSON) }}</code></td>
            <td>{{ row.隔离时间 }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条预处理管道记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type QualityReport = {
  运行编号: string
  任务编号: string
  本次处理: number
  跳过已处理: number
  入管道: number
  隔离区新增: number
  其中重复: number
  异常比例: string
  异常阈值: string
  是否超阈值: boolean
  失败原因: string
  输出文件: string
  配置警告: string[]
}

const ENDPOINT = '/api/pipeline'
const columns = ["管道编号", "数据来源", "字段映射", "去重字段", "清洗表达式", "格式化规则", "输出格式", "异常行策略"]
const actions = ["提交校验", "启用管道", "废弃管道"]
const statuses = ["草稿", "校验中", "已启用", "已废弃"]
const stats = [{"label": "处理管道总数", "value": 0}, {"label": "已启用管道", "value": 0}, {"label": "校验中管道", "value": 0}]
const quarantineColumns = ["隔离编号", "任务编号", "失败字段", "失败原因", "原始JSON", "隔离时间"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const qualityReport = ref<QualityReport | null>(null)
const quarantineRows = ref<Record<string, unknown>[]>([])
const quarantineTotal = ref(0)

const reportStats = computed(() => {
  if (!qualityReport.value) return []
  const report = qualityReport.value
  return [
    { label: '本次处理', value: report.本次处理 },
    { label: '跳过已处理', value: report.跳过已处理 },
    { label: '入管道', value: report.入管道 },
    { label: '隔离失败行', value: report.隔离区新增 },
    { label: '其中重复', value: report.其中重复 },
    { label: '异常比例/阈值', value: `${report.异常比例} / ${report.异常阈值}` },
  ]
})

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '处理管道登记入口尚未接入审批流'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('预处理管道动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '预处理管道操作失败'
  }
}

async function runQualityChain(row: Row) {
  errorMessage.value = ''
  qualityReport.value = null
  try {
    const response = await request('/api/qualitychain/run', {
      method: 'POST',
      body: JSON.stringify({ values: { 管道编号: row.管道编号 } }),
    })
    const payload = await response.json()
    if (!payload.ok) {
      throw new Error(payload.message || '质量链路运行失败')
    }
    qualityReport.value = payload.entry as QualityReport
    await loadQuarantine(String(row.管道编号))
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '质量链路运行失败'
  }
}

async function loadQuarantine(pipelineCode: string) {
  const query = new URLSearchParams({ 管道编号: pipelineCode, size: '50' }).toString()
  try {
    const response = await request(`/api/qualitychain/quarantine?${query}`)
    if (!response.ok) {
      throw new Error('异常隔离区读取失败')
    }
    const payload = await response.json()
    quarantineRows.value = payload.items ?? []
    quarantineTotal.value = payload.total ?? quarantineRows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '异常隔离区读取失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('处理管道列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '预处理管道列表读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.quality-panel {
  margin-top: 16px;
  padding: 12px;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: #fff;
}

.quality-panel h3 {
  margin: 0 0 10px;
  font-size: 14px;
}

.warn-text {
  color: #b54708;
}
</style>
