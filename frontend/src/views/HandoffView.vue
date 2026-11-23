<template>
  <section class="page" data-module="partition_handoff">
    <header class="page-head">
      <div>
        <h2>分区移交记录</h2>
        <p class="page-desc">
          两个消息流源可以拥有同名 Topic 的同一个分区，但分区归属和偏移进度各记各的。
          受理移交会签发新旧两把凭据，只能按各自区间读取和确认检查点。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="run-demo" type="button" @click="loadDemo" :disabled="busy">
          {{ busy ? '准备中…' : '搭出 orders/0 移交场景' }}
        </button>
        <button class="btn" data-testid="refresh" type="button" @click="reload">刷新移交记录</button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="handoff-error">{{ errorMessage }}</p>
    <p v-if="notice" class="notice-text" data-testid="handoff-notice">{{ notice }}</p>

    <table class="data-table">
      <thead>
        <tr>
          <th>ID</th>
          <th>来源源</th>
          <th>目标源</th>
          <th>消费组</th>
          <th>Topic</th>
          <th>分区</th>
          <th>旧方归属区间</th>
          <th>允许接续区间</th>
          <th>允许接续边界</th>
          <th>当前状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="row in rows"
          :key="row.id"
          :data-testid="`handoff-row-${row.id}`"
          :data-status="row.status"
          :class="{ selected: row.id === activeId }"
          @click="selectHandoff(row)"
        >
          <td>{{ row.id }}</td>
          <td>{{ row['来源源'] }}</td>
          <td>{{ row['目标源'] }}</td>
          <td>{{ row['消费组'] }}</td>
          <td>{{ row['Topic/Queue'] }}</td>
          <td>{{ row['分区/分片'] }}</td>
          <td>{{ row['旧方归属区间'] }}</td>
          <td>{{ row['允许接续区间'] }}</td>
          <td>{{ row['允许接续边界'] }}</td>
          <td>{{ row.status }}</td>
          <td class="row-actions" @click.stop>
            <button
              v-for="action in actionsFor(row)"
              :key="action"
              class="link"
              type="button"
              :data-testid="`handoff-action-${row.id}-${action}`"
              @click="perform(row, action)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td colspan="11" class="empty-state">暂无移交记录，可先搭出 orders/0 场景或登记一条</td>
        </tr>
      </tbody>
    </table>

    <section v-if="activeEntry" class="handoff-panel">
      <h3>
        移交 #{{ activeEntry.id }} · {{ activeEntry['来源源'] }} → {{ activeEntry['目标源'] }}
        <span class="muted">（{{ activeEntry['Topic/Queue'] }}/{{ activeEntry['分区/分片'] }}，消费组 {{ activeEntry['消费组'] }}）</span>
      </h3>

      <div class="credential-grid">
        <article
          v-for="credential in credentials"
          :key="credential.token"
          class="credential-card"
          :data-testid="`credential-${credential.role}`"
        >
          <header>
            <strong>{{ credential.role }}</strong>
            <span class="tag">{{ credential.state }}</span>
          </header>
          <p>来源：{{ credential['来源源'] }}</p>
          <p>允许区间：<code>{{ credential['允许区间'] }}</code></p>
          <p>接续边界：{{ credential['允许接续边界'] }}</p>
          <p>可共享区间：{{ credential['可共享区间'] }}</p>
          <p class="token-line">
            凭据：<code :data-testid="`token-${credential.role}`">{{ credential.token }}</code>
          </p>
          <button
            class="btn"
            type="button"
            :data-testid="`use-${credential.role}`"
            @click="useCredential(credential)"
          >
            用此凭据试读
          </button>
        </article>
      </div>

      <form class="analysis-form" @submit.prevent="doTrialRead">
        <label>
          <span>试读凭据</span>
          <select v-model="form.token" data-testid="trial-token">
            <option v-for="credential in credentials" :key="credential.token" :value="credential.token">
              {{ credential.role }}（{{ credential['允许区间'] }}）
            </option>
          </select>
        </label>
        <label>
          <span>试读起点</span>
          <input v-model.number="form.start" type="number" data-testid="trial-start" />
        </label>
        <label>
          <span>试读终点</span>
          <input v-model.number="form.end" type="number" data-testid="trial-end" />
        </label>
        <button class="btn primary" data-testid="trial-read" type="submit" :disabled="busy || !form.token">
          试读分区数据
        </button>
        <button class="btn" data-testid="checkpoint-trial" type="button" :disabled="busy || !form.token" @click="doCheckpointTrial">
          用检查点视角试读
        </button>
      </form>

      <div v-if="trial" class="trial-result" data-testid="trial-result">
        <p>
          <strong>{{ trial.role }}</strong> 凭据实际读到
          <code>{{ trial.read_offsets.join(', ') || '无' }}</code>；
          有效断点 <strong data-testid="effective-breakpoint">{{ trial.effective_breakpoint }}</strong>
        </p>
        <p>
          被拒绝的 offset：<code data-testid="denied-offsets">{{ trial.denied_offsets.join(', ') || '无' }}</code>
        </p>
        <ul class="denied-list">
          <li v-for="item in groupedDenials" :key="`${item.owner}-${item.reason}`">
            {{ item.range }}（归属 {{ item.owner }}）：{{ item.reason }}
          </li>
        </ul>
      </div>

      <form class="analysis-form" @submit.prevent="doConfirmCheckpoint">
        <label>
          <span>确认检查点值（offset 区间）</span>
          <input v-model="form.checkpointValue" data-testid="checkpoint-value" placeholder="如 46-50" />
        </label>
        <button class="btn primary" data-testid="confirm-checkpoint" type="submit" :disabled="busy || !form.token">
          以当前凭据确认检查点
        </button>
        <button class="btn" data-testid="available-history" type="button" :disabled="busy || !form.token" @click="loadHistory">
          查看可接续历史
        </button>
      </form>

      <div v-if="lastConfirm" class="trial-result" data-testid="confirm-result">
        <p :class="lastConfirm.ok ? 'ok-text' : 'error-text'">{{ lastConfirm.message }}</p>
        <p v-if="lastConfirm.owner">归属：{{ lastConfirm.owner }}</p>
      </div>

      <div v-if="history" class="trial-result" data-testid="history-result">
        <p>
          {{ history.role }} 可接续 <code>{{ history.available_range }}</code>，
          边界 {{ history.boundary }}，有效断点 {{ history.effective_breakpoint }}
        </p>
        <p>旧方在途区间 {{ history.old_inflight_range }}；可共享区间 {{ history.shareable_range }}</p>
      </div>

      <p v-if="activeEntry['冲突说明']" class="error-text" data-testid="conflict-note">
        {{ activeEntry['冲突说明'] }}
      </p>
      <p v-if="activeEntry['驳回原因']" class="error-text" data-testid="reject-note">
        驳回原因：{{ activeEntry['驳回原因'] }}
      </p>

      <details class="handoff-note">
        <summary>接口速查（脚本联调）</summary>
        <pre>{{ endpoints }}</pre>
      </details>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

import { listEntries } from '@/api/backend-service'
import {
  availableHistory,
  confirmCheckpointWithCredential,
  createCheckpoint,
  handoffAction,
  runDemoScenario,
  trialRead,
  trialReadCheckpoint,
  type CredentialResult,
  type HandoffCredential,
  type HandoffEntry,
  type TrialReadResult,
} from '@/api/handoff'

const rows = ref<HandoffEntry[]>([])
const activeId = ref<number | null>(null)
const credentials = ref<HandoffCredential[]>([])
const trial = ref<TrialReadResult | null>(null)
const history = ref<CredentialResult | null>(null)
const lastConfirm = ref<{ ok: boolean; message: string; owner?: string } | null>(null)
const errorMessage = ref('')
const notice = ref('')
const busy = ref(false)

const form = reactive({ token: '', start: 40 as number | undefined, end: 50 as number | undefined, checkpointValue: '46-50' })

const endpoints = [
  'POST /api/partition_handoff/demo',
  'POST /api/partition_handoff/{id}/accept | /confirm | /reject',
  'POST /api/partition_handoff/{id}/inflight        {token,start,end}',
  'POST /api/partition_handoff/{id}/resolve-inflight {token}',
  'POST /api/partition_handoff/{id}/trial-read       {token,start,end}',
  'POST /api/record_checkpoint/available-history     {token}',
  'POST /api/record_checkpoint/trial-read            {token,start,end}',
  'POST /api/record_checkpoint/{cpId}/confirm-with-credential {token}',
].join('\n')

const activeEntry = computed(() => rows.value.find((row) => row.id === activeId.value) ?? null)

const groupedDenials = computed(() => {
  if (!trial.value) return []
  const groups = new Map<string, { owner: string; reason: string; offsets: number[] }>()
  for (const item of trial.value.denied_ranges) {
    const key = `${item.owner}-${item.reason}`
    const group = groups.get(key) ?? { owner: item.owner, reason: item.reason, offsets: [] }
    group.offsets.push(item.offset)
    groups.set(key, group)
  }
  return [...groups.values()].map((group) => ({
    owner: group.owner,
    reason: group.reason,
    range: group.offsets.length ? `${group.offsets[0]}-${group.offsets[group.offsets.length - 1]}` : '无',
  }))
})

type HandoffAction = '受理移交' | '确认移交' | '驳回移交'

function actionsFor(row: HandoffEntry): HandoffAction[] {
  if (row.status === '待受理') return ['受理移交', '驳回移交']
  if (row.status === '已受理' || row.status === '待旧方收尾') return ['确认移交', '驳回移交']
  return []
}

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listEntries('partition_handoff', {})
    rows.value = payload.items as HandoffEntry[]
    if (activeId.value !== null) {
      const current = rows.value.find((row) => row.id === activeId.value)
      if (!current) {
        activeId.value = null
        credentials.value = []
      } else {
        await loadSummary(activeId.value)
      }
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '移交记录读取失败'
  }
}

async function loadSummary(handoffId: number) {
  const response = await fetch(`/api/partition_handoff/${handoffId}/summary`)
  if (!response.ok) return
  const payload = await response.json()
  credentials.value = (payload.credentials ?? []) as HandoffCredential[]
  if (!form.token && credentials.value.length) {
    form.token = credentials.value[0].token
  }
}

async function selectHandoff(row: HandoffEntry) {
  activeId.value = row.id
  trial.value = null
  history.value = null
  lastConfirm.value = null
  credentials.value = []
  form.token = ''
  await loadSummary(row.id)
}

function useCredential(credential: HandoffCredential) {
  form.token = credential.token
}

async function perform(row: HandoffEntry, action: HandoffAction) {
  busy.value = true
  errorMessage.value = ''
  notice.value = ''
  try {
    const result = await handoffAction(row.id, action)
    notice.value = result.message
    await reload()
    activeId.value = row.id
    await loadSummary(row.id)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '移交动作失败'
    await reload()
  } finally {
    busy.value = false
  }
}

async function loadDemo() {
  busy.value = true
  errorMessage.value = ''
  notice.value = ''
  try {
    const result = await runDemoScenario()
    notice.value = result.message
    if (result.conflict_before_resolve?.message) {
      notice.value += `；移交前冲突：${result.conflict_before_resolve.message}`
    }
    await reload()
    activeId.value = result.handoff_id
    await loadSummary(result.handoff_id)
    form.token = result.tokens['新接入方'] ?? result.tokens['旧接入方'] ?? ''
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '演示场景准备失败'
  } finally {
    busy.value = false
  }
}

async function doTrialRead() {
  if (activeId.value === null || !form.token) return
  busy.value = true
  errorMessage.value = ''
  try {
    trial.value = await trialRead(activeId.value, form.token, form.start, form.end)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '试读失败'
  } finally {
    busy.value = false
  }
}

async function doCheckpointTrial() {
  if (!form.token) return
  busy.value = true
  errorMessage.value = ''
  try {
    trial.value = await trialReadCheckpoint(form.token, form.start, form.end)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '检查点视角试读失败'
  } finally {
    busy.value = false
  }
}

async function doConfirmCheckpoint() {
  if (!form.token) return
  busy.value = true
  errorMessage.value = ''
  lastConfirm.value = null
  try {
    const entry = activeEntry.value
    if (!entry) return
    const created = await createCheckpoint({
      关联接入任务: `${tokenRoleSource()}@${entry['Topic/Queue']}/${entry['分区/分片']}#${entry['消费组']}`,
      检查点类型: '分区移交-凭据确认',
      检查点值: form.checkpointValue,
      来源源: tokenRoleSource(),
      消费组: entry['消费组'],
      'Topic/Queue': entry['Topic/Queue'],
      '分区/分片': entry['分区/分片'],
      移交记录: String(entry.id),
      凭据: form.token,
    })
    if (!created.ok || !created.entry) {
      lastConfirm.value = { ok: false, message: created.message }
      return
    }
    const result = await confirmCheckpointWithCredential(Number(created.entry.id), form.token)
    lastConfirm.value = { ok: result.ok, message: result.message, owner: result.owner }
    if (result.ok) await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '检查点确认失败'
  } finally {
    busy.value = false
  }
}

function tokenRoleSource(): string {
  const credential = credentials.value.find((item) => item.token === form.token)
  return credential?.['来源源'] ?? ''
}

async function loadHistory() {
  if (!form.token) return
  busy.value = true
  errorMessage.value = ''
  try {
    history.value = await availableHistory(form.token)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '可接续历史读取失败'
  } finally {
    busy.value = false
  }
}

onMounted(reload)
</script>

<style scoped>
.muted { color: var(--muted); font-weight: 400; font-size: 12px; }
.notice-text { color: #1f6feb; font-size: 13px; }
.ok-text { color: #15803d; }
.handoff-panel { margin-top: 16px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px; }
.credential-grid { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 12px; }
.credential-card { flex: 1; min-width: 260px; border: 1px solid var(--border); border-radius: 8px; padding: 12px; font-size: 13px; }
.credential-card header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
.credential-card p { margin: 4px 0; }
.tag { font-size: 11px; background: #eef2ff; color: #1f6feb; border-radius: 10px; padding: 1px 8px; }
.token-line code { word-break: break-all; }
.trial-result { margin: 10px 0; padding: 10px 12px; background: #f8fafc; border: 1px solid var(--border); border-radius: 8px; font-size: 13px; }
.trial-result p { margin: 4px 0; }
.denied-list { margin: 6px 0 0; padding-left: 18px; color: #b42318; }
.handoff-note { margin-top: 14px; font-size: 12px; color: var(--muted); }
.handoff-note pre { background: #f8fafc; border: 1px solid var(--border); border-radius: 6px; padding: 10px; overflow-x: auto; }
.data-table tr.selected { outline: 2px solid var(--brand); outline-offset: -2px; }
</style>
