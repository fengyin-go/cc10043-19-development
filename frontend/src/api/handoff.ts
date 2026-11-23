/** 分区移交接口：通用资源路由之外的动作、凭据试读和检查点确认。 */
import { request } from './client'
import type { EntryRow } from '@/data/types'

export type HandoffCredential = {
  token: string
  role: '旧接入方' | '新接入方'
  handoff_id: number
  来源源: string
  对端源: string
  消费组: string
  'Topic/Queue': string
  '分区/分片': string
  允许区间: string
  允许接续边界: number
  旧方在途区间: string
  可共享区间: string
  state: string
}

export type HandoffEntry = EntryRow & {
  来源源: string
  目标源: string
  消费组: string
  'Topic/Queue': string
  '分区/分片': string
  旧方归属区间: string
  允许接续区间: string
  允许接续边界: number
}

export type DemoResult = {
  ok: boolean
  message: string
  handoff_id: number
  tokens: Record<string, string>
  credentials: HandoffCredential[]
  conflict_before_resolve: { ok: boolean; status: string; message: string }
}

export type TrialReadResult = {
  ok: boolean
  message: string
  role?: string
  source?: string
  allowed_range?: string
  boundary?: number
  read_offsets: number[]
  denied_offsets: number[]
  denied_ranges: { offset: number; owner: string; reason: string }[]
  effective_breakpoint?: number
}

export type CredentialResult = {
  ok: boolean
  message: string
  role?: string
  source?: string
  available_range?: string
  boundary?: number
  effective_breakpoint?: number
  old_inflight_range?: string
  [key: string]: unknown
}

async function post<T>(path: string, body: Record<string, unknown> = {}): Promise<T> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  if (!response.ok) {
    const detail = await response.text()
    throw new Error(detail || `接口返回 ${response.status}`)
  }
  return (await response.json()) as T
}

export async function runDemoScenario(): Promise<DemoResult> {
  return await post<DemoResult>('/api/partition_handoff/demo')
}

export async function handoffAction(
  handoffId: number,
  action: '受理移交' | '确认移交' | '驳回移交',
): Promise<{ ok: boolean; message: string; entry?: HandoffEntry }> {
  const path =
    action === '受理移交'
      ? `/api/partition_handoff/${handoffId}/accept`
      : action === '确认移交'
        ? `/api/partition_handoff/${handoffId}/confirm`
        : `/api/partition_handoff/${handoffId}/reject`
  return await post(path)
}

export async function trialRead(
  handoffId: number,
  token: string,
  start?: number,
  end?: number,
): Promise<TrialReadResult> {
  return await post<TrialReadResult>(`/api/partition_handoff/${handoffId}/trial-read`, {
    token,
    start,
    end,
  })
}

export async function availableHistory(token: string): Promise<CredentialResult> {
  return await post<CredentialResult>('/api/record_checkpoint/available-history', { token })
}

export async function trialReadCheckpoint(
  token: string,
  start?: number,
  end?: number,
): Promise<TrialReadResult> {
  return await post<TrialReadResult>('/api/record_checkpoint/trial-read', { token, start, end })
}

export async function confirmCheckpointWithCredential(
  checkpointId: number,
  token: string,
  start?: number,
  end?: number,
): Promise<CredentialResult & { conflict?: boolean; owner?: string }> {
  return await post(`/api/record_checkpoint/${checkpointId}/confirm-with-credential`, {
    token,
    start,
    end,
  })
}

export async function createCheckpoint(values: Record<string, string>): Promise<{
  ok: boolean
  message: string
  entry?: EntryRow
}> {
  return await post('/api/record_checkpoint', { values })
}
