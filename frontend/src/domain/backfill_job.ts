/** 补数任务 领域差异：值班人提交实际读取的版本、复查补数执行记录。
 *
 * 后端对应接口见 backend/app/routers/backfill_compat.py。
 */
import { request } from '@/api/client'
import type { ActionResult } from '@/data/types'
import type { SchemaField } from './schema_registry'

/** 单次执行记录：按补数兼容版本索引核定后的留痕。 */
export interface BackfillExecution {
  executed_at: string
  schema_version: string
  written_fields: SchemaField[]
  source_partition: string
  核定方式: string
}

/** 补数执行记录：所用版本、实际写入字段清单、来源分区与历次执行。 */
export interface BackfillExecutionRecord {
  job_id: string
  asset_id: string
  interval: string
  status: string
  source_partition: string
  field_requirement: string
  schema_version: string
  written_fields: SchemaField[]
  updated_at: string
  executions: BackfillExecution[]
}

/** 值班人提交实际读取的版本：校验通过后固定到补数作业。 */
export async function bindSchemaVersion(
  jobId: number,
  versionId: string,
  verifiedExtra: string[] = [],
): Promise<ActionResult> {
  const response = await request('/api/backfill-compat/bind', {
    method: 'POST',
    body: JSON.stringify({
      job_id: jobId,
      version_id: versionId,
      verified_extra: verifiedExtra,
    }),
  })
  return (await response.json()) as ActionResult
}

/** 复查补数执行记录：已有作业始终按当时选择的版本重跑。 */
export async function fetchExecutionRecord(
  jobId: number,
): Promise<BackfillExecutionRecord> {
  const response = await request(`/api/backfill-compat/jobs/${jobId}/execution`)
  if (!response.ok) throw new Error('补数执行记录读取失败')
  return (await response.json()) as BackfillExecutionRecord
}
