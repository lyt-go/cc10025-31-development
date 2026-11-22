/** Schema注册 领域差异：补数兼容版本索引的查询比较与字段校验。
 *
 * 后端对应接口见 backend/app/routers/backfill_compat.py；
 * 这里保留类型与请求封装，供工作台后续的补数版本选择界面复用。
 */
import { request } from '@/api/client'

/** 字段清单里的一列：name:type。 */
export interface SchemaField {
  name: string
  type: string
}

/** 需求列核验明细里的一行。 */
export interface ColumnCheck {
  name: string
  type?: string
  来源?: string
  核验: string
  说明: string
}

/** 补数兼容版本索引里的候选版本。 */
export interface CompatCandidate {
  版本编号: string
  注册时间: string
  /** 恢复原字段 / 旧字段+衍生列 / 不可用 */
  匹配方式: string
  可用: boolean
  覆盖列: string[]
  缺失列: string[]
  类型不符列: { name: string; require_type: string; version_type: string }[]
  衍生列: SchemaField[]
  核验结果: {
    需求列核验: string
    需求列明细: ColumnCheck[]
    衍生列核验: string
  }
  /** 两套结果的消费口径可能冲突，这里给出该版本的口径说明。 */
  口径提示: string
}

/** 补数兼容版本索引：按旧分区字段需求过滤候选并排序后的结果。 */
export interface BackfillCompatIndex {
  asset_id: string
  partition: string
  required_fields: SchemaField[]
  generated_at: string
  排序说明: string
  candidates: CompatCandidate[]
}

/** 字段校验结果：可写入字段清单只含已核验列。 */
export interface ValidationResult {
  asset_id: string
  partition: string
  version_id: string
  结果: '通过' | '不通过'
  columns: ColumnCheck[]
  可写入字段清单: SchemaField[]
  verified_extra: string[]
  未匹配的核验列: string[]
  口径提示: string
  validated_at: string
}

/** 按旧分区的字段要求过滤候选版本，返回补数兼容版本索引。 */
export async function fetchCompatCandidates(
  assetId: string,
  partition: string,
  required: string,
): Promise<BackfillCompatIndex> {
  const query = new URLSearchParams({
    asset_id: assetId,
    partition,
    required,
  }).toString()
  const response = await request(`/api/backfill-compat/candidates?${query}`)
  if (!response.ok) throw new Error('补数兼容版本索引读取失败')
  return (await response.json()) as BackfillCompatIndex
}

/** 值班人字段校验：verifiedExtra 里显式确认的衍生列才会进入可写入清单。 */
export async function validateCandidate(
  assetId: string,
  partition: string,
  versionId: string,
  required: string,
  verifiedExtra: string[] = [],
): Promise<ValidationResult> {
  const response = await request('/api/backfill-compat/validate', {
    method: 'POST',
    body: JSON.stringify({
      asset_id: assetId,
      partition,
      version_id: versionId,
      required,
      verified_extra: verifiedExtra,
    }),
  })
  if (!response.ok) throw new Error('字段校验请求失败')
  return (await response.json()) as ValidationResult
}
