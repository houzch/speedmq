// 管理接口功能组定义。
//
// 注意：这里的功能组 id 必须与后端 internal/management/server.go 的 apiGroup* 常量保持一致，
// 增删或改名时两边要同步，否则授权界面勾中的组名会被后端 normalizeAPIGroups 静默丢弃。
// 名称与说明的文案在 locales/*.json 的 apiGroup.<id>.label / .desc 下，随界面语言切换。

import { i18n } from '@/locales'

/** 单个管理接口功能组：id（与后端一致） */
export interface ApiGroupInfo {
  id: string
}

/** 固定 10 个功能组，顺序与授权界面一致 */
export const API_GROUPS: ApiGroupInfo[] = [
  { id: 'overview' },
  { id: 'topology' },
  { id: 'connections' },
  { id: 'accounts' },
  { id: 'policies' },
  { id: 'vhosts' },
  { id: 'limits' },
  { id: 'feature_flags' },
  { id: 'cluster' },
  { id: 'plugins' },
]

/** 返回功能组名称；缺文案时原样返回 id，便于排查前后端不一致 */
export function apiGroupLabel(id: string): string {
  const key = `apiGroup.${id}.label`
  const label = i18n.global.t(key)
  return label === key ? id : label
}

/** 返回功能组说明；缺文案时返回空串 */
export function apiGroupDesc(id: string): string {
  const key = `apiGroup.${id}.desc`
  const desc = i18n.global.t(key)
  return desc === key ? '' : desc
}
