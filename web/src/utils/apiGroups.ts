// 管理接口功能组定义。
//
// 注意：这里的功能组 id 必须与后端 internal/management/server.go 的 apiGroup* 常量保持一致，
// 增删或改名时两边要同步，否则授权界面勾中的组名会被后端 normalizeAPIGroups 静默丢弃。

/** 单个管理接口功能组：id（与后端一致）、中文名、给用户看的说明 */
export interface ApiGroupInfo {
  id: string
  label: string
  desc: string
}

/** 固定 10 个功能组，顺序与授权界面一致 */
export const API_GROUPS: ApiGroupInfo[] = [
  { id: 'overview', label: '概览与节点', desc: '概览页、节点状态、Prometheus 指标' },
  {
    id: 'topology',
    label: '队列与交换机',
    desc: '队列/交换机/绑定/消费者；含清空、删除、发布、取消息、仲裁副本扩缩',
  },
  { id: 'connections', label: '连接与通道', desc: '查看连接与通道；含强制关闭连接' },
  { id: 'accounts', label: '账号与权限', desc: '建号、改密/改名、启用停用、配置 vhost 权限' },
  { id: 'policies', label: '策略', desc: '策略的增删改查' },
  { id: 'vhosts', label: '虚拟主机', desc: '新建/删除 vhost' },
  { id: 'limits', label: '虚拟主机限制', desc: '按 vhost 配置连接数与队列数上限' },
  { id: 'feature_flags', label: '特性开关', desc: '特性开关的查看与启停、弃用特性清单' },
  { id: 'cluster', label: '集群', desc: '集群状态与成员变更' },
  { id: 'plugins', label: '插件', desc: '插件启用/停用' },
]

/** 返回功能组中文名；未知 id 原样返回，便于排查前后端不一致 */
export function apiGroupLabel(id: string): string {
  return API_GROUPS.find((group) => group.id === id)?.label ?? id
}
