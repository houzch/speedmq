<script setup lang="ts">
// 应用外壳：顶部导航 + vhost 全局选择器 + 侧边菜单 + 登录框
import { computed, onMounted, ref, watch } from 'vue'
import type { Component } from 'vue'
import { RouterView, useRoute, useRouter } from 'vue-router'
import { ArrowDown, Connection, Files, Grid, Histogram, Odometer, Postcard, SetUp, Share, SwitchButton, User, Warning } from '@element-plus/icons-vue'
import LoginDialog from '@/components/LoginDialog.vue'
import ForcePasswordDialog from '@/components/ForcePasswordDialog.vue'
import ChangePasswordDialog from '@/components/ChangePasswordDialog.vue'
import { setUnauthorizedHandler } from '@/api/client'
import { useAuthStore } from '@/stores/auth'
import { useVhostStore } from '@/stores/vhost'
import { REFRESH_OPTIONS, useRefreshStore } from '@/stores/refresh'
import { formatTimestamp } from '@/utils/format'

const auth = useAuthStore()
const vhostStore = useVhostStore()
const refresh = useRefreshStore()
const route = useRoute()
const router = useRouter()

/** 自动刷新间隔选择器：读全局 store，写入时做合法性校验并持久化 */
const refreshInterval = computed<number>({
  get: () => refresh.intervalSeconds,
  set: (value: number) => refresh.setIntervalSeconds(value),
})

/** 未认证（或凭据失效）时展示登录框 */
const loginVisible = ref(false)

/** 侧边菜单：index 使用路由名，便于高亮；group 是对应的管理接口功能组 */
const menuItems: { index: string; label: string; icon: Component; group: string }[] = [
  { index: 'overview', label: '概览', icon: Odometer, group: 'overview' },
  { index: 'queues', label: '队列', icon: Postcard, group: 'topology' },
  { index: 'exchanges', label: '交换机', icon: Share, group: 'topology' },
  { index: 'connections', label: '连接', icon: Connection, group: 'connections' },
  { index: 'users', label: '账号', icon: User, group: 'accounts' },
  { index: 'vhosts', label: '虚拟主机', icon: Files, group: 'vhosts' },
  { index: 'policies', label: '策略', icon: SetUp, group: 'policies' },
  { index: 'limits', label: '限制', icon: Histogram, group: 'limits' },
  { index: 'feature-flags', label: '特性开关', icon: SwitchButton, group: 'feature_flags' },
  { index: 'deprecated-features', label: '弃用特性', icon: Warning, group: 'feature_flags' },
  { index: 'cluster', label: '集群', icon: Grid, group: 'cluster' },
]

/** 路由名 → 所属功能组（详情页归到其列表页的功能组） */
const ROUTE_GROUPS: Record<string, string> = {
  overview: 'overview',
  queues: 'topology',
  'queue-detail': 'topology',
  exchanges: 'topology',
  'exchange-detail': 'topology',
  cluster: 'cluster',
  connections: 'connections',
  users: 'accounts',
  vhosts: 'vhosts',
  policies: 'policies',
  limits: 'limits',
  'feature-flags': 'feature_flags',
  'deprecated-features': 'feature_flags',
}

/** 当前路由所属的功能组；未知路由返回 null（不参与权限收窄） */
function currentRouteGroup(): string | null {
  return ROUTE_GROUPS[String(route.name ?? '')] ?? null
}

/**
 * 落到无权访问的页面会一直 403，因此认证成功或切换账号后，
 * 若当前路由对应的功能组不可访问，就跳到第一个可访问的页面（都没有则停在概览）。
 */
async function ensureAccessibleRoute(): Promise<void> {
  if (!auth.authenticated || auth.mustChangePassword) return
  const group = currentRouteGroup()
  if (group === null || auth.canAccessApi(group)) return
  const target = menuItems.find((item) => auth.canAccessApi(item.group)) ?? menuItems[0]
  if (target.index === 'queues' || target.index === 'exchanges') {
    // 队列/交换机页需要 vhost 参数：先确保列表已加载，避免落到不存在的 vhost
    if (!vhostStore.loaded) {
      try {
        await vhostStore.loadVhosts()
      } catch {
        // 加载失败时退回默认 vhost，vhost 选择器与列表页会各自给出提示
      }
    }
    await router.replace({ name: target.index, params: { vhost: vhostStore.current } })
    return
  }
  await router.replace({ name: target.index })
}

/** 详情页高亮其所属的列表菜单 */
const activeMenu = computed<string>(() => {
  const name = String(route.name ?? '')
  if (name.startsWith('queue')) return 'queues'
  if (name.startsWith('exchange')) return 'exchanges'
  // 其余页面（含集群、虚拟主机、策略、限制、特性开关…）的菜单 index 就是路由名；
  // 认不出来时退回概览，避免高亮消失。
  return menuItems.some((item) => item.index === name) ? name : 'overview'
})

/** 当前 vhost：列表/详情路由参数优先，其次取全局状态 */
const currentVhost = computed<string>({
  get: () => {
    const fromRoute = route.params.vhost
    if (typeof fromRoute === 'string' && fromRoute) return fromRoute
    return vhostStore.current
  },
  set: (value: string) => {
    void handleVhostChange(value)
  },
})

/** 切换 vhost：同步路由（列表页原地替换，详情页回到列表） */
async function handleVhostChange(value: string): Promise<void> {
  vhostStore.setCurrent(value)
  const name = String(route.name ?? '')
  if (name === 'queues' || name === 'exchanges') {
    await router.replace({ name, params: { vhost: value } })
    return
  }
  if (name.startsWith('queue')) {
    await router.push({ name: 'queues', params: { vhost: value } })
    return
  }
  if (name.startsWith('exchange')) {
    await router.push({ name: 'exchanges', params: { vhost: value } })
  }
}

function handleMenuSelect(index: string): void {
  if (index === 'queues' || index === 'exchanges') {
    void router.push({ name: index, params: { vhost: currentVhost.value } })
    return
  }
  void router.push({ name: index })
}

function handleLogout(): void {
  auth.logout()
  loginVisible.value = true
}

/** 「修改密码」对话框是否可见 */
const changePasswordVisible = ref(false)

/** 处理顶栏「当前用户」下拉的选择 */
function handleUserCommand(command: string | number | object): void {
  if (command === 'password') {
    changePasswordVisible.value = true
    return
  }
  handleLogout()
}

// 路由中的 vhost 反向同步到全局状态（支持直接粘贴 URL 访问）
watch(
  () => route.params.vhost,
  (value) => {
    if (typeof value === 'string' && value) vhostStore.setCurrent(value)
  },
)

// 认证成功后加载 vhost 列表；强制改密期间不加载任何业务数据
watch(
  () => [auth.authenticated, auth.mustChangePassword] as const,
  ([ok, must]) => {
    if (ok && !must) void vhostStore.loadVhosts()
  },
  { immediate: true },
)

// 认证成功或切换账号后，若当前路由的功能组不可访问，自动挪到第一个可访问的页面
watch(
  () => [auth.authenticated, auth.mustChangePassword, auth.apiGroups] as const,
  () => {
    void ensureAccessibleRoute()
  },
  { immediate: true },
)

onMounted(async () => {
  // 401 → 清空凭据并重新弹框
  setUnauthorizedHandler(() => {
    auth.clearLocal()
    loginVisible.value = true
  })

  if (auth.hasStoredCredentials()) {
    try {
      await auth.verify()
    } catch {
      // 401 已由统一处理器弹出登录框，这里无需再处理
    }
  } else {
    loginVisible.value = true
  }
})
</script>

<template>
  <el-container class="app-container">
    <el-header class="app-header">
      <div class="app-brand">
        <span class="app-logo">SwiftMQ</span>
        <span class="app-subtitle">管理后台</span>
      </div>
      <div class="app-header-actions">
        <span class="vhost-label">虚拟主机</span>
        <el-select
          v-model="currentVhost"
          class="vhost-select"
          :disabled="!auth.authenticated || auth.mustChangePassword"
          placeholder="选择 vhost"
        >
          <el-option v-for="item in vhostStore.vhosts" :key="item.name" :label="item.name" :value="item.name" />
        </el-select>
        <template v-if="auth.authenticated">
          <span class="refresh-label">
            已刷新 {{ refresh.lastRefreshed === null ? '—' : formatTimestamp(refresh.lastRefreshed) }}
          </span>
          <el-select v-model="refreshInterval" class="refresh-select">
            <el-option v-for="item in REFRESH_OPTIONS" :key="item.seconds" :label="item.label" :value="item.seconds" />
          </el-select>
          <el-dropdown trigger="click" @command="handleUserCommand">
            <span class="user-trigger">
              <el-icon><User /></el-icon>
              当前用户：{{ auth.user }}
              <el-icon class="user-caret"><ArrowDown /></el-icon>
            </span>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item command="password">修改密码</el-dropdown-item>
                <el-dropdown-item command="logout" divided>退出登录</el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
        </template>
      </div>
    </el-header>

    <el-container class="app-body">
      <el-aside width="180px" class="app-aside">
        <el-menu :default-active="activeMenu" @select="handleMenuSelect">
          <el-menu-item
            v-for="item in menuItems"
            :key="item.index"
            :index="item.index"
            :disabled="auth.mustChangePassword || !auth.canAccessApi(item.group)"
            :title="auth.canAccessApi(item.group) ? undefined : '当前账号未被授予该模块权限'"
          >
            <el-icon><component :is="item.icon" /></el-icon>
            <span>{{ item.label }}</span>
          </el-menu-item>
        </el-menu>
      </el-aside>
      <el-main class="app-main">
        <router-view />
      </el-main>
    </el-container>
  </el-container>

  <LoginDialog v-model="loginVisible" />
  <ChangePasswordDialog v-model="changePasswordVisible" />
  <ForcePasswordDialog v-if="auth.mustChangePassword" />
</template>

<style scoped>
/* 顶栏自动刷新控件：字号与颜色跟「虚拟主机」标签保持一致 */
.refresh-label {
  font-size: 13px;
  color: #a3b1c2;
  white-space: nowrap;
}

.refresh-select {
  width: 140px;
}

/* 顶栏「当前用户」下拉：可点击 → 修改密码 / 退出登录 */
.user-trigger {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 14px;
  color: #fff;
  cursor: pointer;
  outline: none;
  white-space: nowrap;
}

.user-trigger:hover {
  opacity: 0.85;
}

.user-caret {
  font-size: 12px;
}
</style>
