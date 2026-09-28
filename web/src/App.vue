<script setup lang="ts">
// 应用外壳：顶部导航 + vhost 全局选择器 + 侧边菜单 + 登录框
import { computed, onMounted, ref, watch } from 'vue'
import { RouterView, useRoute, useRouter } from 'vue-router'
import { Connection, Odometer, Postcard, Share } from '@element-plus/icons-vue'
import LoginDialog from '@/components/LoginDialog.vue'
import { setUnauthorizedHandler } from '@/api/client'
import { useAuthStore } from '@/stores/auth'
import { useVhostStore } from '@/stores/vhost'

const auth = useAuthStore()
const vhostStore = useVhostStore()
const route = useRoute()
const router = useRouter()

/** 未认证（或凭据失效）时展示登录框 */
const loginVisible = ref(false)

/** 侧边菜单：index 使用路由名，便于高亮 */
const menuItems = [
  { index: 'overview', label: '概览', icon: Odometer },
  { index: 'queues', label: '队列', icon: Postcard },
  { index: 'exchanges', label: '交换机', icon: Share },
  { index: 'connections', label: '连接', icon: Connection },
]

/** 详情页高亮其所属的列表菜单 */
const activeMenu = computed<string>(() => {
  const name = String(route.name ?? '')
  if (name.startsWith('queue')) return 'queues'
  if (name.startsWith('exchange')) return 'exchanges'
  if (name === 'connections') return 'connections'
  return 'overview'
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

// 路由中的 vhost 反向同步到全局状态（支持直接粘贴 URL 访问）
watch(
  () => route.params.vhost,
  (value) => {
    if (typeof value === 'string' && value) vhostStore.setCurrent(value)
  },
)

// 认证成功后加载 vhost 列表
watch(
  () => auth.authenticated,
  (ok) => {
    if (ok) void vhostStore.loadVhosts()
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
          :disabled="!auth.authenticated"
          placeholder="选择 vhost"
        >
          <el-option v-for="item in vhostStore.vhosts" :key="item.name" :label="item.name" :value="item.name" />
        </el-select>
        <el-tag v-if="auth.user" type="info" effect="plain">当前用户：{{ auth.user }}</el-tag>
        <el-button :disabled="!auth.authenticated" @click="handleLogout">退出登录</el-button>
      </div>
    </el-header>

    <el-container class="app-body">
      <el-aside width="180px" class="app-aside">
        <el-menu :default-active="activeMenu" @select="handleMenuSelect">
          <el-menu-item v-for="item in menuItems" :key="item.index" :index="item.index">
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
</template>
