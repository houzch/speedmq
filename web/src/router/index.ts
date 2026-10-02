// 路由：产物由静态文件服务托管，必须使用 hash 模式

import { createRouter, createWebHashHistory } from 'vue-router'
import type { RouteRecordRaw } from 'vue-router'

const routes: RouteRecordRaw[] = [
  { path: '/', redirect: '/overview' },
  {
    path: '/overview',
    name: 'overview',
    component: () => import('@/views/OverviewView.vue'),
  },
  {
    path: '/queues/:vhost',
    name: 'queues',
    component: () => import('@/views/QueuesView.vue'),
  },
  {
    path: '/queues/:vhost/:name',
    name: 'queue-detail',
    component: () => import('@/views/QueueDetailView.vue'),
  },
  {
    path: '/exchanges/:vhost',
    name: 'exchanges',
    component: () => import('@/views/ExchangesView.vue'),
  },
  {
    path: '/exchanges/:vhost/:name',
    name: 'exchange-detail',
    component: () => import('@/views/ExchangeDetailView.vue'),
  },
  {
    path: '/cluster',
    name: 'cluster',
    component: () => import('@/views/ClusterView.vue'),
  },
  {
    path: '/connections',
    name: 'connections',
    component: () => import('@/views/ConnectionsView.vue'),
  },
  {
    path: '/users',
    name: 'users',
    component: () => import('@/views/UsersView.vue'),
  },
  {
    path: '/vhosts',
    name: 'vhosts',
    component: () => import('@/views/VHostsView.vue'),
  },
  {
    path: '/policies',
    name: 'policies',
    component: () => import('@/views/PoliciesView.vue'),
  },
  {
    path: '/limits',
    name: 'limits',
    component: () => import('@/views/LimitsView.vue'),
  },
  {
    path: '/feature-flags',
    name: 'feature-flags',
    component: () => import('@/views/FeatureFlagsView.vue'),
  },
  {
    path: '/deprecated-features',
    name: 'deprecated-features',
    component: () => import('@/views/DeprecatedFeaturesView.vue'),
  },
  { path: '/:pathMatch(.*)*', redirect: '/overview' },
]

const router = createRouter({
  history: createWebHashHistory(),
  routes,
})

export default router
