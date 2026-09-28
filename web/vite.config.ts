import { fileURLToPath, URL } from 'node:url'
import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import AutoImport from 'unplugin-auto-import/vite'
import Components from 'unplugin-vue-components/vite'
import { ElementPlusResolver } from 'unplugin-vue-components/resolvers'

// 构建产物由 SwiftMQ 内核通过 :15672 以静态文件方式托管，
// 因此必须使用相对 base，避免部署在子路径时资源 404。
export default defineConfig({
  base: './',
  plugins: [
    vue(),
    AutoImport({
      // 自动导入 Vue / Vue Router / Pinia 的组合式 API
      imports: ['vue', 'vue-router', 'pinia'],
      // 按需自动导入 ElMessage / ElMessageBox 等函数式组件（含样式）
      resolvers: [ElementPlusResolver()],
      dts: 'auto-imports.d.ts',
    }),
    Components({
      // 按需自动注册 Element Plus 组件（模板中直接写 <el-xxx>）
      resolvers: [ElementPlusResolver()],
      dts: 'components.d.ts',
    }),
  ],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  server: {
    port: 5173,
    // 开发态把管理 API 与指标接口代理到本地内核
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:15672',
        changeOrigin: true,
      },
      '/metrics': {
        target: 'http://127.0.0.1:15672',
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    chunkSizeWarningLimit: 1500,
  },
})
