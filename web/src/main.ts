import { createApp } from 'vue'
import { createPinia } from 'pinia'
import App from './App.vue'
import router from './router'
import { applyInitialLocale, i18n, loadServerDefaultLocale } from './locales'
import './styles/main.css'

// 先把 <html lang/dir> 与初始语言对齐，再挂载：避免阿拉伯语先按 LTR 渲染再翻转。
applyInitialLocale()

const app = createApp(App)

app.use(createPinia())
app.use(router)
app.use(i18n)
app.mount('#app')

// 登录前就拉一次服务端"安装时按系统时区推断"的默认语言；
// 只有用户从未手动选择过语言时才会应用（见 locales/index.ts）。
void loadServerDefaultLocale()
