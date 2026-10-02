// 国际化基础设施：vue-i18n 实例、受支持语言清单、初始语言解析与 Element Plus locale 映射。
//
// 初始语言的解析顺序（先命中者优先）：
//   1. 用户在本机手动选择过（localStorage，见 setLocale 的 persist）；
//   2. 服务端安装时按部署地时区推断的默认值（GET /api/default-language，免认证）；
//   3. 浏览器语言；
//   4. 浏览器时区；
//   5. 兜底 en。
// 服务端默认值在应用启动后异步拉取：它只影响"用户没选过"的场景，因此不会覆盖用户的手动选择。

import { createI18n } from 'vue-i18n'
import type { Language } from 'element-plus/es/locale'
import type { DefaultLanguage } from '@/api/types'
import elZhCn from 'element-plus/es/locale/lang/zh-cn'
import elZhTw from 'element-plus/es/locale/lang/zh-tw'
import elEn from 'element-plus/es/locale/lang/en'
import elJa from 'element-plus/es/locale/lang/ja'
import elKo from 'element-plus/es/locale/lang/ko'
import elEs from 'element-plus/es/locale/lang/es'
import elDe from 'element-plus/es/locale/lang/de'
import elFr from 'element-plus/es/locale/lang/fr'
import elAr from 'element-plus/es/locale/lang/ar'
import elRu from 'element-plus/es/locale/lang/ru'
import elIt from 'element-plus/es/locale/lang/it'
import elNl from 'element-plus/es/locale/lang/nl'
import elPt from 'element-plus/es/locale/lang/pt'
import elId from 'element-plus/es/locale/lang/id'
import elTh from 'element-plus/es/locale/lang/th'
import elVi from 'element-plus/es/locale/lang/vi'
import elMs from 'element-plus/es/locale/lang/ms'

import zhCN from './zh-CN.json'
import zhTW from './zh-TW.json'
import en from './en.json'
import ja from './ja.json'
import ko from './ko.json'
import es from './es.json'
import de from './de.json'
import fr from './fr.json'
import ar from './ar.json'
import ru from './ru.json'
import it from './it.json'
import nl from './nl.json'
import pt from './pt.json'
import id from './id.json'
import th from './th.json'
import vi from './vi.json'
import ms from './ms.json'
import fil from './fil.json'

/** 用户在浏览器里手动选择的语言（localStorage 键） */
const STORAGE_KEY = 'swiftmq.locale'

/** 兜底语言：任何解析失败都落回它 */
export const FALLBACK_LOCALE = 'en'

/** 单个受支持语言的元数据 */
export interface LocaleMeta {
  /** 语言 code，与 internal/config/language.go 的 supportedLanguages 一一对应 */
  code: string
  /** 语言自身的名称，下拉按母语展示 */
  name: string
  /** Element Plus 组件文案；fil 无对应包，回退 en */
  element: Language
  /** 文本方向：阿拉伯语为 rtl */
  dir: 'ltr' | 'rtl'
}

/** 受支持语言（顺序即语言下拉的展示顺序） */
export const SUPPORTED_LOCALES: LocaleMeta[] = [
  { code: 'zh-CN', name: '简体中文', element: elZhCn, dir: 'ltr' },
  { code: 'zh-TW', name: '繁體中文', element: elZhTw, dir: 'ltr' },
  { code: 'en', name: 'English', element: elEn, dir: 'ltr' },
  { code: 'ja', name: '日本語', element: elJa, dir: 'ltr' },
  { code: 'ko', name: '한국어', element: elKo, dir: 'ltr' },
  { code: 'es', name: 'Español', element: elEs, dir: 'ltr' },
  { code: 'de', name: 'Deutsch', element: elDe, dir: 'ltr' },
  { code: 'fr', name: 'Français', element: elFr, dir: 'ltr' },
  { code: 'ar', name: 'العربية', element: elAr, dir: 'rtl' },
  { code: 'ru', name: 'Русский', element: elRu, dir: 'ltr' },
  { code: 'it', name: 'Italiano', element: elIt, dir: 'ltr' },
  { code: 'nl', name: 'Nederlands', element: elNl, dir: 'ltr' },
  { code: 'pt', name: 'Português', element: elPt, dir: 'ltr' },
  { code: 'id', name: 'Bahasa Indonesia', element: elId, dir: 'ltr' },
  { code: 'th', name: 'ไทย', element: elTh, dir: 'ltr' },
  { code: 'vi', name: 'Tiếng Việt', element: elVi, dir: 'ltr' },
  { code: 'ms', name: 'Bahasa Melayu', element: elMs, dir: 'ltr' },
  { code: 'fil', name: 'Filipino', element: elEn, dir: 'ltr' },
]

/** 消息目录：code → 扁平键值表 */
const messages: Record<string, Record<string, string>> = {
  'zh-CN': zhCN,
  'zh-TW': zhTW,
  en,
  ja,
  ko,
  es,
  de,
  fr,
  ar,
  ru,
  it,
  nl,
  pt,
  id,
  th,
  vi,
  ms,
  fil,
}

/** 是否受支持 */
export function isSupportedLocale(code: string | null | undefined): boolean {
  return code !== null && code !== undefined && SUPPORTED_LOCALES.some((item) => item.code === code)
}

/** 取语言元数据；未知 code 返回 en */
export function localeMeta(code: string): LocaleMeta {
  return SUPPORTED_LOCALES.find((item) => item.code === code) ?? SUPPORTED_LOCALES[2]
}

/** 把浏览器语言标签归一化到受支持的语言 code（如 zh-Hans → zh-CN） */
function normalizeLanguageTag(tag: string | null | undefined): string | null {
  if (!tag) return null
  const lower = tag.toLowerCase()
  if (lower === 'zh' || lower.startsWith('zh-cn') || lower.startsWith('zh-hans') || lower === 'zh-sg') return 'zh-CN'
  if (lower.startsWith('zh-tw') || lower.startsWith('zh-hk') || lower.startsWith('zh-mo') || lower.startsWith('zh-hant')) {
    return 'zh-TW'
  }
  const base = lower.split('-')[0]
  return isSupportedLocale(base) ? base : null
}

/** 按 IANA 时区名推断语言（服务端默认值不可用时的兜底；规则与后端保持同一口径但不穷举） */
function languageForTimezone(tz: string): string | null {
  const exact: Record<string, string> = {
    'Asia/Shanghai': 'zh-CN',
    'Asia/Chongqing': 'zh-CN',
    'Asia/Harbin': 'zh-CN',
    'Asia/Urumqi': 'zh-CN',
    'Asia/Taipei': 'zh-TW',
    'Asia/Hong_Kong': 'zh-TW',
    'Asia/Macau': 'zh-TW',
    'Asia/Tokyo': 'ja',
    'Asia/Seoul': 'ko',
    'Asia/Jakarta': 'id',
    'Asia/Makassar': 'id',
    'Asia/Bangkok': 'th',
    'Asia/Ho_Chi_Minh': 'vi',
    'Asia/Kuala_Lumpur': 'ms',
    'Asia/Manila': 'fil',
    'Europe/Moscow': 'ru',
    'Europe/Berlin': 'de',
    'Europe/Vienna': 'de',
    'Europe/Zurich': 'de',
    'Europe/Paris': 'fr',
    'Europe/Brussels': 'fr',
    'Europe/Rome': 'it',
    'Europe/Amsterdam': 'nl',
    'Europe/Lisbon': 'pt',
    'America/Sao_Paulo': 'pt',
    'Europe/Madrid': 'es',
    'America/Mexico_City': 'es',
    'America/Bogota': 'es',
    'America/Argentina/Buenos_Aires': 'es',
    'Africa/Cairo': 'ar',
    'Asia/Dubai': 'ar',
    'Asia/Riyadh': 'ar',
  }
  return exact[tz] ?? null
}

/** 读取用户手动选择的语言；未选择或非法返回 null */
function storedLocale(): string | null {
  const raw = localStorage.getItem(STORAGE_KEY)
  return isSupportedLocale(raw) ? raw : null
}

/** 解析"应用启动时"的初始语言（不含服务端默认值，那一项随后异步应用） */
function resolveInitialLocale(): string {
  const stored = storedLocale()
  if (stored) return stored

  const browser = normalizeLanguageTag(navigator.language) ?? normalizeLanguageTag(navigator.languages?.[0])
  if (browser) return browser

  try {
    const tz = Intl.DateTimeFormat().resolvedOptions().timeZone
    const byTz = languageForTimezone(tz)
    if (byTz) return byTz
  } catch {
    // Intl 不可用时忽略，落回兜底语言
  }
  return FALLBACK_LOCALE
}

export const i18n = createI18n({
  legacy: false,
  globalInjection: true,
  // 语言包使用"扁平点号键"（如 menu.overview），flatJson 让 t() 按字面量解析而不去拆嵌套。
  flatJson: true,
  locale: resolveInitialLocale(),
  fallbackLocale: FALLBACK_LOCALE,
  messages,
})

/** 应用当前语言 */
function currentLocale(): string {
  return String(i18n.global.locale.value)
}

/**
 * 切换语言：更新 i18n、<html lang/dir>，并在 persist 时写入 localStorage。
 *
 * 初始语言（浏览器/服务端推断出的）不写入 localStorage —— 否则会把它误当成
 * "用户手动选择"，从此再也不跟随服务端默认值。只有用户在界面上主动切换才 persist。
 */
export function setLocale(code: string, persist = true): void {
  if (!isSupportedLocale(code)) return
  i18n.global.locale.value = code
  const meta = localeMeta(code)
  document.documentElement.setAttribute('lang', code)
  document.documentElement.setAttribute('dir', meta.dir)
  // 标签页标题跟随语言：产品名 + 本地化的"管理后台"
  document.title = `SwiftMQ ${i18n.global.t('app.subtitle')}`
  if (persist) localStorage.setItem(STORAGE_KEY, code)
}

/** 应用启动时把 <html lang/dir> 与初始语言对齐（不写 localStorage） */
export function applyInitialLocale(): void {
  setLocale(currentLocale(), false)
}

/**
 * 拉取服务端"安装时按系统时区推断"的默认语言并在用户未手动选择时应用。
 *
 * 该接口免认证，因此登录前即可生效；失败时静默保持当前语言（不影响可用性）。
 */
export async function loadServerDefaultLocale(): Promise<void> {
  if (storedLocale()) return
  try {
    // 这里刻意直接用 fetch 而不走 src/api：本模块是整个前端的依赖叶子，
    // 一旦反向依赖 API 层就会出现循环（api → client → locales）。
    const response = await fetch('/api/default-language', { headers: { Accept: 'application/json' }, cache: 'no-store' })
    if (!response.ok) return
    const body = (await response.json()) as DefaultLanguage
    if (isSupportedLocale(body.default_language)) setLocale(body.default_language, false)
  } catch {
    // 网络/接口不可用时保持浏览器推断出的语言
  }
}

/** 取当前语言对应的 Element Plus locale（供 ElConfigProvider 使用） */
export function currentElementLocale(): Language {
  return localeMeta(currentLocale()).element
}
