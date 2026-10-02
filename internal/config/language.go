package config

import (
	"strings"
)

// supportedLanguages 是管理 UI 支持的语言 code，**必须与前端 web/src/locales/*.json 一一对应**。
//
// 顺序仅用于错误提示与文档，不代表优先级。后端只负责"按系统时区推断一个默认值"，
// 真正的文案都在前端；因此这里出现一个前端没有的语言，会让管理 UI 回退到英文 ——
// 增删语言时两边必须同步（与 apiGroups 前后端同步的约定一致）。
var supportedLanguages = []string{
	"zh-CN", "zh-TW", "en", "ja", "ko", "es", "de", "fr", "ar", "ru",
	"it", "nl", "pt", "id", "th", "vi", "ms", "fil",
}

// SupportedLanguages 返回支持的语言 code 副本。
func SupportedLanguages() []string {
	return append([]string(nil), supportedLanguages...)
}

// IsSupportedLanguage 判断 code 是否是受支持的管理 UI 语言。
func IsSupportedLanguage(code string) bool {
	for _, item := range supportedLanguages {
		if item == code {
			return true
		}
	}
	return false
}

// languageHint 返回 "a / b / c" 形式的可选值清单，用于配置错误提示。
func languageHint() string {
	return strings.Join(supportedLanguages, " / ")
}

// LanguageFromTimezone 把 IANA 时区名映射为管理 UI 默认语言，无法识别时回退 en。
//
// 映射刻意只覆盖"主要城市"而不是穷举 zoneinfo：默认语言只是**安装后的起点**，
// 用户在管理 UI 里可以随时改；漏判的代价是多点一次下拉，而穷举表既难维护又容易过时。
// 因此这里采用"精确表 + 少量区域前缀"两条规则，宁可回退到 en，也不做模糊猜测。
func LanguageFromTimezone(tz string) string {
	tz = strings.TrimSpace(tz)
	if tz == "" {
		return "en"
	}
	if code, ok := exactTimezoneLanguage[tz]; ok {
		return code
	}
	// America/Argentina/* 等跨多个城市的国家/地区前缀，按前缀收敛。
	for prefix, code := range timezonePrefixLanguage {
		if strings.HasPrefix(tz, prefix) {
			return code
		}
	}
	return "en"
}

// exactTimezoneLanguage 是主要城市 → 语言的精确映射。
var exactTimezoneLanguage = map[string]string{
	// 简体中文
	"Asia/Shanghai":  "zh-CN",
	"Asia/Chongqing": "zh-CN",
	"Asia/Harbin":    "zh-CN",
	"Asia/Urumqi":    "zh-CN",
	"Asia/Kashgar":   "zh-CN",
	"Asia/Beijing":   "zh-CN",
	// 繁体中文
	"Asia/Taipei":    "zh-TW",
	"Asia/Hong_Kong": "zh-TW",
	"Asia/Macau":     "zh-TW",
	"Asia/Macao":     "zh-TW",
	// 日语 / 韩语
	"Asia/Tokyo":     "ja",
	"Asia/Seoul":     "ko",
	"Asia/Pyongyang": "ko",
	// 东南亚
	"Asia/Jakarta":      "id",
	"Asia/Pontianak":    "id",
	"Asia/Makassar":     "id",
	"Asia/Jayapura":     "id",
	"Asia/Bangkok":      "th",
	"Asia/Ho_Chi_Minh":  "vi",
	"Asia/Saigon":       "vi",
	"Asia/Kuala_Lumpur": "ms",
	"Asia/Kuching":      "ms",
	"Asia/Manila":       "fil",
	// 俄语
	"Europe/Moscow":      "ru",
	"Europe/Kaliningrad": "ru",
	"Europe/Samara":      "ru",
	"Europe/Volgograd":   "ru",
	"Europe/Simferopol":  "ru",
	"Europe/Astrakhan":   "ru",
	"Europe/Saratov":     "ru",
	"Asia/Yekaterinburg": "ru",
	"Asia/Omsk":          "ru",
	"Asia/Novosibirsk":   "ru",
	"Asia/Barnaul":       "ru",
	"Asia/Tomsk":         "ru",
	"Asia/Novokuznetsk":  "ru",
	"Asia/Krasnoyarsk":   "ru",
	"Asia/Irkutsk":       "ru",
	"Asia/Chita":         "ru",
	"Asia/Yakutsk":       "ru",
	"Asia/Khandyga":      "ru",
	"Asia/Vladivostok":   "ru",
	"Asia/Ust-Nera":      "ru",
	"Asia/Magadan":       "ru",
	"Asia/Sakhalin":      "ru",
	"Asia/Srednekolymsk": "ru",
	"Asia/Kamchatka":     "ru",
	"Asia/Anadyr":        "ru",
	// 德语
	"Europe/Berlin":   "de",
	"Europe/Vienna":   "de",
	"Europe/Zurich":   "de",
	"Europe/Busingen": "de",
	// 法语
	"Europe/Paris":      "fr",
	"Europe/Monaco":     "fr",
	"Europe/Brussels":   "fr",
	"Europe/Luxembourg": "fr",
	// 意大利语
	"Europe/Rome":       "it",
	"Europe/Malta":      "it",
	"Europe/San_Marino": "it",
	"Europe/Vatican":    "it",
	// 荷兰语
	"Europe/Amsterdam": "nl",
	// 葡萄牙语（含巴西）
	"Europe/Lisbon":        "pt",
	"Atlantic/Azores":      "pt",
	"Atlantic/Madeira":     "pt",
	"America/Sao_Paulo":    "pt",
	"America/Bahia":        "pt",
	"America/Fortaleza":    "pt",
	"America/Recife":       "pt",
	"America/Belem":        "pt",
	"America/Manaus":       "pt",
	"America/Cuiaba":       "pt",
	"America/Campo_Grande": "pt",
	"America/Porto_Velho":  "pt",
	"America/Boa_Vista":    "pt",
	"America/Rio_Branco":   "pt",
	"America/Noronha":      "pt",
	"America/Maceio":       "pt",
	"America/Araguaina":    "pt",
	"America/Santarem":     "pt",
	// 西班牙语
	"Europe/Madrid":         "es",
	"Africa/Ceuta":          "es",
	"Atlantic/Canary":       "es",
	"America/Mexico_City":   "es",
	"America/Tijuana":       "es",
	"America/Chihuahua":     "es",
	"America/Cancun":        "es",
	"America/Bogota":        "es",
	"America/Lima":          "es",
	"America/Santiago":      "es",
	"America/Caracas":       "es",
	"America/Montevideo":    "es",
	"America/Guayaquil":     "es",
	"America/La_Paz":        "es",
	"America/Asuncion":      "es",
	"America/Panama":        "es",
	"America/Costa_Rica":    "es",
	"America/Guatemala":     "es",
	"America/Havana":        "es",
	"America/Santo_Domingo": "es",
	"America/Puerto_Rico":   "es",
	"America/El_Salvador":   "es",
	"America/Tegucigalpa":   "es",
	"America/Managua":       "es",
	// 阿拉伯语
	"Africa/Cairo":      "ar",
	"Africa/Casablanca": "ar",
	"Africa/Algiers":    "ar",
	"Africa/Tunis":      "ar",
	"Africa/Tripoli":    "ar",
	"Africa/Khartoum":   "ar",
	"Asia/Riyadh":       "ar",
	"Asia/Dubai":        "ar",
	"Asia/Kuwait":       "ar",
	"Asia/Qatar":        "ar",
	"Asia/Bahrain":      "ar",
	"Asia/Muscat":       "ar",
	"Asia/Amman":        "ar",
	"Asia/Beirut":       "ar",
	"Asia/Damascus":     "ar",
	"Asia/Baghdad":      "ar",
	"Asia/Gaza":         "ar",
	"Asia/Hebron":       "ar",
	"Asia/Aden":         "ar",
	"Asia/Sanaa":        "ar",
	// 英语（显式列出常见首都/大区，其余落回默认 en）
	"Europe/London":       "en",
	"Europe/Dublin":       "en",
	"America/New_York":    "en",
	"America/Chicago":     "en",
	"America/Denver":      "en",
	"America/Los_Angeles": "en",
	"America/Phoenix":     "en",
	"America/Anchorage":   "en",
	"America/Halifax":     "en",
	"America/St_Johns":    "en",
	"Pacific/Honolulu":    "en",
	"Pacific/Auckland":    "en",
	"Asia/Singapore":      "en",
	"Asia/Kolkata":        "en",
	"Asia/Calcutta":       "en",
	"Asia/Colombo":        "en",
	"Africa/Johannesburg": "en",
	"Africa/Nairobi":      "en",
	"Africa/Lagos":        "en",
	"UTC":                 "en",
}

// timezonePrefixLanguage 是按前缀匹配的兜底规则，用于覆盖一个国家下的多个城市。
var timezonePrefixLanguage = map[string]string{
	"America/Argentina/": "es",
	"Australia/":         "en",
}
