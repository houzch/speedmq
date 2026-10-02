//go:build windows

package config

import (
	"strings"
	"syscall"
	"unsafe"
)

// Windows 把当前时区记在注册表里，值是自有命名（"China Standard Time"），与 IANA 无关，
// 且两者之间**没有算法可转换**，只能查表。
const (
	timeZoneKeyPath   = `SYSTEM\CurrentControlSet\Control\TimeZoneInformation`
	timeZoneValueName = "TimeZoneKeyName"

	hkeyLocalMachine = 0x80000002 // HKEY_LOCAL_MACHINE
	keyRead          = 0x20019    // KEY_READ

	// GetUserDefaultLocaleName 的缓冲上限，见 Windows SDK 的 LOCALE_NAME_MAX_LENGTH
	localeNameMaxLength = 85
)

var (
	advapi32 = syscall.NewLazyDLL("advapi32.dll")
	kernel32 = syscall.NewLazyDLL("kernel32.dll")

	procRegOpenKeyExW            = advapi32.NewProc("RegOpenKeyExW")
	procRegQueryValueExW         = advapi32.NewProc("RegQueryValueExW")
	procRegCloseKey              = advapi32.NewProc("RegCloseKey")
	procGetUserDefaultLocaleName = kernel32.NewProc("GetUserDefaultLocaleName")
)

// platformTimezone 在 Windows 上从注册表读当前时区 ID，再映射成 IANA 时区名；无法确定时返回空串。
//
// 映射分两种情形：
//
//  1. 一个 Windows ID 只对应一种语言地区（如 China Standard Time）→ 直接给出 IANA 名；
//  2. 一个 Windows ID 横跨多种语言地区（如 W. Europe Standard Time 同时覆盖德语、荷兰语、意大利语区；
//     SE Asia Standard Time 同时覆盖泰语、印尼语、越南语区）→ **不猜**，用系统区域设置消歧；
//     区域设置也对不上（例如德区里的 en-US 用户）就返回空串，交回默认语言。
func platformTimezone() string {
	id := windowsTimeZoneID()
	if id == "" {
		return ""
	}
	if iana, ok := windowsTimeZone[id]; ok {
		return iana
	}
	if byLanguage, ok := windowsAmbiguousTimeZone[id]; ok {
		return lookupAmbiguousByLocale(byLanguage, windowsLocale())
	}
	return ""
}

// windowsTimeZoneID 读注册表里的当前时区 ID（如 "China Standard Time"）；读不到返回空串。
func windowsTimeZoneID() string {
	subKey, err := syscall.UTF16PtrFromString(timeZoneKeyPath)
	if err != nil {
		return ""
	}
	valueName, err := syscall.UTF16PtrFromString(timeZoneValueName)
	if err != nil {
		return ""
	}

	var hKey syscall.Handle
	if r, _, _ := procRegOpenKeyExW.Call(hkeyLocalMachine, uintptr(unsafe.Pointer(subKey)), 0, keyRead, uintptr(unsafe.Pointer(&hKey))); r != 0 {
		return ""
	}
	defer procRegCloseKey.Call(uintptr(hKey))

	// 第一次调用只问长度，第二次取值。值是 UTF-16，长度按字节计。
	var size uint32
	if r, _, _ := procRegQueryValueExW.Call(uintptr(hKey), uintptr(unsafe.Pointer(valueName)), 0, 0, 0, uintptr(unsafe.Pointer(&size))); r != 0 {
		return ""
	}
	if size == 0 || size > 4096 {
		return ""
	}
	buf := make([]uint16, size/2+1)
	if r, _, _ := procRegQueryValueExW.Call(uintptr(hKey), uintptr(unsafe.Pointer(valueName)), 0, 0, uintptr(unsafe.Pointer(&buf[0])), uintptr(unsafe.Pointer(&size))); r != 0 {
		return ""
	}
	return syscall.UTF16ToString(buf)
}

// windowsLocale 返回系统区域设置（用户界面语言）的小写标签，如 "zh-cn"、"de-de"；取不到返回空串。
func windowsLocale() string {
	var buf [localeNameMaxLength]uint16
	if r, _, _ := procGetUserDefaultLocaleName.Call(uintptr(unsafe.Pointer(&buf[0])), uintptr(len(buf))); r == 0 {
		return ""
	}
	return strings.ToLower(syscall.UTF16ToString(buf[:]))
}

// lookupAmbiguousByLocale 用一个可能横跨多语言的 Windows 时区 ID 消歧：
// 先按完整区域标签（zh-tw、de-de）匹配，再退到主语言子标签（zh、de）；都对不上则返回空串。
func lookupAmbiguousByLocale(byLanguage map[string]string, locale string) string {
	if locale == "" {
		return ""
	}
	if iana, ok := byLanguage[locale]; ok {
		return iana
	}
	if idx := strings.IndexByte(locale, '-'); idx > 0 {
		if iana, ok := byLanguage[locale[:idx]]; ok {
			return iana
		}
	}
	return ""
}

// windowsTimeZone 是"一种 Windows 时区 ID 只对应一种语言地区"的精确映射。
//
// 只收录能落到 supportedLanguages 里的时区：其余（乌尔都语、波斯语、哈萨克语…）本就回退英文，
// 收进来只会让表变长而没有实际效果。ID 名称取自 Windows 注册表
// `HKLM\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Time Zones`（共 141 项），不是凭印象写的。
var windowsTimeZone = map[string]string{
	// 中文
	"China Standard Time":  "Asia/Shanghai",
	"Taipei Standard Time": "Asia/Taipei",

	// 日语 / 韩语
	"Tokyo Standard Time":       "Asia/Tokyo",
	"Korea Standard Time":       "Asia/Seoul",
	"North Korea Standard Time": "Asia/Pyongyang",

	// 俄语（俄罗斯横跨 11 个时区，Windows 给每个区单独的 ID）
	"Russian Standard Time":         "Europe/Moscow",
	"Kaliningrad Standard Time":     "Europe/Kaliningrad",
	"Volgograd Standard Time":       "Europe/Volgograd",
	"Russia Time Zone 3":            "Europe/Samara",
	"Astrakhan Standard Time":       "Europe/Astrakhan",
	"Saratov Standard Time":         "Europe/Saratov",
	"Ekaterinburg Standard Time":    "Asia/Yekaterinburg",
	"Omsk Standard Time":            "Asia/Omsk",
	"N. Central Asia Standard Time": "Asia/Novosibirsk",
	"North Asia Standard Time":      "Asia/Krasnoyarsk",
	"North Asia East Standard Time": "Asia/Irkutsk",
	"Altai Standard Time":           "Asia/Barnaul",
	"Tomsk Standard Time":           "Asia/Tomsk",
	"Transbaikal Standard Time":     "Asia/Chita",
	"Yakutsk Standard Time":         "Asia/Yakutsk",
	"Vladivostok Standard Time":     "Asia/Vladivostok",
	"Magadan Standard Time":         "Asia/Magadan",
	"Sakhalin Standard Time":        "Asia/Sakhalin",
	"Russia Time Zone 10":           "Asia/Srednekolymsk",
	"Russia Time Zone 11":           "Asia/Kamchatka",

	// 葡萄牙语
	"E. South America Standard Time": "America/Sao_Paulo",
	"Bahia Standard Time":            "America/Bahia",
	"Tocantins Standard Time":        "America/Araguaina",
	"Azores Standard Time":           "Atlantic/Azores",

	// 西班牙语
	"Central Standard Time (Mexico)":  "America/Mexico_City",
	"Pacific Standard Time (Mexico)":  "America/Tijuana",
	"Mountain Standard Time (Mexico)": "America/Chihuahua",
	"Eastern Standard Time (Mexico)":  "America/Cancun",
	"Central America Standard Time":   "America/Guatemala",
	"Pacific SA Standard Time":        "America/Santiago",
	"Montevideo Standard Time":        "America/Montevideo",
	"Paraguay Standard Time":          "America/Asuncion",
	"Venezuela Standard Time":         "America/Caracas",
	"Cuba Standard Time":              "America/Havana",
	"Argentina Standard Time":         "America/Argentina/Buenos_Aires",

	// 阿拉伯语
	"Arabic Standard Time":      "Asia/Baghdad",
	"Arab Standard Time":        "Asia/Riyadh",
	"Arabian Standard Time":     "Asia/Dubai",
	"Egypt Standard Time":       "Africa/Cairo",
	"Morocco Standard Time":     "Africa/Casablanca",
	"Sudan Standard Time":       "Africa/Khartoum",
	"Libya Standard Time":       "Africa/Tripoli",
	"Jordan Standard Time":      "Asia/Amman",
	"Middle East Standard Time": "Asia/Beirut",
	"Syria Standard Time":       "Asia/Damascus",
	"West Bank Standard Time":   "Asia/Hebron",

	// 英语
	"UTC":                             "UTC",
	"Eastern Standard Time":           "America/New_York",
	"Central Standard Time":           "America/Chicago",
	"Mountain Standard Time":          "America/Denver",
	"Pacific Standard Time":           "America/Los_Angeles",
	"US Mountain Standard Time":       "America/Phoenix",
	"Alaskan Standard Time":           "America/Anchorage",
	"Hawaiian Standard Time":          "Pacific/Honolulu",
	"Atlantic Standard Time":          "America/Halifax",
	"Newfoundland Standard Time":      "America/St_Johns",
	"AUS Eastern Standard Time":       "Australia/Sydney",
	"AUS Central Standard Time":       "Australia/Darwin",
	"Cen. Australia Standard Time":    "Australia/Adelaide",
	"W. Australia Standard Time":      "Australia/Perth",
	"Tasmania Standard Time":          "Australia/Hobart",
	"New Zealand Standard Time":       "Pacific/Auckland",
	"South Africa Standard Time":      "Africa/Johannesburg",
	"E. Africa Standard Time":         "Africa/Nairobi",
	"W. Central Africa Standard Time": "Africa/Lagos",
	"India Standard Time":             "Asia/Kolkata",
	"Sri Lanka Standard Time":         "Asia/Colombo",
}

// windowsAmbiguousTimeZone 收录"一个 Windows 时区 ID 横跨多种受支持语言"的情形，
// 用区域设置的主语言子标签消歧。表里没有的语言（或区域设置取不到）会落到空串，即不猜。
//
// 例：W. Europe Standard Time 同时是柏林 / 阿姆斯特丹 / 罗马；一个 en-US 用户在这三个地方
// 都不该被推断成 de/nl/it，因此他不在这张表的任何键里，结果是回退英文。
var windowsAmbiguousTimeZone = map[string]map[string]string{
	// 柏林 / 阿姆斯特丹 / 罗马（另有斯德哥尔摩、奥斯陆等不在支持语言内）
	"W. Europe Standard Time": {
		"de": "Europe/Berlin",
		"nl": "Europe/Amsterdam",
		"it": "Europe/Rome",
	},
	// 巴黎 / 马德里（另有哥本哈根、布鲁塞尔等）
	"Romance Standard Time": {
		"fr": "Europe/Paris",
		"es": "Europe/Madrid",
	},
	// 曼谷 / 雅加达 / 胡志明市（同属 UTC+7）
	"SE Asia Standard Time": {
		"th": "Asia/Bangkok",
		"id": "Asia/Jakarta",
		"vi": "Asia/Ho_Chi_Minh",
	},
	// 新加坡 / 吉隆坡 / 马尼拉：新加坡本地是英语，故没有 en 项
	"Singapore Standard Time": {
		"ms":  "Asia/Kuala_Lumpur",
		"fil": "Asia/Manila",
	},
	// 伦敦 / 都柏林 / 里斯本：前两者是英语，只有葡萄牙那项需要列
	"GMT Standard Time": {
		"pt": "Europe/Lisbon",
	},
	// 波哥大 / 利马 / 巴拿马，同时含牙买加（英语）等
	"SA Pacific Standard Time": {
		"es": "America/Bogota",
	},
}
