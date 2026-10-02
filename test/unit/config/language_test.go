package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/houzch/swiftmq/internal/config"
)

// 本文件覆盖"安装时按系统时区推断管理 UI 默认语言"这一能力。
//
// 只断言**对外可观察**的结果：时区 → 语言 code 的映射，以及配置校验行为；
// 不依赖真实系统时区（用显式 language 或直接调纯函数）。

func TestLanguageFromTimezone(t *testing.T) {
	cases := []struct {
		name string
		tz   string
		want string
	}{
		{name: "上海-简体中文", tz: "Asia/Shanghai", want: "zh-CN"},
		{name: "乌鲁木齐-简体中文", tz: "Asia/Urumqi", want: "zh-CN"},
		{name: "台北-繁体中文", tz: "Asia/Taipei", want: "zh-TW"},
		{name: "香港-繁体中文", tz: "Asia/Hong_Kong", want: "zh-TW"},
		{name: "东京-日语", tz: "Asia/Tokyo", want: "ja"},
		{name: "首尔-韩语", tz: "Asia/Seoul", want: "ko"},
		{name: "雅加达-印尼语", tz: "Asia/Jakarta", want: "id"},
		{name: "曼谷-泰语", tz: "Asia/Bangkok", want: "th"},
		{name: "胡志明-越南语", tz: "Asia/Ho_Chi_Minh", want: "vi"},
		{name: "吉隆坡-马来语", tz: "Asia/Kuala_Lumpur", want: "ms"},
		{name: "马尼拉-菲律宾语", tz: "Asia/Manila", want: "fil"},
		{name: "莫斯科-俄语", tz: "Europe/Moscow", want: "ru"},
		{name: "符拉迪沃斯托克-俄语", tz: "Asia/Vladivostok", want: "ru"},
		{name: "柏林-德语", tz: "Europe/Berlin", want: "de"},
		{name: "巴黎-法语", tz: "Europe/Paris", want: "fr"},
		{name: "罗马-意大利语", tz: "Europe/Rome", want: "it"},
		{name: "阿姆斯特丹-荷兰语", tz: "Europe/Amsterdam", want: "nl"},
		{name: "里斯本-葡萄牙语", tz: "Europe/Lisbon", want: "pt"},
		{name: "圣保罗-葡萄牙语", tz: "America/Sao_Paulo", want: "pt"},
		{name: "马德里-西班牙语", tz: "Europe/Madrid", want: "es"},
		{name: "阿根廷-西班牙语", tz: "America/Argentina/Buenos_Aires", want: "es"},
		{name: "开罗-阿拉伯语", tz: "Africa/Cairo", want: "ar"},
		{name: "迪拜-阿拉伯语", tz: "Asia/Dubai", want: "ar"},
		{name: "伦敦-英语", tz: "Europe/London", want: "en"},
		{name: "未知时区回退英语", tz: "Mars/Olympus", want: "en"},
		{name: "空时区回退英语", tz: "", want: "en"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := config.LanguageFromTimezone(tc.tz); got != tc.want {
				t.Fatalf("LanguageFromTimezone(%q) = %q, 期望 %q", tc.tz, got, tc.want)
			}
		})
	}
}

// TestSystemTimezoneIsIANA 断言系统时区探测要么取不到（空串），要么返回 **IANA 名**，
// 而不是平台自有的时区标识（如 Windows 的 "China Standard Time"）。
//
// 这条断言专门抓"忘了做 Windows 映射、把注册表里的时区 ID 原样返回"这类 bug：
// 那种情况下 LanguageFromTimezone 会静默回退 en，只看语言值根本发现不了。
func TestSystemTimezoneIsIANA(t *testing.T) {
	tz := config.SystemTimezone()
	if tz == "" {
		t.Skip("当前环境取不到系统时区（未设 TZ 且平台探测未命中）")
	}
	if strings.ContainsAny(tz, " ") {
		t.Fatalf("SystemTimezone() 返回的像是平台自有标识而非 IANA 名: %q", tz)
	}
	if tz != "UTC" && !strings.Contains(tz, "/") {
		t.Fatalf("SystemTimezone() 返回值不像 IANA 名（既不是 UTC 也不含 /）: %q", tz)
	}
	// 探测结果最终要能落到一个受支持的语言上，否则等于白探测
	if code := config.LanguageFromTimezone(tz); !config.IsSupportedLanguage(code) {
		t.Fatalf("SystemTimezone()=%q 推断出的语言 %q 不在支持列表内", tz, code)
	}
}

func TestIsSupportedLanguage(t *testing.T) {
	if !config.IsSupportedLanguage("zh-CN") {
		t.Fatalf("zh-CN 应当是受支持的语言")
	}
	if config.IsSupportedLanguage("xx-YY") {
		t.Fatalf("xx-YY 不应被当成受支持的语言")
	}
}

func TestLoadDefaultsManagementLanguage(t *testing.T) {
	// 不指定 language 时，配置里必须落一个**受支持**的默认语言（具体值随系统时区而定）。
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if !config.IsSupportedLanguage(cfg.Management.Language) {
		t.Fatalf("默认管理语言 %q 不在支持列表内", cfg.Management.Language)
	}
}

func TestLoadRejectsUnsupportedLanguage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "swiftmqd.json")
	if err := os.WriteFile(path, []byte(`{"management":{"language":"xx-YY"}}`), 0o600); err != nil {
		t.Fatalf("写临时配置失败: %v", err)
	}
	if _, err := config.Load(path); err == nil {
		t.Fatalf("非法 language 应当导致启动失败，但 Load 成功了")
	}
}

func TestLoadAcceptsExplicitLanguage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "swiftmqd.json")
	if err := os.WriteFile(path, []byte(`{"management":{"language":"ja"}}`), 0o600); err != nil {
		t.Fatalf("写临时配置失败: %v", err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load 失败: %v", err)
	}
	if cfg.Management.Language != "ja" {
		t.Fatalf("显式 language 未被保留: %q", cfg.Management.Language)
	}
}
