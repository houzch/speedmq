// Command version 是仓库的“版本号同步”工具：改版本号的唯一入口。
//
// 用法（在仓库根目录执行）：
//
//	go run ./scripts/version 1.2.0
//
// 它把新版本号写进所有引用处：内核 broker.Version、前端 web/package{,-lock}.json、
// docker-compose.yml、README.md / README-cn.md、docs/（含 i18n 各语言译文）与同级 speedmq-test 的镜像 tag。
// 替换是“按模式认版本、不看旧值”，因此顺带修正历史遗漏（例如某些 i18n 译文还停在旧版本）。
//
// 为什么用同步命令而不是“单一来源文件 + 构建期注入”：版本号要同时出现在 Go 常量、npm 元数据、
// compose 与给人看的文档里，构建期注入覆盖不到文档；一条命令改齐比让每个消费方各自去读同一个
// 文件更直接，也不给编译与镜像构建增加额外步骤（口径见 AGENTS.md §12.1）。
//
// 刻意不动的位置：文档里的实测记录与线协议报文样例（`speedmq:1.1.01`、`kernel=1.1.01`、
// `"kernel_version": "1.1.01"`）——它们记录的是某一时刻的事实，改了就是编造。
package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// semver 匹配版本串。用它收紧模式，避免把端口号之类（speedmq:5672）当成版本。
const semver = `[0-9]+\.[0-9]+\.[0-9]+[0-9A-Za-z.+-]*`

var (
	// constRE 命中 broker.go 里的 `const Version = "x"`。
	constRE = regexp.MustCompile(`(?m)^const Version = "[^"]*"`)
	// jsonVerRE 命中 package.json / package-lock.json 里的 `"version": "x"`。
	jsonVerRE = regexp.MustCompile(`"version"\s*:\s*"[^"]*"`)
	// prefixedTagRE 命中带 houzch/ 前缀的版本引用：houzch/speedmq:1.1.02、houzch/speedmq@v1.1.02。
	// 第 1 组是分隔符（`:` 或 `@v`），替换时原样保留。
	prefixedTagRE = regexp.MustCompile(`houzch/speedmq(:|@v)` + semver)
	// bareTagRE 命中 speedmq-test 里不带前缀的镜像 tag：speedmq:1.1.02（端口号 speedmq:5672 不匹配）。
	bareTagRE = regexp.MustCompile(`speedmq:` + semver)
	// semverRE 校验命令行传入的版本号形状。
	semverRE = regexp.MustCompile(`^` + semver + `$`)
)

// editKind 决定一处文件用哪种替换规则。
type editKind int

const (
	constVersion editKind = iota // broker.go 的 `const Version = "..."`
	jsonVersion1                 // package.json：只改第 1 个 `"version"`
	jsonVersion2                 // package-lock.json：只改前 2 个（根包 + packages[""]）
	prefixedTag                  // 带 houzch/ 前缀的镜像 tag 与 go get 版本
	bareTag                      // speedmq-test 里不带前缀的镜像 tag
)

func main() {
	if len(os.Args) != 2 {
		usage()
	}
	ver := strings.TrimPrefix(os.Args[1], "v")
	if !semverRE.MatchString(ver) {
		fmt.Fprintf(os.Stderr, "版本号格式非法: %q（应为 x.y.z，例如 1.2.0）\n", os.Args[1])
		os.Exit(2)
	}

	root, err := repoRoot()
	if err != nil {
		fatal(err)
	}

	var (
		lines []string
		total int
	)
	apply := func(path string, k editKind) {
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		n, err := syncFile(path, ver, k)
		if err != nil {
			fatal(fmt.Errorf("%s: %w", rel, err))
		}
		if n > 0 {
			lines = append(lines, fmt.Sprintf("  %-54s %d 处", rel, n))
			total += n
		}
	}

	apply(filepath.Join(root, "internal", "broker", "broker.go"), constVersion)
	apply(filepath.Join(root, "web", "package.json"), jsonVersion1)
	apply(filepath.Join(root, "web", "package-lock.json"), jsonVersion2)
	apply(filepath.Join(root, "docker-compose.yml"), prefixedTag)
	apply(filepath.Join(root, "README.md"), prefixedTag)
	apply(filepath.Join(root, "README-cn.md"), prefixedTag)

	if err := walk(filepath.Join(root, "docs"), func(p string) {
		if strings.EqualFold(filepath.Ext(p), ".md") {
			apply(p, prefixedTag)
		}
	}); err != nil {
		fatal(err)
	}

	// speedmq-test 与 speedmq 同级。发布 CI 只检出 speedmq，目录不存在时跳过。
	// REPORT.md 与 artifacts/ 是历史实测记录，钉在当时的版本上，不参与同步。
	testRoot := filepath.Clean(filepath.Join(root, "..", "speedmq-test"))
	if fi, err := os.Stat(testRoot); err == nil && fi.IsDir() {
		if err := walk(testRoot, func(p string) {
			if strings.EqualFold(filepath.Base(p), "REPORT.md") {
				return
			}
			switch strings.ToLower(filepath.Ext(p)) {
			case ".md", ".yml", ".yaml", ".py":
				apply(p, bareTag)
			}
		}); err != nil {
			fatal(err)
		}
	} else {
		lines = append(lines, "  （未找到同级目录 speedmq-test，已跳过其镜像 tag）")
	}

	for _, l := range lines {
		fmt.Println(l)
	}
	if total == 0 {
		fmt.Printf("版本号已经是 %s，无需改动。\n", ver)
		return
	}
	fmt.Printf("已把 %d 处版本号更新为 %s。\n", total, ver)
}

// syncFile 按 kind 把文件里的版本号替换为 ver，返回改动处数；内容未变则不落盘。
func syncFile(path, ver string, k editKind) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	src := string(b)

	var (
		out string
		n   int
	)
	switch k {
	case constVersion:
		out, n = replaceAll(constRE, src, `const Version = "`+ver+`"`)
	case jsonVersion1:
		out, n = replaceFirstN(jsonVerRE, src, `"version": "`+ver+`"`, 1)
	case jsonVersion2:
		out, n = replaceFirstN(jsonVerRE, src, `"version": "`+ver+`"`, 2)
	case prefixedTag:
		out, n = replaceAll(prefixedTagRE, src, `houzch/speedmq${1}`+ver)
	case bareTag:
		out, n = replaceAll(bareTagRE, src, `speedmq:`+ver)
	default:
		return 0, fmt.Errorf("未知的替换类型 %d", k)
	}
	if out == src {
		return 0, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if err := os.WriteFile(path, []byte(out), info.Mode().Perm()); err != nil {
		return 0, err
	}
	return n, nil
}

// replaceAll 替换全部匹配，返回替换处数。
func replaceAll(re *regexp.Regexp, src, repl string) (string, int) {
	n := len(re.FindAllStringIndex(src, -1))
	if n == 0 {
		return src, 0
	}
	return re.ReplaceAllString(src, repl), n
}

// replaceFirstN 只替换前 n 个匹配。package-lock.json 里前两个 `"version"` 是根包自己的，
// 其余是依赖的版本，不能动（依赖版本由 npm 管理）。
func replaceFirstN(re *regexp.Regexp, src, repl string, n int) (string, int) {
	locs := re.FindAllStringIndex(src, n)
	if len(locs) == 0 {
		return src, 0
	}
	var sb strings.Builder
	last := 0
	for _, loc := range locs {
		sb.WriteString(src[last:loc[0]])
		sb.WriteString(repl)
		last = loc[1]
	}
	sb.WriteString(src[last:])
	return sb.String(), len(locs)
}

// walk 递归访问目录下所有文件；跳过产物/缓存目录。
func walk(dir string, visit func(string)) error {
	return filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case "artifacts", "node_modules", ".gotmp", "data":
				return fs.SkipDir
			}
			return nil
		}
		visit(p)
		return nil
	})
}

// repoRoot 从当前目录向上找 speedmq 仓库根（module 声明所在的 go.mod）。
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if b, err := os.ReadFile(filepath.Join(dir, "go.mod")); err == nil &&
			strings.Contains(string(b), "module github.com/houzch/speedmq") {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("未找到 speedmq 仓库根（请在 speedmq/ 目录下执行）")
		}
		dir = parent
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "用法: go run ./scripts/version <新版本号>")
	fmt.Fprintln(os.Stderr, "示例: go run ./scripts/version 1.2.0")
	os.Exit(2)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "错误:", err)
	os.Exit(1)
}
