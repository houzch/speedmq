// Package web 把管理 UI 的构建产物嵌入二进制。
//
// 为什么放在这里而不是 internal/management：go:embed 只能引用**本包目录之内**的文件，
// 而前端工程在 web/ 下。于是这里只做一件事——把 dist 暴露成 embed.FS，
// 由 internal/management 负责路由与缓存策略（对齐设计 9.3：单个二进制、无额外静态服务）。
//
// 注意：web/dist 是 `npm run build` 的产物，**必须随仓库提交**：
//   - 部署只拷一个二进制，不装 Node；
//   - CI 会执行一次构建并校验 `git diff --exit-code web/dist`，
//     防止"改了源码忘记重新构建"导致二进制里跑着旧页面。
package web

import "embed"

// Dist 是管理 UI 的构建产物（index.html + assets/）。
//
// 用 all: 前缀而不是默认规则：默认规则会跳过以 "_" 或 "." 开头的文件，
// 而前端构建产物里出现这类文件名是完全可能的。
//
//go:embed all:dist
var Dist embed.FS
