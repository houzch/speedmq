// Package web 把管理 UI 的构建产物嵌入二进制。
//
// 为什么放在这里而不是 internal/management：go:embed 只能引用**本包目录之内**的文件，
// 而前端工程在 web/ 下。于是这里只做一件事——把 dist 暴露成 embed.FS，
// 由 internal/management 负责路由与缓存策略（对齐设计 9.3：单个二进制、无额外静态服务）。
//
// 关于构建产物是否入库：**不入库**（见根 .gitignore）。仓库只放前端源码，
// web/dist 由 `npm run build`（本地 / CI / Docker 的前端构建阶段）生成。
// 但 go:embed 在**编译期**必须至少匹配到一个文件，目录缺失时 `go build ./...` 会直接
// 报 `pattern all:dist: no matching files found`（Go 不支持"可选 embed"），
// 因此仓库保留一个被跟踪的占位文件 web/dist/.gitkeep：
//   - 没构建前端时：编译照常通过，管理面访问 `/` 会返回"管理 UI 未构建"的明确提示
//     （见 internal/management/static.go），而不是让人对着一个白屏或编译错误排查；
//   - 构建前端时：`npm run build` 覆盖 dist，postbuild 钩子再把 .gitkeep 写回来
//     （Vite 的 emptyOutDir 会清空目录）。
package web

import "embed"

// Dist 是管理 UI 的构建产物（index.html + assets/）。
//
// 用 all: 前缀而不是默认规则：默认规则会跳过以 "_" 或 "." 开头的文件，
// 而这里需要连占位文件 web/dist/.gitkeep 一起匹配到（它保证未构建时也能编译）。
//
//go:embed all:dist
var Dist embed.FS
