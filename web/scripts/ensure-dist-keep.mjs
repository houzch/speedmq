// 构建后补回 web/dist/.gitkeep 占位文件。
//
// 为什么需要它：web/dist 是构建产物、不入库（见根 .gitignore），但 go:embed 在
// **编译期**必须至少匹配到一个文件，否则 `go build ./...` 直接失败
// （Go 不支持"可选 embed"）。Vite 构建时会清空 outDir（emptyOutDir），
// 会把被跟踪的 .gitkeep 一起删掉，于是每次构建完 git status 都会多出一条删除记录。
// 这个脚本在构建后把它写回来，让"构建产物不入库"与"编译期有文件可 embed"同时成立。
import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const target = resolve(root, 'dist/.gitkeep')

const content = `# 占位文件：不要删除

本目录（web/dist）是前端构建产物，**不入库**（见根目录 .gitignore）。
保留这个占位文件的原因：\`web/embed.go\` 用 \`//go:embed all:dist\` 把管理 UI 打进二进制，
而 go:embed 在**编译期**必须至少匹配到一个文件 —— 目录为空或不存在时
\`go build ./...\` 会直接报 \`pattern all:dist: no matching files found\`
（Go 不支持"可选 embed"，没有别的写法能绕开）。

所以：
- 未构建前端时：编译照常通过，管理面访问 \`/\` 会返回"管理 UI 未构建"的提示；
- 构建前端时：\`npm run build\` 产出真实页面覆盖本目录，\`postbuild\` 钩子会把本文件补回来
  （Vite 的 emptyOutDir 会清空目录）。
`

mkdirSync(dirname(target), { recursive: true })
writeFileSync(target, content)
console.log('已补回占位文件: web/dist/.gitkeep')
