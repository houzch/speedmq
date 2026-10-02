# SwiftMQ GitHub Releases 与 Packages 操作说明

> 适用仓库：`github.com/houzch/swiftmq`；本文以当前版本 `1.0.0` 为例。
>
> 目标：让仓库首页右侧的 **Releases**（发布版本 + 二进制附件）与 **Packages**（容器镜像）从「No releases / No packages published」变成可下载、可 `docker pull` 的正式产物。

---

## 1. 先分清两件事

| | **Releases** | **Packages**（本文特指 GHCR 容器镜像） |
| --- | --- | --- |
| 是什么 | 围绕某个 git tag 的**发布记录**：标题、发行说明、附件（二进制、校验和） | **制品仓库**：把构建好的镜像推到 `ghcr.io`，供 `docker pull` |
| 典型内容 | `swiftmqd` / `swiftmqctl` 各平台二进制、`SHA256SUMS` | `ghcr.io/houzch/swiftmq:1.0.0` 等多架构镜像 |
| 入口 | 仓库首页右侧 Releases / `https://github.com/houzch/swiftmq/releases` | 仓库首页右侧 Packages / 个人 → Packages |
| 可见性 | 跟随仓库（公开仓库的 Release 即公开） | **默认私有**，需手动改为 Public（见 §6） |
| 触发方式 | 推 `v*` tag | 推 `v*` tag（同一次发布可同时产出两者） |

一句话：**Release 管"下载二进制"，Packages 管"拉镜像"**；两者通常由同一个 tag 触发。

---

## 2. 前置条件

**自动化（推荐，第 4 节）**：什么都不用装，GitHub Actions 自带 `gh`、`docker`、`buildx`。只需仓库的 Actions 处于启用状态（默认启用）。

**手动发布（第 5 节）**，需要本机具备：

| 工具 | 用途 | 校验 |
| --- | --- | --- |
| `git` | 打 tag / 推送 | `git --version` |
| `gh`（GitHub CLI） | 创建 Release、上传附件 | `gh --version`、`gh auth status` |
| `docker` + `buildx` | 构建并推送镜像 | `docker buildx version` |
| `node` 22+ / `npm` | 构建管理 UI 产物（`go:embed` 需要 `web/dist`） | `node -v` |
| Go 1.24+ | 编译二进制 | `go version` |

**权限**：

- Actions 里用内置的 `secrets.GITHUB_TOKEN`：在 workflow 顶部声明
  `permissions: { contents: write, packages: write }` 即可（`contents: write` 建 Release，`packages: write` 推 GHCR）。
- 本机手动推送镜像：用 PAT（classic 勾 `write:packages`、`read:packages`、`delete:packages`、`repo`），
  或先 `gh auth login` 并确保含 `write:packages` 权限。

---

## 3. 本仓库的发布产物约定

| 项 | 约定 |
| --- | --- |
| tag 名 | `v1.0.0`（语义化版本，**带 `v` 前缀**） |
| 版本号来源 | 源码里的 `broker.Version`（`internal/broker/broker.go`）；tag 必须与它一致 |
| Release 附件 | 6 个平台归档 + `SHA256SUMS`：`swiftmq-1.0.0-<os>-<arch>.tar.gz`（linux / darwin）、`.zip`（windows）；每个归档内含 `swiftmqd`、`swiftmqctl`（Windows 为 `.exe`）、`README.md`、`LICENSE` |
| 二进制平台 | Linux（amd64 / arm64）、macOS（amd64 / arm64）、Windows（amd64 / arm64） |
| 镜像地址 | `ghcr.io/houzch/swiftmq` |
| 镜像 tag | `1.0.0`、`1.0`、`1`、`latest` |
| 镜像平台 | `linux/amd64`、`linux/arm64`（**容器只出 Linux**，不提供 Windows 容器镜像） |

> ⚠️ **发布二进制前必须先构建管理 UI**。`web/dist` 不入库，`go:embed` 在缺少产物时仍能编译，但访问 `/` 会提示「管理 UI 未构建」——
> 那样发出去的二进制是**没有管理后台**的。CI 里必须加 `cd web && npm ci && npm run build` 这一步（Dockerfile 中的 ui 阶段已自动做了）。

> ℹ️ **平台支持口径**：**Linux / Windows / macOS 都在支持范围内**（amd64 与 arm64 都要能跑）。三者的磁盘可用空间与物理内存总量探测均已实现
> （`internal/store/sysinfo_linux.go` / `sysinfo_windows.go` / `sysinfo_darwin.go`），所以水位流控在各平台都真实生效；
> 其它尚未实现的平台会**显式返回不支持**，流控按"未超限"处理，不会静默假装成功。
> 容器镜像只出 Linux（不提供 Windows 容器镜像）。
>
> - **macOS**：首次运行可能被 Gatekeeper 拦截（未签名）——`xattr -d com.apple.quarantine ./swiftmqd`，或右键 →「打开」。
> - **Windows**：未签名的 `.exe` 可能触发 SmartScreen 提示，选择「仍要运行」即可。
>   默认语言按注册表里的时区 ID（如 `China Standard Time` → `Asia/Shanghai`）查表推断；
>   对横跨多种语言的时区 ID（如 `W. Europe Standard Time` 同属德语/荷兰语/意大利语区）用系统区域设置消歧，
>   仍无法确定就回退英文；也可以在配置项 `management.language` 里直接指定。
> - 需要 32 位 / armv7 时，在矩阵里加一行即可（见 4.1 的注释）。

---

## 4. 方式一：自动化发布（推荐）

### 4.1 工作流 `.github/workflows/release.yml`

仓库里**已经内置**这份工作流，直接用即可；下面保留完整内容，便于阅读与改动（改动请直接改仓库里的文件，避免两份不一致）。

```yaml
name: Release

# 只在推 v 开头的 tag 时触发（例如 v1.0.0）
on:
  push:
    tags: ['v*']

# 建 Release 需要 contents: write；推 GHCR 需要 packages: write
permissions:
  contents: write
  packages: write

env:
  # GHCR 要求镜像名全小写；github.repository 形如 houzch/swiftmq
  IMAGE: ghcr.io/${{ github.repository }}

jobs:
  # ---------- 1) 交叉编译各平台二进制并打包 ----------
  binaries:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        include:
          - { goos: linux,   goarch: amd64 }
          - { goos: linux,   goarch: arm64 }
          - { goos: darwin,  goarch: amd64 }   # macOS Intel
          - { goos: darwin,  goarch: arm64 }   # macOS Apple Silicon
          - { goos: windows, goarch: amd64 }
          - { goos: windows, goarch: arm64 }
          # 需要 32 位或 armv7 时按同样格式加一行即可：
          # - { goos: windows, goarch: 386 }
          # - { goos: linux,   goarch: 386 }
    steps:
      - uses: actions/checkout@v4

      # ① 先产出 web/dist：否则二进制里没有管理 UI
      - uses: actions/setup-node@v4
        with:
          node-version: '22'
          cache: npm
          cache-dependency-path: web/package-lock.json
      - run: npm ci
        working-directory: web
      - run: npm run build
        working-directory: web

      # ② 纯 Go + CGO_ENABLED=0：六个平台都能交叉编译，无需 QEMU / macOS runner
      - uses: actions/setup-go@v5
        with:
          go-version: '1.24'
          cache: false   # 零第三方依赖，无 go.sum，模块缓存无意义

      - name: Build and package
        env:
          CGO_ENABLED: '0'
          GOOS: ${{ matrix.goos }}
          GOARCH: ${{ matrix.goarch }}
          REF: ${{ github.ref_name }}
        run: |
          set -euo pipefail
          ver="${REF#v}"                                  # v1.0.0 -> 1.0.0
          ext=""
          if [ "${GOOS}" = "windows" ]; then ext=".exe"; fi

          mkdir -p stage
          go build -trimpath -ldflags="-s -w" -o "stage/swiftmqd${ext}"   ./cmd/swiftmqd
          go build -trimpath -ldflags="-s -w" -o "stage/swiftmqctl${ext}" ./cmd/swiftmqctl
          cp README.md LICENSE stage/                     # 归档里带上文档与许可证

          base="swiftmq-${ver}-${GOOS}-${GOARCH}"
          mkdir -p dist
          if [ "${GOOS}" = "windows" ]; then
            (cd stage && zip -q -r "../dist/${base}.zip" .)      # Windows -> zip
          else
            tar -czf "dist/${base}.tar.gz" -C stage .            # 其余 -> tar.gz
          fi

      - uses: actions/upload-artifact@v4
        with:
          name: swiftmq-${{ matrix.goos }}-${{ matrix.goarch }}
          path: dist/*
          if-no-files-found: error

  # ---------- 2) 汇总附件 + 生成校验和 + 创建 Release ----------
  release:
    needs: binaries
    runs-on: ubuntu-latest
    steps:
      - uses: actions/download-artifact@v4
        with:
          path: dist
          merge-multiple: true

      - name: Checksums
        run: |
          set -euo pipefail
          cd dist
          sha256sum * > SHA256SUMS
          cat SHA256SUMS

      - name: Create GitHub Release
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          set -euo pipefail
          gh release create "${GITHUB_REF_NAME}" dist/* \
            --repo "${GITHUB_REPOSITORY}" \
            --title "SwiftMQ ${GITHUB_REF_NAME}" \
            --generate-notes

  # ---------- 3) 构建多架构镜像并推到 GHCR ----------
  image:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-qemu-action@v3      # 为 arm64 提供模拟
      - uses: docker/setup-buildx-action@v3

      - name: Login to GHCR
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: echo "${GH_TOKEN}" | docker login ghcr.io -u "${{ github.actor }}" --password-stdin

      - name: Build and push
        run: |
          set -euo pipefail
          version="${GITHUB_REF_NAME#v}"   # v1.0.0 -> 1.0.0
          minor="${version%.*}"            # 1.0.0 -> 1.0
          major="${version%%.*}"           # 1.0.0 -> 1
          docker buildx build \
            --platform linux/amd64,linux/arm64 \
            -t "${IMAGE}:${version}" \
            -t "${IMAGE}:${minor}" \
            -t "${IMAGE}:${major}" \
            -t "${IMAGE}:latest" \
            --push .
```

> 版本号不是通过 build-arg 注入的：`broker.Version` 编译进二进制，**tag 必须与源码里的版本一致**（见 §10 检查清单）。

### 4.2 触发一次发布

> ⚠️ **tag 必须推到 GitHub 远端。** 本仓库同时配了两个远端：默认 `origin` 指向 **Gitee**
> （`gitee.com/xhou/swiftmq`），GitHub 远端名是 **`swiftmq`**（`github.com/houzch/swiftmq`）。
> 推到 `origin` 只会同步到 Gitee，**不会触发 GitHub Actions**。

```bash
# 0) 确认远端映射（本仓库现状：origin -> Gitee，swiftmq -> GitHub）
git remote -v

# 1) 确认版本号已改好（broker.Version / web/package.json / web/package-lock.json / docker-compose.yml）
# 2) 打附带说明的 tag，并推到 **GitHub** 远端
git tag -a v1.0.0 -m "SwiftMQ 1.0.0"
git push swiftmq v1.0.0
```

推送后到 GitHub 仓库的 **Actions** 页看 `Release` 工作流：`binaries` → `release` → `image` 三个 job 依次跑完，
首页右侧的 Releases 与 Packages 就会出现内容。

> Actions 这条路径**不需要本机安装 `gh`**（runner 自带）；`gh` 只在第 5 节的手动发布里用得到。

### 4.3 只想跑其中一部分

- 只发二进制：在 workflow 里删掉 `image` job。
- 只发镜像：删掉 `binaries` + `release` 两个 job。

### 4.4 代码签名（可选，需自备证书）

不签名时：Windows 会弹 SmartScreen、macOS 会被 Gatekeeper 拦。要消除提示，必须有**付费证书**：

| 平台 | 需要什么 | 说明 |
| --- | --- | --- |
| Windows | 代码签名证书（OV / EV） | 2023 年后 CA 不再签发可直接导出的 PFX；实际多走 **Azure Trusted Signing**（约 $10/月）或 DigiCert KeyLocker / EV 硬件令牌 |
| macOS | Apple Developer Program（$99/年）→ **Developer ID Application** 证书 | 还必须**过公证（notarization）**，否则首次运行仍会被拦 |

仓库已提供两个脚本，**未配置证书时会自动跳过（打印警告并成功退出）**，所以不影响现有发布流程：

```bash
# Windows：PFX（本地调试/自签证书）
pwsh ./scripts/sign-windows.ps1 -Files dist/swiftmqd.exe,dist/swiftmqctl.exe `
  -PfxPath certs/codesign.pfx -PfxPassword '***'

# Windows：Azure Trusted Signing（推荐用于正式发布）
pwsh ./scripts/sign-windows.ps1 -Files dist/swiftmqd.exe,dist/swiftmqctl.exe -AzureTrustedSigning `
  -AzureMetadataPath azure-signing-metadata.json `
  -AzureDlibPath 'C:\azure-signing\Azure.CodeSigning.Dlib.dll'

# macOS：签名 + 公证（只能在 macOS 上执行）
./scripts/sign-macos.sh --files dist/swiftmqd dist/swiftmqctl \
  --p12 certs/developer-id.p12 --p12-password '***' \
  --notary-key keys/AuthKey_XXXXXX.p8 --notary-key-id XXXXXX \
  --notary-issuer 69a6de70-0000-0000-0000-000000000000 \
  --zip-out dist/notarize.zip
```

**接入 Actions** 的两条硬性约束：

1. **签名必须在打包之前**（先签二进制，再压 tar.gz/zip；否则改动了文件签名就失效）；
2. **必须在对应系统的 runner 上跑**：`signtool.exe` 只有 Windows 有，`codesign` / `notarytool` 只有 macOS 有——
   所以 ubuntu 上交叉编译出的 Windows/macOS 二进制要签名，得下载到对应 runner 再签名。

推荐做法：给矩阵加上 `runner` 维度（windows 目标用 `windows-latest`、darwin 目标用 `macos-latest`、linux 用 `ubuntu-latest`），把签名步骤插在 `go build` 之后、打包之前：

```yaml
      - name: Sign (Windows)
        if: matrix.goos == 'windows'
        shell: pwsh
        env:
          PFX_B64: ${{ secrets.WIN_PFX_BASE64 }}
          PFX_PASSWORD: ${{ secrets.WIN_PFX_PASSWORD }}
        run: |
          ./scripts/sign-windows.ps1 -Files (Join-Path stage 'swiftmqd.exe'),(Join-Path stage 'swiftmqctl.exe') `
            -PfxBase64 $env:PFX_B64 -PfxPassword $env:PFX_PASSWORD -RequireSigning

      - name: Sign + notarize (macOS)
        if: matrix.goos == 'darwin'
        env:
          MACOS_P12_BASE64: ${{ secrets.MACOS_P12_BASE64 }}
          MACOS_P12_PASSWORD: ${{ secrets.MACOS_P12_PASSWORD }}
          MACOS_NOTARY_KEY_ID: ${{ secrets.MACOS_NOTARY_KEY_ID }}
          MACOS_NOTARY_ISSUER: ${{ secrets.MACOS_NOTARY_ISSUER }}
          MACOS_NOTARY_KEY_BASE64: ${{ secrets.MACOS_NOTARY_KEY_BASE64 }}
          MACOS_CERTIFICATE_PASSWORD: ${{ secrets.MACOS_P12_PASSWORD }}
        run: |
          set -euo pipefail
          # secrets 以 base64 存放，先落盘到临时目录（CI 目录是临时的）
          echo "$MACOS_P12_BASE64"        | base64 -d > /tmp/cert.p12
          echo "$MACOS_NOTARY_KEY_BASE64" | base64 -d > /tmp/notary.p8
          chmod +x ./scripts/sign-macos.sh
          ./scripts/sign-macos.sh --files stage/swiftmqd stage/swiftmqctl \
            --p12 /tmp/cert.p12 --p12-password "$MACOS_CERTIFICATE_PASSWORD" \
            --notary-key /tmp/notary.p8 --notary-key-id "$MACOS_NOTARY_KEY_ID" \
            --notary-issuer "$MACOS_NOTARY_ISSUER" --require-signing
```

需要的 GitHub Secrets：

| Secret | 用途 |
| --- | --- |
| `WIN_PFX_BASE64` / `WIN_PFX_PASSWORD` | Windows 签名证书（base64）与口令 |
| `MACOS_P12_BASE64` / `MACOS_P12_PASSWORD` | macOS Developer ID 证书（base64）与口令 |
| `MACOS_NOTARY_KEY_BASE64` / `MACOS_NOTARY_KEY_ID` / `MACOS_NOTARY_ISSUER` | App Store Connect API Key（`.p8`）用于公证；也可改用 `APPLE_ID` + App 专用密码 |

两个注意点：

- **`secrets` 不能直接用在 job 级 `if:`**（GitHub 不支持）。要"有证书才签名"请用仓库变量控制，例如 `if: vars.SIGN_WINDOWS == 'true'`；不判断也行——脚本在没有凭据时会自行跳过。
- `scripts/sign-windows.ps1` 以 **UTF-8 with BOM** 保存：Windows PowerShell 5.1 在没有 BOM 时会按 ANSI/GBK 解码，中文注释会让脚本直接语法报错。

---

## 5. 方式二：本机手动发布

适合没有 CI、或临时补发产物。以下命令以 bash 为例（Windows 用 PowerShell 时见 5.4）。

### 5.1 准备环境与前端产物

```bash
gh auth status                     # 未登录则 gh auth login
docker login ghcr.io -u <你的GitHub用户名>   # 口令填 PAT（含 write:packages）

cd web && npm ci && npm run build && cd ..   # 必须：否则二进制没有管理 UI
```

### 5.2 交叉编译二进制并生成校验和

```bash
mkdir -p dist
for arch in amd64 arm64; do
  CGO_ENABLED=0 GOOS=linux GOARCH=$arch go build -trimpath -ldflags="-s -w" \
    -o dist/swiftmqd-linux-$arch   ./cmd/swiftmqd
  CGO_ENABLED=0 GOOS=linux GOARCH=$arch go build -trimpath -ldflags="-s -w" \
    -o dist/swiftmqctl-linux-$arch ./cmd/swiftmqctl
done

cd dist && sha256sum * > SHA256SUMS && cd ..
```

### 5.3 创建 Release 并上传附件

```bash
git tag -a v1.0.0 -m "SwiftMQ 1.0.0"
git push origin v1.0.0

gh release create v1.0.0 dist/* \
  --repo houzch/swiftmq \
  --title "SwiftMQ v1.0.0" \
  --generate-notes
```

已有 Release 想补传附件：

```bash
gh release upload v1.0.0 dist/SHA256SUMS --clobber
```

### 5.4 构建并推送镜像到 GHCR

```bash
IMAGE=ghcr.io/houzch/swiftmq
VER=1.0.0

# 单架构（amd64，最省事）
docker buildx build --platform linux/amd64 \
  -t "$IMAGE:$VER" -t "$IMAGE:1.0" -t "$IMAGE:1" -t "$IMAGE:latest" --push .

# 多架构（需要 buildx + QEMU）
docker run --privileged --rm tonistiigi/binfmt --install arm64
docker buildx create --use --name swiftmq-builder 2>/dev/null || true
docker buildx build --platform linux/amd64,linux/arm64 \
  -t "$IMAGE:$VER" -t "$IMAGE:1.0" -t "$IMAGE:1" -t "$IMAGE:latest" --push .
```

Windows PowerShell 生成校验和：

```powershell
Get-ChildItem dist\* -File | ForEach-Object {
  "$((Get-FileHash $_.FullName -Algorithm SHA256).Hash.ToLower())  $($_.Name)"
} | Set-Content dist\SHA256SUMS
```

---

## 6. 把镜像包设为公开

GHCR 的包**默认是私有**，即便仓库是公开的。不改可见性，别人 `docker pull` 会 401（或要求登录）。

路径：GitHub 个人头像 → **Packages** → `swiftmq` → 包详情页右下 **Package settings** → **Danger Zone** → **Change visibility** → 选 **Public**（需输入包名确认）。

> 该包在首次推送时会自动关联到 `houzch/swiftmq` 仓库（因为用的是本仓库的 `GITHUB_TOKEN`）。若用 PAT 从别的仓库推送，需在包设置里手动 Add repository。

---

## 7. 发布后验证

```bash
# Release 与附件
gh release view v1.0.0 --repo houzch/swiftmq

# 镜像可拉取
docker pull ghcr.io/houzch/swiftmq:1.0.0
docker image inspect ghcr.io/houzch/swiftmq:1.0.0 --format '{{.Size}}'

# 跑起来（管理 UI：http://localhost:15672，默认 guest/guest，首登强制改密）
docker run -d --name swiftmq -p 5672:5672 -p 1883:1883 -p 15672:15672 \
  ghcr.io/houzch/swiftmq:1.0.0
curl -s http://127.0.0.1:15672/api/default-language   # 应返回 {"default_language":"..."}
```

二进制归档验证（按平台解包）：

```bash
# ① 校验下载完整性（Linux）
sha256sum -c SHA256SUMS            # macOS 用 shasum -a 256 -c SHA256SUMS

# ② Linux / macOS：tar.gz 里是 swiftmqd / swiftmqctl（无扩展名）
tar -xzf swiftmq-1.0.0-linux-amd64.tar.gz
./swiftmqd -config configs/swiftmqd.json         # 日志里 version=1.0.0，管理 UI 在 :15672

# ③ macOS 首次运行若被 Gatekeeper 拦下
xattr -d com.apple.quarantine ./swiftmqd
```

```powershell
# ④ Windows（PowerShell）：zip 里是 swiftmqd.exe / swiftmqctl.exe
Expand-Archive swiftmq-1.0.0-windows-amd64.zip -DestinationPath .
.\swiftmqd.exe -config configs\swiftmqd.json
```

---

## 8. 回滚 / 删除

```bash
# 删 Release（并连带删除 tag）
gh release delete v1.0.0 --repo houzch/swiftmq --cleanup-tag --yes

# 只删远端/本地 tag
git push origin :refs/tags/v1.0.0
git tag -d v1.0.0

# 删除某个镜像版本：包页面 → 版本列表 → 右侧 ... → Delete
# 或走 REST API（需 packages:delete 权限）
gh api -X GET  /user/packages/container/swiftmq/versions            # 取 id
gh api -X DELETE /user/packages/container/swiftmq/versions/<id>
```

> 已发布的镜像 tag **不要复用**：例如把 `1.0.0` 指向新构建会让已拉取的环境产生"同 tag 不同内容"。出问题请发 `1.0.1` 而不是覆盖 `1.0.0`。

---

## 9. 常见问题

| 现象 | 原因 / 处理 |
| --- | --- |
| `docker pull` 报 `unauthorized` / `denied` | 包还是 private：按 §6 改成 Public，或先 `docker login ghcr.io` |
| `docker push` 报 `invalid reference format` | 镜像名含大写。GHCR 要求全小写，用 `ghcr.io/houzch/swiftmq` |
| Actions 里 `gh release create` 403 | 缺 `permissions: contents: write`（仓库 Settings → Actions 若为只读，需在 workflow 顶部显式声明） |
| Actions 里 `docker push` 403 | 缺 `permissions: packages: write` |
| 拉下来的二进制没有管理后台 | 构建前没跑 `npm ci && npm run build`（`web/dist` 不入库） |
| arm64 镜像构建卡住/失败 | 本地多架构需要 `tonistiigi/binfmt`；CI 需 `docker/setup-qemu-action` |
| Release 附件上传失败 | 单文件上限 2 GB；单个 Release 附件总量也有限制 |
| tag 与产品版本不一致 | tag 必须是 `v` + `broker.Version`；`/api/overview` 的 `product_version` 应等于去掉 `v` 的 tag |
| 只在 tag 推送时触发，手动跑没反应 | workflow 只监听 `push tags`；手动跑请用 `workflow_dispatch`（见下） |

需要"手动触发"时，把 `on:` 改成：

```yaml
on:
  push:
    tags: ['v*']
  workflow_dispatch:
    inputs:
      tag:
        description: '已存在的 tag（如 v1.0.0）'
        required: true
```

并在用 tag 的地方改为 `"${{ inputs.tag || github.ref_name }}"`、`${GITHUB_REF_NAME}` 改为 `"${{ inputs.tag }}"`。

---

## 10. 发布前检查清单

- [ ] `internal/broker/broker.go` 的 `broker.Version` 已改为 `1.0.0`
- [ ] `web/package.json` 与 `web/package-lock.json` 的 `version` 同步为 `1.0.0`
- [ ] `docker-compose.yml` 的 `image: swiftmq:1.0.0` 同步
- [ ] `gofmt -l .` 无输出；`go build ./...`、`go vet ./...`、`go test ./...` 通过
- [ ] GitHub CI 三平台全绿（`ci.yml`：ubuntu / windows / macos 各自跑 gofmt、vet、单测）
- [ ] `cd web && npm run type-check && npm run build` 通过
- [ ] README 的「已具备的能力」与实际一致
- [ ] tag 与版本号一致（`v1.0.0` ↔ `1.0.0`），且**该 tag 尚未存在**（复用 tag 会引发"同 tag 不同内容"）
- [ ] tag 推到的是 **GitHub 远端**：`git push swiftmq v1.0.0`（不是 Gitee 的 `origin`）
- [ ] 发布后：Releases 有附件 + `SHA256SUMS`，Packages 有 4 个 tag，包可见性符合预期
