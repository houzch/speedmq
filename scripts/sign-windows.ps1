<#
.SYNOPSIS
    为 SpeedMQ 的 Windows 二进制做 Authenticode 代码签名（signtool）。

.DESCRIPTION
    支持两种证书形态：

      1) PFX 文件 —— 本地开发证书，或允许导出 PFX 的证书：
         -PfxPath（文件）或 -PfxBase64（CI 里把证书放 secret）+ -PfxPassword
      2) Azure Trusted Signing —— 正式发布推荐（私钥不进内存/磁盘、无需采购 HSM）：
         -AzureTrustedSigning -AzureMetadataPath <metadata.json> -AzureDlibPath <Azure.CodeSigning.Dlib.dll>

    签名用 SHA256 + 时间戳（默认 DigiCert），签完立刻 `signtool verify` 自检。

    未提供任何证书凭据时：**默认跳过并成功退出**（打印警告），这样没有证书的人也能跑通发布流水线；
    要"没证书就报错"时加 -RequireSigning。

.PARAMETER Files
    要签名的文件（必填），例如 .\dist\speedmqd.exe, .\dist\speedmqctl.exe

.PARAMETER TimestampUrl
    RFC3161 时间戳服务，默认 http://timestamp.digicert.com

.EXAMPLE
    # 本地：用 PFX 签名
    .\scripts\sign-windows.ps1 -Files .\dist\speedmqd.exe,.\dist\speedmqctl.exe `
        -PfxPath .\certs\codesign.pfx -PfxPassword '***'

.EXAMPLE
    # CI：证书以 base64 放在 secret 中
    .\scripts\sign-windows.ps1 -Files $exe -PfxBase64 $env:PFX_B64 -PfxPassword $env:PFX_PASSWORD -RequireSigning

.EXAMPLE
    # Azure Trusted Signing
    .\scripts\sign-windows.ps1 -Files $exe -AzureTrustedSigning `
        -AzureMetadataPath .\azure-signing-metadata.json `
        -AzureDlibPath 'C:\azure-signing\Azure.CodeSigning.Dlib.dll'
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string[]]$Files,

    [string]$PfxPath,
    [string]$PfxPassword,
    [string]$PfxBase64,

    [string]$TimestampUrl = 'http://timestamp.digicert.com',

    [switch]$AzureTrustedSigning,
    [string]$AzureMetadataPath,
    [string]$AzureDlibPath,

    [switch]$RequireSigning
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# signtool.exe 不在 PATH 时，去 Windows SDK 目录里找最新的 x64 版本
function Resolve-SignTool {
    $cmd = Get-Command signtool.exe -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }

    $roots = @()
    if (${env:ProgramFiles(x86)}) { $roots += (Join-Path ${env:ProgramFiles(x86)} 'Windows Kits\10\bin') }
    if ($env:ProgramFiles) { $roots += (Join-Path $env:ProgramFiles 'Windows Kits\10\bin') }

    foreach ($root in $roots) {
        if (-not (Test-Path -LiteralPath $root)) { continue }
        $hit = Get-ChildItem -LiteralPath $root -Filter 'signtool.exe' -Recurse -ErrorAction SilentlyContinue |
            Where-Object { $_.FullName -match '\\x64\\' } |
            Sort-Object -Property FullName -Descending |
            Select-Object -First 1
        if ($hit) { return $hit.FullName }
    }
    return $null
}

# 目标文件必须存在，避免"签了个空文件还报成功"
$targets = @()
foreach ($f in $Files) {
    if (-not (Test-Path -LiteralPath $f)) { throw "待签名文件不存在: $f" }
    $targets += (Resolve-Path -LiteralPath $f).Path
}

$configured = $AzureTrustedSigning -or $PfxPath -or $PfxBase64
if (-not $configured) {
    if ($RequireSigning) { throw '未提供签名凭据（-PfxPath/-PfxBase64/-AzureTrustedSigning），但指定了 -RequireSigning' }
    Write-Warning '未配置 Windows 代码签名凭据，跳过签名（产物可用，但会触发 SmartScreen 提示）。'
    exit 0
}

$signtool = Resolve-SignTool
if (-not $signtool) {
    if ($RequireSigning) { throw '找不到 signtool.exe（需安装 Windows SDK 的「Signing Tools」组件）' }
    Write-Warning '找不到 signtool.exe，跳过签名。'
    exit 0
}

# 声明在分支之外：finally 里要统一清理（StrictMode 下未定义变量会直接报错）
$tempPfx = $null

if ($AzureTrustedSigning) {
    if (-not $AzureMetadataPath -or -not $AzureDlibPath) {
        throw 'Azure Trusted Signing 需要同时提供 -AzureMetadataPath 与 -AzureDlibPath'
    }
    if (-not (Test-Path -LiteralPath $AzureMetadataPath)) { throw "metadata 文件不存在: $AzureMetadataPath" }
    if (-not (Test-Path -LiteralPath $AzureDlibPath)) { throw "dlib 文件不存在: $AzureDlibPath" }
    Write-Host "使用 Azure Trusted Signing（metadata: $AzureMetadataPath）"
}
else {
    # CI 场景：把 base64 证书落到临时文件，签完立即删除
    if (-not $PfxPath -and $PfxBase64) {
        $tempPfx = Join-Path ([System.IO.Path]::GetTempPath()) ("speedmq-sign-" + [guid]::NewGuid().ToString('N') + ".pfx")
        [System.IO.File]::WriteAllBytes($tempPfx, [Convert]::FromBase64String($PfxBase64))
        $PfxPath = $tempPfx
    }
    if (-not $PfxPath) { throw '未提供 -PfxPath 或 -PfxBase64' }
    if (-not (Test-Path -LiteralPath $PfxPath)) { throw "证书文件不存在: $PfxPath" }
}

$commonArgs = @('sign', '/fd', 'SHA256', '/td', 'SHA256', '/tr', $TimestampUrl, '/v')
if ($AzureTrustedSigning) {
    $commonArgs += @('/dlib', (Resolve-Path -LiteralPath $AzureDlibPath).Path,
                     '/dmdf', (Resolve-Path -LiteralPath $AzureMetadataPath).Path)
}
else {
    $commonArgs += @('/f', (Resolve-Path -LiteralPath $PfxPath).Path)
    if ($PfxPassword) { $commonArgs += @('/p', $PfxPassword) }
}

try {
    foreach ($target in $targets) {
        Write-Host "签名: $target"
        & $signtool @commonArgs $target
        if ($LASTEXITCODE -ne 0) { throw "签名失败: $target（signtool 退出码 $LASTEXITCODE）" }

        Write-Host "校验: $target"
        & $signtool verify /pa /v $target
        if ($LASTEXITCODE -ne 0) { throw "签名校验失败: $target（signtool 退出码 $LASTEXITCODE）" }
    }
    Write-Host "Windows 代码签名完成：共 $($targets.Count) 个文件。"
}
finally {
    if ($tempPfx -and (Test-Path -LiteralPath $tempPfx)) {
        Remove-Item -LiteralPath $tempPfx -Force -ErrorAction SilentlyContinue
    }
}
