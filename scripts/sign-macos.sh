#!/usr/bin/env bash
#
# 为 SpeedMQ 的 macOS 二进制签名（codesign）并公证（notarytool），最后自检。
#
# 为什么需要两步：
#   - codesign 用「Developer ID Application」证书签名，并开启 hardened runtime；
#   - 只有通过「公证（notarization）」，用户首次运行时才不会被 Gatekeeper 拦下。
#   注意：裸可执行文件（非 .app/.pkg/.dmg）**无法 staple**（票据不能写进文件本身），
#   Gatekeeper 会联网查 Apple 的公证记录来放行；如需离线可验证，请改发 .pkg 并用 --staple。
#
# 用法（本地）：
#   ./scripts/sign-macos.sh \
#     --files dist/speedmqd dist/speedmqctl \
#     --p12 ~/certs/developer-id.p12 --p12-password '***' \
#     --notary-key ~/keys/AuthKey_ABC12345.p8 --notary-key-id ABC12345 \
#     --notary-issuer 69a6de70-0000-0000-0000-000000000000 \
#     --zip-out dist/notarize.zip
#
#   # 也可以改用 Apple ID + App 专用密码的形式：
#   ./scripts/sign-macos.sh --files ... --p12 ... --p12-password ... \
#     --apple-id you@example.com --apple-password 'app-specific-pw' --team-id TEAMID123
#
# 未提供证书时：**默认跳过并成功退出**（打印警告），保证没有证书的人也能跑通流水线；
# 要"没证书就报错"时加 --require-signing。
#
set -euo pipefail

FILES=()
P12=""
P12_PASSWORD="${P12_PASSWORD:-}"
IDENTITY=""
KEYCHAIN=""
ZIP_OUT=""
ENTITLEMENTS=""

NOTARY_KEY=""
NOTARY_KEY_ID=""
NOTARY_ISSUER=""
APPLE_ID=""
APPLE_PASSWORD="${APPLE_PASSWORD:-}"
TEAM_ID=""

STAPLE=""
REQUIRE_SIGNING=0

usage() {
  sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --files)            shift; while [ $# -gt 0 ] && [ "${1#--}" = "$1" ]; do FILES+=("$1"); shift; done ;;
    --p12)              P12="${2:?}"; shift 2 ;;
    --p12-password)     P12_PASSWORD="${2:?}"; shift 2 ;;
    --identity)         IDENTITY="${2:?}"; shift 2 ;;
    --keychain)         KEYCHAIN="${2:?}"; shift 2 ;;
    --entitlements)     ENTITLEMENTS="${2:?}"; shift 2 ;;
    --zip-out)          ZIP_OUT="${2:?}"; shift 2 ;;
    --notary-key)       NOTARY_KEY="${2:?}"; shift 2 ;;
    --notary-key-id)    NOTARY_KEY_ID="${2:?}"; shift 2 ;;
    --notary-issuer)    NOTARY_ISSUER="${2:?}"; shift 2 ;;
    --apple-id)         APPLE_ID="${2:?}"; shift 2 ;;
    --apple-password)   APPLE_PASSWORD="${2:?}"; shift 2 ;;
    --team-id)          TEAM_ID="${2:?}"; shift 2 ;;
    --staple)           STAPLE="${2:?}"; shift 2 ;;
    --require-signing)  REQUIRE_SIGNING=1; shift ;;
    -h|--help)          usage 0 ;;
    *) echo "未知参数: $1" >&2; usage 1 ;;
  esac
done

log()  { printf '  %s\n' "$*"; }

if [ "${#FILES[@]}" -eq 0 ]; then
  echo "错误：至少要用 --files 指定一个待签名文件" >&2
  usage 1
fi

if [ "$(uname -s)" != "Darwin" ]; then
  if [ "$REQUIRE_SIGNING" -eq 1 ]; then
    echo "错误：macOS 签名/公证只能在 macOS 上执行（当前 $(uname -s)）" >&2
    exit 1
  fi
  echo "警告：当前不是 macOS，跳过签名与公证。"
  exit 0
fi

for f in "${FILES[@]}"; do
  [ -f "$f" ] || { echo "错误：待签名文件不存在: $f" >&2; exit 1; }
done

# ---------- 未配置证书：按需跳过 ----------
if [ -z "$P12" ]; then
  if [ "$REQUIRE_SIGNING" -eq 1 ]; then
    echo "错误：未提供 --p12（Developer ID 证书），但指定了 --require-signing" >&2
    exit 1
  fi
  echo "警告：未配置 macOS 签名证书（--p12），跳过签名与公证。"
  echo "      产物仍可使用，但用户首次运行会被 Gatekeeper 拦截。"
  exit 0
fi
[ -f "$P12" ] || { echo "错误：证书文件不存在: $P12" >&2; exit 1; }

# ---------- 临时 keychain ----------
TEMP_KEYCHAIN=0
if [ -z "$KEYCHAIN" ]; then
  KEYCHAIN="$(mktemp -u "${TMPDIR:-/tmp}/speedmq-sign-XXXXXX.keychain-db")"
  TEMP_KEYCHAIN=1
fi
KC_PASSWORD="$(openssl rand -hex 16 2>/dev/null || date +%s%N)"

cleanup() {
  if [ "$TEMP_KEYCHAIN" -eq 1 ]; then
    security delete-keychain "$KEYCHAIN" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "==> 准备临时 keychain"
security create-keychain -p "$KC_PASSWORD" "$KEYCHAIN"
security set-keychain-settings -lut 3600 "$KEYCHAIN"
security unlock-keychain -p "$KC_PASSWORD" "$KEYCHAIN"
security import "$P12" -k "$KEYCHAIN" -P "$P12_PASSWORD" -T /usr/bin/codesign -T /usr/bin/security >/dev/null
# 允许 codesign 非交互式访问私钥（不加这一步会弹窗/失败）
security set-key-partition-list -S apple-tool:,apple:,codesign: -s -k "$KC_PASSWORD" "$KEYCHAIN" >/dev/null

# 把临时 keychain 加进搜索列表，同时保留原有列表
ORIGINAL_KEYCHAINS="$(security list-keychains -d user | sed 's/^[[:space:]]*"//; s/"$//' | tr '\n' ' ')"
# shellcheck disable=SC2086
security list-keychains -d user -s "$KEYCHAIN" $ORIGINAL_KEYCHAINS

# ---------- 解析签名身份 ----------
if [ -z "$IDENTITY" ]; then
  IDENTITY="$(security find-identity -v -p codesigning "$KEYCHAIN" \
    | awk -F'"' '/Developer ID Application/ { print $2; exit }')"
fi
if [ -z "$IDENTITY" ]; then
  echo "错误：在证书里找不到「Developer ID Application」身份；请确认 p12 是 Developer ID 证书，或用 --identity 指定。" >&2
  security find-identity -v -p codesigning "$KEYCHAIN" >&2 || true
  exit 1
fi
log "签名身份：$IDENTITY"

# ---------- 签名 + 校验 ----------
for f in "${FILES[@]}"; do
  echo "==> 签名 $f"
  sign_args=(--force --options runtime --timestamp --sign "$IDENTITY")
  if [ -n "$ENTITLEMENTS" ]; then sign_args+=(--entitlements "$ENTITLEMENTS"); fi
  codesign "${sign_args[@]}" "$f"
  codesign --verify --strict --verbose=2 "$f"
done

# ---------- 公证 ----------
NOTARY_MODE=""
if [ -n "$NOTARY_KEY" ] && [ -n "$NOTARY_KEY_ID" ] && [ -n "$NOTARY_ISSUER" ]; then
  NOTARY_MODE="key"
elif [ -n "$APPLE_ID" ] && [ -n "$APPLE_PASSWORD" ] && [ -n "$TEAM_ID" ]; then
  NOTARY_MODE="appleid"
fi

if [ -z "$NOTARY_MODE" ]; then
  echo "警告：未提供公证凭据（--notary-key/--notary-key-id/--notary-issuer 或 --apple-id/--apple-password/--team-id），跳过公证。"
  echo "      只签名不公证，用户首次运行仍可能被 Gatekeeper 拦截。"
  exit 0
fi

# 公证用的 zip 必须放在 stage 目录**之外**，否则 ditto 会把自己也打进去
TMPROOT="$(mktemp -d "${TMPDIR:-/tmp}/speedmq-notarize-XXXXXX")"
STAGE="$TMPROOT/payload"
mkdir -p "$STAGE"
trap 'rm -rf "$TMPROOT"; cleanup' EXIT
for f in "${FILES[@]}"; do cp "$f" "$STAGE/"; done

if [ -z "$ZIP_OUT" ]; then ZIP_OUT="$TMPROOT/speedmq-notarize.zip"; fi
mkdir -p "$(dirname "$ZIP_OUT")"
rm -f "$ZIP_OUT"
# 公证要求 zip（用 ditto 打包，不要用 zip 命令）
ditto -c -k --keepParent "$STAGE" "$ZIP_OUT"

echo "==> 提交公证（可能需要几分钟）"
if [ "$NOTARY_MODE" = "key" ]; then
  xcrun notarytool submit "$ZIP_OUT" \
    --key "$NOTARY_KEY" --key-id "$NOTARY_KEY_ID" --issuer "$NOTARY_ISSUER" --wait
else
  xcrun notarytool submit "$ZIP_OUT" \
    --apple-id "$APPLE_ID" --password "$APPLE_PASSWORD" --team-id "$TEAM_ID" --wait
fi

# ---------- 公证后自检 ----------
for f in "${FILES[@]}"; do
  echo "==> Gatekeeper 校验 $f"
  # 裸二进制无法 staple，这里依赖 Apple 的在线公证记录
  spctl -a -vvv -t exec "$f"
done

# 只有 .app/.pkg/.dmg 才能 staple
if [ -n "$STAPLE" ]; then
  echo "==> staple $STAPLE"
  xcrun stapler staple "$STAPLE"
  xcrun stapler validate "$STAPLE"
fi

echo "macOS 签名 + 公证完成：共 ${#FILES[@]} 个文件。"
