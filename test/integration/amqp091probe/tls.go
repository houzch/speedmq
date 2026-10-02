package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// M8-3 的端到端验证入口：TLS 监听是否真的能用。
//
// 探针的**每一条**连接都必须经过 dial()：TLS 模式下连错协议会直接握手失败，
// 因此"24 条用例全部跑通"本身就是接线正确的证据，不需要额外的断言。

var (
	tlsCA = flag.String("tls-ca", "", "TLS 连接：用于校验服务端证书的 CA（PEM 文件）；留空则走明文")
	gcert = flag.String("gencert", "", "只生成自签证书（cert.pem / key.pem）到该目录后退出")
)

// dialConfig 给出连接所需的 scheme 与 TLS 配置（由 -tls-ca 决定）。
//
// 抽出来是为了让需要自定义 amqp.Config 的用例（如 frame-max）复用同一份 TLS 判断 ——
// 各写一份迟早会出现"TLS 只在一个用例里生效"的假绿。
func dialConfig() (scheme string, tlsConf *tls.Config, err error) {
	if *tlsCA == "" {
		return "amqp", nil, nil
	}
	pemBytes, err := os.ReadFile(*tlsCA)
	if err != nil {
		return "", nil, fmt.Errorf("读取 -tls-ca 失败: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return "", nil, fmt.Errorf("-tls-ca 不是有效的 PEM 证书: %s", *tlsCA)
	}
	return "amqps", &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
}

// probeURL 是按当前模式（明文 / TLS）拼好的目标地址。
func probeURL() string {
	scheme, _, err := dialConfig()
	if err != nil {
		scheme = "amqp"
	}
	return strings.Replace(url(""), "amqp://", scheme+"://", 1) //nolint:gocritic // 明确只换一次前缀
}

// dial 按 -tls-ca 决定走明文还是 TLS。
//
// 刻意不依赖系统根证书池：e2e 用的是自签证书，只有显式给出 CA 才应该通过校验 ——
// 这同时验证了"服务端确实用了我们配置的那张证书"。
func dial(u string) (*amqp.Connection, error) {
	_, tlsConf, err := dialConfig()
	if err != nil {
		return nil, err
	}
	if tlsConf == nil {
		return amqp.Dial(u)
	}
	// 必须把 scheme 换成 amqps —— 该库的 DialTLS **只在 URL 是 amqps:// 时才真正启用 TLS**
	// （见 connection.go 的注释）。传 amqp:// 进去会得到一条**明文**连接，
	// 而服务端是 TLS 端口，表现为握手层 "first record does not look like a TLS handshake"。
	// 这个坑实测踩过一次。
	return amqp.DialTLS(strings.Replace(u, "amqp://", "amqps://", 1), tlsConf)
}

// generateCert 生成一对自签证书（证书同时充当 CA），写入 dir。
//
// 放在探针里是为了让 e2e 脚本不依赖 openssl：Windows 上装 openssl 是额外门槛，
// 而这里本来就有 crypto/x509。
func generateCert(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "swiftmq-probe"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "cert.pem"),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		return err
	}
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "key.pem"),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600)
}
