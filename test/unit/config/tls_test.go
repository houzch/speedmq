package config_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/config"
)

// 本文件覆盖 M8-3 的**配置面**：TLS 配置的校验与 *tls.Config 的构造。
//
// "TLS 监听真的能握手"属于接线层，由真进程端到端验证覆盖
// （test/integration/amqp091probe 的 -tls-ca / -gencert）。

// selfSigned 生成一对自签证书（证书同时充当自己的 CA）并写盘。
func selfSigned(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("生成私钥失败: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "swiftmq-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("签发证书失败: %v", err)
	}
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	writePEM(t, certPath, "CERTIFICATE", der)
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("序列化私钥失败: %v", err)
	}
	writePEM(t, keyPath, "EC PRIVATE KEY", kb)
	return certPath, keyPath
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	buf := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatalf("写入 %s 失败: %v", path, err)
	}
}

func TestTLSConfigDisabled(t *testing.T) {
	var nilTLS *config.TLS
	if cfg, err := nilTLS.Config(); cfg != nil || err != nil {
		t.Fatalf("未配置 TLS 时应返回 (nil, nil)，实际 (%v, %v)", cfg, err)
	}
	if nilTLS.Enabled() {
		t.Fatal("nil 的 TLS 配置不应被视为启用")
	}
	if cfg, err := (&config.TLS{}).Config(); cfg != nil || err != nil {
		t.Fatalf("空 TLS 配置应返回 (nil, nil)，实际 (%v, %v)", cfg, err)
	}
}

func TestTLSConfigLoadsCertificate(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := selfSigned(t, dir)

	cfg, err := (&config.TLS{CertFile: certPath, KeyFile: keyPath}).Config()
	if err != nil {
		t.Fatalf("构造 tls.Config 失败: %v", err)
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("应加载 1 张证书，实际 %d", len(cfg.Certificates))
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("默认最低版本应为 TLS 1.2，实际 %d", cfg.MinVersion)
	}
	if cfg.ClientAuth != tls.NoClientCert {
		t.Fatalf("默认不应要求客户端证书，实际 %v", cfg.ClientAuth)
	}
}

func TestTLSConfigClientAuthAndMinVersion(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := selfSigned(t, dir)

	cfg, err := (&config.TLS{
		CertFile: certPath, KeyFile: keyPath, CAFile: certPath,
		ClientAuth: "require_and_verify", MinVersion: "1.3",
	}).Config()
	if err != nil {
		t.Fatalf("构造 tls.Config 失败: %v", err)
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("require_and_verify 应映射为 RequireAndVerifyClientCert，实际 %v", cfg.ClientAuth)
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Fatalf("min_version=1.3 应生效，实际 %d", cfg.MinVersion)
	}
	if cfg.ClientCAs == nil {
		t.Fatal("提供了 ca_file 时应设置 ClientCAs")
	}
}

func TestTLSConfigRejectsBadInput(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := selfSigned(t, dir)
	badCA := filepath.Join(dir, "bad-ca.pem")
	if err := os.WriteFile(badCA, []byte("not a pem"), 0o600); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		desc string
		tls  *config.TLS
	}{
		{"只有 cert 没有 key", &config.TLS{CertFile: certPath}},
		{"证书文件不存在", &config.TLS{CertFile: filepath.Join(dir, "nope.pem"), KeyFile: keyPath}},
		{"ca_file 不是 PEM", &config.TLS{CertFile: certPath, KeyFile: keyPath, CAFile: badCA}},
	}
	for _, c := range cases {
		if _, err := c.tls.Config(); err == nil {
			t.Fatalf("%s：应当报错", c.desc)
		}
	}
}

// TestLoadValidatesTLS 覆盖配置校验：这些错误必须在**启动时**被拒绝，
// 而不是等第一个客户端连上来才暴露。
func TestLoadValidatesTLS(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := selfSigned(t, dir)

	write := func(t *testing.T, body string) string {
		t.Helper()
		p := filepath.Join(t.TempDir(), "swiftmqd.json")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatalf("写配置失败: %v", err)
		}
		return p
	}

	// 合法：协议监听与管理面同时开 TLS
	ok := write(t, fmt.Sprintf(`{
		"listeners": {"amqp091": [{"addr": ":5671", "tls": {"cert_file": %q, "key_file": %q}}]},
		"management": {"enabled": true, "addr": ":15671", "tls": {"cert_file": %q, "key_file": %q}}
	}`, certPath, keyPath, certPath, keyPath))
	if _, err := config.Load(ok); err != nil {
		t.Fatalf("合法的 TLS 配置被拒: %v", err)
	}

	bad := []struct {
		desc string
		body string
		want string
	}{
		{
			"监听只给了 cert_file",
			fmt.Sprintf(`{"listeners": {"amqp091": [{"addr": ":5671", "tls": {"cert_file": %q}}]}}`, certPath),
			"同时提供 cert_file 与 key_file",
		},
		{
			"client_auth 取值非法",
			fmt.Sprintf(`{"listeners": {"amqp091": [{"addr": ":5671", "tls": {"cert_file": %q, "key_file": %q, "client_auth": "maybe"}}]}}`, certPath, keyPath),
			"client_auth 取值非法",
		},
		{
			"要求校验客户端证书却没给 ca_file",
			fmt.Sprintf(`{"listeners": {"amqp091": [{"addr": ":5671", "tls": {"cert_file": %q, "key_file": %q, "client_auth": "require_and_verify"}}]}}`, certPath, keyPath),
			"需要同时提供 ca_file",
		},
		{
			"管理面 TLS 只给了 ca_file",
			`{"management": {"enabled": true, "tls": {"ca_file": "/tmp/ca.pem"}}}`,
			"缺少 cert_file / key_file",
		},
		{
			"证书文件不存在",
			`{"listeners": {"amqp091": [{"addr": ":5671", "tls": {"cert_file": "/nonexistent/cert.pem", "key_file": "/nonexistent/key.pem"}}]}}`,
			"listeners.amqp091[0].tls 无效",
		},
	}
	for _, c := range bad {
		_, err := config.Load(write(t, c.body))
		if err == nil {
			t.Fatalf("%s：应当被拒绝", c.desc)
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s：错误信息应包含 %q，实际 %q", c.desc, c.want, err.Error())
		}
	}
}
