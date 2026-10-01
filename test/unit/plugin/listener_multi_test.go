// Package plugin_test 的监听清单用例：覆盖 M8-3 的接线层。
//
// 关注点是"配置里写了几个监听，就真的监听几个端口"，以及 TLS 端口是否真的能握手 ——
// 而不是"配置结构体的字段被填充了"。
package plugin_test

import (
	"context"
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
	"testing"
	"time"

	"github.com/houzch/swiftmq/internal/config"
	"github.com/houzch/swiftmq/internal/protocol/amqp091"
)

// selfSignedCert 生成一对自签证书（证书同时充当自己的 CA）并写盘。
func selfSignedCert(t *testing.T, dir string) (certPath, keyPath string) {
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
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("写证书失败: %v", err)
	}
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("序列化私钥失败: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600); err != nil {
		t.Fatalf("写私钥失败: %v", err)
	}
	return certPath, keyPath
}

// loadConfig 把 JSON 落到临时目录并走一遍真实的 config.Load。
func loadConfig(t *testing.T, body string) *config.Config {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "swiftmqd.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("写测试配置失败: %v", err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("加载测试配置失败: %v", err)
	}
	return cfg
}

// TestConfigDeclaresMultipleListeners 覆盖"一个插件同时开明文与 TLS 两个端口"。
//
// 这是 M8-3 的关键接线：早先的实现只覆盖默认监听的前 N 项，多出来的配置项被静默丢弃，
// 于是 TLS 端口压根没监听、客户端也连不上，而日志里看不出任何异常。
func TestConfigDeclaresMultipleListeners(t *testing.T) {
	certPath, keyPath := selfSignedCert(t, t.TempDir())
	cfg := loadConfig(t, fmt.Sprintf(`{
		"listeners": {"amqp091": [
			{"addr": "127.0.0.1:0"},
			{"addr": "127.0.0.1:0", "tls": {"cert_file": %q, "key_file": %q}}
		]}
	}`, certPath, keyPath))

	manager, server := newGovernanceEnv(t, cfg)
	if err := manager.Load(context.Background(), amqp091.New()); err != nil {
		t.Fatalf("加载 amqp091 插件失败: %v", err)
	}

	snap := server.Listeners()
	if len(snap) != 2 {
		t.Fatalf("配置里写了 2 个监听，实际只起了 %d 个: %+v", len(snap), snap)
	}
	byName := map[string]string{}
	for _, s := range snap {
		byName[s.Listener] = s.Addr
	}
	// 第 1 项沿用默认名字 amqp；超出默认数量的监听按 <默认名>-<序号> 命名。
	if _, ok := byName["amqp"]; !ok {
		t.Fatalf("第 1 个监听应名为 amqp，实际 %v", byName)
	}
	tlsAddr, ok := byName["amqp-2"]
	if !ok {
		t.Fatalf("第 2 个监听应名为 amqp-2，实际 %v", byName)
	}

	// 真握手：TLS 客户端能在这个端口上完成握手。
	conn, err := tls.Dial("tcp", tlsAddr, &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("TLS 监听 %s 握手失败: %v", tlsAddr, err)
	}
	_ = conn.Close()

	// 明文客户端连同一端口必须拿不到任何协议数据（服务端在握手上就拒绝）。
	pconn, err := net.Dial("tcp", tlsAddr)
	if err != nil {
		return
	}
	defer pconn.Close()
	_, _ = pconn.Write([]byte("AMQP\x00\x00\x09\x01"))
	_ = pconn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if n, err := pconn.Read(make([]byte, 8)); err == nil && n > 0 {
		t.Fatalf("明文客户端竟然在 TLS 端口上收到了 %d 字节数据", n)
	}
}

// TestTLSListenerRejectsUnreadableCert 覆盖"证书读不出来就拒绝启动"。
//
// 断言落在配置加载阶段：这样协议监听与管理面的行为才一致 ——
// 若只在校验通过后、插件启动时才读证书，证书错误会退化成"插件 failed、内核照常启动"。
func TestTLSListenerRejectsUnreadableCert(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "swiftmqd.json")
	body := `{
		"listeners": {"amqp091": [
			{"addr": "127.0.0.1:0", "tls": {"cert_file": "/nonexistent/cert.pem", "key_file": "/nonexistent/key.pem"}}
		]}
	}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("写测试配置失败: %v", err)
	}
	if _, err := config.Load(path); err == nil {
		t.Fatal("证书读不出来时应当在配置加载阶段就被拒绝")
	}
}
