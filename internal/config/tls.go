package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLS 是监听上的 TLS 配置。
//
// 同一份配置服务两类监听：协议接入（AMQP / MQTT）与管理面 HTTP ——
// 这样运维只需要理解一套字段。范围刻意收窄到"服务端证书 + 可选客户端证书校验"：
// 覆盖单向与双向认证这两个真实场景，SNI 多证书、密码套件白名单等有需求再加。
type TLS struct {
	// CertFile 与 KeyFile 是服务端证书链与私钥（PEM）。
	CertFile string `json:"cert_file,omitempty"`
	KeyFile  string `json:"key_file,omitempty"`
	// CAFile 是用于校验**客户端**证书的 CA（PEM）；双向认证时要提供。
	CAFile string `json:"ca_file,omitempty"`
	// ClientAuth 是客户端证书策略：none（默认）/ request / require / verify_if_given / require_and_verify。
	ClientAuth string `json:"client_auth,omitempty"`
	// MinVersion 是最低 TLS 版本：1.2（默认）或 1.3。
	MinVersion string `json:"min_version,omitempty"`
}

// Enabled 表示该配置要求开启 TLS。nil 接收者安全，便于"没配 tls 段"的常态。
func (t *TLS) Enabled() bool {
	return t != nil && (t.CertFile != "" || t.KeyFile != "")
}

// normalize 校验并补齐 TLS 配置。where 形如 `listeners.amqp091[1]`，用于把错误定位到具体监听。
func (t *TLS) normalize(where string) error {
	if t == nil {
		return nil
	}
	if !t.Enabled() {
		if t.CAFile != "" || t.ClientAuth != "" || t.MinVersion != "" {
			return fmt.Errorf("%s.tls 缺少 cert_file / key_file（只给了其它字段）", where)
		}
		return nil
	}
	if t.CertFile == "" || t.KeyFile == "" {
		return fmt.Errorf("%s.tls 需要同时提供 cert_file 与 key_file", where)
	}
	switch t.ClientAuth {
	case "", "none", "request", "require", "verify_if_given", "require_and_verify":
	default:
		return fmt.Errorf("%s.tls.client_auth 取值非法: %q（可选 none / request / require / verify_if_given / require_and_verify）",
			where, t.ClientAuth)
	}
	switch t.MinVersion {
	case "", "1.2", "1.3":
	default:
		return fmt.Errorf("%s.tls.min_version 取值非法: %q（可选 1.2 / 1.3）", where, t.MinVersion)
	}
	// 最容易被忽略、后果又最糟的一条：要求校验客户端证书却没给 CA，会让**所有**客户端连不上，
	// 且现场很难看出是配置问题。启动时就拒绝。
	if (t.ClientAuth == "require_and_verify" || t.ClientAuth == "verify_if_given") && t.CAFile == "" {
		return fmt.Errorf("%s.tls.client_auth=%s 需要同时提供 ca_file，否则客户端证书无法校验",
			where, t.ClientAuth)
	}
	// 真正读一遍证书：路径写错、文件损坏都要在**配置加载**阶段暴露。
	//
	// 只在这里做"存在性/可解析性"校验，是为了让协议监听与管理面**行为一致**：
	// 监听侧若只在校验通过、启动时才读证书，证书错误会退化成"插件被置为 failed、
	// 内核照常启动"，与"配错了就该拒绝启动"的预期不符。错误信息里带上 where，便于定位。
	if _, err := t.Config(); err != nil {
		return fmt.Errorf("%s.tls 无效: %w", where, err)
	}
	return nil
}

// Config 构造运行时用的 *tls.Config；未启用 TLS 时返回 nil。
//
// 证书在**启动时**读取并解析：配置写错要立刻拒绝启动，而不是等第一个客户端连上来才暴露。
func (t *TLS) Config() (*tls.Config, error) {
	if !t.Enabled() {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("加载服务端证书失败（cert=%s key=%s）: %w", t.CertFile, t.KeyFile, err)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if t.MinVersion == "1.3" {
		cfg.MinVersion = tls.VersionTLS13
	}
	if t.CAFile != "" {
		pem, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("读取 ca_file 失败（%s）: %w", t.CAFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_file 不是有效的 PEM 证书: %s", t.CAFile)
		}
		cfg.ClientCAs = pool
	}
	switch t.ClientAuth {
	case "", "none":
		cfg.ClientAuth = tls.NoClientCert
	case "request":
		cfg.ClientAuth = tls.RequestClientCert
	case "require":
		cfg.ClientAuth = tls.RequireAnyClientCert
	case "verify_if_given":
		cfg.ClientAuth = tls.VerifyClientCertIfGiven
	case "require_and_verify":
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}
