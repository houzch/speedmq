// Package config 负责内核配置的加载与默认值。
//
// M1 只支持"默认值 + JSON 配置文件覆盖"；YAML 与 SWIFTMQ_* 环境变量在 M5 随管理面一起补齐。
package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// User 是内置用户表的一条记录（v1 仅内存用户表，见设计文档 1.4 认证决策）。
type User struct {
	// Password 明文口令。v1 仅用于内网开发环境，后续由认证插件提供哈希/外部后端。
	Password string `json:"password"`
	// Tags 用户标签（administrator / management / monitoring），M3 起生效。
	Tags []string `json:"tags,omitempty"`
	// RemoteAccess 为 true 时允许从非本机地址登录。
	// 对齐 RabbitMQ：内置 guest 用户默认仅允许本机登录。
	RemoteAccess bool `json:"remote_access,omitempty"`
}

// Listener 是监听配置。
type Listener struct {
	// Addr 形如 ":5672"。
	Addr string `json:"addr"`
}

// Config 是内核配置。
type Config struct {
	// DataDir 节点数据目录（对齐 RABBITMQ_MNESIA_DIR 的定位，见设计文档 16.1）。
	DataDir string `json:"data_dir"`
	// DefaultVHost 默认 vhost 名。
	DefaultVHost string `json:"default_vhost"`
	// VHosts v1 的 vhost 清单；M2 起改为元数据存储中的动态资源。
	VHosts []string `json:"vhosts"`
	// Listeners 按插件名覆盖监听地址，如 {"amqp091": ":5672"}。
	Listeners map[string][]Listener `json:"listeners,omitempty"`
	// Plugins 插件的配置段，按插件名索引；插件经 Host.Config 读取。
	Plugins map[string]json.RawMessage `json:"plugins,omitempty"`
	// Users 内置用户表。
	Users map[string]User `json:"users"`
}

// Default 返回默认配置。
func Default() *Config {
	return &Config{
		DataDir:      "data",
		DefaultVHost: "/",
		VHosts:       []string{"/"},
		Plugins:      map[string]json.RawMessage{},
		Users: map[string]User{
			"guest": {
				Password:     "guest",
				Tags:         []string{"administrator"},
				RemoteAccess: false,
			},
		},
	}
}

// Load 读取配置文件；path 为空时直接返回默认配置。
func Load(path string) (*Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}
	if err := json.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}
	if cfg.DefaultVHost == "" {
		cfg.DefaultVHost = "/"
	}
	if cfg.Plugins == nil {
		cfg.Plugins = map[string]json.RawMessage{}
	}
	return cfg, nil
}

// PluginConfig 返回某个插件的配置段；不存在时返回空对象。
func (c *Config) PluginConfig(name string) json.RawMessage {
	if raw, ok := c.Plugins[name]; ok {
		return raw
	}
	return json.RawMessage("{}")
}
