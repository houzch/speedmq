// 本文件实现 speedmqctl 的管理 API HTTP 客户端。
//
// 设计决策：为什么走 HTTP 管理 API 而不是直连内核？
//  1. 管理 API 是运行态的唯一事实来源，CLI 只做薄客户端，不 import 仓库的
//     internal/*，因此不会与内核内部结构、版本、存储布局耦合；
//  2. 运维命令天然需要远程执行，HTTP 让它既能本机也能跨机使用；
//  3. 只用标准库实现，部署运维工具时无需额外依赖，也满足内核"零第三方依赖"的约定。
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// maxRespBytes 限制单次读取的响应体大小，避免异常服务端撑爆内存。
const maxRespBytes = 32 << 20

// client 是与管理 API 交互的薄封装。
type client struct {
	baseURL string // 已去掉尾部斜杠的基地址
	user    string
	pass    string
	http    *http.Client
	jsonOut bool // -json：以原始 JSON 输出
}

func newClient(baseURL, user, pass string, timeout time.Duration, jsonOut bool) *client {
	return &client{
		baseURL: strings.TrimRight(baseURL, "/"),
		user:    user,
		pass:    pass,
		http:    &http.Client{Timeout: timeout},
		jsonOut: jsonOut,
	}
}

// get 发起 GET 请求并返回原始响应体。
func (c *client) get(path string, q url.Values) ([]byte, error) {
	return c.do(http.MethodGet, path, q, nil)
}

// put 发起 PUT 请求（body 为 nil 时表示无请求体）。
func (c *client) put(path string, q url.Values, body any) ([]byte, error) {
	return c.do(http.MethodPut, path, q, body)
}

// delete 发起 DELETE 请求。
func (c *client) delete(path string, q url.Values) ([]byte, error) {
	return c.do(http.MethodDelete, path, q, nil)
}

// do 是核心请求方法：拼装 URL、附带 Basic Auth、解析错误响应。
func (c *client) do(method, path string, q url.Values, body any) ([]byte, error) {
	target := c.baseURL + path
	if len(q) > 0 {
		target += "?" + q.Encode()
	}

	var payload io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("编码请求体失败: %w", err)
		}
		payload = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, target, payload)
	if err != nil {
		return nil, fmt.Errorf("构造 %s 请求失败: %w", method, err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.SetBasicAuth(c.user, c.pass)
	req.Header.Set("User-Agent", "speedmqctl")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, &connError{method: method, target: target, err: err}
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxRespBytes))
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, parseAPIError(resp.StatusCode, data)
	}
	return data, nil
}

// apiError 表示管理 API 返回的 4xx/5xx 错误（契约：{"error":"...","reason":"..."}）。
type apiError struct {
	status int
	errObj string
	reason string
	raw    string
}

func parseAPIError(status int, data []byte) *apiError {
	e := &apiError{status: status}
	var body struct {
		Error  string `json:"error"`
		Reason string `json:"reason"`
	}
	if json.Unmarshal(data, &body) == nil {
		e.errObj = body.Error
		e.reason = body.Reason
	}
	if e.errObj == "" && e.reason == "" {
		e.raw = strings.TrimSpace(string(data))
	}
	return e
}

func (e *apiError) Error() string {
	msg := e.reason
	if msg == "" {
		msg = e.errObj
	}
	if msg == "" {
		msg = e.raw
	}
	if msg == "" {
		msg = http.StatusText(e.status)
	}
	return fmt.Sprintf("管理 API 返回 HTTP %d: %s", e.status, msg)
}

// isNotFound 判断错误是否为 404，供各命令给出更具体的中文提示。
func isNotFound(err error) bool {
	var ae *apiError
	return errors.As(err, &ae) && ae.status == http.StatusNotFound
}

// connError 表示网络层失败（服务端未启动、端口不可达、超时等）。
type connError struct {
	method string
	target string
	err    error
}

func (e *connError) Error() string {
	return fmt.Sprintf("无法连接管理 API（%s %s）：%v；请确认 speedmqd 正在运行，且 -url 指向正确的管理地址",
		e.method, e.target, e.err)
}

func (e *connError) Unwrap() error { return e.err }

// formatError 把错误翻译为面向运维人员的中文提示。
func formatError(err error) string {
	var ae *apiError
	if errors.As(err, &ae) {
		switch ae.status {
		case http.StatusUnauthorized:
			return "认证失败（HTTP 401）：请检查 -user / -pass 是否正确"
		case http.StatusForbidden:
			return "权限不足（HTTP 403）：该用户无权执行此操作，请检查其 vhost 权限或用户 tags"
		}
	}
	return err.Error()
}
