// Command amqp091probe 是 M1 的连通性验证：用**真实客户端库**（rabbitmq/amqp091-go）
// 验证 SwiftMQ 的 AMQP 0-9-1 握手、Channel 开关与错误路径。
//
// 用法：先启动 swiftmqd，再运行本程序。
//
//	go run . -addr 127.0.0.1:5672
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	addr   = flag.String("addr", "127.0.0.1:5672", "broker 地址")
	user   = flag.String("user", "guest", "用户名")
	pass   = flag.String("pass", "guest", "口令")
	expect = flag.String("expect-product", "SwiftMQ", "期望的 server product 名")
)

// testCase 是一条验证用例。
type testCase struct {
	name string
	run  func() error
}

func main() {
	flag.Parse()

	cases := []testCase{
		{"M1 正常连接 + Channel 开关 + 优雅关闭", testHappyPath},
		{"M1 错误口令应被拒绝", testBadPassword},
		{"M1 不存在的 vhost 应被拒绝", testUnknownVHost},
	}
	cases = append(cases, m2Cases()...)
	cases = append(cases, m3Cases()...)
	cases = append(cases, m4Cases()...)

	var failed int
	for _, tc := range cases {
		if err := tc.run(); err != nil {
			failed++
			fmt.Printf("FAIL  %s\n      %v\n", tc.name, err)
			continue
		}
		fmt.Printf("PASS  %s\n", tc.name)
	}

	if failed > 0 {
		fmt.Printf("\n%d/%d 项失败\n", failed, len(cases))
		os.Exit(1)
	}
	fmt.Printf("\n全部通过（%d/%d）：真实客户端可完成连接、拓扑声明、四种路由、发布消费与确认、持久消息与能力声明\n",
		len(cases), len(cases))
}

func url(vhost string) string {
	return fmt.Sprintf("amqp://%s:%s@%s/%s", *user, *pass, *addr, vhost)
}

// testHappyPath 覆盖 M1 的核心路径。
func testHappyPath() error {
	conn, err := amqp.Dial(url(""))
	if err != nil {
		return fmt.Errorf("拨号失败: %w", err)
	}
	defer conn.Close()

	// 校验 server-properties：至少 product 要对，否则说明握手内容有误
	product, _ := conn.Properties["product"].(string)
	if *expect != "" && product != *expect {
		return fmt.Errorf("server product 期望 %q，实际 %q", *expect, product)
	}
	product2 := ""
	if v, ok := conn.Properties["capabilities"].(amqp.Table); ok {
		product2 = fmt.Sprintf("%d 项 capabilities", len(v))
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("打开 channel 失败: %w", err)
	}
	if err := ch.Close(); err != nil {
		return fmt.Errorf("关闭 channel 失败: %w", err)
	}

	// 再开一个 channel，验证 channel 号可复用（兼容性高危清单第 15 条）
	ch2, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("复用 channel 号失败: %w", err)
	}
	if err := ch2.Close(); err != nil {
		return fmt.Errorf("关闭第二个 channel 失败: %w", err)
	}

	if err := conn.Close(); err != nil {
		return fmt.Errorf("关闭连接失败: %w", err)
	}
	fmt.Printf("      product=%s %s\n", product, product2)
	return nil
}

func testBadPassword() error {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", *user, "wrong-password", *addr))
	if err == nil {
		conn.Close()
		return fmt.Errorf("期望认证失败，但连接成功了")
	}
	// 客户端必须能把它识别为"凭证问题"（403 ACCESS_REFUSED），而不是当成网络故障 ——
	// 这正是 authentication_failure_close 能力的作用。
	var amqpErr *amqp.Error
	if !errors.As(err, &amqpErr) {
		return fmt.Errorf("返回的不是 AMQP 协议错误，无法判断失败原因: %v", err)
	}
	if amqpErr.Code != amqp.AccessRefused {
		return fmt.Errorf("期望 403 ACCESS_REFUSED，实际 code=%d reason=%q", amqpErr.Code, amqpErr.Reason)
	}
	return nil
}

func testUnknownVHost() error {
	conn, err := amqp.Dial(url("no-such-vhost"))
	if err == nil {
		conn.Close()
		return fmt.Errorf("期望 vhost 不存在而失败，但连接成功了")
	}
	if !strings.Contains(err.Error(), "vhost") && !strings.Contains(err.Error(), "not found") {
		return fmt.Errorf("错误信息未指出 vhost 问题: %v", err)
	}
	return nil
}
