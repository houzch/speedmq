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
	// declareDurable 只声明一个 durable 队列后退出，供 M6 的集群验证使用：
	// durable 队列属集群元数据，声明后应在**所有**节点上可见（用管理 API 佐证）。
	declareDurable = flag.String("declare-durable", "", "只声明指定名字的 durable 队列后退出")
)

// testCase 是一条验证用例。
type testCase struct {
	name string
	run  func() error
}

func main() {
	flag.Parse()

	if *gcert != "" {
		if err := generateCert(*gcert); err != nil {
			fmt.Fprintf(os.Stderr, "生成证书失败: %v\n", err)
			os.Exit(2)
		}
		fmt.Printf("OK    已生成证书: %s/{cert.pem,key.pem}\n", *gcert)
		return
	}

	if *declareDurable != "" {
		if err := declareDurableQueue(*declareDurable); err != nil {
			fmt.Printf("FAIL  声明 durable 队列 %s: %v\n", *declareDurable, err)
			os.Exit(1)
		}
		fmt.Printf("OK    已声明 durable 队列 %s\n", *declareDurable)
		return
	}

	// 跨节点转发验证：只跑这一条（它是双节点协作的验证，与单节点的用例清单互斥）。
	if *clusterPeer != "" {
		if err := runClusterCheck(); err != nil {
			fmt.Printf("FAIL  M6b 跨节点转发（非 Owner 节点 %s，Owner 节点 %s）\n      %v\n", *addr, *clusterPeer, err)
			os.Exit(1)
		}
		fmt.Printf("PASS  M6b 跨节点转发：在非 Owner 节点发布与消费均正常\n")
		return
	}

	// 仲裁队列验证的两步（中间由外部脚本杀掉组 leader）。
	if *quorumProduce != "" {
		if err := runQuorumProduce(); err != nil {
			fmt.Printf("FAIL  仲裁队列发布（%s）：%v\n", *addr, err)
			os.Exit(1)
		}
		fmt.Printf("PASS  仲裁队列已发布并确认\n")
		return
	}
	if *quorumVerify != "" {
		if err := runQuorumVerify(); err != nil {
			fmt.Printf("FAIL  仲裁队列校验（%s）：%v\n", *addr, err)
			os.Exit(1)
		}
		fmt.Printf("PASS  仲裁队列的消息在 leader 宕机后仍然完整\n")
		return
	}

	cases := []testCase{
		{"M1 正常连接 + Channel 开关 + 优雅关闭", testHappyPath},
		{"M1 错误口令应被拒绝", testBadPassword},
		{"M1 不存在的 vhost 应被拒绝", testUnknownVHost},
	}
	cases = append(cases, m2Cases()...)
	cases = append(cases, m3Cases()...)
	cases = append(cases, m4Cases()...)
	cases = append(cases, m8Cases()...)

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

// declareDurableQueue 只声明一个 durable 队列后断开（集群元数据复制的验证入口）。
func declareDurableQueue(name string) error {
	return withChannel(func(ch *amqp.Channel) error {
		if _, err := ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
			return fmt.Errorf("声明 durable 队列失败: %w", err)
		}
		return nil
	})
}

// testHappyPath 覆盖 M1 的核心路径。
func testHappyPath() error {
	conn, err := dial(url(""))
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
	conn, err := dial(fmt.Sprintf("amqp://%s:%s@%s/", *user, "wrong-password", *addr))
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
	conn, err := dial(url("no-such-vhost"))
	if err == nil {
		conn.Close()
		return fmt.Errorf("期望 vhost 不存在而失败，但连接成功了")
	}
	if !strings.Contains(err.Error(), "vhost") && !strings.Contains(err.Error(), "not found") {
		return fmt.Errorf("错误信息未指出 vhost 问题: %v", err)
	}
	return nil
}
