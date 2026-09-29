// Command swiftmqctl 是 SwiftMQ 的运维命令行工具。
//
// 它通过 RabbitMQ 兼容的管理 HTTP API（默认 :15672）操作 broker，是纯标准库实现的
// 薄客户端：不依赖任何第三方库，也不 import 仓库的 internal/*。
// 这样做的原因见 client.go 顶部注释——管理 API 是唯一事实来源，CLI 因此无需与
// 内核版本耦合，也能安全地远程运维。
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	os.Exit(run())
}

// run 执行主流程并返回进程退出码：0 成功，1 运行期错误，2 用法错误。
func run() int {
	fs := flag.NewFlagSet("swiftmqctl", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var (
		baseURL = fs.String("url", "http://127.0.0.1:15672", "管理 API 基地址")
		user    = fs.String("user", "guest", "管理 API 用户名")
		pass    = fs.String("pass", "guest", "管理 API 口令")
		timeout = fs.Duration("timeout", 10*time.Second, "HTTP 请求超时")
		jsonOut = fs.Bool("json", false, "以原始 JSON 输出而非表格")
	)
	fs.Usage = printUsage

	if err := fs.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	rest := fs.Args()
	if len(rest) == 0 {
		printUsage()
		return 0
	}

	c := newClient(*baseURL, *user, *pass, *timeout, *jsonOut)
	if err := dispatch(c, rest[0], rest[1:]); err != nil {
		var se silentExit
		if errors.As(err, &se) {
			return se.code
		}
		var ue usageError
		if errors.As(err, &ue) {
			fmt.Fprintln(os.Stderr, ue.Error())
			fmt.Fprintln(os.Stderr, "运行 swiftmqctl -h 查看完整用法。")
			return 2
		}
		fmt.Fprintf(os.Stderr, "错误: %s\n", formatError(err))
		return 1
	}
	return 0
}

// usageError 表示用户传参不合法（缺参/多参），由 main 统一格式化。
type usageError struct{ msg string }

func (e usageError) Error() string { return e.msg }

func usagef(format string, a ...any) error { return usageError{msg: fmt.Sprintf(format, a...)} }

// silentExit 表示某处已经自行打印了提示与用法，main 只需以指定码退出。
type silentExit struct{ code int }

func (silentExit) Error() string { return "silent exit" }

func printUsage() {
	fmt.Print(`swiftmqctl —— SwiftMQ 运维命令行工具（通过管理 HTTP API 操作）

用法:
  swiftmqctl [全局参数] <命令> [参数...]

全局参数:
  -url string        管理 API 基地址（默认 http://127.0.0.1:15672）
  -user string       用户名（默认 guest）
  -pass string       口令（默认 guest）
  -timeout duration  HTTP 请求超时（默认 10s）
  -json              以原始 JSON 输出而非表格

命令:
  status                                                     查看节点概览（节点、版本、运行时长、对象统计、内存/磁盘）
  cluster_status                                             查看集群状态（模式、角色、领导者、成员、共识进度）
  list_members                                               列出集群成员（投票成员 / 非投票成员）
  add_member <node_id> <rpc_addr>                            把节点加入集群（先作 learner 追平，再提升为投票成员）
  remove_member <node_id>                                    把节点移出集群（无需改配置文件与重启）
  list_queues [vhost]                                        列出队列
  list_connections                                           列出连接
  list_exchanges [vhost]                                     列出交换机
  list_bindings [vhost]                                      列出绑定
  add_user <name> <password> [tags]                          创建或更新用户（tags 默认 administrator）
  set_permissions <user> <vhost> <configure> <write> <read>  设置用户在 vhost 上的权限
  close_connection <name> [reason]                           关闭指定连接
  plugins list                                               列出插件
  plugins show <name>                                        查看插件详情
  plugins enable <name>                                      启用插件
  plugins disable <name>                                     禁用插件

示例:
  swiftmqctl status
  swiftmqctl -url http://10.0.0.5:15672 -user admin -pass s3cret list_queues /
  swiftmqctl -json list_exchanges

提示: vhost 用 "/" 表示默认虚拟主机，命令行参数会自动做 URL 编码。
`)
}
