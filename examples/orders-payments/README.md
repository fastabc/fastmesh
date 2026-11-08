# 独立部署的 HTTP 服务调用

Controller、payments、orders 分别运行。Controller 负责注册、发现和授权，orders 使用
`Node.HTTPClient` 直接调用 payments 的 `Node.HTTPHandler`。业务流量不经过 Controller。

## 构建与目录

从仓库根目录执行，Go 版本见 `go.mod`：

```sh
GOWORK=off go build -o /tmp/fastmesh-http ./examples/orders-payments
/tmp/fastmesh-http -role setup -state /tmp/http-demo
```

setup 要求新的输出根目录，生成三份独立部署目录：

```text
/tmp/http-demo/
├── controller/   # ca.pem、controller.pem、controller-key.pem
├── payments/     # ca.pem、payments.pem、payments-key.pem
└── orders/       # ca.pem、orders.pem、orders-key.pem
```

运行时 `-state` 指向具体角色目录。Controller 的存储和 token 保存在 controller/；
payments 和 orders 的 Node 状态分别写入自己的 `state/`。目录可以独立搬迁。

## 分别运行

终端 A：

```sh
/tmp/fastmesh-http -role controller -state /tmp/http-demo/controller
```

等待 `controller-ready`，通过可信渠道将演示 token 交付到两个节点；同机用：

```sh
cp /tmp/http-demo/controller/join-token /tmp/http-demo/payments/join-token
cp /tmp/http-demo/controller/join-token /tmp/http-demo/orders/join-token
```

终端 B：

```sh
/tmp/fastmesh-http -role payments -state /tmp/http-demo/payments
```

等待 `payments-ready`，终端 C：

```sh
/tmp/fastmesh-http -role orders -state /tmp/http-demo/orders
```

成功输出 `paid` 并退出。payments 默认监听随机 loopback 端口，通过端点租约发布地址，orders 无需配置其端口。
策略允许 orders 执行 `GET /pay` 路径段前缀下的请求；可验证越界路径被拒绝：

```sh
/tmp/fastmesh-http -role orders -state /tmp/http-demo/orders -path /payment
# payments status: 403，退出码非零
```

`/pay/../admin`、编码后的 dot segment 和双斜杠路径也不能绕过授权。
本例的 payments handler 始终返回 `paid`，用来隔离验证 SDK 的认证、发现和 L7 授权，不包含支付业务逻辑。

## 不同机器

将每个角色自己的目录部署到对应机器，交付 token，不复制其它角色私钥或 state。
所有进程都传入同一个可达的 `-controller 10.0.0.10:19190`；Controller 需要能绑定该本机地址。
payments 另加：

```sh
/tmp/fastmesh-http -role payments -state /var/lib/payments \
  -controller 10.0.0.10:19190 -listen 0.0.0.0:18443 -advertise 10.0.0.20:18443
```

允许节点访问 Controller 的 TCP 19190，以及 orders 访问 payments 的 TCP 18443。
orders 仍通过服务引用发现 payments，无需写死业务地址。

凭据有效期 24 小时，演示 token 有效期一小时；生产身份供应可改用 Workload API。
成功调用后立即停止 Controller，再运行 orders，可用未过期的持久化计划直连。
端点租约默认 30 秒、授权默认一分钟；过期后新调用失败，重启 Controller 后恢复续租与计划分发。
payments 保留实例 ID，在租约过期后重新发布时增加持久化 generation。
Ctrl-C 停止常驻进程。

复制成独立模块的步骤和通用约定见[示例总览](../README.md)。
`python3 test/smoke/examples.py` 会独立构建并验证调用成功与路径拒绝。
