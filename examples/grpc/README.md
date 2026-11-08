# 独立部署的 gRPC 方法授权

Controller、payments 健康服务、orders 客户端分别运行，各自持有独立部署目录。
orders 直接调用 payments，策略只放行 unary `Health/Check`，拒绝 streaming `Health/Watch`。
本例使用标准 health 服务，集中展示 `NewGRPCServer` 和 `GRPCDialOptions`，无需生成业务 protobuf。

## 构建与目录

从仓库根目录执行，Go 版本见 `go.mod`：

```sh
GOWORK=off go build -o /tmp/fastmesh-grpc ./examples/grpc
/tmp/fastmesh-grpc -role setup -state /tmp/grpc-demo
```

setup 的输出根目录必须不存在。生成三份独立部署目录：

```text
/tmp/grpc-demo/
├── controller/   # ca.pem、controller.pem、controller-key.pem
├── payments/     # ca.pem、payments.pem、payments-key.pem
└── orders/       # ca.pem、orders.pem、orders-key.pem
```

运行时 `-state` 指向单个角色目录。Controller 只写自己的存储与 token；两个 Node 只写各自的 `state/`。
目录可以分别部署到不同机器，不需要共享磁盘。

## 分别运行

终端 A：

```sh
/tmp/fastmesh-grpc -role controller -state /tmp/grpc-demo/controller
```

等待 `controller-ready`，另一个终端交付演示 token：

```sh
cp /tmp/grpc-demo/controller/join-token /tmp/grpc-demo/payments/join-token
cp /tmp/grpc-demo/controller/join-token /tmp/grpc-demo/orders/join-token
```

终端 B：

```sh
/tmp/fastmesh-grpc -role payments -state /tmp/grpc-demo/payments
```

等待 `payments-ready`，终端 C：

```sh
/tmp/fastmesh-grpc -role orders -state /tmp/grpc-demo/orders
```

预期输出：

```text
health: SERVING
watch: PermissionDenied
```

`NewGRPCServer` 提供 mesh transport credentials 和 unary/streaming 授权拦截器；业务服务在 Serve 前注册。
`GRPCDialOptions` 通过认证后的 Node 连接路由，调用方使用正常 gRPC 客户端。
方法按 POST 与完整 `/package.Service/Method` 路径授权；stream 权限在消息边界重检。
创建 Watch 客户端对象不意味着已获授权，本例通过第一次 Recv 验证拒绝。

## 不同机器与生命周期

所有进程传入同一个可达的 `-controller 10.0.0.10:19190`，Controller 需要能绑定该本机地址。
payments 可以固定监听和发布地址：

```sh
/tmp/fastmesh-grpc -role payments -state /var/lib/payments \
  -controller 10.0.0.10:19190 -listen 0.0.0.0:18443 -advertise 10.0.0.20:18443
```

放行节点到 Controller 的 TCP 19190 和 orders 到 payments 的 TCP 18443。
分别部署对应角色目录，通过 SSH/scp 等可信渠道交付 token；不要向应用交付 Controller 私钥。
凭据有效期 24 小时、演示 token 有效期一小时；生产可替换身份源与 enrollment 供应流程。

Ctrl-C 停止常驻进程。本例用 Stop 实现有界退出；生产宿主可先撤销 readiness，给 GracefulStop 设定外部时间预算后再强制 Stop。

复制成独立模块的步骤见[示例总览](../README.md)。
`python3 test/smoke/examples.py` 会搬离 setup 目录后独立构建、运行三个进程并验证两个 RPC 的结果。
消息中途撤权的回归测试见 Node 的 gRPC 测试；安全边界见[安全文档](../../docs/security.md)。
