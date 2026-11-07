# FastMesh

[English](README.md) · [中文](README.zh.md)

FastMesh 是可嵌入 Go 应用的通信 SDK，提供经过 SPIFFE 身份认证的 TCP、HTTP 和 gRPC。
Controller 下发签名的发现与授权计划；业务流量在 Node 之间直连。

当前准备发布 **v0.1.0**，只提供一个计划协议：`fastmesh.plan.v1`。
以当前源码为准，Go 工具链版本见 [go.mod](go.mod)。

## 快速运行

```sh
GOWORK=off go build -o /tmp/fastmesh-config ./examples/app-config
/tmp/fastmesh-config -role setup -dir /tmp/fastmesh-demo
/tmp/fastmesh-config -role controller -dir /tmp/fastmesh-demo
```

看到 `controller-ready` 后，在两个终端分别运行：

```sh
/tmp/fastmesh-config -role app -name app-1 -dir /tmp/fastmesh-demo -http 127.0.0.1:8081
/tmp/fastmesh-config -role app -name app-2 -dir /tmp/fastmesh-demo -http 127.0.0.1:8082
curl -fsS http://127.0.0.1:8081/config
```

setup 需要新目录，生成有效期 24 小时的演示凭据。更新和部署方法见
[应用配置示例](examples/app-config/README.md)。发布标签后可用
`go get github.com/fastabc/fastmesh@v0.1.0` 安装。

## 文档

| 内容 | 入口 |
| --- | --- |
| HTTP 服务调用 | [Orders / Payments](examples/orders-payments/README.md) |
| gRPC 方法授权 | [gRPC 示例](examples/grpc/README.md) |
| 应用配置分发 | [配置示例](examples/app-config/README.md) |
| 观测回调 | [观测示例](examples/observability/README.md) |
| 配置与生命周期 | [接入](docs/integration.md) |
| 模块边界与运行流程 | [架构](docs/architecture.md) |
| 身份、授权、恢复 | [安全](docs/security.md) |
| 协议与代码生成 | [API](api/README.md) |
| 测试与 CI | [测试](docs/testing.md) |
| 发布要求 | [v0.1.0](docs/release.md) |

需要显式集成 Go SDK。HTTP 授权支持方法和路径，不支持 header/query/body 条件。
Controller 高可用使用共享 etcd；文件存储仅允许单个独占持有者。
业务配置的校验、持久化和发布确认由应用负责。

## 验证

```sh
GOWORK=off go test ./...
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
python3 test/smoke/examples.py
```

真实 etcd 与 fuzz 检查见[测试文档](docs/testing.md)。[MIT](LICENSE)。
