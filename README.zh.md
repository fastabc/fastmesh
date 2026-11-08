# FastMesh

[English](README.md) · [中文](README.zh.md)

FastMesh 是可嵌入 Go 应用的通信 SDK，提供经过 SPIFFE 身份认证的 TCP、HTTP 和 gRPC。
Controller 下发签名的发现与授权计划；业务流量在 Node 之间直连。

当前准备发布 **v0.1.0**，只提供一个计划协议：`fastmesh.plan.v1`。
以当前源码为准，Go 工具链版本见 [go.mod](go.mod)。

## 架构与适用范围

FastMesh 适合能够显式接入通信 SDK 的 Go 服务，提供 SPIFFE mTLS、服务发现、
服务发布与消费授权，以及签名策略的本地恢复。Controller 协调注册、端点租约和
策略更新；应用之间的业务流量直接通信。

```text
宿主配置 → Controller → 签名计划 → Node 验签与持久化应用
                                    ↓
                               应用 ↔ mTLS ↔ 应用
```

Node 在发布执行状态前检查原始签名字节、接收方、有效期和 revision。
重启恢复会重新认证保存的策略，并保留原始截止时间。控制面断连时可继续使用
尚未过期的 last-good；过期策略不能授权新连接或 HTTP/gRPC 请求。
已建立的原始 TCP 连接不逐次读写重新鉴权，策略更新时会重新检查其 owner 权限。

| 能力 | 当前行为 |
| --- | --- |
| TCP | 身份认证直连、端点选择与熔断 |
| HTTP | 每请求方法/路径授权，前缀按路径段匹配 |
| gRPC | unary 与 streaming RPC 的方法授权 |
| 发现 | 完整签名计划、ACK/NACK、端点租约与重连退避 |
| 存储 | memory 易失；file 由单个 Controller 独占；共享 etcd 用于 HA |
| 观测 | Status 与 Observer 回调；exporter 由应用提供 |

协议采用 `fastmesh.plan.v1`，ADS 风格 API 是项目自己的契约；mesh delta 响应
仍携带完整签名计划。`node` 和 `controller` 的生产包编译依赖不包含 etcd，
由宿主选择具体存储后端；根 Go 模块目前仍包含 etcd 适配器及相关测试。

注册在同一事务中提交 token 使用次数、完整 Node 与重试结果。新 Node 在注册前
持久化 request ID；token 仍有效时，重试返回同一个 active Node。没有 request ID
的旧客户端获得原子性，但不保证响应丢失后的幂等恢复。
签名计划 body 上限为 3 MiB，完整 mesh protobuf 响应为 4 MiB，
本地 snapshot/journal 解压 JSON 为 64 MiB。超限会拒绝新计划，last-good 的
原始过期时间保持不变。编译缓存默认保留最多 64 MiB 序列化拓扑，非进程总内存上限。

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

## 接入

为 `node.Config` 提供 SPIFFE 身份源、Controller 地址及身份 allowlist、trust domain、
tenant、首次注册 JoinToken 和独占状态目录。调用 `node.New`、`Start`；服务端使用
`Listen`、`HTTPHandler` 或 `NewGRPCServer`，客户端使用 `Dial`、`HTTPClient` 或
`GRPCDialOptions`，退出时调用 `Close`。

Controller 管理 Service、BindGrant、ServiceAccess 和 WorkloadGroup；提供方通过
认证后的 Catalog API 注册独立 Endpoint 租约。应用用完 HTTP 客户端后关闭空闲连接，
共享身份源在使用它的 Node 全部停止后关闭；Observer 回调需要及时返回。

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
| 架构评价、实测与开源项目对比 | [2026-11-07 深度评审](docs/plans/archive/2026-11-07-architecture-review.md) |
| 重构建议与问题执行计划 | [Spec](docs/plans/archive/2026-11-07-refactoring-spec.md) · [Bug 描述](docs/plans/archive/2026-11-07-bugs.md) · [Phased Plan](docs/plans/archive/2026-11-07-phased-plan.md) |

需要显式集成 Go SDK。HTTP 授权支持方法和路径，不支持 header/query/body 条件。
Controller 高可用使用共享 etcd；文件存储仅允许单个独占持有者。
业务配置的校验、持久化和发布确认由应用负责。
SDK 升级随应用发布；透明跨语言拦截、通用流量路由不属于当前功能。
实现状态、实测收益与暂缓项见[执行记录](docs/plans/archive/2026-11-08-execution-report.md)；
条件性重构和发布验收均单独标记。

## 验证

```sh
GOWORK=off go test ./...
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
python3 test/smoke/examples.py
```

真实 etcd 与 fuzz 检查见[测试文档](docs/testing.md)。[MIT](LICENSE)。
