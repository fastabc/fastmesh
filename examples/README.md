# FastMesh 示例

这些示例演示如何把通信 SDK 嵌入应用：Controller 负责身份注册、发现和授权，业务进程通过 Node 直接通信。
每个运行进程持有自己的部署目录；同机运行只是方便验证，不依赖共享凭据目录或共享状态。

| 示例 | 进程与独立目录 | 验证目标 |
| --- | --- | --- |
| [app-config](app-config/README.md) | controller、config-server、app-1、app-2 | 经授权的配置拉取、校验和热更新 |
| [orders-payments](orders-payments/README.md) | controller、payments、orders | HTTP 服务发现、直连与路径授权 |
| [grpc](grpc/README.md) | controller、payments、orders | gRPC unary 放行、streaming 方法拒绝 |
| [observability](observability/README.md) | 宿主中的 Observer 适配器 | 无额外 exporter 依赖的事件计数 |

`setup` 是本地演示的离线身份供应步骤，输出根目录必须不存在，父目录须存在。
它为每个角色生成独立目录，其中只有公共 CA 和该角色的证书、私钥（有效期 24 小时），不保存 CA 私钥。
运行时的 `-dir` / `-state` 指向具体角色目录，而非 setup 的输出根目录。
Node 状态写到自己目录下的 `state/`；Controller 存储只存在于 Controller 目录。

Controller 启动后生成一小时有效的演示 join token。按各例说明将它通过可信渠道交付给节点；示例用 `cp` 模拟交付。
跨主机时只部署对应角色的整个目录，使用 SSH/scp 等已有渠道交付 token，不给应用 Controller 私钥或其他应用私钥。
这里的演示 token 允许多个节点注册，生产环境应使用自己的身份供应与 enrollment 流程。

所有示例只使用公开 API，不需要运行额外的 fastmesh 守护进程，也不引入通用业务配置协议。
如需复制成独立工程，将对应目录的非测试 `.go` 文件复制出去：

```sh
go mod init example.org/demo
go mod edit -require=github.com/fastabc/fastmesh@v0.0.0 \
  -replace=github.com/fastabc/fastmesh=/absolute/path/to/fastmesh
GOWORK=off go mod tidy
GOWORK=off go build .
```

Go 版本见根目录 `go.mod`。从仓库根目录运行 `python3 test/smoke/examples.py`，会在独立 Go 模块中构建并验证三个通信示例。
