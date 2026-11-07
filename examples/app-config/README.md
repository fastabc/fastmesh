# 多机同步业务配置

一个配置机、两台应用机器，共用一份业务配置。应用通过 fastmesh 的 `Node.HTTPClient` 拉取配置，校验成功后一次替换内存值；下一次请求直接使用新配置，无需重启应用。

这个例子使用已有应用 API，不新增 Admin API、通用配置 SDK 或 ADS 资源类型。Controller 签名分发的是 mesh 发现/授权计划；**业务 JSON 通过认证的 mesh HTTP 连接拉取，不是独立签名的配置资源**。ETag 用于内容比较和条件请求，不是数字签名。

| 进程 | 职责 | 端口 |
| --- | --- | --- |
| 配置机 `controller` | 运行 Controller，并嵌入一个 Node 提供 `GET /config` | 19190：mTLS 控制面；19191：mesh 配置服务 |
| 应用机 `app-1` | 每 2 秒拉取并应用配置，提供演示业务接口 | 8081：本机 HTTP |
| 应用机 `app-2` | 同上，使用独立身份与 state 目录 | 8082：本机 HTTP |

只授权 app-1、app-2 对配置服务执行 `GET /config`。应用的演示 HTTP 接口默认绑定 loopback，验证时在对应机器上执行 curl。19191 是 mesh 协议端口，不能用普通 curl 直接访问。

## 1. 构建与同机三进程验证

从仓库根目录执行，需要 Go 1.27.3 或更新版本：

```sh
GOWORK=off go build -o /tmp/app-config ./examples/app-config
/tmp/app-config -role setup -dir /tmp/config-demo
```

setup 创建演示 CA、Controller/app-1/app-2 各自的证书和私钥，以及初始 app.json。证书有效期 24 小时；CA 私钥只在生成时使用，不写盘。目标目录必须不存在，避免覆盖旧身份；父目录需已存在。

终端 A：

```sh
/tmp/app-config -role controller -dir /tmp/config-demo
```

等待 `controller-ready`。此时 `/tmp/config-demo/join-token` 已生成。另开两个终端：

```sh
# 终端 B
/tmp/app-config -role app -name app-1 -dir /tmp/config-demo -http 127.0.0.1:8081

# 终端 C
/tmp/app-config -role app -name app-2 -dir /tmp/config-demo -http 127.0.0.1:8082
```

日志先出现 `app-listening`，收到首份有效配置后出现 `config-applied ... revision=1`。Node 获得可用 mesh 计划并启动本地 HTTP 后，首份有效业务配置到达前返回 503；若首次 enrollment/mesh 计划尚不可用，则仍在等待 Node.Start，HTTP 端口尚未监听。各进程使用独立的 `controller-state`、`app-1-state`、`app-2-state` 子目录。

```sh
curl -fsS http://127.0.0.1:8081/config
curl -fsS http://127.0.0.1:8082/config
curl -fsS http://127.0.0.1:8081/checkout
```

`/config` 返回 revision、greeting、checkout_enabled 和 last_success；`/` 返回本实例名及正在使用的业务配置。`/checkout` 在功能关闭时返回 503，在开启时返回 200。

## 2. 更新配置，无需重启

在配置机将新文件写完整，再在同一目录内 rename，避免客户端读到半个 JSON：

```sh
cat > /tmp/config-demo/app.json.next <<'JSON'
{
  "revision": 2,
  "greeting": "hello from configuration v2",
  "checkout_enabled": false
}
JSON
mv /tmp/config-demo/app.json.next /tmp/config-demo/app.json
```

默认轮询周期为 2 秒，每次请求超时 3 秒；健康情况下通常在下一轮收到更新，不保证两台机器在同一时刻切换。等待两台应用分别输出 revision=2，再验证：

```sh
curl -fsS http://127.0.0.1:8081/config
curl -fsS http://127.0.0.1:8082/config
curl -sS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8081/checkout
curl -sS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8082/checkout
```

两份配置都应是 revision=2，checkout 都返回 503。要重新打开，请发布 **revision=3** 且 checkout_enabled=true；不要复用 1 或 2。所有字段必填，未知字段、空 greeting、非正 revision、超过 64 KiB 的输入均拒绝。

## 3. 部署到三台机器

示例地址：配置机 10.0.0.10，应用机 10.0.0.11 和 10.0.0.12。替换为实际可达 IP；配置机放行应用机到 TCP 19190、19191。二进制须针对各目标 OS/架构构建，例如在 Go 开发机：

```sh
GOOS=linux GOARCH=amd64 GOWORK=off go build -o /tmp/app-config-linux ./examples/app-config
```

以下假设二进制已放到各机 `/opt/app-config/app-config`，运行账户可读写 `/var/lib/app-config`。在配置机使用 setup 生成新目录，然后启动：

```sh
/opt/app-config/app-config -role setup -dir /var/lib/app-config
/opt/app-config/app-config -role controller -dir /var/lib/app-config \
  -controller 10.0.0.10:19190 \
  -config-listen 0.0.0.0:19191 -advertise 10.0.0.10:19191
```

等待 controller-ready，再通过 SSH/scp 等已有安全渠道把下列文件分别传给应用机；只复制本机身份，不复制整个配置机目录：

| 应用机目录 `/var/lib/app-config/` | 所需文件（来源：配置机相同目录） |
| --- | --- |
| 10.0.0.11 | `ca.pem`、`app-1.pem`、`app-1-key.pem`、`join-token` |
| 10.0.0.12 | `ca.pem`、`app-2.pem`、`app-2-key.pem`、`join-token` |

目录权限建议 0700、文件 0600。不要复制 Controller 私钥、controller-store.json 或任何 `*-state` 目录；state 属于单个节点进程。演示 join token 有效期 1 小时，允许这些演示节点注册；Controller 每次启动会写出一个新 token。已经注册的节点保留原 state 后不重新消耗 token，但示例启动仍会读取 join-token 文件。生产系统应使用自己的身份与 enrollment 供应流程。

应用机 10.0.0.11：

```sh
/opt/app-config/app-config -role app -name app-1 -dir /var/lib/app-config \
  -controller 10.0.0.10:19190 -http 127.0.0.1:8081 -poll 2s
curl -fsS http://127.0.0.1:8081/config
```

应用机 10.0.0.12：

```sh
/opt/app-config/app-config -role app -name app-2 -dir /var/lib/app-config \
  -controller 10.0.0.10:19190 -http 127.0.0.1:8082 -poll 2s
curl -fsS http://127.0.0.1:8082/config
```

进程前台运行；在另一个 SSH 会话执行 curl。按 §2 的方式在配置机更新 `/var/lib/app-config/app.json`，然后分别检查两台应用机的 revision。**本例只有一台配置源，不支持多个 Controller 同写文件或配置机 HA**；跨主机同步不依赖共享磁盘。

## 4. 故障行为与适用范围

- **业务请求原子读配置**：一个请求读取一个不可变对象，不会把两个版本的字段混合。两台应用之间是最终一致，不是分布式事务或全网发布回执。
- **无效更新/回退**：源文件无效时返回 503；应用收到低于当前 revision 或相同 revision 不同内容时拒绝更新。日志记录失败，正在运行的应用保留最后有效值。
- **配置机断连**：应用仍使用最后有效的业务配置；last_success 不再推进。该演示没有业务配置过期策略，保留时间可无限长。mesh 授权计划和 endpoint 租约仍按框架规则过期，不会因此延长。
- **重启应用**：业务配置只在内存，不保存到磁盘；重启后须重新拉取，拉取前 HTTP 为 503。revision 防回退仅覆盖本进程已观察到的版本，不提供跨重启的防回滚保证。Node 自身的 enrollment/mesh 计划持久化与此不同。
- **恢复连接**：源恢复后继续尝试拉取最新有效版本；连续拨号失败会触发当前 runtime 默认 5 次失败/30 秒冷却的断路器，因此恢复可能晚于下一轮轮询；控制面重连也有退避。中间版本可以被跳过。修改业务文件不需要重启 Controller。
- **演示用途**：使用 greeting 和功能开关，不传密钥或安全授权策略。业务配置的持久化、过期/降级、变更审批和发布回执应由具体应用另行定义；本示例没有宣称这些能力。

可以停止配置机，确认应用仍返回旧 revision；再重启一个应用，它应等待首份配置；最后重启配置机，两台应用应重新收敛。使用 Ctrl-C 停止进程；共享身份源的生命周期仍由宿主管理。

## 5. 示例代码与验证

- `main.go`：Controller/Node 生命周期、权限和应用轮询。
- `config.go`：JSON 校验、ETag、内存原子发布及业务处理。
- `identity.go`：短期演示身份生成与文件读取；真实部署可换为 Workload API。
- `config_test.go`：无效输入、条件请求、回退/断连保留旧值、ETag 不一致及功能开关行为。

```sh
GOWORK=off go test -race ./examples/app-config
GOWORK=off go vet ./examples/app-config
```

也可把本目录全部 `.go` 文件复制到独立目录，`go mod init example.org/app-config`，然后 `go mod edit -require=github.com/fastabc/fastmesh@v0.0.0 -replace=github.com/fastabc/fastmesh=/absolute/path/to/fastmesh`，执行 `GOWORK=off go mod tidy` 和 `GOWORK=off go build .`。示例仅使用 fastmesh 公开包，不依赖任何产品或内部实现。
