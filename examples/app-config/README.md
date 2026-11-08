# 独立配置服务与两个应用

本例将配置服务作为普通 Go 应用接入 FastMesh。Controller 只下发发现与授权计划；
`config-server` 通过 Node 提供配置，`app-1`、`app-2` 通过各自的 Node 直接拉取。
配置服务没有嵌入 Controller，四个进程各有自己的部署目录，可以分别放在四台机器上。

```text
controller ──签名发现/授权计划──→ config-server、app-1、app-2
                                     ↑             │
                                     └──mTLS GET───┘
```

## 1. 构建并生成四份独立部署目录

从仓库根目录执行，Go 版本见 `go.mod`：

```sh
GOWORK=off go build -o /tmp/app-config ./examples/app-config
/tmp/app-config -role setup -dir /tmp/config-demo
```

`/tmp/config-demo` 必须不存在。输出为：

```text
/tmp/config-demo/
├── controller/       # ca.pem、controller.pem、controller-key.pem
├── config-server/    # ca.pem、config-server.pem、config-server-key.pem、app.json
├── app-1/            # ca.pem、app-1.pem、app-1-key.pem
└── app-2/            # ca.pem、app-2.pem、app-2-key.pem
```

每个进程只读取自己的目录。运行后 Controller 在自己的目录内创建 `controller-store.json` 和 `join-token`；
三个 Node 在各自目录下创建独占 `state/`。各目录可以搬到不同路径，不依赖它们有共同的父目录。

## 2. 启动控制面，交付 enrollment token

终端 A：

```sh
/tmp/app-config -role controller -dir /tmp/config-demo/controller
```

等待 `controller-ready`，另一个终端执行一次 token 交付：

```sh
cp /tmp/config-demo/controller/join-token /tmp/config-demo/config-server/join-token
cp /tmp/config-demo/controller/join-token /tmp/config-demo/app-1/join-token
cp /tmp/config-demo/controller/join-token /tmp/config-demo/app-2/join-token
```

token 文件权限为 0600，身份目录为 0700。Controller 每次启动生成新 token；首次注册前如旧 token 已过期，重新交付。
已有节点保留自己的 state 后通常无需重新注册，但本例启动仍读取本地 token 文件。

## 3. 启动独立配置服务与应用

终端 B：

```sh
/tmp/app-config -role config-server -dir /tmp/config-demo/config-server
```

等待 `config-server-ready`。终端 C、D 分别执行：

```sh
/tmp/app-config -role app -name app-1 -dir /tmp/config-demo/app-1 -http 127.0.0.1:8081
/tmp/app-config -role app -name app-2 -dir /tmp/config-demo/app-2 -http 127.0.0.1:8082
```

收到首份配置后日志出现 `config-applied ... revision=1`：

```sh
curl -fsS http://127.0.0.1:8081/config
curl -fsS http://127.0.0.1:8082/config
curl -fsS http://127.0.0.1:8081/checkout
```

`/config` 显示 revision、greeting、checkout_enabled 和 last_success；`/` 展示当前实例及配置。
`/checkout` 初始返回 200。应用在 Node.Start 成功后监听本机 HTTP，首份有效业务配置到达前返回 503。
配置服务端口 19191 使用 mesh 协议，应用通过 `Node.HTTPClient` 访问，不能直接用普通 curl 请求。
策略只授权两个应用执行 `GET /config`，只有 config-server 身份可以发布该服务。

## 4. 更新配置并验证

只修改配置服务自己的文件，先写临时文件再在同目录 rename：

```sh
cat > /tmp/config-demo/config-server/app.json.next <<'JSON'
{
  "revision": 2,
  "greeting": "hello from configuration v2",
  "checkout_enabled": false
}
JSON
mv /tmp/config-demo/config-server/app.json.next /tmp/config-demo/config-server/app.json

curl -fsS http://127.0.0.1:8081/config
curl -fsS http://127.0.0.1:8082/config
curl -sS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8081/checkout
```

应用默认每两秒拉取一次，每次超时三秒。等两个应用均显示 revision=2，checkout 应返回 503。
再次开启时发布 revision=3，不复用旧 revision。字段均必填，未知字段、空 greeting、非正 revision 或超过 64 KiB 的输入被拒绝。

业务 JSON 的 ETag 用于比较与条件请求；业务配置经认证连接传送，没有独立数字签名。
FastMesh 签名的是发现与授权计划，业务配置的语义和发布流程仍由应用负责。

## 5. 分机器部署

例如 Controller 在 `10.0.0.10`、配置服务在 `10.0.0.20`，应用在另外两台机器。
将四份目录分别部署到对应机器的 `/var/lib/config-demo`，只复制该角色自己的目录内容。
二进制须针对目标 OS/架构构建。通过可信渠道交付 token 后，在各机运行：

```sh
# Controller 机器：19190 必须是本机可绑定且其他节点可达的地址
./app-config -role controller -dir /var/lib/config-demo -controller 10.0.0.10:19190

# 配置服务机器
./app-config -role config-server -dir /var/lib/config-demo \
  -controller 10.0.0.10:19190 \
  -config-listen 0.0.0.0:19191 -advertise 10.0.0.20:19191

# 两台应用机器，分别使用自己的目录和身份
./app-config -role app -name app-1 -dir /var/lib/config-demo \
  -controller 10.0.0.10:19190 -http 127.0.0.1:8081
./app-config -role app -name app-2 -dir /var/lib/config-demo \
  -controller 10.0.0.10:19190 -http 127.0.0.1:8082
```

放行所有 Node 到 Controller 的 TCP 19190，以及应用到配置服务的 TCP 19191。
在各应用所在机器执行 curl。配置更新只发生在配置服务机器，不依赖共享磁盘。
本例每个角色只有一个实例，不演示配置源或 Controller HA。

## 6. 故障行为与验证

- 配置服务停止或返回无效 JSON，应用保留内存中的最后有效配置；重启配置服务后继续同步。失败重试可能受断路器冷却影响。
- Controller 停止时，已有未过期的计划允许继续直连；端点租约或授权过期后，新请求失败。业务配置的内存保留不会延长 mesh 授权。
- 单个请求只读取一个不可变配置对象，两个应用分别更新，允许跳过中间版本，不提供同时切换或发布回执。
- revision 防回退仅覆盖本进程已见过的版本。业务配置不落盘，应用重启须重新拉取，首份配置前返回 503。
- 本例不对最后有效业务配置设置保留期限，不传密钥或安全授权策略；业务过期、持久化与审批由应用定义。

Ctrl-C 停止各进程。代码分为生命周期与授权（`main.go`）、业务校验与应用（`config.go`）、演示身份供应（`identity.go`）。
身份材料有效期 24 小时，不保存 CA 私钥；生产环境可替换为 Workload API 身份源。

```sh
GOWORK=off go test -race ./examples/app-config
GOWORK=off go vet ./examples/app-config
python3 test/smoke/examples.py
```

冒烟测试会搬离 setup 目录后运行四个进程，验证独立目录、热更新、无效输入保留旧值、配置源和控制面停止及恢复。
