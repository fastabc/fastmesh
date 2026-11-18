# FastMesh 独立架构评估 · 2026-11-18

**综合评分 7.8/10。建议保留嵌入式 Go SDK 和单一签名计划，下一轮先解决写入围栏、状态读取上限和发布文档，再优化 Controller 冷启动。** etcd 已经与 Node 和 Controller 的生产包依赖隔离。现在拆成多个 Go module 会增加发版与集成测试成本，现有数据更支持先消除读放大和重复解码。

本次从干净的 `master@454b7006aa053365abbd1a0019eb244612dd32b5` 重新取证。依据为当前 Go 源码、入口调用关系、测试、CI 配置、本机测量及上游官方资料。未读取 `docs/plans/` 下的原计划。工作范围为研究与设计。可独立交付的改动、示例和验收条件见 [问题与技术规格](spec.md)，实施次序、目录结构、双语 README 拟稿及回退条件见 [分阶段执行计划](phases.md)。

**实施进度（2026-11-18）。** 此页的评分和数据描述 `454b700` 基线。当日完成的修复、阶段 4 决策与最终测试结果见[阶段交付记录](phases.md#阶段交付记录)，重建、冷编译和 ACK 对照见[原始测量](measurements.md)。

## 项目解决什么问题

对可以随业务升级而发布 Go SDK 的团队，FastMesh 把服务发布权、调用权、端点租约和可恢复的签名策略放在同一条信任链上。应用直接持有经过身份认证的连接，Controller 离线期间，Node 仍能在已签名的有效期内做本地决定。代价是每个应用都要正确接入、管理状态目录并部署 SDK 升级。已有多语言应用若不能改代码，透明代理提供的是另一种部署前提。仅凭省去代理进程，无法推算端到端时延或总成本。

| 层次 | 现有职责与所有权 | 主要入口 |
| --- | --- | --- |
| 宿主和身份源 | 宿主选择对象与租约存储、信任域、Controller 身份白名单、SPIFFE 身份源及观测回调。轮换取材和进程退出时的资源归属影响实际部署 | [controller.New](../../../controller/controller.go)、[node.New](../../../node/node.go)、[身份源实现](../../../identity/spiffe/workloadapi/source.go) |
| 控制面 | Bootstrap 绑定工作负载；Catalog 管端点；选举和写入围栏约束写者；共享索引给编译器提供同一修订的租户视图 | [注册](../../../controller/enrollment/register.go)、[发现入口](../../../controller/discovery.go)、[索引](../../../internal/controller/index/index.go) |
| 分发与策略 | 将服务、授权、组、端点编译为收件人专属 Plan；以带类型的原始字节签名；节点验证签名、身份、修订和时限，再持久化和发布 | [编译](../../../internal/controller/catalog/compile.go)、[验签](../../../policy/verify/plan.go)、[应用](../../../policy/apply/apply.go) |
| 数据面 | TCP 握手、端点选择与熔断；HTTP 逐请求授权；gRPC 按方法和流消息检查；状态和观测随应用暴露 | [拨号](../../../internal/node/dataplane/dial.go)、[HTTP](../../../internal/node/dataplane/http.go)、[gRPC](../../../node/grpc.go) |
| 存储与恢复 | memory 易失；file 独占、整份状态原子替换；etcd 用事务、租约和 revision watch 支持共享控制面。Node 另有独占本地日志 | [存储契约](../../../store/store.go)、[file](../../../store/file/store.go)、[etcd](../../../store/etcd/transaction.go)、[Node 日志](../../../policy/state/journal.go) |

## 三条决定正确性的流程

**注册与服务发布。** 首次启动先在本地持久化 enrollment request ID，随后通过经过认证的 Bootstrap 请求换取 Node ID。Controller 将令牌使用、完整 Node 与可选重试记录放在同一笔事务中。返回途中断线时，新客户端可以恢复已提交的结果。Node 绑定服务时先预留本地绑定位置，再分别发布初始和 serving 状态。后续续租和撤销都依赖 instance 与 generation。BindGrant 表示谁可以发布服务，ServiceAccess 表示谁可以消费服务，端点仍需满足有效租约、owner 和服务端口条件。把这些过程压成单个“注册成功”状态会丢失排障所需的信息。[Node 启动](../../../node/lifecycle.go) · [监听器](../../../node/listener.go) · [端点生命周期](../../../internal/controller/endpoint/registration.go)

**策略编译与送达。** Index 先建立全局 revision watch，再抓取一致视图，忽略已被基线覆盖的批次；watch 失效后重建基线。每个租户的会话订阅变动提示，从完整快照编译。缓存键包括快照指针和接收方，缓存中复用拓扑字节，每个新有效期仍生成待签正文。服务端合并通知、跟踪待确认 nonce，并按 ACK、NACK 或超时推进会话。Follower 可以处理只读发现；变更操作依赖 leader。线上 delta 流仍下发完整签名计划。应用部署不需要一套 Envoy xDS 管理服务器。[Index.Run](../../../internal/controller/index/index.go) · [Plan cache](../../../internal/controller/plancache/cache.go) · [会话状态机](../../../controller/discovery/session.go)

**应用、断线和重启。** Node 对带类型的原始正文验签，随后解码并校验 tenant、Node ID、workload、有效期和持久化输入修订水位。Pipeline 串行准备变更，写 pending，再记录 committed 决策，发布执行状态并完成 checkpoint。提交决策之后出错要走向前恢复，控制流不能把它作为普通拒绝发送 NACK。恢复时从已认证原始字节重建执行副本，保留原截止时间；宿主组合模式还需要在同一把锁下合并两个输入。新 TCP 连接根据可用计划与端点准入；已建立 TCP 在策略变更时重新核对 owner。HTTP 每次请求校验；gRPC 流在进入、收发及阻塞接收返回后有相应检查。这些不同的授权时点应留在 API 文档中。[计划接收](../../../node/control/plan.go) · [发布与撤销](../../../internal/node/dataplane/publish.go) · [组合模式](../../../node/composition.go) · [恢复校验](../../../node/plan_verify.go)

## 性能证据与取舍

Go 1.27.3，darwin/arm64，Apple M5 Pro，基准后缀 `-18`。下表取同一基准三轮的中位数，内存为每次操作分配量。授权夹具把目标服务和成功规则放在末尾；两个查找入口使用相同规模的计划。控制面 Hit 使用并发 benchmark，Miss 使用八个并发请求作为一次操作，两行的耗时不能相除当作单请求的加速比。

| 当前微基准 | 中位耗时 | 分配及解释 |
| --- | ---: | --- |
| 1,000 服务 × 1 规则，线性入口 / Runtime 入口 | 1,401 ns / 12.96 ns | 两者 0 alloc；同一已发布计划能命中服务索引 |
| 1,000 服务 × 1,000 规则，线性入口 / Runtime 入口 | 1,707 ns / 272.1 ns | 两者 0 alloc；索引缩短找服务的时间，服务内仍要检查规则 |
| 100 服务，投影命中 | 5.28 µs/op | 约 49.9 kB、7 alloc，0 次编译；仍需生成新正文 |
| 100 服务，同一接收方同时冷请求 | 396.63 µs/批 | 每批 8 请求，1 次编译，约 869 kB/批；夹具已预热 Catalog |
| 完整 Catalog 解码，100 / 1,000 服务 | 0.416 / 4.191 ms | 约 318 kB / 3.71 MB；含有效服务、grant、access 与端点 |

复测命令如下。它们覆盖本地 CPU、序列化及分配，未测得业务 P99、跨主机吞吐、规模上限或与其他 mesh 的性能排名。

```sh
GOWORK=off go test ./internal/node/dataplane -run '^$' -bench '^Benchmark(RuntimeAuthorization|Authorization)$/^services=1000$/^rules=(1|1000)$' -benchmem -benchtime=150ms -count=3
GOWORK=off go test ./internal/controller/plancache -run '^$' -bench '^(BenchmarkCache|BenchmarkCatalogDecode)$/^(Hit|MissSameRecipient|100|1000)$' -benchmem -benchtime=200ms -count=3
```

- **请求路径。** Runtime 用原子指针同时发布执行计划和派生索引，单方向服务条目达到 16 才建表。读到另一代计划时回退到该计划本身查找，避免把两代数据拼起来。连接选择在可用端点间采样并比较连接数，拨号受调用者 deadline、单次连接超时及熔断状态约束。规则多、连接频繁创建、宿主使用自有投影时，应分别测量规则扫描、握手和索引回退。[执行状态](../../../internal/node/dataplane/execution_state.go) · [端点选择](../../../internal/node/dataplane/endpoints.go)
- **控制面。** 同键在途构建共享、有限编译并行度、LRU、pending 数量与序列化字节上限，能限制重复劳动和队列增长。默认 1,024 个完成项、128 个待处理项、64 MiB 拓扑字节；解码对象、索引、活跃请求及签名副本另占内存。租约更新即使没有立刻通知会话，也能产生新快照，心跳下一次取快照时可能冷编译。评估扩大容量应记录快照更新频率和实际分配。[Cache](../../../internal/controller/plancache/cache.go) · [资源组装](../../../internal/controller/plancache/resource.go)
- **成本分布。** 编译会按服务端口遍历 access、grant 和组成员。file 每次持久化改动都涉及完整状态编码、同步和替换；Node 持久化提交也有多次压缩与同步。隔离内存存储、文件系统和 etcd 的计时，才能区分授权查询快、控制面收敛慢或磁盘写慢。当前最容易验证的改善是 etcd 重建少读取几次相同前缀，以及端点 JSON 只解析一次。[编译器](../../../internal/controller/catalog/compile.go) · [file 基准](../../../store/file/store_bench_test.go) · [状态 I/O](../../../policy/state/file_io.go)

## 依赖与代码体量

用 `GOWORK=off go list -deps` 分别统计生产入口，包数含入口和标准库；外部模块数按包的非空 Module.Path 去重并扣除 FastMesh。数量受平台、工具链和 build tags 影响，不表示二进制大小。

| 入口 | 包数 | 外部模块 | 实际边界 |
| --- | ---: | ---: | --- |
| model / policy/apply | 110 / 190 | 1 / 2 | 含身份类型；未导入 gRPC 或 etcd |
| identity/spiffe / identity/spiffe/workloadapi | 169 / 344 | 1 / 8 | Workload API 在选用适配器时引入 gRPC |
| node / controller | 342 / 350 | 7 / 7 | 使用 gRPC；均未导入 etcd 或 Workload API 适配器 |
| store/memory / store/file | 78 / 81 | 0 / 1 | 可独立选用；file 有平台锁依赖 |
| store/etcd | 365 | 16 | 选择该实现后才编译 etcd 依赖链 |

按 Git 已跟踪的 Go 文件计算，手写非测试 Go 为 185 文件、15,734 物理行（含例程、支撑包、空行和注释）；测试为 135 文件、15,272 行；4 个生成文件共 2,011 行。手写大文件集中在 etcd/store、Controller 索引和 enrollment。行数提供阅读范围，不能充当覆盖率或重复率。具备删减依据的具体对象是两份几乎相同的 revision 广播、Catalog 两次解码端点，以及少量测试夹具导致生产结果持有原始对象。[依赖边界测试](../../../test/release/dependencies_test.go) · [Catalog](../../../internal/controller/catalog/catalog.go)

## 同类项目与后续方向

| 方案 | 策略执行位置与部署成本 | 对 FastMesh 选型的启示 |
| --- | --- | --- |
| FastMesh | 进程内 Go SDK；同一套收件人计划服务 TCP、HTTP、gRPC；业务和 SDK 一起升级，宿主管理身份源与状态盘 | Go 团队若重视断连期间有时限的本地授权，并能控制调用入口，当前架构直接匹配 |
| Linkerd | 透明 Rust 微代理承接应用流量，控制面提供身份、目的地等服务；代理自带 TCP/HTTP 指标、TLS 与负载均衡 | 多语言透明接入与统一观测更符合代理模式。FastMesh 可借鉴“发现、身份、连接、请求”的排障划分，沿用现有 Observer 由宿主接导出器。[官方架构](https://linkerd.io/docs/reference/architecture/) |
| Istio | sidecar 模式按工作负载部署代理；ambient 通过节点 ztunnel 提供 L4，按需用 waypoint 执行 L7 | 平台统一治理、无需改应用是不同的购买理由。FastMesh 值得借鉴 L4 与 L7 成本分开度量；引入透明接管将改变部署、授权责任及升级模型。[数据面模式](https://istio.io/latest/docs/overview/dataplane-modes/) · [ambient 架构](https://istio.io/latest/docs/ambient/overview/) |
| Envoy + xDS | 代理通过类型化的 listener、route、cluster、endpoint 等资源订阅控制面，支持全量和增量协议 | 标准互操作是采用 Envoy 生态的理由。FastMesh 的 delta 传完整计划，因此其带宽、原子发布及离线恢复须按自己的协议评估。未来若有互操作需求，先做一条资源的协议映射和兼容性验证。[官方协议](https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol) |
| gRPC 的 proxyless xDS | 以 gRPC Java 官方实例为例，客户端和服务器通过 bootstrap 接受 xDS 配置，可选用 xDS 凭据建立 mTLS；执行点同样在应用内 | 全部流量为 gRPC 且已有 xDS 管理面时，这是较接近的进程内方案。FastMesh 的额外价值在 TCP、HTTP 与 gRPC 共享授权模型、完整签名与本地恢复。官方实例支持的能力不外推到所有语言版本。[gRPC Java 示例](https://github.com/grpc/grpc-java/blob/master/examples/example-xds/README.md) |

建议后续以两类实际负载决定路线。对独立部署的 Go 应用，先验证安装、身份轮换、控制面断连、恢复和权限撤销，把三种协议的同一使用体验做稳。对多租户 HA 用户，先量化索引重建耗时、租约写放大及从权限变更到 Node ACK 的延迟。若真实消耗集中在传输，再评估减少签名计划字节；这需要定义增量的基线认证、乱序恢复和快照回退，不能只改 protobuf 消息。若消费方主要要求透明跨语言接入，应先评估已有代理方案再立项。

## 分项评价

| 维度 | 权重 | 分数 | 判断依据 |
| --- | ---: | ---: | --- |
| 架构合理性 | 20% | 8.5 | 控制与业务路径分离；存储和身份可注入。组合模式、存储装饰器需额外遵守调用契约 |
| 可读性 | 15% | 7.8 | 包名贴近领域，关键持久化顺序有注释；少数注释描述旧宿主，状态机跨多个入口 |
| 可维护性 | 15% | 7.5 | conformance、race、smoke、fuzz 入口齐备；包装器方法集与本地状态读入口仍有覆盖缺口 |
| 性能设计与证据 | 20% | 8.0 | 已测量索引快路径和编译合并；etcd 冷启动、重复解码及内存放大仍有明确改进空间 |
| 安全与一致性 | 20% | 7.2 | mTLS、收件人校验、抗回退和提交恢复设计扎实；HA 写围栏组合和本地解压边界存在静态缺陷 |
| 重复与代码气味 | 10% | 7.2 | 后端共享契约明确；部分复制与重复解码可以局部消除，公共扩展点需要保留兼容性 |

加权和为 7.755，四舍五入为 7.8。评分衡量当前提交在“可嵌入 Go mesh”目标下的工程成熟度。安全维度基于防御性代码审阅和仓库测试，没有做渗透测试或生产容量认证。

## 验证记录

- `GOWORK=off go test -count=1 -timeout=180s ./...` 失败，仅失败包为 `test/release`。中英文 README 各 5 个归档链接触发同一条显式规则。
- 在随机本地端口、全新临时数据目录的 etcd **3.7.3** 上设置 `FASTMESH_ETCD_ENDPOINTS`，执行 `GOWORK=off go test -json -race -count=1 -timeout=180s ./...`。685 个测试或子测试通过，1 个失败（同一文档测试），无具名测试 skip。另有 11 个无测试包被 Go 标记 skip。这里验证了真实单实例后端和多 Controller 交接用例；CI 固定的 etcd 3.7.2、跨机器 quorum 故障与网络分区不在本次实测范围。
- `GOWORK=off go vet ./...` 通过。`python3 test/smoke/examples.py` 通过隔离模块构建、gRPC 方法授权、HTTP 路径授权及四个独立目录的配置更新、断连与恢复。
- 与必现的 README 测试失败分开记录：围栏方法集、状态读取上限、重复解码与前缀读放大来自源码追踪；此次没有为这些事项修改生产代码、注入故障或运行漏洞复现。它们各自的修复验收在 [Spec](spec.md)。长时 fuzz、漏洞数据库扫描、Linux CI 和生产 P99 未在本次重新运行。
