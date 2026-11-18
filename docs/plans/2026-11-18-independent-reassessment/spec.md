# 问题与技术规格

基线、实测数字和总体判断见[独立评估](README.md)。下文的 P1 表示发布或一致性优先项，P2 表示有明确工程成本、可独立交付的改进，P3 表示低风险整理或条件性决策。每项说明证据、影响面、解决方案及验收。以下代码为拟议实现片段或测量示例，尚未写入产品。

## B1 · P1 · 观测包装改变 etcd 单对象写入的围栏

**证据与影响。** `controller.New` 根据传入的 Objects 创建 fenced writer。直接传入 etcd 时，`FencedFunc` 识别原生工厂，把租约比较和单次写入放进同一笔 etcd 事务。宿主若先用 `NewMetered` 包装同一个后端，包装器虽然转发 `ApplyObjectsFenced`，却没有转发 `FencedObjectStore`。单对象写入因此落入通用的“先读租约、随后写对象”两步路径。leader 交接期间，这两个动作之间的租约状态可能改变。问题针对原本有原生单写围栏的后端及其包装顺序，注册使用的多对象事务走另一条路径。现有 HA 测试通过，不能证明所有包装组合具有相同的原子性。[工厂选择](../../../store/fence.go) · [包装器](../../../store/metered.go) · [事务转发](../../../store/transaction.go) · [原生写入](../../../store/etcd/objects.go)

**规格与验收。** 由观测装饰器先从内部对象取得与裸后端相同的 fenced writer，再在返回值外加计时。保留原有结果、错误、调用者传入的租约来源和有界指标名；对原来只支持通用逻辑的后端仍使用原有路径。先单独完成 B2，以免把新暴露的批量方法误认作已经具备围栏。验收矩阵包括裸 etcd、带 recorder 的 etcd、nil recorder 及非原生后端；覆盖 create、put、按版本删除和多对象事务。每项同时检查提交结果、错误分类和期望的观测次数。在交接测试中以存储最终状态作为依据，不能仅检查 leader 布尔值。memory/file 的通用单写仍有分步校验限制，其单进程部署前提需要在集成说明中明示。

```go
// store 包内。对 next 选择与直接使用该后端相同的写入实现。
func (s *MeteredStore) FencedObjectStore(leases LeaseStore, source FenceTokenSource) ObjectStore {
    return NewMetered(FencedFunc(s.next, leases, source), s.recorder)
}
```

## B2 · P1 · etcd 围栏视图意外继承普通批量删除

**证据与影响。** etcd 的内部 `fencedObjectStore` 匿名内嵌 `*Store`。单对象删除和多对象事务有专门的围栏版本，批量删除却来自被提升的普通 Store 方法。通用批量入口按动态接口选择实现，普通实现分批提交只含删除操作的事务。API 调用方若持有受围栏保护的视图，会合理地期望同一个写入约束适用于每一批。当前源码能确认方法集与事务条件不一致；本次未发现内置 Controller 在业务流程中调用批量清理，因此将影响范围记为可供宿主使用的存储组合接口，没有据此认定线上已经发生误删。[围栏视图定义](../../../store/etcd/store.go) · [批量入口](../../../store/bulk.go) · [原生批处理](../../../store/etcd/objects.go)

**规格与验收。** 优先采用最小修复，为 fenced writer 明确实现批量方法，经现有受保护的删除操作逐键处理，保留已完成数量、缺失键幂等、上下文取消和中途错误。若宿主确实有批量性能要求，再将每一批的 lease compare 与该批删除放在同一笔事务，并实测服务端操作数上限。一次批量调用应按既定任期处理，避免跨任期自动续作；实现时在入口捕获 token 用于该次调用，错误后由上层决定重试。测试覆盖空集、缺失、有效任期、任期终止及部分完成，保留裸 Store 的现有多批次行为。任何优化都应检查额外方法提升，优先显式收窄内部写视图的能力。

```go
// 验收中的结果约束。交接发生时，返回值仅计入已确认删除的键。
if removed != committedBeforeHandoff {
    t.Fatalf("removed = %d, committed = %d", removed, committedBeforeHandoff)
}
```

## B3 · P1 · 读取修订水位时缺少本地状态的统一大小限制

**证据与影响。** 完整 last-good 与 apply journal 文件通过 `readGzipJSON` 读取，限制解压后的字节数并检验 JSON 完整性。`loadInputRevision` 的 last-good 分支重新创建 gzip reader，直接使用 JSON Decoder，只提取 `input_revision`。目标结构只含一个整数并不能限制解压器或解码器读取整个值所需的资源，也不能保证读取到压缩流尾部。该方法用于接收计划时读取高水位，也用于构造下一次持久化封装，因此同一个本地文件会因读取入口不同受到不同的预算和完整性约束。此处是受宿主管理的状态文件；现有审阅没有发现远端可直接提交本地任意文件的依据。[水位读取](../../../policy/state/revision.go) · [通用读取器](../../../policy/state/file_io.go) · [既有边界测试](../../../policy/state/size_test.go)

**规格与验收。** 保留 committed journal 优先、pending 不抬升水位、文件不存在返回零的分支，仅替换打开 last-good 后的解码逻辑。复用仓库已有的有界读取器，保留 metadata 小结构，无须解密材料或实例化完整快照。有效历史文件的水位与升级前一致。用同一组边界契约验证完整读取和水位读取都能区分合法输入、超限、尾随 JSON 及压缩流校验失败。此改法的峰值分配与已有 helper 有关，之后若要引入小型内存水位缓存，需先单独验证崩溃恢复、组合模式和 journal 决策，不能在这次修复中引入第二份持久化真相。

```go
var metadata struct {
    InputRevision int64 `json:"input_revision"`
}
if err := readGzipJSON(f, &metadata); err != nil {
    return 0, err
}
return metadata.InputRevision, nil
```

## B4 · P1 · README 的历史链接阻断当前发布测试

**证据与影响。** 普通全量和真实 etcd 的 race 全量均在 `TestDocumentationLinks` 失败。该测试主动拒绝面向用户的发布文档指向 archive，英文 README 的两条表格记录及末尾执行报告共五处，中文 README 同样五处。在保留了本地历史文件的开发机上，单纯检查路径存在可能误判为可发布；显式归档规则仍会让 CI 变红。对首次集成 SDK 的读者，真正需要的资料是可安装版本、三种后端的部署前提、示例、当前协议、故障时的保证以及可重复验证的命令。原有 README 已有可运行的多终端示例，应保留其顺序和示例凭据的适用范围。[英文 README](../../../README.md) · [中文 README](../../../README.zh.md) · [链接检查](../../../test/release/docs_test.go)

**规格与验收。** 在独立文档提交中按两种语言同步删除两行历史导航，将末尾旧报告引用改为当前测试与发布指南。写清生产包 import 图与根 go.mod 的差别，保持 v0.1.0 在本 checkout 中的发布前措辞；发布时再核对远端 tag。可直接采用[中英文拟稿](phases.md#readme-拟稿)。当前计划目录被 gitignore 排除，README 不应依赖该目录才能解释功能或通过发布检查。验收使用干净 checkout 跑文档测试和现有 smoke，逐项核对命令、示例目录、文件名与输出。保留测试中的 archive 拒绝规则，使同类问题将来仍会显现。

```sh
GOWORK=off go test -count=1 ./test/release -run '^TestDocumentationLinks$'
python3 test/smoke/examples.py
```

## O1 · P2 · 降低 etcd 全租户索引重建的读取放大

**证据与影响。** `Index.Run` 首次建立 watch 或检测到 watch 失效后，抓取五个 Catalog kind 和一个 Node kind 的完整一致视图。etcd 的 key 排列依次为租户、kind、对象名；租户为空时各 kind 得到相同的 objects 前缀。`View` 按 range 逐次请求，每次取回相同前缀的数据、解码，再由本地过滤。本路径一次基线有六个宽范围读取。此外 watch 建立前的 Get 为取得 header revision 而请求全部 key。对于同一前缀的 N 个对象，这意味着重复的后端传输与解码；已证实的是操作形状，具体带宽和 P99 取决于对象分布、值大小和重建频率。[Index.Run](../../../internal/controller/index/index.go) · [前缀映射](../../../store/etcd/store.go) · [一致视图](../../../store/etcd/view.go) · [revision watch](../../../store/etcd/revision_watch.go)

**规格与验收。** 先为两类 watch 的基线 Get 增加 `WithLimit(1)`，仍从线性一致响应头的下一修订建立订阅。再在 View 内合并相同物理前缀的读取，一次解码后将对象分配给所有匹配的逻辑 range。保留显式空 range、确定性排序、不同前缀固定首个 revision，以及压缩导致读取失败时放弃整幅视图的行为。重叠 range 的返回对象需要独立的可变字段副本。第一版只对小规模基准中证实的重复前缀去重；若大视图需要分页，每页也须固定 revision，直到完整返回才发布。在真实 etcd 上同时记录 Range RPC 数、返回字节、重建耗时、峰值内存及重建期间 ACK 延迟。相同六范围且无需分页的夹具，目标为一次基线数据 Range，watch 的轻量定位另计。[etcd Range 与修订语义](https://etcd.io/docs/v3.7/learning/api/)

```go
resp, err := s.client.Get(ctx, prefix,
    clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithLimit(1))
if err != nil {
    return nil, err
}
startRevision := resp.Header.Revision + 1 // 后续 Watch 使用该修订
```

## O2 · P2 · Catalog 只解码一次端点，移除测试驱动的生产数据滞留

**证据与影响。** `catalog.Decode` 第一遍为每个端点调用 `DecodeEndpoint` 校验，然后把原始对象存入 `EndpointObjects`。第二遍再调用相同函数，取出已验证字段组装 Service.Status.Endpoints。另有一份 `Objects` 保存原始 Service。当前源码中，可见这两份原始映射被 endpoint 测试的过期夹具用于取遍对象；生产编译实际关心的是服务、成员、授权与有效端点。缓存会在快照上保留解码结果，因此用于中转的原始索引也有存活成本。当前 1,000 服务完整解码约 4.19 ms、3.71 MB/op，这一结果是所有解码工作的总数，不能把它全部归因于第二遍解析。[Catalog.Decode](../../../internal/controller/catalog/catalog.go) · [实际编译输入](../../../internal/controller/catalog/compile.go) · [测试夹具](../../../internal/controller/endpoint/helpers_test.go)

**规格与验收。** 让过期测试直接从同修订的 View 获取需要的原始对象，保留夹具中“仅处理合法对象”的预期；随后让生产编译只持有语义对象。可先收集已校验端点记录，再在所有服务加载完毕后关联，以保持顺序独立。若规范要求重复 key 只保留最后一条，候选集合继续按 key 存储，不能无意改变去重。保留忽略坏对象、已移除端点、未知端口以及稳定排序的行为。对相同输入固定 recipient、now 和 validity，改动前后比较完整拓扑、hash、权限及端点租期。先看相同基准的 B/op 和 allocs/op，再用真实端点分布评估是否值得进一步按 service 分桶 access/grant；分桶作为有独立证据的后续工作。

```go
record, err := DecodeEndpoint(obj)
if err != nil {
    slog.Warn("catalog: ignoring invalid endpoint", "key", obj.Key, "error", err)
    continue
}
endpoints[obj.Key] = record // 本次 Decode 的临时集合，关联后即可释放
```

## O3 · P2 · 维持轻量消费入口，将拆 module 设为有条件决策

**证据与影响。** `go.mod` 直接声明了 etcd API 与 client；但 `go list -deps` 表明 node、controller 都只有七个外部生产模块且没有 etcd。Workload API 也已在单独包内。gRPC 服务于控制流和公开的 gRPC API，SPIFFE ID 是公开领域类型。此时大规模替换为自定义传输、字符串 ID 或重新实现 TLS 会扩大兼容性和验证成本。真正尚未隔离的是根模块的版本选择、依赖维护、仓库测试及两个直接引入 etcd 适配器的 Controller 集成测试。编译闭包、模块清单、构建时间和二进制体积是四个独立指标，前两者已经可以测量，后两者须在实际消费者中采样。[go.mod](../../../go.mod) · [发布边界测试](../../../test/release/dependencies_test.go) · [独立消费者构建](../../../test/smoke/examples.py)

**规格与验收。** 默认保留单 module，延用生产导入边界测试，将示例用户实际使用的 `go list -deps` 结果与独立 consumer 构建作为发版证据。若用户明确要求根模块清单无 etcd，或实测冷构建、依赖升级联动具有显著成本，再评估保持导入路径的 `store/etcd/go.mod`。届时适配器依赖已发布核心；会使根 tidy 重新引入适配器的跨模块测试需要独立测试 module 或迁到适配器侧。根与子模块都应 `GOWORK=off` 单独 tidy/test，子模块使用带目录前缀的 tag，增加真实消费者从版本代理安装验证。常规用户不能依赖仓库本地 replace。只有收益覆盖双发版和漏洞修复协调成本时才进入此分支。[Go Modules Reference](https://go.dev/ref/mod)

```sh
GOWORK=off go list -deps ./node ./controller
GOWORK=off go list -deps ./store/etcd
GOWORK=off go test -count=1 ./test/release -run '^TestProductionDependencyBoundary$'
```

## O4 · P3 · 局部合并 memory/file 的 revision 过滤逻辑

**证据与影响。** 两个本地后端的 `broadcastRevisionLocked` 都按 tenant/kind 过滤同一批事件，深拷贝对象，忽略空批并尝试非阻塞交付。队列溢出就关掉该订阅，让消费者在下次重连时重建一致视图。这段复制的风险是某次调整只改一边，导致相同仓库契约在 memory 测试和 file 部署下行为不同。另一方面，file 有关闭检查和持久化失败后的失效处理，不能为了消除行数把锁、持久化提交和 goroutine 生命周期搬进一个通用引擎。etcd 的网络 watch 有历史修订与压缩语义，也不属于这段共享逻辑。[memory 广播](../../../store/memory/revision_watch.go) · [file 广播](../../../store/file/revision_watch.go) · [已有深拷贝工具](../../../internal/storeutil/clone.go)

**规格与验收。** 仅把“按一个订阅筛选并复制一个 batch”提取为 internal/storeutil 的纯函数，保持每个消费者独立持有 Data 和 Labels。由两个原调用者继续持锁、处理空集、关闭与 dropped 统计，外部 API 及返回错误维持现状。已有 conformance 可共同验证顺序、修订完整性和溢出处理，增加针对两个订阅同时读写各自副本的回归断言。用小批及多订阅基准对比分配，不能因“双层复制”使广播次数与内存额外相乘。如果提取后只是把六行代码换成更长的配置结构，保留重复并用共同契约测试保护会更容易维护。

```go
filtered := storeutil.FilterRevision(batch, watcher.tenant, watcher.kinds)
if len(filtered.Events) == 0 {
    continue
}
// 交付、溢出及关闭仍由当前持锁的 Store 决定。
```

## O5 · P3 · 清理失真的注释，保留公开扩展和历史恢复路径

**证据与影响。** `Client.Run` 的注释说只在上下文结束时返回，函数还会因无地址、StopOnError、拨号配置失败及提交后需要恢复而提前返回。这会误导宿主把终止性错误当作普通重连。`etcd.Store.Prefix` 的注释解释一个 accounting plane 的余额位置，却没有说明读者在本项目实际获得的是哪个命名空间。`BulkObjectDeleter` 还用早期离线清理作主语，并称一次持久化转换，etcd 实现则能分批完成。仓库内调用少也不能证明导出方法没有外部使用者。相反，历史签名者恢复、已持久化数据和宿主组合接口各有明确行为或兼容性测试，应依据产品契约决定其寿命。[重连](../../../node/control/reconnect.go) · [etcd 前缀](../../../store/etcd/store.go) · [批量契约](../../../store/bulk.go)

**规格与验收。** 文档只描述当前可验证语义：Run 轮转与终止条件、Prefix 返回对象和租约的命名空间根、批量操作允许部分完成并返回计数。清理提交逐条列出对应实现和测试；公开符号即使在仓库内无调用也先保留。CI deadcode 报告只用作调查入口，生成 protobuf、故障分类、平台特有实现及已持久化字段从改名或删除候选中排除，除非有独立的兼容设计。对内部 Catalog 冗余字段的移除属于 O2，有实际调用者迁移和预算验证。此轮将注释修订与行为修复分开，方便读者在 git 历史中区分原因及回退影响。

```go
// Run rotates through controllers after retryable stream failures.
// It returns on context cancellation, invalid dial configuration,
// a caller-selected terminal error, or a required local recovery.
```
