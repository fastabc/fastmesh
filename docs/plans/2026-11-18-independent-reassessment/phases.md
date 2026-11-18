# 分阶段执行计划

**2026-11-18 本地实施与验收完成。** 基线为 `454b700`，已验收的代码和回归测试截至 `7a865f0`。阶段 4 已作出两项决策。下表给出交付证据，后文保留原设计、回退条件和部署检查，便于发布者沿用。[基线评估](README.md) · [详细 Spec](spec.md) · [原始测量与夹具说明](measurements.md)

## 阶段交付记录

| 阶段 | 已交付 | 验收证据 |
| --- | --- | --- |
| 0 | `96499ee` 更新中英文 README，移除指向本地归档的发布导航 | 干净导出中发布文档测试、普通全量和三个独立模块示例均通过 |
| 1A | `ac2cc56` 让一次批量删除使用同一任期，返回已确认删除数；`402aa1e` 保留指标包装后的原生围栏；`dc536fb`、`7a865f0` 补齐失败路径 | 真实 etcd 上完成任期交接，旧持有者经裸后端和 metered 后端均返回冲突，继任者的对象版本保持一致；中断返回已确认数，重试可完成剩余删除；成功操作各记录一次指标 |
| 1B | `8b2fb5c` 让 InputRevision 与快照使用相同的有界解码 | 超限、格式错误、重启回退用例通过。用基线版本落盘 last-good，候选从同一目录读回节点 one、输入修订 197 |
| 2 | `76ece2b` 限制 watch 定位读取；`6e87c17` 合并 View 的相同物理前缀；`dd315f3` 复用已解码的端点并移除 Catalog 原始对象映射 | 测试覆盖重叠范围的数据所有权、顺序、空范围和修订压缩后的整幅失败。固定输入的四组收件人计划哈希和完整 WirePlan 投影摘要一致。冷解码、View、Node 授权及变更至 ACK 计数见测量记录 |
| 3 | `f04f14b` 将 memory 和 file 用到的 revision 过滤与复制收敛到共享函数；`5c95800` 校正重试及后端契约说明，并在独立模块构建中检查依赖 | gRPC、HTTP 和多进程配置恢复三个示例均通过。三个消费者的生产依赖图均未引入 etcd。1 和 16 个订阅者的广播分配次数分别保持在 4 和 64 |
| 4 | 保留单 Go module；另立按服务索引授权规则的性能原型方向 | 三个独立消费者已验证可隔离重依赖。1,000 服务的 CPU profile 中 Compile 累计占 57.5%，CanBind 累计占 13.33%。下一次原型应在 100 和 1,000 服务、不同授权密度下比较编译 CPU、构建索引的内存开销及收件人计划等价性，再决定是否合入 |

**性能结果。** 四租户、600 对象、六个逻辑范围，同一批 30 次 View 的数据 Range 从 180 次降为 30 次；响应总量从 80,056,620 B 降至 13,342,770 B，P95 从 16.57 ms 降至 3.37 ms。1,000 服务的 Catalog 冷解码十轮中位耗时从 4.142 ms 降至 2.943 ms，每次分配字节数减少 35.93%。在每场景 20 个 ACK 的对照里，watch 重连 P95 从 49.09 ms 升至 51.67 ms；缓存拒绝数均为零。这组短样本支持冷重建开销下降，端到端尾延迟仍需按实际负载观察。

**最终验证。** macOS arm64、Go 1.27.4、本机独立 etcd 3.7.3。已提交源码的普通和 race 全量分别通过 691 项测试及子测试，含前一轮两项 ACK 精简的工作区分别通过 693 项，四次运行均零跳过、零失败。CI 指定的 watch、租约过期、注册和交接用例均实际执行。六个 fuzz 目标各运行 60 秒；go vet、三组独立消费者、固定版本 protobuf 生成比对、go mod verify、go mod tidy -diff 均通过。原工具链的 govulncheck 命中可达的标准库问题 GO-2026-6859，`8159d6c` 将最低 Go 提升到 1.27.4 并整理模块清单，复扫通过。托管 Linux CI、CI 固定的 etcd 3.7.2、生产 HA 灰度和发布需在相应环境执行。

## 优先顺序与交付边界

| 阶段 | 依赖与工期估算 | 交付物 | 进入下一阶段前的检查 |
| --- | --- | --- | --- |
| 0 · 恢复发布基线 | 可立即安排，0.5 人日 | B4。更新两个 README，移除旧导航，保留可运行示例 | 文档测试在干净 checkout 通过，三个 smoke 场景通过，普通全量变绿 |
| 1A · 控制面写入一致性 | 不依赖性能工作，1.5–3 人日 | B2 后 B1。分两次提交修复原生 fenced 批量操作、观测包装转发 | 同一真实 etcd 上裸后端及包装后端得到相同的任期约束；无具名测试 skip |
| 1B · Node 状态边界 | 与 1A 独立，0.5–1 人日 | B3。水位读取复用有界解码，保留旧文件格式 | 状态边界、pending/committed、重启与回退保护通过；旧格式有效样本结果一致 |
| 2 · 减少冷重建工作 | 先有绿色基线，2–4 人日 | O1 分 watch header 与 View 两次提交；O2 独立提交 | 同修订输出、旧新编译结果与授权判断等价；无分页六范围夹具的数据 Range 从 6 降至 1；记录解码分配与重建延迟 |
| 3 · 代码和文档精简 | 1、2 的相关接口稳定后，1–2 人日 | O4 的小型纯函数，O5 的注释校正；O3 的消费者检查和依赖说明 | race、真实 etcd、独立消费者、公开 API 和示例通过；新增抽象有两个真实调用者 |
| 4 · 条件性决策 | 先收集消费者和负载数据，评审约 0.5 人日 | 记录是否立项拆 etcd module、是否值得按服务分组编译规则 | 可量化的构建/维护成本或 CPU profile 支持立项；证据不足时结束本轮 |

### 阶段 0 的核验与提交

在未改动代码时保留当前两条红色基线的摘要，然后用一次仅涉及 `README.md`、`README.zh.md` 的提交修复导航。开发者可直接从已有 guide 表抵达架构、集成、安全、测试和发布指南，因此无需用另一组研究文档填满删掉的行。运行下面的文档测试、普通全量和 smoke，若另有失败按其根因单独处理。CLI 示例中的四个独立目录、token 分发和多终端启动顺序，均作为 smoke 已验证的读者路径保留。文档如需回退，沿用仍然可发布的稳定链接。

### 阶段 1 的修复顺序与验收

1. **先查写视图的方法集，再改实现。** 将对象单写、创建、条件删除、批量删除、多对象提交列成表，对裸 etcd、fenced etcd、metered 后再 fenced 的组合分别核对。B2 先交付受围栏约束的批量实现；B1 再转发原生工厂。测量装饰器的记录次数，避免同一次业务操作重复计数。运行已有 leader 交接、注册原子性和存储 conformance；新增缺失的组合契约断言即可。上线先选一个 HA 实例组，观察交接时的冲突、失败类型和目录状态。若出现错误写入状态，暂停该写路径并核对实际提交记录；回滚应用版本不会撤销已持久化的数据。
2. **把水位读入口纳入既有解码契约。** 用已有大小和完整性夹具验证 `InputRevision`，并通过已有重启测试验证 journal 决策优先级。仅修改读取入口，发布前保留一份合法旧状态作读兼容验证。已有超出约定边界的本地文件应产生可辨识错误，让操作者保留现场；不能用“清空水位继续启动”作为修复。回退不涉及数据迁移。
3. **共同门槛。** 阶段 0、1 通过后，对两个 README 和实际运行能力再做一次核对。若时间有限，先发布这一组正确性修复；阶段 2 的测量与后续包整理均可独立排期。

### 阶段 2 的测量设计

| 实验 | 固定输入 | 观测量与通过条件 |
| --- | --- | --- |
| etcd 索引恢复 | 同一数据集，1/多租户、空 kind、大小配置不同的对象；包含 watch 重启与历史修订压缩情形 | 按逻辑 range 比较对象、顺序、修订及错误；统计 Range 次数、服务端发回字节、客户端峰值内存、收到首份可用计划的时间。六范围同前缀只读取一份基线数据；失败时整幅视图重建 |
| 冷编译 | 固定 now、租户、recipient；100/1,000 服务与相同端点、组和许可；同时包含非法对象与 tombstone | 改动前后比较完整计划、hash、端点有效期及权限。相同机器至少十轮，用 benchstat 比较 Decode 的 B/op、allocs/op 及 ns/op。建议以 1,000 服务下分配减少、重复解码消失且无显著小规模退化作为合并门槛 |
| 运行中收敛 | 固定负载与租约 TTL，分别观察正常续约、权限撤销、watch 重连 | 用同一客户端时钟记录变更发起至相应 input revision 被 ACK 的延迟；记录样本数及 P50/P95/P99、缓存拒绝和进程内存。没有基线前不设绝对毫秒承诺 |
| Node 热路径 | 复用现有 Runtime / 线性授权 benchmark；TLS 握手或业务请求另测 | 分清已发布索引、线性回退及规则密度。阶段 2 的更改不应改变协议字节或授权语义；端到端结果单独报告 |

性能改动须能逐项撤销。O1 沿用现有键布局、协议和修订；如有范围缺失、排序变化或内存回退，先回退 View 优化，保留独立验证过的 watch 定位改动。O2 只有内部中间结果发生变化，可单独恢复两遍解码实现。已有 64 MiB 缓存指标只计算序列化拓扑，做回滚判断时应同时看堆和进程内存。

### 阶段 3、4 的目录与发布策略

默认目标继续沿用单 Go module。下图仅展开与本次设计相关的包；共享过滤函数位于 internal/storeutil。仓库已经有合适的功能边界，目录移动本身无法减少一个生产依赖。

```text
fastmesh/
├── go.mod
├── api/                          # proto 源码与生成包
├── model/                        # 可公开使用的领域类型
├── controller/                   # Controller 生命周期和宿主入口
│   ├── admission/
│   ├── discovery/
│   └── enrollment/
├── node/                         # 使用方 API、控制客户端、熔断器
│   ├── control/
│   └── breaker/
├── identity/spiffe/
│   └── workloadapi/              # 可选身份源适配器
├── policy/{apply,material,snapshot,state,verify}/
├── internal/
│   ├── controller/{catalog,catalogrpc,endpoint,expiry,index,plancache}/
│   ├── node/dataplane/
│   ├── protocol/planwire/
│   ├── fslock/
│   └── storeutil/revision.go     # O4，共享过滤和复制
├── store/
│   ├── memory/                   # 易失对象与租约
│   ├── file/                     # 独占的持久化后端
│   ├── etcd/                     # 独立导入的 HA 后端
│   ├── election/
│   ├── key/
│   └── storetest/                # 可复用契约测试
├── examples/
├── test/{fixture,release,smoke}/
├── docs/                         # 可发布的现行指南
│   └── plans/                    # 本地评估和执行计划
└── README.md、README.zh.md
```

在这一结构中，Node 与 Controller 依赖存储或身份的消费接口，宿主选择具体实现。Go import 方向和包的公共 API 才是边界。O3 的条件分支只在有明确消费者约束时另立 Spec，届时目标仍是现有 `store/etcd` 导入路径，新增的是该子目录下的 module 边界与相应的独立测试入口。要先解决核心与适配器发版次序、老消费者升级、跨 module 测试与版本代理可安装性，再批准移动测试或加 CI。O4 若没有降低理解成本，允许只共用测试；O5 注释修订可逐条回退。

## README 原设计与发布入口

**替换位置。** 两个 README 各删除指南表里两行引用归档的历史导航，把 Boundaries / 适用边界末尾指向执行报告的句子替换为下方“验证”段。将现有生产依赖表述收拢为“存储选择”段，已存在的架构图、协议能力表和 Quick start 继续承担各自的说明。链接按 README 在仓库根目录时书写。下方是当时的设计示意，实际交付见[英文 README](../../../README.md) 和[中文 README](../../../README.zh.md)。

**中文**

```markdown
### 存储选择

Node 与 Controller 的生产包均可在不编译 etcd 客户端的情况下使用。
宿主负责选择后端。memory 适用于易失状态，file 由一个 Controller 独占，
需要多个 Controller 共享状态时使用 etcd。根 Go 模块同时维护 etcd 适配器和集成测试。

### 验证部署

先按[集成指南](docs/integration.md)配置身份源、Controller 白名单和独占状态目录。
控制面断连后，Node 仅在已验证策略的原有效期内接受新请求。
可用[测试指南](docs/testing.md)验证示例和真实 etcd 交接，
发布前的检查项见[发布指南](docs/release.md)。
```

**English**

```markdown
### Choosing a store

Applications can use the Node and Controller packages without compiling the etcd client.
The host selects the backend. Memory is volatile. The file store has one exclusive
Controller owner. Multiple Controllers can share state through etcd.
The root Go module also maintains the etcd adapter and its integration tests.

### Verifying a deployment

Use the [integration guide](docs/integration.md) to configure the identity source,
allowed Controller identities, and exclusive state directory. During a control-plane
outage, a Node can admit new requests only within the original lifetime of verified policy.
The [testing guide](docs/testing.md) covers examples and real-etcd handoff checks.
The [release guide](docs/release.md) lists the release criteria.
```

## 可执行验证清单

以下命令可复核实施结果；实施前的测量见[基线验证记录](README.md#验证记录)，本轮的结果见文首交付记录。恢复、鉴权与跨后端契约优先于代码行数和时间指标。

```sh
GOWORK=off go test -count=1 ./test/release
GOWORK=off go test -count=1 ./policy/state ./policy/apply ./node/...
GOWORK=off go test -count=1 ./internal/controller/... ./store/...
GOWORK=off go test -count=1 -timeout=180s ./...
GOWORK=off go test -race -count=1 -timeout=180s ./...
GOWORK=off go vet ./...
python3 test/smoke/examples.py
```

etcd 阶段使用独立临时实例，并在 CI 的 `FASTMESH_ETCD_ENDPOINTS` 已设置的环境运行下列命令；通过现有 CI 对具名用例及零跳过的检查。消费者侧继续从独立模块构建，保留现有 CI 的平台、生成代码、静态分析和定期 fuzz 检查。[CI](../../../.github/workflows/ci.yml) · [fuzz](../../../.github/workflows/fuzz.yml)

```sh
test -n "$FASTMESH_ETCD_ENDPOINTS" &&
  GOWORK=off go test -json -race -count=1 -timeout=180s ./store/etcd ./controller ./controller/enrollment
```

**完成定义。** 阶段 0–3 的四项缺陷均有对应回归证据，原有示例、签名原文、对象键、状态文件和公共导入路径保持兼容，性能变更附同机对照和原始计数。面向用户的 README 描述当次实际发布状态。阶段 4 以有依据的立项或暂缓决定收尾，无须以新增模块数量作为完成指标。
