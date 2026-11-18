# 让一次策略更新更容易读懂

基于 `454b7006aa05`。FastMesh 已经用 `controller` 和 `node` 给出了两个清晰的阅读入口：Controller 根据某一修订生成某个 Node 的计划，Node 接收、应用并回复。维护者真正容易迷失的地方集中在这条路径的几个函数里。本轮建议保留现有包布局，按下面的顺序做包内调整。每项可独立评审；与改变错误处理或存储语义的修复分开提交。

## 1. 让 Node 的两种响应共用一条应用后处理路径

`HandleFull` 和 `HandleDelta` 各有一份相同的处理顺序：应用返回需要恢复的错误时立即结束；其他结果交给 `AfterApply`；回调后的结果成功才记录控制面联系。两份实现分开后，修改“哪种情况代表联系成功”就需要核对两个分支。建议保留两个协议入口，在各自开头显式保留需要恢复时提前退出的一行判断，把随后相同的回调和联系记录收进一个私有方法。这样读者在入口能直接看到什么情况下不会发送任何回复，也能在一处找到联系状态变化的规则。Full 的 NACK 携带上一版本，Delta 的 NACK 和流降级有自己的顺序，协议报文仍由各自入口构造。[响应处理](../../../node/control/response.go) · [现有 Full 测试](../../../node/control/client_test.go)

示意调用点如下。保留回调前的恢复判断很重要，否则宿主回调有机会把已经进入恢复流程的结果改写成普通响应。

```go
applyErr := c.applyFull(ctx, resp)
if errors.Is(applyErr, apply.ErrRecoveryRequired) {
    return applyErr
}
applyErr = c.afterApply(ctx, resp.GetVersionInfo(), resp.GetNode(), applyErr)
// 下方按 Full 协议构造 ACK 或 NACK。
```

验收同时检查 Full 和 Delta 的成功、普通拒绝、需要恢复三种结果。记录回调次数、发送次数和报文内容；另外覆盖 `AfterApply` 改写普通错误的现有行为。这是四项中最适合作为首个提交的一项。

## 2. 把一次等待 ACK 的推送表示成一条记录

Controller 的 `sessionState` 把一次在途推送拆成四个字段。发送成功后同时设置 nonce、下发版本、输入修订和发送时间；收到 ACK 时逐个读取，完成后只清空前两个字段。现有代码依靠 nonce 是否为空判断有没有待确认推送，留存的修订和时间容易被读者误当成仍然有效。建议用私有值类型 `pendingPush` 绑定这四项数据。发送成功后整体赋值，确认完成后整体归零；继续使用 nonce 判断有效性。这样能从字段归属看出“收到哪个回复”“对哪个输入修订记账”和“耗时从何时起算”。会话级的 dirty、上一条已确认版本以及 delta 基线继续随会话保存，它们有跨越多个推送的生命周期。[会话状态与事件循环](../../../controller/discovery/session.go) · [等待 ACK 时的变更合并测试](../../../controller/discovery/coalesce_stream_test.go)

```go
type pendingPush struct {
    nonce         string
    revision      string
    inputRevision int64
    sentAt        time.Time
}
// 收到匹配回执，完成记账后清理本次推送。
state.pending = pendingPush{}
```

验收覆盖旧 nonce、Full ACK 版本不匹配、Delta NACK 回退、待确认期间再次收到变更、超时与心跳。计时器仍归事件循环管理，确保改动只涉及数据的表达。

## 3. 让 Controller 索引显式区分 Node 事件与租户目录事件

`Index.apply` 当前同时维护 Node 表、判断哪些租户需要唤醒、复制受影响的租户快照、按需复制授权表，再推进全局 revision。读代码时很容易误以为一条 Node 心跳也会重建租户拓扑。实际存在两个清楚的对象边界：Node 事件修改注册状态并决定通知谁；目录事件修改下一版租户快照。建议先把 Node 分支移入已有的 `node.go`，用方法名说明它在持有 Index 锁时运行，并让它填充本批次待通知的租户集合。随后把 `Snapshot` 类型及其只读查询集中到同包的 `snapshot.go`。原来的 `apply` 继续负责一批事件的单次发布，读者可以在同一处看到何时推进修订、更新到期调度并通知订阅者。[批次发布](../../../internal/controller/index/index.go) · [已有 Node 判断逻辑](../../../internal/controller/index/node.go)

```go
if event.Object.Key.Kind == model.NodeKind {
    i.applyNodeEventLocked(event, tenantsToNotify)
    continue
}
// 按租户构造本 revision 的新目录快照。
```

验收保留旧快照可读、组成员变更不修改历史快照、Node 跨租户变更能通知相关方、仅刷新联系时间不会触发无关推送、watch 丢失后可重建。拆文件和抽取分支适合分两次审阅。

## 4. 在组合模式的调用点写明输入来自宿主还是 mesh

`HostApplier` 用 `composedInput{n.composition, false}` 创建宿主入口，Node 初始化则用同一类型加 `true` 创建 mesh 入口。这个布尔值决定了 `Snapshot` 向调用方暴露哪些字段，也决定两个并发输入如何合并到同一份日志。误读其含义时，很容易把宿主 revision 和 mesh input revision 当作同一个版本。此处只需要一项很小的整理：把私有字段命名为 `fromMesh`，在两个构造点和已有测试中使用带字段名的字面量。阅读者从调用点就能理解所选路径，再到唯一的合并函数确认哪些字段属于各自输入。两种输入的交错更新仍由现有 pipeline 的同一把锁串行化。[组合与合并](../../../node/composition.go) · [Node 装配](../../../node/lifecycle.go) · [组合恢复测试](../../../node/composition_test.go)

```go
// HostApplier 的返回值
return composedInput{composition: n.composition, fromMesh: false}

// newControlConfig 选用的输入
input = composedInput{composition: n.composition, fromMesh: true}
```

验收看两个具体结果：宿主取到的快照隐藏本地 mesh 计划，任一输入更新后都保留另一方最近成功提交的字段。这个改动规模很小，可以在下一次修改组合模式时顺带完成。

## 实施与验证

建议优先提交第 1、2 项，分别从 Node 和 Controller 两端降低理解 ACK 处理的成本。第 3 项放在优化 etcd 读取前独立完成，便于对照结构调整与性能变化。第 4 项可随组合模式维护机会并入。建议评审时用一个问题验收：给定一次策略更新，维护者能否指出哪条输入修订被发布、何时可以 ACK，以及宿主快照中的哪些字段归 mesh 所有？

```sh
GOWORK=off go test -race -count=1 ./node/control ./controller/discovery ./internal/controller/index
GOWORK=off go test -race -count=1 ./node -run '^TestCompositionSerializesIndependentInputsAndRestoresTuple$'
```
