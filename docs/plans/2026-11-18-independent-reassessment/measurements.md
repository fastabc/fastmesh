# 2026-11-18 本地验收原始测量

两端使用同一台 Apple M5 Pro、darwin/arm64 和 Go 1.27.3，基线为 `454b700`。候选含本轮存储与 Catalog 改动，以及工作区此前已实现的 ACK 处理精简。下面保留各实验的单位与样本数。最终正确性和安全扫描使用 Go 1.27.4。性能数字代表这些夹具的本机结果。

## Catalog 冷解码

复跑仓库内的 `GOWORK=off go test ./internal/controller/plancache -run '^$' -bench '^BenchmarkCatalogDecode$' -benchmem -count=10 -benchtime=200ms`。每行依次为迭代数、ns/op、B/op 和 allocs/op。

### 基线


```text
BenchmarkCatalogDecode/100-18         	     486	    457822 ns/op	  317296 B/op	    2550 allocs/op
BenchmarkCatalogDecode/100-18         	     559	    417783 ns/op	  317201 B/op	    2550 allocs/op
BenchmarkCatalogDecode/100-18         	     549	    435774 ns/op	  317388 B/op	    2550 allocs/op
BenchmarkCatalogDecode/100-18         	     529	    424604 ns/op	  317281 B/op	    2550 allocs/op
BenchmarkCatalogDecode/100-18         	     585	    414857 ns/op	  317206 B/op	    2549 allocs/op
BenchmarkCatalogDecode/100-18         	     566	    426474 ns/op	  317233 B/op	    2550 allocs/op
BenchmarkCatalogDecode/100-18         	     554	    450535 ns/op	  317176 B/op	    2549 allocs/op
BenchmarkCatalogDecode/100-18         	     576	    407377 ns/op	  317151 B/op	    2549 allocs/op
BenchmarkCatalogDecode/100-18         	     592	    424299 ns/op	  317290 B/op	    2550 allocs/op
BenchmarkCatalogDecode/100-18         	     589	    423567 ns/op	  317196 B/op	    2549 allocs/op
BenchmarkCatalogDecode/1000-18        	      50	   4449172 ns/op	 3713456 B/op	   25138 allocs/op
BenchmarkCatalogDecode/1000-18        	      52	   4266946 ns/op	 3713159 B/op	   25137 allocs/op
BenchmarkCatalogDecode/1000-18        	      55	   4382897 ns/op	 3713637 B/op	   25140 allocs/op
BenchmarkCatalogDecode/1000-18        	      52	   4431173 ns/op	 3713023 B/op	   25138 allocs/op
BenchmarkCatalogDecode/1000-18        	      56	   4130894 ns/op	 3713874 B/op	   25139 allocs/op
BenchmarkCatalogDecode/1000-18        	      57	   4096532 ns/op	 3713842 B/op	   25139 allocs/op
BenchmarkCatalogDecode/1000-18        	      56	   4140551 ns/op	 3712965 B/op	   25138 allocs/op
BenchmarkCatalogDecode/1000-18        	      60	   4130288 ns/op	 3713507 B/op	   25139 allocs/op
BenchmarkCatalogDecode/1000-18        	      57	   4143779 ns/op	 3713196 B/op	   25138 allocs/op
BenchmarkCatalogDecode/1000-18        	      56	   4140956 ns/op	 3712985 B/op	   25136 allocs/op
```


### 候选


```text
BenchmarkCatalogDecode/100-18         	     795	    297686 ns/op	  220928 B/op	    1937 allocs/op
BenchmarkCatalogDecode/100-18         	     835	    288377 ns/op	  220973 B/op	    1938 allocs/op
BenchmarkCatalogDecode/100-18         	     819	    291343 ns/op	  220991 B/op	    1938 allocs/op
BenchmarkCatalogDecode/100-18         	     830	    289113 ns/op	  220911 B/op	    1937 allocs/op
BenchmarkCatalogDecode/100-18         	     826	    298858 ns/op	  220963 B/op	    1938 allocs/op
BenchmarkCatalogDecode/100-18         	     805	    301087 ns/op	  221006 B/op	    1938 allocs/op
BenchmarkCatalogDecode/100-18         	     783	    292052 ns/op	  221017 B/op	    1938 allocs/op
BenchmarkCatalogDecode/100-18         	     836	    293550 ns/op	  220951 B/op	    1937 allocs/op
BenchmarkCatalogDecode/100-18         	     771	    300501 ns/op	  221046 B/op	    1938 allocs/op
BenchmarkCatalogDecode/100-18         	     795	    299394 ns/op	  221037 B/op	    1938 allocs/op
BenchmarkCatalogDecode/1000-18        	      84	   2978804 ns/op	 2378948 B/op	   19120 allocs/op
BenchmarkCatalogDecode/1000-18        	      80	   2965696 ns/op	 2378904 B/op	   19120 allocs/op
BenchmarkCatalogDecode/1000-18        	      67	   3075363 ns/op	 2378978 B/op	   19120 allocs/op
BenchmarkCatalogDecode/1000-18        	      79	   2960664 ns/op	 2379331 B/op	   19121 allocs/op
BenchmarkCatalogDecode/1000-18        	      81	   2919812 ns/op	 2378720 B/op	   19120 allocs/op
BenchmarkCatalogDecode/1000-18        	      80	   2784734 ns/op	 2378999 B/op	   19120 allocs/op
BenchmarkCatalogDecode/1000-18        	      82	   2869315 ns/op	 2378653 B/op	   19120 allocs/op
BenchmarkCatalogDecode/1000-18        	      76	   2947893 ns/op	 2379034 B/op	   19120 allocs/op
BenchmarkCatalogDecode/1000-18        	      86	   2841837 ns/op	 2379172 B/op	   19121 allocs/op
BenchmarkCatalogDecode/1000-18        	      80	   2937738 ns/op	 2378599 B/op	   19119 allocs/op
```

## 一致视图与计划等价

在本机独立 etcd 3.7.3 写入 4 个租户、5 个 kind 的 600 个对象，每条载荷 128 至 768 B。以 6 个跨租户逻辑范围读取，其中一个为空。连续读 30 幅视图，每轮核对各范围数量。计数器位于客户端 KV.Get，字节为 RangeResponse 的 protobuf 编码大小。峰值 RSS 为测试进程，包括夹具和客户端。

- 基线。180 gets 80056620 bytes p50=8.875125ms p95=16.57425ms p99=17.942084ms；进程峰值 RSS 39,190,528 B。
- 候选。30 gets 13342770 bytes p50=1.616083ms p95=3.373625ms p99=9.254334ms；进程峰值 RSS 35,438,592 B。

固定时间、修订 123、2 个租户、每租户 100 个服务，分别为拥有者和另一收件人编译。每五个端点含一个 tombstone，每个租户另有一条非法端点记录。各行依次为租户、身份、Plan.Hash、JSON 序列化完整 Plan.Wire() 的 SHA-256。两端的这四行逐字节相同。


```text
PLAN tenant-0 spiffe://test/owner sha256:aae4557f0b8b5d94e0489a61c5761cad5dc39b80ddbb68afbfab9a8d87d0427e 742c4a98cb35f3d529c68885bb9b7770f9e1dfa2f098ee7b2c2352e16cd8784f
PLAN tenant-0 spiffe://test/other sha256:3cd6cee22d6513152f4c6c0d5f6fa171289d355e43672b9dd9349d028639f8c2 4a51ef0f7a4a4f67a978a8f59db1cf8b1607e8e93283163061d401f257a7f30a
PLAN tenant-1 spiffe://test/owner sha256:2a3de1920166df75aa7668654a8dc5d85c7ee713fba5c23b2c7194ef0bd73a4e 496666f3f0e51f3cf91993082cd3beff1523123c049bb00d2d7501a676f9c2bd
PLAN tenant-1 spiffe://test/other sha256:7c6bf9e9158a81a00e826b36735c5c16b07d15dce99e41c40bd4f76be553e301 8d5823fd51e7fdbf6b7a096699eb8b5e3bee61091a2b8c0c104b261ca77155aa
```

## 变更到 ACK 的延迟

在两端相同的临时测量夹具中，以独立 etcd 替换 BenchmarkControlPlane 的内存后端。在服务端 RecordPlanApplied 回调中，匹配相同 node 和输入修订，记录从写入发起到收到 ACK 的时间。10 个服务、1 个流、目标每秒 5 次变更、200 ms 心跳；每个场景每版采集 20 次。续租更新端点过期时间；授权交替删除和恢复 BindGrant；重连场景每次先取消当前 watch，再执行同一授权变更。本地连接使用开发传输配置。首个 ACK 单独统计，各分位数沿用 benchmark 输出。

| 场景 | 版本 | P50 / P95 / P99（ms） | 首个 ACK（ms） | 缓存拒绝 | watch 开启次数 | 峰值 RSS（B） |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| 续租 | 基线 | 145.6 / 152.2 / 157.3 | 36.90 | 0 | 1.000 | 34,177,024 |
| 续租 | 候选 | 140.8 / 152.1 / 155.7 | 38.39 | 0 | 1.000 | 34,160,640 |
| 撤销与恢复 | 基线 | 35.15 / 41.90 / 53.59 | 35.82 | 0 | 1.000 | 34,783,232 |
| 撤销与恢复 | 候选 | 35.55 / 44.24 / 47.07 | 35.84 | 0 | 1.000 | 34,488,320 |
| watch 重连 | 基线 | 43.06 / 49.09 / 49.72 | 31.80 | 0 | 21.00 | 35,045,376 |
| watch 重连 | 候选 | 45.02 / 51.67 / 51.96 | 40.78 | 0 | 21.00 | 34,979,840 |

按事件发生顺序记录的 ACK 延迟。


```text
基线 续租 (ms): 150.757458, 152.155041, 149.619625, 149.751792, 147.6795, 157.332375, 146.194458, 146.991375, 147.4565, 145.746417, 145.612792, 144.217459, 142.281334, 139.162875, 142.998208, 138.87225, 139.901458, 137.1635, 135.702667, 138.108958
候选 续租 (ms): 150.439542, 151.093875, 155.668125, 152.143292, 150.088625, 146.943542, 141.906, 144.842458, 143.664333, 142.637917, 140.771375, 135.016083, 132.873458, 137.220958, 133.97775, 132.682083, 133.097334, 130.806833, 128.586792, 126.61825
基线 撤销与恢复 (ms): 35.204209, 32.184417, 33.209709, 33.087417, 36.099291, 53.585583, 41.902208, 35.722584, 32.442542, 33.122459, 31.979959, 31.61775, 37.218791, 37.426416, 34.965584, 32.762291, 35.147, 38.576958, 37.92575, 36.923625
候选 撤销与恢复 (ms): 31.3825, 35.350542, 40.296625, 40.139375, 37.8545, 33.081042, 34.03925, 35.552625, 32.099125, 33.796416, 36.647958, 35.667459, 36.327333, 31.12325, 47.071042, 36.111417, 44.239083, 40.215125, 35.079667, 33.922625
基线 watch 重连 (ms): 42.287041, 47.406, 44.283875, 40.358208, 41.305875, 41.224041, 42.275417, 44.085292, 46.611541, 45.521541, 41.089709, 43.258417, 43.056917, 49.088125, 49.033666, 49.719125, 44.559125, 42.520458, 39.661, 38.060875
候选 watch 重连 (ms): 41.200875, 47.616417, 47.608666, 46.515083, 49.117375, 48.315083, 44.517833, 41.50075, 45.517041, 43.160125, 43.320208, 43.188083, 42.240875, 44.459417, 43.519291, 45.015208, 48.232458, 51.671834, 51.955458, 45.280458
```

## Node 本地授权与 revision 广播

复用 BenchmarkAuthorization 和 BenchmarkRuntimeAuthorization，各跑一轮 30 ms 子基准。前者线性查服务，后者在已发布大计划中使用服务索引；两者均在目标服务内逐条匹配许可。目标服务与成功规则位于末尾。数据仅用于定位路径和核对分配。

### 基线


```text
BenchmarkAuthorization/services=1/rules=1-18         	 5872396	         6.121 ns/op	       0 B/op	       0 allocs/op
BenchmarkAuthorization/services=1/rules=100-18       	 1000000	        36.81 ns/op	       0 B/op	       0 allocs/op
BenchmarkAuthorization/services=1/rules=1000-18      	  110904	       320.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkAuthorization/services=100/rules=1-18       	  183345	       196.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkAuthorization/services=100/rules=100-18     	  159969	       221.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkAuthorization/services=100/rules=1000-18    	   71979	       486.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkAuthorization/services=1000/rules=1-18      	   22500	      1779 ns/op	       0 B/op	       0 allocs/op
BenchmarkAuthorization/services=1000/rules=100-18    	   19737	      1828 ns/op	       0 B/op	       0 allocs/op
BenchmarkAuthorization/services=1000/rules=1000-18   	   20738	      1760 ns/op	       0 B/op	       0 allocs/op
BenchmarkRuntimeAuthorization/services=1/rules=1-18  	 6853063	         5.427 ns/op	       0 B/op	       0 allocs/op
BenchmarkRuntimeAuthorization/services=1/rules=100-18         	 1202406	        30.36 ns/op	       0 B/op	       0 allocs/op
BenchmarkRuntimeAuthorization/services=1/rules=1000-18        	  133394	       273.7 ns/op	       0 B/op	       0 allocs/op
BenchmarkRuntimeAuthorization/services=100/rules=1-18         	 2104939	        14.32 ns/op	       0 B/op	       0 allocs/op
BenchmarkRuntimeAuthorization/services=100/rules=100-18       	  882483	        40.12 ns/op	       0 B/op	       0 allocs/op
BenchmarkRuntimeAuthorization/services=100/rules=1000-18      	  128572	       276.1 ns/op	       0 B/op	       0 allocs/op
BenchmarkRuntimeAuthorization/services=1000/rules=1-18        	 2770508	        12.97 ns/op	       0 B/op	       0 allocs/op
BenchmarkRuntimeAuthorization/services=1000/rules=100-18      	  802227	        38.26 ns/op	       0 B/op	       0 allocs/op
BenchmarkRuntimeAuthorization/services=1000/rules=1000-18     	  132891	       264.8 ns/op	       0 B/op	       0 allocs/op
```


### 候选


```text
BenchmarkAuthorization/services=1/rules=1-18         	 5977501	         5.803 ns/op	       0 B/op	       0 allocs/op
BenchmarkAuthorization/services=1/rules=100-18       	 1233762	        30.99 ns/op	       0 B/op	       0 allocs/op
BenchmarkAuthorization/services=1/rules=1000-18      	  131622	       260.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkAuthorization/services=100/rules=1-18       	  168775	       179.4 ns/op	       0 B/op	       0 allocs/op
BenchmarkAuthorization/services=100/rules=100-18     	  183974	       203.1 ns/op	       0 B/op	       0 allocs/op
BenchmarkAuthorization/services=100/rules=1000-18    	   84218	       432.4 ns/op	       0 B/op	       0 allocs/op
BenchmarkAuthorization/services=1000/rules=1-18      	   23328	      1541 ns/op	       0 B/op	       0 allocs/op
BenchmarkAuthorization/services=1000/rules=100-18    	   23258	      1484 ns/op	       0 B/op	       0 allocs/op
BenchmarkAuthorization/services=1000/rules=1000-18   	   21598	      1918 ns/op	       0 B/op	       0 allocs/op
BenchmarkRuntimeAuthorization/services=1/rules=1-18  	 6810818	         5.360 ns/op	       0 B/op	       0 allocs/op
BenchmarkRuntimeAuthorization/services=1/rules=100-18         	 1000000	        30.56 ns/op	       0 B/op	       0 allocs/op
BenchmarkRuntimeAuthorization/services=1/rules=1000-18        	  139855	       257.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkRuntimeAuthorization/services=100/rules=1-18         	 2870880	        12.98 ns/op	       0 B/op	       0 allocs/op
BenchmarkRuntimeAuthorization/services=100/rules=100-18       	  913802	        38.70 ns/op	       0 B/op	       0 allocs/op
BenchmarkRuntimeAuthorization/services=100/rules=1000-18      	  136377	       272.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkRuntimeAuthorization/services=1000/rules=1-18        	 2652984	        14.30 ns/op	       0 B/op	       0 allocs/op
BenchmarkRuntimeAuthorization/services=1000/rules=100-18      	  949657	        39.81 ns/op	       0 B/op	       0 allocs/op
BenchmarkRuntimeAuthorization/services=1000/rules=1000-18     	  136431	       286.6 ns/op	       0 B/op	       0 allocs/op
```

memory 后端对 1 或 16 个 revision 订阅者广播同一条带标签和数据的事件，每轮等待全部收到。10 轮 benchstat 结果如下。单订阅者中位耗时增加 5.3 ns，16 个订阅者的时间差异未达显著；两组分配次数和字节数相同。


```text
goos: darwin
goarch: arm64
pkg: github.com/fastabc/fastmesh/store/memory
cpu: Apple M5 Pro
                     │ baseline │ candidate │
                     │                  sec/op                   │            sec/op             vs base               │
PhaseBroadcast/1-18                                  175.3n ± 2%                    180.6n ± 3%  +2.99% (p=0.006 n=10)
PhaseBroadcast/16-18                                 2.519µ ± 4%                    2.582µ ± 5%       ~ (p=0.353 n=10)
geomean                                              664.6n                         682.9n       +2.75%

                     │ baseline │ candidate │
                     │                   B/op                    │            B/op             vs base                 │
PhaseBroadcast/1-18                                   472.0 ± 0%                   472.0 ± 0%       ~ (p=1.000 n=10) ¹
PhaseBroadcast/16-18                                7.375Ki ± 0%                 7.375Ki ± 0%       ~ (p=1.000 n=10) ¹
geomean                                             1.844Ki                      1.844Ki       +0.00%
¹ all samples are equal

                     │ baseline │ candidate │
                     │                 allocs/op                 │         allocs/op           vs base                 │
PhaseBroadcast/1-18                                   4.000 ± 0%                   4.000 ± 0%       ~ (p=1.000 n=10) ¹
PhaseBroadcast/16-18                                  64.00 ± 0%                   64.00 ± 0%       ~ (p=1.000 n=10) ¹
geomean                                               16.00                        16.00       +0.00%
¹ all samples are equal
```

## 阶段 4 的 CPU 采样

候选的 BenchmarkCacheCapacity/1000 采样覆盖多个 goroutine。cum 为包含被调函数的时间，Compile 与 CanBind 的百分比不能相加。


```text
File: capacity.test
Type: cpu
Time: 2026-11-18 13:43:38 CST
Duration: 303.66ms, Total samples = 1200ms (395.18%)
Showing nodes accounting for 1000ms, 83.33% of 1200ms total
Showing top 12 nodes out of 138
      flat  flat%   sum%        cum   cum%
     280ms 23.33% 23.33%      280ms 23.33%  runtime.memequal
     180ms 15.00% 38.33%      690ms 57.50%  github.com/fastabc/fastmesh/internal/controller/catalog.Compile
     180ms 15.00% 53.33%      180ms 15.00%  runtime.madvise
      70ms  5.83% 59.17%       70ms  5.83%  runtime.memclrNoHeapPointers
      70ms  5.83% 65.00%       70ms  5.83%  runtime.usleep
      50ms  4.17% 69.17%      160ms 13.33%  github.com/fastabc/fastmesh/internal/controller/catalog.Catalog.CanBind (inline)
      40ms  3.33% 72.50%       40ms  3.33%  runtime.pthread_cond_wait
      30ms  2.50% 75.00%      100ms  8.33%  encoding/json/v2.makeStructArshaler.func2
      30ms  2.50% 77.50%       30ms  2.50%  runtime.pthread_cond_signal
      30ms  2.50% 80.00%       30ms  2.50%  runtime.tryDeferToSpanScan
      20ms  1.67% 81.67%       20ms  1.67%  runtime.asyncPreempt
      20ms  1.67% 83.33%       20ms  1.67%  runtime.memmove
```

## Fuzz 验收

按仓库计划的每目标 60 秒、2 个 worker 运行。
- FuzzDecodePlan 通过，1,730,489 次执行。
- FuzzExecutablePlanFiltersDecodedPlans 通过，1,361,080 次执行。
- FuzzHTTPMatchPrefixIsCleanStable 通过，5,930,430 次执行。
- FuzzPlanUnknownPermissions 通过，14,586 次执行。
- FuzzPrincipal 通过，2,093,549 次执行。
- FuzzVerifyPlanBody 通过，2,634,852 次执行。
