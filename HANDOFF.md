# bsc-binance-radar 交接文档

更新时间：2026-05-03

## 项目目标

监控 **BSC 链** 上 **币安 USDT-M 永续合约已上线代币** 的 DEX 异动。生产 detector 触发 **vol_spike**（5min vol > 5×24h均值）和 **combo**（vol_spike ∧ fee/TVL 年化 ≥ 100%）→ 飞书推送。

新增 4 维 AND 门「启动信号」回测算法（未合入生产，见 `scripts/backtest-launch-detector.ts`）。

## 当前状态

- ✅ **生产 stream 部署中**，跑在 `107.175.35.109:/opt/bsc-binance-radar`（Docker）
- ✅ 飞书 webhook 工作正常
- ✅ 白名单 **303 个 token**（含 LAB / 币安人生 / 4 个中文系列）
- ✅ **PG 已迁移到 TimescaleDB**（image `timescale/timescaledb:latest-pg17`），`swaps` 表 hypertable 9 chunks（每 7d 一个）。**stream INSERT 速度恒定不会随表增长劣化，老 chunk 可一行 `drop_chunks()` 归档**
- 🔄 **90d 历史 backfill 跑中**（2026-02-02 ~ 现在），仅自建节点 + staging 模式 + 拆 worker
  - tmux `bf-self`：跑 block 78852395 → 96092395（全 90d，17240 batches）
  - 模式：fetcher × 8 写 `swaps_staging`（无 index），跑完一次性 migrate 到 swaps hypertable（PG hash semi-join）
  - 设了 `BF_SKIP_REBUILD=1`，**跑完后必须手动跑 `scripts/rebuild-buckets.ts 90d`** 重建 buckets
- ✅ **PancakeSwap V4 CL** 池监控已上线（dex=`pancakeswap-v4-cl`）
- ✅ **PancakeSwap V2 WBNB/USDT** 池监控（仅记录 BNB 历史价 → `bnb_price_history` 表）
- ✅ **swaps 唯一索引** `uq_swaps_dedup(tx_hash, pool_address, amount0, amount1, timestamp)` —— hypertable 要求 unique 包含 partition key (timestamp)
- ✅ **PG 调优**：shared_buffers 2GB + TimescaleDB hypertable + backfill 8 项性能优化（见下文 § 16）
- ✅ 4 维 AND 门启动信号回测器（spec + 报告齐全，未合入生产）
- ⏳ Phase 4：把启动信号合入生产 detector，未做

## 关键链接 / 资源

| 项 | 值 |
|---|---|
| 本地代码 | `~/code/bsc-binance-radar` |
| GitHub repo | https://github.com/wxtsky/bsc-binance-radar （public） |
| 服务器 | `107.175.35.109` (root，密码在工作目录全局 CLAUDE.md) |
| 服务器路径 | `/opt/bsc-binance-radar` |
| PG | `radar-pg` 容器（image: `timescale/timescaledb:latest-pg17`），宿主 `127.0.0.1:5434`，密码在 `.env`；shared_buffers=2GB；`swaps` 表 hypertable |
| BSC RPC | `151.123.172.62:81` (HTTP，backfill 用) / `:82` (WSS，实时流) |
| BSC RPC（备用，公网） | `https://bsc-mainnet.nodereal.io/v1/b13fcff9775e4d1bb28a0735292a1819`（节点限连接强但单调用慢，用 16+ worker 才有优势） |
| 飞书 webhook | `https://open.feishu.cn/open-apis/bot/v2/hook/f5f91e24-7108-4fde-b0d8-724f81c4ca31` (已配在服务器 `.env`) |
| OKX 凭据 | 复用 lp-bot 的，已配在 `.env` |

## 架构

```
fapi seed (mac sync) ─┐
                      ├─→ token-tracker → binance_bsc_tokens (303 个)
capital getall ───────┘                          ↑
                                                 │ enrich 补漏
                            web3 token search ───┤
                            链上 ERC20.symbol() ─┘ (严格校验)

BSC WSS → swap-listener → 白名单过滤 → token_1min_stats / pool_1min_stats / swaps
   ├── uniswap-v3                                    ↓
   ├── pancakeswap-v3                          detector (30s tick) → vol_spike / combo
   ├── uniswap-v4                                    │       │
   ├── pancakeswap-v4-cl  ← 新                      │     anomaly_events + 飞书
   └── PancakeV2 WBNB/USDT （单池） → bnb_price_history (BNB 历史价基准)
                                                     ↑
                                              TVL cache (5min)
```

## 模块清单

```
src/
├── index.ts                           入口；启动 stream 4 dex listener + BNB price listener + detector
├── config/
│   ├── chains.ts                      base tokens (USDT/USDC/USD1/WBNB) + filters
│   ├── contracts.ts                   合约地址（V3 factory / V4 PoolManager / PCS V4 CL / BNB price pool）
│   └── abis.ts                        UNI V3/V4 + PCS V3/V4 CL + V2 Pair Swap event ABI
├── clients/viem-clients.ts            BSC WSS PublicClient
├── core/
│   ├── swap-listener.ts               5 类 Swap 事件订阅 + 入库（实时 / batch buffer 双模）
│   │                                   ├── processV3SwapLog (UNI / PCS V3)
│   │                                   ├── processV4SwapLog (UniV4)
│   │                                   ├── processPcsV4ClSwapLog (新: PancakeV4 CL)
│   │                                   ├── processV2BnbPriceSwap (新: 反算 BNB 价)
│   │                                   └── prefetchPcsV4ClPoolInfo / prefetchV3PoolInfo / prefetchV4PoolInfo (新: viem multicall 批量预热 pool 元数据)
│   ├── price-service.ts               OKX BNB 价 + 链上 ERC20 metadata cache
│   │                                   └── prewarmMetadataCache (新: 启动时从 DB 预加载 303 token 的 decimals)
│   ├── tvl-calculator.ts              池 TVL（V3 only）
│   └── livenessProbe.ts               60s 没 swap 自动 remount
├── db/
│   ├── index.ts                       pg.Pool (max=40) + initSchema + migrateSwapsDedup
│   └── queries.ts                     批量 multi-row INSERT/UPSERT
├── token-tracker/
│   ├── binance-futures.ts             fapi + GitHub seed fallback
│   ├── binance-coin-info.ts           bapi capital getNetworkCoinAll (BSC 充提)
│   ├── binance-web3-search.ts         web3.binance.com token search (用于 enrich)
│   ├── builder.ts                     fapi ∩ capital → 主白名单
│   ├── tracker.ts                     启动 + 6h 定时刷新（失败保留旧数据）
│   └── watchlist.ts                   内存 Set + isWatchedToken
├── anomaly/
│   ├── aggregator.ts                  SQL：active tokens / 5min+24h baseline / cooldown
│   ├── tvl-cache.ts                   token TVL（活跃池 sum），5min 缓存
│   ├── rules.ts                       AnomalyRule (`vol_spike`, `combo`) + env 阈值
│   ├── detector.ts                    30s tick，CONCURRENCY=5
│   └── events.ts                      EventEmitter
├── notifier/
│   ├── feishu.ts                      飞书 webhook + 富文本卡片 + 可选签名
│   └── index.ts                       订阅 anomaly 事件
├── utils/ttlCache.ts                  LRU + TTL
└── types/index.ts                     ChainId='bsc' / SwapRecord / AnomalyTrigger 等
                                       DexType: 'uniswap-v3' | 'uniswap-v4' | 'pancakeswap-v3' | 'pancakeswap-v4-cl'

scripts/
├── sync-perpetuals.ts                 mac 跑：拉 fapi → seed/binance-perpetuals.json
├── enrich-bsc-mapping.ts              web3 search 反查 + 链上 symbol() 校验（--reverify 重校验）
├── backfill.ts                        历史 swap 回补；支持 24/24h/30d/90d 时长语法
│                                       完成后自动从 swaps 重建 buckets（事务+LOCK+ON CONFLICT）
├── backfill-range.ts                  单 worker 补救脚本，处理 backfill 中 deadlock 失败的 batch
├── backtest-launch-detector.ts        4 维 AND 门启动信号回测（vol/swap_count/价格涨幅/买入比）
├── analyze-launch-tops.ts             从 phaseB.json 聚合每 token 的 30d 涨幅 + 启动后 24h 涨幅
└── test-feishu.ts                     发样本卡片测群里展示效果

docs/superpowers/specs/
└── 2026-05-02-lab-launch-backtest-design.md  4 维 AND 门设计文档

db/init.sql                            schema bootstrap（含 uq_swaps_dedup + bnb_price_history 新表）
seed/binance-perpetuals.json           fapi 永续 baseAsset 镜像（geo-block fallback）
Dockerfile                             oven/bun:1.3-alpine 运行时
docker-compose.yml                     radar + postgres；PG 跑 shared_buffers=2GB 等调优参数
```

## DB schema

| 表 | 用途 | 数量级（90d） |
|---|---|---|
| `pools` / `v4_pools` | 池子元信息（cache） | 几千；v4_pools 现存 UniV4 + PCS V4 CL（后者用 `pcsv4cl:` 前缀 namespace） |
| `swaps` | 原始 swap，**TimescaleDB hypertable**（每 7d 一个 chunk） | ~150M（90d 满后） |
| `swaps_staging` | backfill 中转表（无 index 无 PK），fetcher 写它跑完 migrate 到 swaps | 临时（migrate 完 TRUNCATE） |
| `pool_1min_stats` | 池粒度 1min 桶 | ~6M+ |
| `token_1min_stats` | **token 粒度** 1min 桶（异动检测核心） | ~3.5M+ |
| `binance_bsc_tokens` | 白名单 | 303 |
| `anomaly_events` | 触发事件审计 | 几百 |
| `bnb_price_history` | BNB/USD 历史价（来自 PancakeV2 WBNB/USDT 池） | ~10M+（每秒级） |

**唯一索引**：
- `swaps.uq_swaps_dedup (tx_hash, pool_address, amount0, amount1, timestamp)` —— hypertable 要求 unique 含 partition key (timestamp)；同一 swap block ts 固定，加 timestamp 不影响去重效果
- `swaps.swaps_pkey (id, timestamp)` —— 复合 PK，对应用透明（pgserial id 仍唯一）
- `bnb_price_history.uq_bnb_price_dedup (tx_hash, log_index)` —— 同 tx 多笔 swap 也能区分

**Hypertable 维护**（TimescaleDB 特有）：
```sql
-- 看 chunks
SELECT chunk_name, pg_size_pretty(total_bytes) FROM chunks_detailed_size('swaps') ORDER BY chunk_name;

-- 归档老数据（180d 前，省空间）
SELECT drop_chunks('swaps', older_than => bigint '600000000000');  -- ms timestamp

-- 压缩老 chunks（节省 ~90% 空间，但变只读）
ALTER TABLE swaps SET (timescaledb.compress);
SELECT compress_chunk(c) FROM show_chunks('swaps', older_than => bigint '...') c;
```

## env 配置（服务器）

```ini
BSC_WSS_URL=ws://151.123.172.62:82
BSC_HTTP_URL=http://151.123.172.62:81  # backfill 用（默认）
OKX_API_KEY / OKX_SECRET_KEY / OKX_PASSPHRASE
DATABASE_URL=postgresql://radar:<强密码>@localhost:5434/radar
POSTGRES_PASSWORD=<强密码>
PG_POOL_MAX=40                         # pg pool 连接数（默认 10 不够 8 worker × 4 INSERT）
# Backfill 性能调优（默认值都已最优，覆盖时小心）
# BF_STAGING=1                         # backfill 写 swaps_staging 中转表（默认开）
# BF_FLUSH_WORKERS=4                   # 后台 flush worker 数
# BF_QUEUE_MAX=16                      # fetcher → flusher channel 上限（防 OOM）
# BF_CONCURRENCY=8                     # fetch worker 数（自建节点甜点）
# BF_BATCH_SIZE=1000                   # 每 batch block 数
# BF_RPC_URL=                          # 覆盖默认 RPC（用 NodeReal 等公网做 sharding）
# BF_FROM_BLOCK / BF_TO_BLOCK          # 显式 block 范围（覆盖 latest-Nh 算法）
# BF_SKIP_REBUILD=1                    # 跳过 buckets 重建（多 shard 时只让最后一个 shard 重建）
# BF_SHARD_LABEL=self                  # 日志前缀
ANOMALY_VOL_SPIKE_RATIO=5
ANOMALY_FEE_TVL_APR=100
ANOMALY_DETECT_INTERVAL_MS=30000
ANOMALY_COOLDOWN_MS=300000
ANOMALY_BASELINE_MIN_COVERAGE_MS=3600000
NOTIFY_FEISHU_WEBHOOK=https://open.feishu.cn/open-apis/bot/v2/hook/f5f91e24-7108-4fde-b0d8-724f81c4ca31
```

## 维护流程

### 升级代码
```bash
ssh root@107.175.35.109
cd /opt/bsc-binance-radar
git pull
docker compose up -d --build radar
```

### sync 永续 list（mac，每天/感觉数据陈旧时）
```bash
cd ~/code/bsc-binance-radar
bun run sync-perpetuals
git add seed/ && git commit -m "sync binance perpetuals" && git push
# 服务器最迟 6h 内自动 refresh
```

### enrich 白名单（fapi 上有但 capital 没充提的，如 LAB）
```bash
docker compose run --rm --no-deps radar bun scripts/enrich-bsc-mapping.ts
docker compose run --rm --no-deps radar bun scripts/enrich-bsc-mapping.ts --reverify
```

### 回补历史 swap

**单节点（30d 用，约 6.7h）**
```bash
# stream 不停，backfill 边跑边写（uq_swaps_dedup + ON CONFLICT 保证不重复）
# 跑完自动从 swaps 重建 buckets
ssh root@107.175.35.109
cd /opt/bsc-binance-radar

docker compose run --rm --no-deps radar bun scripts/backfill.ts 30d 8

# 长任务用 tmux：
tmux new -d -s bf "docker compose run --rm --no-deps radar bun scripts/backfill.ts 30d 8 > backups/bf.log 2>&1"
```

**当前推荐：单 self shard + staging 模式（90d 预期 ~15h）**
```bash
# 算 block 范围
LATEST=$(curl -s http://151.123.172.62:81 -X POST -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}' | \
  python3 -c 'import json,sys; print(int(json.load(sys.stdin)["result"], 16))')
START=$((LATEST - 17240000))   # latest - 90d 块

# 启 backfill（默认 BF_STAGING=1：fetcher 写 swaps_staging，跑完 migrate 到 swaps）
tmux new -d -s bf-self "docker compose run --rm --no-deps \
  -e BF_FROM_BLOCK=$START -e BF_TO_BLOCK=$LATEST \
  -e BF_SHARD_LABEL=self -e BF_SKIP_REBUILD=1 \
  radar bun scripts/backfill.ts 90d > backups/bf-self.log 2>&1"

# 监控：tail -f /opt/bsc-binance-radar/backups/bf-self.log
# log 行格式：% completed/total (fetched=X q=Y) | logs= ok= err= | elapsed rate ETA | avg ms: fetch= prefetch= process= flush=
#   q = channel queue 当前长度（fetcher 推 / flusher 拿）

# 跑完后必须重建 buckets（BF_SKIP_REBUILD=1 跳过的）
docker compose run --rm --no-deps radar bun scripts/rebuild-buckets.ts 90d
```

**双 RPC sharding（备用，依赖 NodeReal 公网）**
- 自建 + NodeReal 同时跑 = 总吞吐翻倍，节点带宽不互抢
- 启第二个 tmux 加 `-e BF_RPC_URL=https://bsc-mainnet.nodereal.io/v1/<key>`
- ⚠️ NodeReal `BF_CONCURRENCY` 不要超 8，超会撞限
- 多 shard 都设 `BF_SKIP_REBUILD=1`，跑完跑 `rebuild-buckets.ts` 统一重建

### 补救 backfill 中 deadlock 失败的 batch
```bash
# 从 backfill log 抓失败 ranges
grep "failed:" backups/backfill-90d.log | awk -F'[()]' '{print $2}' | tr ' ' '-' | paste -sd,

# 单 worker 顺序跑（避并发死锁）
docker compose run --rm --no-deps radar bun scripts/backfill-range.ts <fromBlock-toBlock,...>
```

### 跑启动信号回测
```bash
# 本地（需 ssh tunnel 或服务器 PG 直连）
DATABASE_URL=... bun scripts/backtest-launch-detector.ts A          # LAB 单点
DATABASE_URL=... bun scripts/backtest-launch-detector.ts B          # 全 303 token
BT_VOL_RATIO=10 BT_MIN_SWAPS=5 BT_PRICE_PCT=1.0 BT_BUY_RATIO=0.8 \
  BT_COOLDOWN_MIN=240 \
  bun scripts/backtest-launch-detector.ts B                          # 自定义阈值 + cooldown
```

### 调阈值
改 `.env` 里 `ANOMALY_*` 后 `docker compose restart radar`。

### 看实时日志
```bash
docker compose logs -f radar
```

## 关键设计决策 / 踩过的坑

### 1. 数据源（geo-block）
- BSC fapi.binance.com 在美国 IP **451**
- 解法：mac 拉 fapi → 写 `seed/binance-perpetuals.json` → git push → 服务器从 GitHub raw 拉 fallback

### 2. 白名单覆盖（fapi ∩ capital 漏洞）
- LAB 在 fapi 永续上有，但币安没开 BSC 充提（capital 里没 LAB）→ 单纯取交集会漏
- 解法：`enrich-bsc-mapping.ts` 用 web3.binance.com token search 反查 + 链上 ERC20.symbol() 严格校验
- 当前 303 个 = 189 (capital) + 114 (web3 enrich)

### 3. timestamp 单位
- 所有 timestamp 在代码里都是**毫秒**
- backfill 模式用 `overrideTimestamp`（block timestamp 线性插值）

### 4. BSC 块时间
- BSC 已升级到 ~0.45s/block（不是 3s）
- backfill 启动时 probe 真实块时间

### 5. backfill 性能优化（30d backfill 70min → 6.7h ←→ 90d 30h）
- swap-listener 加 `buffer` 参数：buffer 给定时不写 DB，累加到内存 BatchBuffer
- 同 (token, bucket) / (pool, bucket) 内存 merge → 单次 multi-row UPSERT
- 多 worker 并发 fetch + flush（推荐 `CONCURRENCY=8`）
- **新**：viem multicall **批量 prefetch pool info**（`prefetchPcsV4ClPoolInfo` 等），降 cache miss 时的串行 readContract
- **新**：`prewarmMetadataCache` 启动时从 DB 预加载 303 token 的 decimals/symbol → process 函数 0 RPC for metadata
- **新**：transport batch `{wait:10, batchSize:2}` 合并 RPC 减半 round trip

### 6. UNIQUE 索引 + ON CONFLICT 防重复
- 之前 stream + backfill 重叠时间段会双写 swaps 表（114 条历史重复）
- 加 `uq_swaps_dedup(tx_hash, pool_address, amount0, amount1)`，INSERT 改 `ON CONFLICT DO NOTHING`
- 现在 stream 不停 backfill 也安全（双写自动去重）
- 注意：stream 的 `upsertPool/Token1minStat` 是**累加 UPSERT**，所以 buckets 还是会双累加。靠 backfill 跑完后 `rebuildBucketsFromSwaps` 重建覆盖（事务 + LOCK + ON CONFLICT DO UPDATE = EXCLUDED）

### 7. backfill 不能 emit 实时事件
process 函数加 `if (overrideTimestamp === undefined)` 守卫，backfill 模式不 emit `swap` event，避免 livenessProbe 误判

### 8. 冷启动 detector 误触发
`baselineMinCoverageMs=1h` —— 数据覆盖不足 1h 不触发 vol_spike

### 9. 飞书 webhook 富文本
`interactive` card type，header template 按 rule 着色（combo 红 / vol_spike 橙）

### 10. PancakeV4 CL 监控
- BSC 上 PancakeV4 PoolManager `0xa0FfB9c1CE1Fe56963B0321B32E7A0302114058b`，CLPositionManager `0x55f4c8abA71A1e923edC303eb4fEfF14608cC226`
- 跟 UniV4 同架构（singleton + bytes32 PoolId），但 PoolKey 多 `parameters` bytes32 字段（顺序：currency0, currency1, hooks, poolManager, fee, parameters）
- swaps 表的 `pool_address` 加 `pcsv4cl:` 前缀 namespace，跟 v4_pools.pool_id 一致；rebuild buckets 时 JOIN 才能匹配上
- Bin pool 暂未加（schema 不同，第 2 阶段）

### 11. BNB 历史价基准（V2 监控）
- 解决 backfill 时用「实时 OKX 价」给「历史 swap」算 volume_usd → 历史价格被高估失真（LAB 30d 涨 10x，4-2 时段 vol 被高估 10x）
- 监控 PancakeV2 WBNB/USDT 池 `0x16b9a82891338f9bA80E2D6970FddA79D1eb0daE`（BSC 上 TVL $35M 最深）
- swap 反算 `price = abs(USDT amount) / abs(WBNB amount)`（USDT/WBNB 都 18 decimals，ratio 不需 decimals 调整；用 BigInt scaled 避免 Number 精度丢失）
- 入 `bnb_price_history` 表，`getBnbPriceAt(ts)` 二分查最近一条
- ⏳ 还没用：把所有 WBNB/X 池的 swaps 历史 volume_usd 用 BNB 历史价重算

### 12. PG 调优（shared_buffers 128MB → 2GB）
- 默认 PG `shared_buffers=128MB` 对 50M+ swap 的 unique 索引 random read 严重击穿 cache
- 症状：PG CPU 310%，wait IO/DataFileRead，backfill 0.04 batch/s
- 解法：docker-compose.yml PG command 设 `shared_buffers=2GB`、`effective_cache_size=5GB`、`work_mem=32MB`、`maintenance_work_mem=512MB`、`max_wal_size=4GB`、`checkpoint_timeout=15min`、`random_page_cost=1.1`、`synchronous_commit=off`（bulk load 加速，crash 丢最近事务可接受）
- 改后 PG CPU 310% → 15%，瓶颈转到 backfill JS 进程 CPU（bun 单线程 event loop）

### 13. 节点限连接
- 自建节点 `151.123.172.62:81` 单 IP 同时处理 ~3-4 个并发请求，超过则排队
- 实测 8 worker × 5 getLogs = 40 concurrent 撞节点限 → 每 batch 慢 4-5x
- 最优：8 worker（单 worker 内 5 getLogs concurrent，节点排队但可控）；少了浪费节点；多了排队
- NodeReal 公网节点（备用）能扛 16+ worker 但单调用慢 4-5x，且 50000 logs/call 上限对 90d 前数据撞限

### 14. backfill JS 进程 CPU 瓶颈
- bun 是 V8 单线程 event loop，8 worker 都在同一线程
- backfill container CPU 100% = 1 核满 = 上限
- 突破要 `worker_threads` 真多线程（未做）

### 15. JSON-RPC transport batch 配置敏感
- 大 batchSize（如 1000）合并多个 getLogs response → 总 size > 节点 30MB 触发 "response too large"
- ~~安全设置：`{wait:10, batchSize:2}`~~ —— **已废弃**，见 § 16.A

### 16. backfill 性能优化轮（2026-05-03，60h → 预期 ~15h，4x）

逐项每项改完都实测了 metrics（fetch / prefetch / process / flush 单 batch ms 视角）。

**A. 关闭 transport batch（最关键的反直觉发现）**
- 旧：`batch:{batchSize:2}` 把 5 个 getLogs RPC 合 1 个 HTTP，节点串行处理 = 单 batch 时长是 sum 不是 max
- 新：完全去掉 `batch` 配置，5 个 getLogs 各自独立 HTTP 请求，节点真并行 = 单 batch 时长 ≈ max
- ⚠️ 旧的 batchSize=2 是为了「response 不超 30MB」做的，但实际单个 getLogs 也就 ~3MB 远低于上限，根本不需要合并

**B. CONCURRENCY 默认 6 → 8**
- HANDOFF § 13 早写了 8 是节点甜点，但默认值还是 6。统一到 8。
- 16 worker 实测撞节点限反而慢 4-5x。

**C. prefetch N+1 PG SELECT → 单 SQL `WHERE address = ANY($1::text[])`**
- 旧：`Promise.all(newAddrs.map(getPoolRecord))` 1000+ pool 每个独立 SELECT，超过 PG_POOL_MAX=40 后排队
- 新：`getPoolRecordsBatch` / `getV4PoolsBatch` 单 SQL 批查 + `bulkUpsertPools` / `bulkUpsertV4Pools` 单 SQL 批写
- 配套：`src/db/queries.ts` 加 4 个 batch 函数；`src/core/swap-listener.ts` 三个 prefetch 函数全部改用 batch
- 效果：prefetch 12.6s → 6-13s（视 cache 状态）

**D. backfill 跳过 buckets 中间 flush**
- 旧 backfill flush 4 张表：swaps + bnb_prices + pool_1min_stats + token_1min_stats
- 但 backfill 跑完后 `rebuildBucketsFromSwaps` 会全量重建 buckets（事务+LOCK+ON CONFLICT），中间累加完全是浪费
- 加 `flushBatchBufferSwapsOnly`，backfill 只写 swaps + bnb_prices
- 效果：flush 49s → 27s + 0 deadlock

**E. swaps INSERT 用 PG `unnest` 替代多行 placeholder**
- 旧：`INSERT INTO swaps VALUES ($1,$2,...,$N)` —— pg-node bind 16-bit 参数上限，CHUNK=1000 → 30k swap 要 30 次 round-trip
- 新：`INSERT INTO swaps SELECT * FROM unnest($1::text[], $2::text[], ...)` —— 固定 10 个 array 参数，单 INSERT 一口气 30k 行
- ⚠️ bigint 列（`timestamp`, `block_number`）必须 string 传，避免 pg-node Number 精度丢失
- 效果：flush 27s → 18-23s

**F. 多 RPC 节点 sharding（self + NodeReal 同时跑）**
- 当前 fetch 22s = 8 worker 撞自建节点 3-4 并发限的排队时间
- 测自建 vs NodeReal 同样 1000-blocks `eth_getLogs`：自建 1.0s, NodeReal 2.0s（NodeReal 单调用慢但限连接松）
- backfill.ts 加 env：`BF_RPC_URL`, `BF_FROM_BLOCK`, `BF_TO_BLOCK`, `BF_SKIP_REBUILD`, `BF_SHARD_LABEL`
- 实测：单 self 0.12 batch/s → 双 shard 0.28 batch/s（self 0.13 + nr 0.15-0.18）
- ⚠️ NodeReal `BF_CONCURRENCY=12` 撞限反而慢；保持 8 最优
- ⚠️ 当前生产**只用自建节点**（Bro 决策，公网 NodeReal 不可控）—— 单 shard

**G. 删 swaps 冗余索引（实测无效，但留作 future-proof）**
- 删 `idx_swaps_chain_dex_time`（1.8GB）+ `idx_swaps_time_chain_valid`（1.4GB），共 3.3GB B-tree
- detector 不查 swaps（grep 验证），rebuild 只用 idx_swaps_time
- 预期 INSERT +30%，**实测 flush 时长几乎不变**（因为瓶颈不在 PG B-tree update，在 pg-node V8 单线程序列化 30k row）
- 索引保持删除状态（init.sql 已改），干净 schema

**H. PG 迁移到 TimescaleDB（hypertable）**
- image 切到 `timescale/timescaledb:latest-pg17`，in-place 升级（同 PGDATA volume）
- `swaps` 表转 hypertable，每 7d 一个 chunk，9 chunks 各 240MB-8GB
- schema 改动（hypertable 要求 unique 含 partition key）：
  - `swaps_pkey: (id) → (id, timestamp)`
  - `uq_swaps_dedup: 4 列 → 5 列加 timestamp`（同一 swap 的 block ts 固定，不影响去重）
  - `ON CONFLICT` 子句加 timestamp（src/db/queries.ts 两处）
- 迁移耗时 31 分钟（DROP unique 1.7s + ALTER PK 66s + create_hypertable migrate_data 23min + 重建 unique 7min）
- ⚠️ **没让 backfill 变快**（flush 27→30s，pg-node 序列化才是真瓶颈不是 PG schema）
- ✅ **长期收益**：stream INSERT 速度恒定，老 chunk 可 `drop_chunks(older_than => '180 days')` 一行归档

**I. backfill 拆 fetch / flush worker + staging 表**（最关键的解耦）
- 之前 fetch 33s + flush 23s 串行，单 worker 56s/batch；fetcher 在等 flush 时节点空闲
- 拆开：fetcher × 8 持续跑节点 → channel queue → flusher × 4 后台写 PG
- channel `BF_QUEUE_MAX=16` backpressure 防 OOM
- 配套 staging 表：fetcher 写 `swaps_staging`（无 index 无 PK 无 unique，INSERT 0 conflict check, 0 B-tree update）
  - 跑完 `migrateStagingToSwaps()`：单 INSERT INTO swaps SELECT FROM staging WHERE NOT EXISTS（PG hash semi-join）
  - DISTINCT ON + ON CONFLICT DO NOTHING 双保险防重复
  - TRUNCATE staging 清场
- env：`BF_STAGING=1`（默认）/ `BF_FLUSH_WORKERS=4` / `BF_QUEUE_MAX=16`
- 预期：单 self shard 47h → ~15h（fetch 物理上限 = 节点 RPC 时间 / 真并发）

**优化路径总结**
| 阶段 | ETA | 说明 |
|---|---|---|
| Baseline (CONCURRENCY=16, batchSize=2) | 60h | 节点严重排队 |
| + A (batch=off) | 47.8h | RPC 真并发 |
| + B (CONCURRENCY=8) | 47.8h | 节点不挤 |
| + C (prefetch batch SQL) | 47.8h | PG 不再瓶颈（PG CPU 178% → 4%）|
| + D (skip buckets flush) | 43.5h | flush 49s → 27s, deadlock 消失 |
| + E (unnest INSERT) | 39.8h | flush 27s → 21s |
| + F (双 RPC sharding 50/50) | 30h | 节点带宽倍增 |
| + F2 (sharding rebalance 42/58) | 17h | self/nr ETA 平衡 |
| + G (删冗余索引) | 17h | 实测无效（pg-node 才是瓶颈）|
| + H (TimescaleDB hypertable) | 18h | 短期略慢，长期 stream 受益 |
| **+ I (拆 worker + staging 表，单 self)** | **~15h（运行中）** | fetch/flush 解耦 + 跨过 ON CONFLICT |

剩下还可能压榨的（未做）：
- worker_threads 真多核 —— 工程量大
- 把 stream + backfill 都用 staging（让 stream 也走 staging）—— 适合超大量历史导入场景
- TimescaleDB compression 老 chunks（未启用）

## 已知未做 / 候选改进

1. **启动信号合入生产 detector**：`backtest-launch-detector.ts` 4 维 AND 门已验证 LAB 02:37 命中（提前生产 detector 19h），但还没合入。需要：
   - cooldown 调到合理水平（4h cooldown 后告警从 1902 → ~50/30d 范围）
   - 阈值精调（vol/swap/price/buy）
   - 合入 `src/anomaly/detector.ts` 作为新 rule `early_launch`
2. **historical volume_usd 重算**：用 `bnb_price_history` 给所有 WBNB/X 池的历史 swap 重算 `volume_usd_corrected`（修 backfill 时 BNB 价失真）
3. **PancakeV4 Bin pool 监控**：BSC 上 Bin 池目前少，第 2 阶段做
4. **launchd / cron 自动 sync-perpetuals**：当前要 mac 手动跑，可配定时
5. **链上 PoolCreated 扫描**：提前发现新池子，做 new_pool 监控
6. **web 仪表盘**：当前所有数据查 PG，可做个 web 看异动历史 / token 趋势
7. **DB 数据增长控制**：现在 swap 永久保留，~5GB/天 with 90d，长期需要归档/分区
8. **backfill 用 worker_threads 真并行**：突破 bun 单核 CPU 上限

## 相关 memory / Obsidian 笔记

vault：
- `Binance-API.md`（endpoint 速查 + geo-block 解法）
- `bsc-binance-radar.md`（项目档案）
- `RackNerd Buffalo 服务器.md`

memory（`~/.claude/projects/-Users-wxt-code-lp-bot/memory/`）：
- `reference_binance_us_geoblock.md`
- `project_racknerd_server.md`
- `project_bsc_binance_radar.md`
- `feedback_deploy_via_git.md`

## 与 lp-bot 的关系

完全独立 —— 自带 PG 容器、自管 BSC WSS 连接、自管 GitHub repo、自管服务器路径。**不影响 lp-bot 任何功能**。
