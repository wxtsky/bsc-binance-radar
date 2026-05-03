# bsc-binance-radar 交接文档

更新时间：2026-05-04（凌晨 — 切策略中：只监控项目方主池子）

## TL;DR — 新会话必读

### 当前状态（2026-05-04 凌晨）
- ✅ **整个项目已 rewrite 到 Rust** + schema 优化（BYTEA/NUMERIC→BYTEA(32)/SMALLINT，row 700→200 bytes）
- ✅ **远程 image 已 build**（rust:1-slim-bookworm + libssl-dev），PG 是 timescale + 新 BYTEA schema
- ✅ **远程 binance_bsc_tokens 表有 303 token**（已 dump TEXT→BYTEA 转换 imported）
- 🛑 **远程 backfill 在 10% 时被 stop**（Bro 主动停，转策略），swaps 表空，staging 25.6M 行（待 truncate）
- 🛑 **新策略待实施**：只监控**每个白名单 token 的主池子**（vs 之前所有 V3+V4+PCS 池）

### 新策略说明（关键转向）
> 「**只监控项目方主池子**：项目方部署时配的那个最深流动性的池子。OKX API 能拿池子 TVL 排序，**最大 TVL 池子 = 项目方池子**」

数据量预期：
- 监控池数：从 ~30,000 池 → **303 池**（每 token 一个主池）
- backfill 时长：3h → **5-10 min**
- 90d 数据量：30GB → **~1GB**
- 144GB VPS 完全够用，不需升级硬件

detector 行为变化：
- 旧：`token_vol = sum(所有 pool 的 swap)`
- 新：`token_vol = 主池的 swap`（更聚焦项目方流动性，跟币安永续 USDT 对齐）

### 第一步要做什么（新会话）
**研究 OKX DEX API 拿 BSC pool TVL ranking endpoint**：
- Bro 提示 OKX API 能拿到，但具体 endpoint 我没找到（搜到的都是 token-level，没有 pool-level ranking）
- 候选 endpoint（待 verify）：
  - `https://web3.okx.com/api/v6/dex/market/...`
  - `https://web3.okx.com/api/v6/dex/aggregator/...`
- OKX 凭据已在 `.env`：`OKX_API_KEY` / `OKX_SECRET_KEY` / `OKX_PASSPHRASE`
- 也可能是 OKX 内部 API（reverse engineer web3.okx.com token 详情页）：
  打开 https://web3.okx.com/token/bsc/<addr> 看 network request

如果 OKX 真没 pool ranking API，备选方案：
1. **UniV4 indexer**：`http://151.123.172.62:8080/v1/graphql` + `x-hasura-admin-secret: testing` —— **只覆盖 UniV4，没 PCS V4 CL**（TVL 字段 `Pool.totalValueLockedUSD`）
2. **TheGraph PCS V3 subgraph**（外部依赖）
3. **balanceOf(pool_address)** 链上读 V3/PCS V3 池的 token0/token1 余额 × 价格 → TVL（自己算）
4. **24h vol 排序**（先短 backfill 一遍数据驱动选 top1）

## 项目目标（不变）

监控 **BSC 链** 上 **币安 USDT-M 永续合约已上线代币** 的 DEX 异动。生产 detector 触发 **vol_spike**（5min vol > 5×24h 均值）和 **combo**（vol_spike ∧ fee/TVL 年化 ≥ 100%）→ 飞书推送。

## 池子收录规则

**当前已实施**（src/chain.rs::pool_includes_pair）：
- target = 303 个币安永续白名单 token（DB 中 `binance_bsc_tokens`）
- base = `{WBNB, USDT, USDC}`
- 收录：恰好 (target, base) 配对，token0/token1 顺序无关

**新策略将进一步限制**（待实施）：
- 每个 target token 只保留 1 个主池
- 主池 = 该 token 所有候选池中 **TVL 最大** 的（OKX API 排序 / GraphQL indexer / 链上 balanceOf）

## Rust 项目结构（不变）

```
bsc-binance-radar/
├── Cargo.toml              # alloy 0.8 + tokio + tokio-postgres + deadpool
├── src/
│   ├── lib.rs              # 模块声明
│   ├── types.rs            # PoolInfo / V4PoolInfo / SwapRecord（用 alloy::Address / B256 / I256）
│   ├── chain.rs            # base tokens (WBNB/USDT/USDC) + pool_includes_pair
│   ├── contracts.rs        # Factory 地址 + V3/V4 deploy block
│   ├── abis.rs             # sol! event 定义（mod 隔离让 SIGNATURE_HASH 正确）
│   ├── clients/bsc.rs      # alloy http/ws client
│   ├── db/
│   │   ├── pool.rs         # deadpool-postgres pool（max=40）
│   │   └── queries.rs      # binary COPY 写 staging + bulk_upsert_pools/v4_pools
│   ├── token_tracker/
│   │   ├── seed.rs         # fapi → GitHub seed → 本地 file fallback
│   │   └── watchlist.rs    # 内存 HashMap<Address>
│   ├── discovery.rs        # 并发扫 PoolCreated/Initialize 全历史
│   ├── swap_processor.rs   # V3/V4/PCS V4 CL/V2 BNB 解码 + USD 计算
│   ├── stream_listener.rs  # WSS 实时订阅 4 dex Swap + V2 BNB
│   ├── anomaly/
│   │   ├── rules.rs        # AnomalyConfig from env
│   │   ├── aggregator.rs   # token baseline SQL + cooldown check
│   │   └── detector.rs     # 30s tick loop
│   ├── notifier/feishu.rs  # 飞书 webhook
│   └── bin/
│       ├── backfill.rs     # 历史回补
│       ├── radar.rs        # 实时 stream + detector + feishu
│       ├── verify_factories.rs
│       └── enrich_bsc_mapping.rs  # stub (phase 3)
├── scripts/
│   └── mid_migrate.sh      # （已废弃，新 schema 不需要 mid-migrate）
├── db/init.sql             # BYTEA + SMALLINT schema（含 hypertable）
├── seed/binance-perpetuals.json
├── docker-compose.yml
├── Dockerfile              # multi-stage rust:1-slim → debian:bookworm-slim
├── archive-ts/             # 旧 TS 代码归档
├── HANDOFF.md
└── README.md
```

## DB schema（已实施 BYTEA 优化）

| 表 | 关键 column 类型 |
|---|---|
| `pools` | address BYTEA(20), token0/token1 BYTEA(20), dex SMALLINT |
| `v4_pools` | pool_id BYTEA(32), currency0/currency1 BYTEA(20), dex SMALLINT |
| `swaps` (hypertable, 7d/chunk) | pool_address BYTEA(变长 20/32), tx_hash BYTEA(32), amount0/amount1 BYTEA(32) i256-be, dex SMALLINT |
| `swaps_staging` | 同 swaps，但 UNLOGGED 无 index |
| `binance_bsc_tokens` | contract_address BYTEA(20) PK |
| `bnb_price_history` | tx_hash BYTEA(32) |
| `pool_1min_stats` / `token_1min_stats` | address BYTEA, chain SMALLINT |
| `anomaly_events` | rule SMALLINT, metrics JSONB |

dex SMALLINT 枚举：
- 1 = uniswap-v3
- 2 = pancakeswap-v3
- 3 = uniswap-v4
- 4 = pancakeswap-v4-cl
- 5 = pancakeswap-v2 (BNB price pool only)

chain SMALLINT：1 = bsc

## 上次 backfill 实测性能（旧策略，全 pool 监控）

| 指标 | 旧 TEXT schema | 新 BYTEA schema | 提升 |
|---|---|---|---|
| ETA (90d) | 6h | **3h** | 2x |
| flush per batch | 200ms | **104ms** | 2x |
| fetch per batch | 9.5s | **4.9s** | 2x |
| process per batch | 60ms | **16ms** | 4x |
| Rate | 0.85 batch/s | **1.6 batch/s** | 2x |
| Row size | ~700 bytes | **~187 bytes** | 3.7x |
| 90d 数据 | 100GB | ~30-40GB | 2.5x |
| CPU 利用 | 122% 满核 | **3%** 完全闲 | - |

binary COPY + alloy 真多核 + 高效解码三剑客效果验证完毕。

**新策略（主池监控）下，预测 ETA**：5-10 分钟（30k pool → 303 pool，工作量 100x 下降）。

## 新策略实施 plan（新会话接手）

### Step 1: 研究 OKX API
- 找 OKX BSC pool TVL ranking endpoint（如不存在用 indexer / balanceOf 备选）
- 测试 1-2 个 token（LAB / USDT / WBNB）拿到 pool 列表 + TVL
- 写一个 `scripts/probe-okx-pool-api.ts` 或 Rust 测试脚本

### Step 2: 实现主池选择逻辑
- 新建 `src/main_pool.rs`：input = 303 token list，output = 303 主池（pool address + dex + token0/token1/fee_tier）
- 选择算法（候选）：
  - 调 OKX API 拿每 token 的 pool 排序
  - top 1 pool（TVL 最大）作主池
  - 写到新表 `main_pools` 或 config
- 缓存 24h（main pool 变化不频繁）

### Step 3: 改 discovery 只填主池
- discovery 完整扫 PoolCreated 拿全集
- 但只把主池子 upsert 到 `pools` / `v4_pools`
- 或者：保留全集 + 加 `is_main_pool` 列标记

### Step 4: 改 backfill 只 fetch 主池
- backfill 启动时 SELECT main pools
- V3/PCS V3 swap getLogs 用 address filter（303 个池，body 极小）
- V4/PCS V4 CL swap getLogs 用 args.id（节点 1000 topic 上限内）
- 跑 backfill：5-10 分钟拿 90d 主池 swap

### Step 5: detector 调整
- token_1min_stats 已经按 token 聚合 swap，主池逻辑下：token vol = 主池 vol（自动）
- 不需改 detector 代码
- TVL 字段：直接用 OKX API 实时 TVL（5min cache）

### Step 6: radar bin 调整
- stream listener 用主池 address filter（V3/PCS V3）+ args.id（V4/PCS V4 CL）
- 高效：只接 303 池子的 swap，几乎零开销

### Step 7: 远程部署
- 当前远程 backfill 已 stop，可以直接：
  - `TRUNCATE swaps_staging`
  - 跑新版 backfill（主池 only）
  - 启动 radar service

## 远程当前状态（2026-05-04 02:50 北京时间）

| | 状态 |
|---|---|
| `bf-90d` tmux | killed |
| `mid-mig` tmux | killed（新 schema 不需要）|
| swaps 表 | 0 rows |
| swaps_staging 表 | 25.6M rows / ~5GB（弃用，待 TRUNCATE）|
| pools | 0 rows（fresh init 后没扫 discovery）|
| v4_pools | 0 rows（同上）|
| binance_bsc_tokens | **303 rows** ✅（已转 BYTEA imported）|
| 磁盘 | 30GB used / 107GB free |
| Docker image | bsc-binance-radar-radar:latest（最新 commit 950d19c）|

## 关键资源 / 凭据

| 项 | 值 / endpoint |
|---|---|
| GitHub | https://github.com/wxtsky/bsc-binance-radar |
| 服务器 | `107.175.35.109` (root，密码在工作目录全局 CLAUDE.md) |
| 服务器路径 | `/opt/bsc-binance-radar` |
| PG | `radar-pg` 容器，timescale/timescaledb:latest-pg17，宿主 `127.0.0.1:5434` |
| BSC 自建节点 | `http://151.123.172.62:81` (HTTP) / `ws://151.123.172.62:82` (WSS) |
| BSC NodeReal | `https://bsc-mainnet.nodereal.io/v1/b13fcff9775e4d1bb28a0735292a1819` |
| **UniV4 indexer GraphQL** | `http://151.123.172.62:8080/v1/graphql` + header `x-hasura-admin-secret: testing` |
| **OKX API** | `https://web3.okx.com/api/v6/dex/market/...`（具体 endpoint 待研究）|
| 飞书 webhook | 在 `.env` `NOTIFY_FEISHU_WEBHOOK` |

## UniV4 indexer GraphQL schema（已 verified）

endpoint: `http://151.123.172.62:8080/v1/graphql`，header `x-hasura-admin-secret: testing`

Pool 实体 fields（subgraph 标准）：
```
id (String, format: "56_0x<32 bytes hex>")
chainId (numeric, BSC=56)
totalValueLockedUSD ← TVL ✅
totalValueLockedToken0 / Token1
totalValueLockedETH
volumeUSD / volumeToken0 / volumeToken1
feesUSD / feesUSDUntracked
liquidity (uint128)
sqrtPrice / token0Price / token1Price
tick / tickSpacing
token0 / token1 (String, format: "56_0x<address>")
feeTier / hooks / name
createdAtBlockNumber / createdAtTimestamp
txCount / liquidityProviderCount
```

**注意**：
1. **只 indexed UniswapV4 BSC**（PoolManager `0x28e2ea...`），**没 PCS V4 CL**（`0xa0FfB9...`）
2. **没 V3 / PCS V3** 数据
3. PCS V4 CL / V3 / PCS V3 池的 TVL 仍要别处拿

## OKX API 探索 plan（新会话第一步）

```bash
# 1. 测试 OKX token search
curl 'https://web3.okx.com/api/v6/dex/market/token/search?keywords=LAB&chainId=56' \
  -H 'OK-ACCESS-KEY: ...' ...

# 2. 测试 token detail（看是否含 pool list）
curl 'https://web3.okx.com/api/v6/dex/market/price-info?...' ...

# 3. reverse engineer web3.okx.com 网页
curl https://web3.okx.com/token/bsc/0xf307910a4c7bbc79691fd374889b36d8531b08e3 \
  | grep -oE 'api/v[0-9]+/[^"]*pool[^"]*'

# 4. 看 OKX 是否有 dex/v3/dex/aggregator/get-tokens 类似 endpoint 含 pool 信息
```

OKX docs: https://web3.okx.com/build/dev-docs/wallet-api/

## env vars（不变）

```ini
DATABASE_URL=postgresql://radar:<密码>@localhost:5434/radar    # 本地
DATABASE_URL=postgresql://radar:<密码>@postgres:5432/radar     # 容器内
BSC_HTTP_URL=http://151.123.172.62:81
BSC_WSS_URL=ws://151.123.172.62:82
OKX_API_KEY=...
OKX_SECRET_KEY=...
OKX_PASSPHRASE=...
NOTIFY_FEISHU_WEBHOOK=https://open.feishu.cn/open-apis/bot/v2/hook/<id>

# Backfill 调优（不需改）
BF_CONCURRENCY=8
BF_BATCH_SIZE=1000
BF_FLUSH_WORKERS=4
BF_QUEUE_MAX=16
BF_FROM_BLOCK / BF_TO_BLOCK
BF_SHARD_LABEL=main
BF_SKIP_REBUILD=0
BF_SKIP_MIGRATE=0
BF_SKIP_DISCOVERY=0
BF_RPC_URL
BF_DISCOVERY_RPC

# Anomaly 阈值
ANOMALY_VOL_SPIKE_RATIO=5
ANOMALY_FEE_TVL_APR=100
ANOMALY_DETECT_INTERVAL_MS=30000
ANOMALY_COOLDOWN_MS=300000
ANOMALY_BASELINE_MIN_COVERAGE_MS=3600000
```

## Bro 偏好（节选）

- 中文回答，开头叫 "Bro"
- 不私自 git commit / push（让我提交时再做）
- 重要操作（删文件、改全局配置）先告诉
- 长任务先列方案 / 拆步骤
- 不确定就直说，不硬编

## Git history（最近）

```
950d19c fix(schema): amount0/1 NUMERIC → BYTEA(32)，binary COPY 一气呵成
e2a3186 fix(db): unnest INSERT 的 BYTEA[] 用 Vec<Vec<u8>>
3862b38 perf(ops): mid_migrate 跑完 VACUUM staging 释放磁盘空间
92d3bfb perf(schema): TEXT → BYTEA + NUMERIC 优化
87771a9 fix(ops): mid_migrate 分批 100k rows 避免大事务卡死
7850568 perf(ops): mid_migrate cutoff 改 MAX-60s + LIMIT 500K
... （详见 git log）
```

## 新会话第一句应该回应

> **Bro 早！我看 HANDOFF，理解新策略：只监控项目方主池子（每 token TVL 最大那个）**。
> 第一步是研究 OKX API 拿 BSC pool TVL ranking endpoint。我先去 reverse engineer 一下
> web3.okx.com 看具体 API，再设计实施。

继续从那里开始。
