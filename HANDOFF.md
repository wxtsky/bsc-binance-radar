# bsc-binance-radar 交接文档

更新时间：2026-05-02

## 项目目标

监控 **BSC 链** 上 **币安 USDT-M 永续合约已上线代币** 的 DEX 异动，**vol_spike**（5min vol > 5×24h均值）和 **combo**（vol_spike ∧ fee/TVL 年化 ≥ 100%）触发飞书推送。

## 当前状态

- ✅ **生产部署中**，跑在 `107.175.35.109:/opt/bsc-binance-radar`（Docker）
- ✅ 飞书 webhook 工作正常，已推送过若干 vol_spike / combo 真异动
- ✅ 24h 历史数据已回补（246 token / 109k 桶 / 1.56M swap）
- ✅ 白名单 **303 个 token**（含 LAB / 币安人生 / 4 个中文系列）
- ⏳ Phase 4 后续：要不要做"启动前"更早信号（趋势/加速度），未做

## 关键链接 / 资源

| 项 | 值 |
|---|---|
| 本地代码 | `~/code/bsc-binance-radar` |
| GitHub repo | https://github.com/wxtsky/bsc-binance-radar （public） |
| 服务器 | `107.175.35.109` (root，密码在工作目录全局 CLAUDE.md) |
| 服务器路径 | `/opt/bsc-binance-radar` |
| PG | `radar-pg` 容器，宿主 `127.0.0.1:5434`，密码在 `.env` |
| BSC RPC | `151.123.172.62:81` (HTTP，backfill 用) / `:82` (WSS，实时流) |
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
                                              ↓
detector (30s tick) → token-level vol_spike / combo → anomaly_events + 飞书
                ↑
TVL cache (5min) ┘
```

## 模块清单

```
src/
├── index.ts                           入口
├── config/{chains,contracts,abis}.ts  BSC + V3/V4/Pancake 合约 + ABI
├── clients/viem-clients.ts            BSC WSS PublicClient
├── core/
│   ├── swap-listener.ts               Swap 事件订阅 + 入库（实时 / batch buffer 双模）
│   ├── price-service.ts               OKX BNB 价 + 链上 ERC20 metadata cache
│   ├── tvl-calculator.ts              池 TVL（V3 only）
│   └── livenessProbe.ts               60s 没 swap 自动 remount
├── db/
│   ├── index.ts                       pg.Pool + initSchema
│   └── queries.ts                     单条 + 批量 multi-row UPSERT
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

scripts/
├── sync-perpetuals.ts                 mac 跑：拉 fapi → seed/binance-perpetuals.json
├── enrich-bsc-mapping.ts              web3 search 反查 + 链上 symbol() 校验
│                                       --reverify 重新校验已存在项目
├── backfill.ts                        历史 swap 回补（批量 + 并发 6）
└── test-feishu.ts                     发样本卡片测群里展示效果

db/init.sql                            schema bootstrap（PG 容器初次启动跑）
seed/binance-perpetuals.json           fapi 永续 baseAsset 镜像（geo-block fallback）
Dockerfile                             oven/bun:1.3-alpine 运行时
docker-compose.yml                     radar + postgres 两个 service
```

## DB schema

| 表 | 用途 | 数量级 |
|---|---|---|
| `pools` / `v4_pools` | 池子元信息（cache） | 几千 |
| `swaps` | 原始 swap（**永久保留**，复盘用） | 1.56M（24h 后） |
| `pool_1min_stats` | 池粒度 1min 桶 | 109k |
| `token_1min_stats` | **token 粒度** 1min 桶（异动检测核心） | 109k |
| `binance_bsc_tokens` | 白名单 | 303 |
| `anomaly_events` | 触发事件审计 | 几十 |

## env 配置（服务器）

```ini
BSC_WSS_URL=ws://151.123.172.62:82
BSC_HTTP_URL=http://151.123.172.62:81  # backfill 用
OKX_API_KEY / OKX_SECRET_KEY / OKX_PASSPHRASE
DATABASE_URL=postgresql://radar:<强密码>@localhost:5434/radar
POSTGRES_PASSWORD=<强密码>
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
# 服务器
docker compose run --rm --no-deps radar bun scripts/enrich-bsc-mapping.ts
# 重新校验已存在项的链上 symbol：
docker compose run --rm --no-deps radar bun scripts/enrich-bsc-mapping.ts --reverify
```

### 回补历史 swap（让 detector 立刻有 baseline）
```bash
docker compose stop radar
docker compose run --rm --no-deps radar bun scripts/backfill.ts 24 6  # 24h, concurrency=6
docker compose start radar
# 24h backfill 现在 ~10min（已批量化 + 并发优化）
```

### 调阈值
改 `.env` 里 `ANOMALY_*` 后 `docker compose restart radar`。

### 看实时日志
```bash
docker compose logs -f radar
```

## 关键设计决策 / 踩过的坑

### 1. 数据源（geo-block）
- BSC fapi.binance.com 在美国 IP **451**（详见 [Binance API geo-block memory](~/.claude/projects/-Users-wxt-code-lp-bot/memory/reference_binance_us_geoblock.md)）
- 解法：mac 拉 fapi → 写 `seed/binance-perpetuals.json` → git push → 服务器从 GitHub raw 拉 fallback
- 仍 100% binance 官方数据，仅借 git 中转

### 2. 白名单覆盖（fapi ∩ capital 漏洞）
- LAB 在 fapi 永续上有，但币安没开 BSC 充提（capital 里没 LAB）→ 单纯取交集会漏
- 解法：`enrich-bsc-mapping.ts` 用 web3.binance.com token search 反查 + 链上 ERC20.symbol() 严格校验
- 当前 303 个 = 189 (capital) + 114 (web3 enrich，链上 symbol verify)

### 3. timestamp 单位
- 所有 timestamp 在代码里都是**毫秒**
- `processV3/V4SwapLog` 实时模式用 `Date.now()`，backfill 模式用 `overrideTimestamp`（block timestamp 线性插值）

### 4. BSC 块时间
- BSC 已升级到 ~0.45s/block（不是 3s）
- backfill 启动时 probe 真实块时间，不能硬编码

### 5. backfill 性能优化
原版 → 批量化版的优化（24h backfill 70min → 10min）：
- swap-listener processV3/V4SwapLog 加可选 `buffer` 参数：buffer 给定时不写 DB，累加到内存 BatchBuffer
- 同 (token, bucket) / (pool, bucket) 内存 merge → 单次 multi-row UPSERT
- 多 worker 并发 fetch + flush（`CONCURRENCY=6`）

### 6. backfill 幂等
跑前 `DELETE` 同时间范围内的 swaps / *_1min_stats，避免双倍累加。

### 7. backfill 不能 emit 实时事件
process 函数加 `if (overrideTimestamp === undefined)` 守卫，backfill 模式不 emit `swap` event，避免 livenessProbe 误判 + console.log 刷屏。

### 8. 冷启动 detector 误触发
- 第一次部署没 baseline，detector 会被 backfill 数据触发狂刷飞书
- 解法：`baselineMinCoverageMs=1h` —— 数据覆盖不足 1h 不触发 vol_spike
- 但实际通过 backfill 24h 让冷启动也有完整 baseline，detector 一启动就工作

### 9. 飞书 webhook 富文本
- 用 `interactive` card type，header template 按 rule 着色（combo 红 / vol_spike 橙）
- 含 BscScan / DexScreener 跳转链接

## 已知未做 / 候选改进

1. **"启动前"更早信号**（Bro 提过）：当前 vol_spike 抓的是"启动初期 0-5min"，真正"启动前"难抓。可加：
   - 1min 滚动窗口（vs 1h 均值，更敏感）
   - 趋势检测（连续 N min 单调递增）
   - 加速度（vol 增速变化）
2. **launchd / cron 自动 sync-perpetuals**：当前要 mac 手动跑，可配定时
3. **链上 PoolCreated 扫描**：提前发现新池子，做 new_pool 监控（之前删了，可加回）
4. **web 仪表盘**：当前所有数据查 PG，可做个 web 看异动历史 / token 趋势
5. **DB 数据增长控制**：现在 swap 永久保留，~100MB/天，长期需要归档/分区

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
