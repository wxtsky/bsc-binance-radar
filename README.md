# bsc-binance-radar

监控 **BSC 链** 上 **币安合约（USDT-M Perpetual）已上线代币** 的异动情况——基于 DEX swap 流的成交量（vol）+ 手续费 / TVL 比率（fee/tvl）触发推送。

## 工作原理

1. **token-tracker**：定期从 Binance Futures `/fapi/v1/exchangeInfo` 拉取所有合约的 `baseAsset`，再用 `/sapi/v1/capital/config/getall` 查每个币在 BSC 网络的官方合约地址，取交集得到白名单
2. **swap-listener**：通过 WSS 订阅 BSC 上 Uniswap V3 / V4 + PancakeSwap V3 的 Swap 事件，过滤出涉及白名单 token 的 swap，按 1 分钟桶聚合到 token 维度
3. **anomaly engine**（Phase 3）：周期性扫描 token 桶，命中规则就触发推送
   - vol spike: 5min volume > N × 24h 均值
   - fee/tvl APR: 当前 fee/tvl 折算年化超过阈值
   - 强信号: 上述两条同时满足
4. **notifier**（Phase 4）：Telegram / webhook 推送

## 进度（Phases）

- [x] **Phase 1** — 项目脚手架 + 内核搬迁 + DB 自管 + 入口（当前可启动空跑）
- [ ] **Phase 2** — token-tracker 拉取 Binance 白名单
- [ ] **Phase 3** — anomaly engine（vol + fee/tvl 规则）
- [ ] **Phase 4** — notifier（Telegram + webhook）

## 安装

```bash
bun install
```

> 注意：lp-bot 主项目使用 bun，本项目沿用。

## 配置

```bash
cp .env.example .env
# 编辑 .env，填入 BSC_WSS_URL、OKX_*、BINANCE_API_KEY/SECRET 等
```

最小启动需要：
- `BSC_WSS_URL`：BSC 节点 WSS（必须）
- `OKX_API_KEY` / `OKX_SECRET_KEY` / `OKX_PASSPHRASE`：用于拉 BNB 原生价格（强烈推荐，否则非稳定币池子无法定价）

## 启动

```bash
bun run dev      # watch 模式
bun run start    # 一次性启动
```

## Phase 1 Smoke Test

Phase 1 阶段白名单为空，所有 swap 都会被过滤。要验证内核工作：

```bash
WATCHLIST_BYPASS=true bun run dev
```

会看到所有 BSC swap 被记录，对照 lp-bot 主项目的 poolpulse 输出可以验证一致性。

## 目录

```
src/
├── index.ts                # 入口
├── config/                 # chains / contracts / abis
├── clients/viem-clients.ts # WSS client
├── core/                   # swap-listener / price-service / tvl-calculator / livenessProbe
├── db/                     # SQLite + queries
├── token-tracker/          # Binance 白名单（Phase 2）
├── anomaly/                # 异动检测（Phase 3）
├── notifier/               # 通知（Phase 4）
├── utils/                  # ttlCache
└── types/
```

## 数据存储

SQLite 文件默认 `./data/radar.db`（可通过 `DATABASE_PATH` 修改）。表：

- `pools` / `v4_pools`：池基础信息
- `swaps`：原始 swap 记录（保留 30min）
- `pool_1min_stats`：池粒度 1min 桶
- `token_1min_stats`：**token 粒度** 1min 桶（异动检测核心数据源）
- `binance_bsc_tokens`：白名单
- `anomaly_events`：触发事件审计

## 与 lp-bot 关系

完全独立，自带 SQLite + WSS 连接，不依赖也不修改 lp-bot 主项目。
