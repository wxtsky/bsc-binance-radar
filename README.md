# bsc-binance-radar

监控 **BSC 链** 上 **币安合约（USDT-M Perpetual）已上线代币** 的 DEX 异动——基于成交量爆发（vol_spike）和成交量+手续费收益率组合（combo 强信号）触发推送到飞书。

## 工作原理

1. **token-tracker**：拉 `fapi.binance.com/fapi/v1/exchangeInfo` 取 USDT-M 永续 baseAsset list，再拉 `www.binance.com/bapi/capital/.../getNetworkCoinAll` 取每个币的 BSC 合约地址，取交集得到白名单（约 189 个 token）
2. **swap-listener**：BSC WSS 订阅 Uniswap V3/V4 + PancakeSwap V3 的 Swap 事件，仅入库白名单内代币的 swap，按 1min 桶聚合到 token 维度
3. **anomaly engine**：每 30s 扫一次 5min 内活跃 token，两条规则：
   - 📈 **vol_spike**：5min volume > N × 24h 均值（默认 N=5）
   - 🔥 **combo**：vol_spike ∧ fee/TVL 年化 ≥ X%（默认 100%/year）
4. **notifier**：飞书自定义机器人 webhook，富文本 interactive card，含 BscScan / DexScreener 跳转

## 进度

- [x] Phase 1 — swap-listener / price-service / DB / livenessProbe
- [x] Phase 2 — token-tracker
- [x] Phase 3 — anomaly engine（含冷启动 grace 1h）
- [x] DB SQLite → PostgreSQL（Docker 容器）
- [x] Phase 4 — notifier（飞书 webhook）
- [x] 容器化 + 部署到 RackNerd 服务器

## 部署到服务器

服务器只需 Docker，一键起：

```bash
git clone https://github.com/wxtsky/bsc-binance-radar.git /opt/bsc-binance-radar
cd /opt/bsc-binance-radar
cp .env.example .env  # 编辑填入 BSC_WSS_URL / OKX / 飞书 webhook 等
docker compose up -d --build
```

升级：`git pull && docker compose up -d --build radar`

## 本地开发

```bash
bun install
docker compose up -d postgres   # 起 PG（仅 PG 容器）
cp .env.example .env             # 填配置
bun run dev                      # watch 模式跑 radar
```

## 配置（最小必填）

| env | 说明 |
|---|---|
| `BSC_WSS_URL` | BSC 节点 WSS endpoint |
| `OKX_API_KEY` / `OKX_SECRET_KEY` / `OKX_PASSPHRASE` | 用于 BNB 原生价格（不填则非稳定币池无法定价） |
| `DATABASE_URL` | PG 连接串（默认 docker compose 起的实例） |
| `POSTGRES_PASSWORD` | **生产必改强密码**，开发可留默认 `radar` |
| `NOTIFY_FEISHU_WEBHOOK` | 飞书机器人 webhook（不填则不推送） |

异动阈值见 `.env.example`（`ANOMALY_VOL_SPIKE_RATIO` / `ANOMALY_FEE_TVL_APR` / `ANOMALY_COOLDOWN_MS` / `ANOMALY_BASELINE_MIN_COVERAGE_MS`）。

## fapi 地理封锁（美国 IP 451）

`fapi.binance.com` 在美国 IP 直接 451。代码内置自动 fallback：fapi 直拉失败 → 拉 `seed/binance-perpetuals.json`（GitHub raw 镜像）。

镜像维护：在非美机器跑 sync + commit + push：

```bash
bun run sync-perpetuals
git add seed/ && git commit -m "sync binance perpetuals" && git push
```

服务器最迟 6h 内（默认刷新间隔）拉到新版，要立即生效就 `docker compose restart radar`。

## 目录

```
src/
├── index.ts             入口
├── config/              chains / contracts / abis
├── clients/             BSC WSS client
├── core/                swap-listener / price-service / tvl-calculator / livenessProbe
├── db/                  pg Pool + schema
├── token-tracker/       白名单（fapi + capital 取交集 + GitHub seed fallback）
├── anomaly/             异动检测（vol_spike / combo）
├── notifier/            飞书 webhook
├── utils/               TTLCache
└── types/

scripts/
├── sync-perpetuals.ts   非美机器跑：拉 fapi → seed/binance-perpetuals.json
└── test-feishu.ts       发样本到群里看卡片排版
```

## 数据存储（PostgreSQL 17）

由 docker compose 起 `postgres:17-alpine` 容器，数据持久化到 named volume `radar-pg-data`。Schema 在 `db/init.sql`。

| 表 | 说明 |
|---|---|
| `pools` / `v4_pools` | 池基础元信息（swap-listener 入库前 cache） |
| `swaps` | 原始 swap 记录（永久保留，方便复盘） |
| `pool_1min_stats` | 池粒度 1min 桶 |
| `token_1min_stats` | **token 粒度** 1min 桶（异动检测核心） |
| `binance_bsc_tokens` | 白名单（symbol → BSC 合约地址） |
| `anomaly_events` | 触发事件审计（rule + metrics JSONB） |

## 与 lp-bot 关系

完全独立——自带 PG、自管 WSS 连接，不依赖也不修改 lp-bot 主项目。
