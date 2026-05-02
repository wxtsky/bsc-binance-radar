---
created: 2026-05-02
status: approved
topic: lab-launch-backtest
---

# LAB 启动信号回测器 — 设计文档

## 概要

为 bsc-binance-radar 设计一个**多维突变 AND 门**检测算法，目标是在 token 启动初期（绝对 vol 仍小但相对突变明显）就能告警，同时把假阳压到「每天 ≤ 5 次」级别。

LAB 在 **2026-05-02 北京时间 02:37**（UTC 2026-05-01 18:37）启动，是 ground truth。当前生产 detector 在北京时间 21:51 才触发，**晚 19h14min**。本回测要找到能在 02:37 抓到、且不产生噪音告警的算法。

## 背景

### 当前生产 detector 漏抓的根因

代码在 `src/anomaly/{aggregator,rules,detector}.ts`。
- 5min vol 滚动 vs **24h** 5min 均值 ≥ 5x → vol_spike
- + fee/TVL APR ≥ 100% → combo
- 受 `baseline_min_coverage_ms=1h` 保护

LAB 02:37 时点漏抓的具体原因：
1. **5min 窗口稀释 1min 突变**（02:37 单分钟 $46，5min 累计仅 $50–60，均值 $1/min × 5 = $5，比值勉强但绝对值太低）
2. **24h baseline 在冷启动期 ≈ 0**，被 baseline_min_coverage 拦下不触发
3. **只看 vol 单维度**，没用 swap_count、价格、买卖方向

### LAB 02:37 实测启动印记

| 维度 | 02:37 之前（22:06–02:35）| 02:37 那一分钟 | 突变倍数 |
|---|---|---|---|
| vol_usd | < $2/min | $46 | ~25x |
| swap_count | < 1/min | 8 | ~10x |
| 价格 | 0.68–0.71 区间无序震荡 | 0.696 → 0.713（连续 4 笔单向）| +2.5% / 1min |
| 买入比例 | ~50% | 5 笔接连买入 | ~100% |

四个维度同时变化是「真启动」的特征签名。

## 设计

### 总体架构

新增独立脚本 `scripts/backtest-launch-detector.ts`，**生产 detector 保持不动**。

```
scripts/backtest-launch-detector.ts
├── data-loader.ts        从 PG 加载 swaps + token_1min_stats + pools
├── feature-builder.ts    每 1min 计算 4 维特征 + 30min rolling baseline
├── decision-gate.ts      4 维 AND 门，输出 alert/no-alert
├── reporter.ts           聚合告警时序 / 假阳清单 / 推荐阈值
└── main.ts               按阶段（A: LAB / B: 全量）调度
```

### 数据流

```
PG (swaps + token_1min_stats + pools)
        ↓
按 token 分组，按 1min 步进遍历时间窗口
        ↓
每 1min 桶计算特征：
  - vol_1min_usd        （直接读 token_1min_stats）
  - swap_count_1min     （直接读）
  - price_change_pct    （从 USDT 池 swaps amount 反算 1min 内价格变化）
  - buy_ratio           （amount1<0 笔数 / 总笔数；token1 = LAB 的池子）
  - vol_30min_avg       （前 30 个 1min 桶 vol 均值）
        ↓
AND 门决策：4 个条件全过才告警
        ↓
报告聚合
```

### 关键组件

#### 1. 价格反算
对每个 token 的 USDT 池子（token0=USDT，token1=token）：
- `price_token = abs(amount0) / abs(amount1) / 10^(decimals0 - decimals1)`
- USDT 是 18 decimals，多数 token 也是 18 → 简化 `abs(amount0) / abs(amount1)`
- 跳过 WBNB 池（价格反算需要 BNB 历史价，引入额外不确定性）

每个 1min 桶：
- 取该桶内所有 USDT 池 swaps，按 timestamp 排序
- `price_change_pct = (last_price - first_price) / first_price * 100`
- 桶内不足 2 笔 → `price_change_pct = 0`

#### 2. 买入占比
**用 amount 符号定方向**（V3 swap 的 delta 语义）：
- LAB 是 token1（USDT 池中）→ `amount1 < 0` = 池子吐 LAB = 用户**买入** LAB
- LAB 是 token0（少数池）→ `amount0 < 0` = 用户买入 LAB

```
buy_ratio = (买入笔数) / (买入笔数 + 卖出笔数)
```

仅统计 USDT 池内的 swap，确保方向语义清晰。

#### 3. 30min rolling baseline
对每个 1min 桶，取前 30 分钟（不含当前桶）：
- `vol_30min_avg = sum(prev_30_buckets_vol) / 30`
- token_1min_stats 缺失某分钟（无 swap）→ 该分钟视为 0
- `baseline_coverage_min` = 从该 token 第一笔 swap 时间到当前桶时间的分钟差，若 < 30 → 不告警（避免冷启动假阳）

#### 4. AND 门决策
```typescript
function shouldAlert(f: Features, t: Thresholds): boolean {
  return (
    f.baseline_coverage_min >= 30 &&
    f.vol_30min_avg > 0 &&
    f.vol_1min / f.vol_30min_avg >= t.volRatio &&
    f.swap_count_1min >= t.minSwapCount &&
    f.price_change_pct >= t.priceChangePct &&
    f.buy_ratio >= t.buyRatio
  );
}
```

#### 5. 报告输出

阶段 A（LAB 单点）：
```jsonc
{
  "ground_truth_cn": "2026-05-02 02:37",
  "first_alert_cn": "2026-05-02 02:38",
  "lag_sec": 60,
  "all_alerts": [{ "cn_time": "...", "metrics": {...} }, ...]
}
```

阶段 B（全 303 token）：
```jsonc
{
  "thresholds": {...},
  "total_alerts_24h": 12,
  "alerts_by_token": [{ "symbol": "ABC", "count": 3, "first_at": "..." }, ...],
  "lab_hit": true,
  "lab_lag_sec": 60
}
```

## 阈值

### 默认值（v0）
| 维度 | 默认 |
|---|---|
| volRatio (vs 30min avg) | 10 |
| minSwapCount | 5 |
| priceChangePct | 1.0 |
| buyRatio | 0.80 |

### 调阈值流程
1. v0 默认值跑阶段 A —— 看 LAB 02:37 是否命中（lag ≤ 5min）
2. 若**没命中**：放宽到 vol 5x / swap 3 / price 0.5% / buy 0.7，重试
3. 若命中：跑阶段 B，统计 303 token 24h 总告警数
4. 若**假阳 > 5/天**：收紧到 vol 20x / swap 8 / price 2% / buy 0.9
5. 迭代到「LAB 命中 + 假阳 ≤ 5/天」

最多 3 轮迭代，不做完整 grid search（避免过拟合）。

## 评估指标

| 指标 | 定义 | 通过条件 |
|---|---|---|
| **召回（阶段 A）** | LAB 02:37 后 ≤ 5min 首次告警 | 是 |
| **假阳（阶段 B）** | 303 token × 24h 总告警数 | ≤ 5 |
| **领先时间** | 第一次告警时间 vs 现 detector 触发时间（21:51）| ≥ 18h |

假阳标定方法：
- 自动：告警后 30min 内若价格涨幅未达 +5% 或 vol 未持续放量，标记为「疑似假阳」
- 人工：Bro 抽查疑似假阳清单确认

## 不做的事（YAGNI）

- ❌ 不改生产 detector（先跑回测看效果）
- ❌ 不做完整 grid search（5 维 × 3 候选 = 243 组合）—— 默认值 + 最多 3 轮调参
- ❌ 不做实时模拟（按历史数据 batch 跑）
- ❌ 不接通飞书 / 不写 web 仪表盘
- ❌ 不引入 BNB 历史价（只用 USDT 池）
- ❌ 不计算 holders / 持币地址数（数据库没存）

## 测试计划

阶段 A 通过：
- LAB 02:37 后 ≤ 5min 首次告警
- 整个 LAB 25h 内告警 ≤ 10 次（避免 LAB 自身刷屏）

阶段 B 通过：
- 303 token × 24h 总告警 ≤ 5
- 抽查 5 个告警样本，至少 3 个是「真启动」（事后 1h 内有持续放量或价格 +5%）

## 风险与缓解

| 风险 | 缓解 |
|---|---|
| 价格反算被低流动性 dust swap 干扰 | 加过滤：单笔 vol_usd ≥ $1 才计入价格变化 |
| LAB 是孤例，泛化不足 | 阶段 B 全量假阳测试兜底；未来可加更多 ground truth |
| 池子 token0/token1 顺序不一致 | 反算价格前先按 contract address 比对 |
| backfill 只到 25h 前，更早 baseline 缺失 | LAB 02:37 之前有 4h 自身数据，足够 30min baseline |
| BSC fapi geo-block 影响阈值热更新 | 阈值写到回测脚本里硬编码，不依赖运行时 fapi |

## 实施分阶段

1. **数据层**：data-loader（按 token 拉 swaps + 1min stats + pools）
2. **特征层**：feature-builder（4 维 + baseline）
3. **决策层**：decision-gate（AND 门 + 阈值配置）
4. **报告层**：reporter（控制台 + JSON 输出）
5. **集成**：main.ts 串起来，跑阶段 A
6. **扩展**：跑阶段 B，输出推荐阈值
7. **结论**：把发现写到 HANDOFF.md，决定是否合入生产 detector

每步可独立验证（先跑 LAB 数据通了再跑全量）。

### 运行方式

数据在生产服务器（107.175.35.109）的 PG，回测脚本在服务器上跑，沿用现有 `docker compose run` 模式：

```bash
# 本地开发：先 git push 脚本到 GitHub
git push

# 服务器：
ssh root@107.175.35.109
cd /opt/bsc-binance-radar
git pull
docker compose run --rm --no-deps radar bun scripts/backtest-launch-detector.ts \
  --phase A \
  --token 0x7ec43cf65f1663f820427c62a5780b8f2e25593a \
  --vol-ratio 10 --min-swaps 5 --price-pct 1.0 --buy-ratio 0.8

# 阶段 B（全量）：
docker compose run --rm --no-deps radar bun scripts/backtest-launch-detector.ts --phase B [...]
```

报告 JSON 落到容器 `/data` 卷或直接 stdout，再 `docker cp` 拉回本地分析。

## 后续

阶段 B 跑完后两个分支：
- ✅ **假阳 ≤ 5/天**：把 AND 门合入生产 detector，作为新 rule（如 `early_launch`）
- ❌ **假阳 > 5/天**：补维度（TVL 增速、unique buyer count），或对真启动 token 做白名单 二次确认（启动后 30min 复检）

## 相关文件

- `src/anomaly/{aggregator,rules,detector}.ts` — 生产 detector，**本设计不改它**
- `db/init.sql` — DB schema，注意 `swaps.amount0/amount1` 是有符号 delta
- `HANDOFF.md` — 项目交接文档，回测结论后更新
