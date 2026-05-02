import { insertAnomalyEvent } from "../db/queries.js";
import { getTokenPrices, getCacheKey } from "../core/price-service.js";
import {
  findActiveTokens,
  getTokenWindowStats,
  findNewlyCreatedPools,
  isRecentlyAlerted,
} from "./aggregator.js";
import { getTokenTVL } from "./tvl-cache.js";
import { loadThresholds, type AnomalyTrigger, type AnomalyMetrics, type AnomalyRule } from "./rules.js";
import { anomalyEvents } from "./events.js";
import { isWatchedToken } from "../token-tracker/watchlist.js";
import { CHAIN_STATIC } from "../config/chains.js";
import type { ChainId } from "../types/index.js";

let timer: ReturnType<typeof setInterval> | null = null;
let running = false;
let thresholds = loadThresholds();
let detectorStartupAt = 0;

const FIVE_MIN_TO_YEAR_MULTIPLIER = (60 / 5) * 24 * 365;

function pickNonBaseToken(token0: string, token1: string, chain: ChainId): string | null {
  const set = CHAIN_STATIC[chain].baseTokens;
  const t0 = token0.toLowerCase();
  const t1 = token1.toLowerCase();
  if (!set.has(t0)) return t0;
  if (!set.has(t1)) return t1;
  return null;
}

async function fireTrigger(
  rule: AnomalyRule,
  tokenAddress: string,
  symbol: string,
  metrics: AnomalyMetrics
): Promise<void> {
  if (await isRecentlyAlerted(tokenAddress, rule, thresholds.cooldownMs)) {
    return;
  }
  const trigger: AnomalyTrigger = {
    rule,
    tokenAddress,
    symbol,
    metrics,
    detectedAt: Date.now(),
  };
  await insertAnomalyEvent(tokenAddress, symbol, rule, metrics, trigger.detectedAt);
  anomalyEvents.emit("anomaly", { trigger });
  console.log(
    `[Anomaly] [${rule}] ${symbol} (${tokenAddress.slice(0, 10)}...) ${JSON.stringify(metrics)}`
  );
}

async function evaluateActiveToken(tokenAddress: string, chain: ChainId): Promise<void> {
  const stats = await getTokenWindowStats(tokenAddress, chain);
  const tvl = await getTokenTVL(tokenAddress, chain);

  const volRatio =
    stats.vol24hAvg5minUsd > 0 ? stats.vol5minUsd / stats.vol24hAvg5minUsd : 0;
  const feeAprPct =
    tvl > 0 ? (stats.fee5minUsd * FIVE_MIN_TO_YEAR_MULTIPLIER * 100) / tvl : 0;

  const baselineReliable = stats.baselineCoverageMs >= thresholds.baselineMinCoverageMs;
  const isVolSpike =
    baselineReliable &&
    stats.vol24hAvg5minUsd > 0 &&
    volRatio >= thresholds.volSpikeRatio;
  const isFeeAprHot = feeAprPct >= thresholds.feeAprPct && tvl > 0;

  if (!isVolSpike && !isFeeAprHot) return;

  // resolve symbol（不阻塞失败：拿不到就用地址前缀）
  let symbol = tokenAddress.slice(0, 8);
  try {
    const prices = await getTokenPrices([{ chain, address: tokenAddress }]);
    symbol = prices.get(getCacheKey(tokenAddress, chain))?.symbol || symbol;
  } catch {
    /* ignore */
  }

  const metrics: AnomalyMetrics = {
    vol5minUsd: stats.vol5minUsd,
    vol24hAvg5minUsd: stats.vol24hAvg5minUsd,
    volRatio,
    fee5minUsd: stats.fee5minUsd,
    tvlUsd: tvl,
    feeAprPct,
  };

  // 优先触发组合规则（强信号），避免同时打 vol_spike + fee_tvl_apr 两次
  if (isVolSpike && isFeeAprHot) {
    await fireTrigger("combo", tokenAddress, symbol, metrics);
  } else if (isVolSpike) {
    await fireTrigger("vol_spike", tokenAddress, symbol, metrics);
  } else if (isFeeAprHot) {
    await fireTrigger("fee_tvl_apr", tokenAddress, symbol, metrics);
  }
}

async function evaluateNewPools(chain: ChainId): Promise<void> {
  // 冷启动 grace：启动后 baselineMinCoverageMs（默认 1h）内不触发 new_pool。
  // 因为 pools.created_at 实际是"我们首次发现"的时间，启动后看到的池子大多是已存在的
  // 老池，要等到 grace 期外，新出现的池才能确定是链上真新池。
  const graceUntil = detectorStartupAt + thresholds.baselineMinCoverageMs;
  if (Date.now() < graceUntil) return;

  const newPools = await findNewlyCreatedPools(thresholds.newPoolWindowMs, chain);
  for (const pool of newPools) {
    if (pool.createdAt < graceUntil) continue;

    const target = pickNonBaseToken(pool.token0, pool.token1, chain);
    if (!target) continue;
    if (!isWatchedToken(target)) continue;

    let symbol = target.slice(0, 8);
    try {
      const prices = await getTokenPrices([{ chain, address: target }]);
      symbol = prices.get(getCacheKey(target, chain))?.symbol || symbol;
    } catch {
      /* ignore */
    }

    const metrics: AnomalyMetrics = {
      newPoolAddress: pool.address,
      newPoolDex: pool.dex,
      newPoolFeeTier: pool.feeTier,
      newPoolAgeMs: Date.now() - pool.createdAt,
    };
    await fireTrigger("new_pool", target, symbol, metrics);
  }
}

async function detectOnce(): Promise<void> {
  if (running) {
    console.warn("[Anomaly] Previous tick still running, skipping");
    return;
  }
  running = true;
  const start = Date.now();
  try {
    const chain: ChainId = "bsc";
    const active = await findActiveTokens(chain);

    // 限制并发：同时算 5 个 token，避免一次性打爆 RPC（TVL 计算要 multicall）
    const CONCURRENCY = 5;
    for (let i = 0; i < active.length; i += CONCURRENCY) {
      const slice = active.slice(i, i + CONCURRENCY);
      await Promise.all(
        slice.map((t) =>
          evaluateActiveToken(t.tokenAddress, chain).catch((err) =>
            console.error(`[Anomaly] evaluate ${t.tokenAddress} failed:`, err)
          )
        )
      );
    }

    await evaluateNewPools(chain).catch((err) =>
      console.error("[Anomaly] evaluateNewPools failed:", err)
    );

    const elapsed = Date.now() - start;
    if (elapsed > thresholds.detectIntervalMs / 2) {
      console.warn(
        `[Anomaly] Tick took ${elapsed}ms (active=${active.length}); approaching interval limit`
      );
    }
  } finally {
    running = false;
  }
}

export function startAnomalyDetector(): void {
  if (timer) return;
  thresholds = loadThresholds();
  detectorStartupAt = Date.now();
  console.log(
    `[Anomaly] Detector starting (volRatio=${thresholds.volSpikeRatio}, ` +
      `feeApr=${thresholds.feeAprPct}%, interval=${thresholds.detectIntervalMs}ms, ` +
      `cooldown=${thresholds.cooldownMs}ms, newPoolWindow=${thresholds.newPoolWindowMs}ms)`
  );

  timer = setInterval(() => {
    detectOnce().catch((err) => console.error("[Anomaly] detect tick error:", err));
  }, thresholds.detectIntervalMs);
  if (typeof timer.unref === "function") timer.unref();
}

export function stopAnomalyDetector(): void {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
}
