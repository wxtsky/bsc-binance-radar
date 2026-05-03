import { insertAnomalyEvent } from "../db/queries.js";
import { getTokenPrices, getCacheKey } from "../core/price-service.js";
import { findActiveTokens, getTokenWindowStats, isRecentlyAlerted } from "./aggregator.js";
import { getTokenTVL } from "./tvl-cache.js";
import { loadThresholds, type AnomalyTrigger, type AnomalyMetrics, type AnomalyRule } from "./rules.js";
import { anomalyEvents } from "./events.js";
import type { ChainId } from "../types/index.js";

let timer: ReturnType<typeof setInterval> | null = null;
let running = false;
let thresholds = loadThresholds();

const FIVE_MIN_TO_YEAR_MULTIPLIER = (60 / 5) * 24 * 365;

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

  const baselineReliable = stats.baselineCoverageMs >= thresholds.baselineMinCoverageMs;
  if (!baselineReliable || stats.vol24hAvg5minUsd <= 0) return;

  const volRatio = stats.vol5minUsd / stats.vol24hAvg5minUsd;
  const isVolSpike = volRatio >= thresholds.volSpikeRatio;
  if (!isVolSpike) return;

  // vol 异动满足，再算 fee/tvl 决定是普通还是强信号
  const tvl = await getTokenTVL(tokenAddress, chain);
  const feeAprPct =
    tvl > 0 ? (stats.fee5minUsd * FIVE_MIN_TO_YEAR_MULTIPLIER * 100) / tvl : 0;
  const isCombo = feeAprPct >= thresholds.feeAprPct && tvl > 0;

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

  await fireTrigger(isCombo ? "combo" : "vol_spike", tokenAddress, symbol, metrics);
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
  console.log(
    `[Anomaly] Detector starting (volRatio=${thresholds.volSpikeRatio}, ` +
      `comboFeeApr=${thresholds.feeAprPct}%, interval=${thresholds.detectIntervalMs}ms, ` +
      `cooldown=${thresholds.cooldownMs}ms)`
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
