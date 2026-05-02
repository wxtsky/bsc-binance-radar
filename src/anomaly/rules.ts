export type AnomalyRule = "vol_spike" | "fee_tvl_apr" | "combo" | "new_pool";

export interface AnomalyMetrics {
  vol5minUsd?: number;
  vol24hAvg5minUsd?: number;
  volRatio?: number;
  fee5minUsd?: number;
  tvlUsd?: number;
  feeAprPct?: number;
  newPoolAddress?: string;
  newPoolDex?: string;
  newPoolFeeTier?: number;
  newPoolAgeMs?: number;
}

export interface AnomalyTrigger {
  rule: AnomalyRule;
  tokenAddress: string;
  symbol: string;
  metrics: AnomalyMetrics;
  detectedAt: number;
}

export interface AnomalyThresholds {
  volSpikeRatio: number;        // ANOMALY_VOL_SPIKE_RATIO
  feeAprPct: number;            // ANOMALY_FEE_TVL_APR (%)
  detectIntervalMs: number;     // ANOMALY_DETECT_INTERVAL_MS
  cooldownMs: number;           // ANOMALY_COOLDOWN_MS
  newPoolWindowMs: number;      // ANOMALY_NEW_POOL_WINDOW_MS
  baselineMinCoverageMs: number; // 数据覆盖少于此值时不触发 vol_spike，避免冷启动假阳
}

export function loadThresholds(): AnomalyThresholds {
  return {
    volSpikeRatio: Number(process.env.ANOMALY_VOL_SPIKE_RATIO) || 5,
    feeAprPct: Number(process.env.ANOMALY_FEE_TVL_APR) || 100,
    detectIntervalMs: Number(process.env.ANOMALY_DETECT_INTERVAL_MS) || 30_000,
    cooldownMs: Number(process.env.ANOMALY_COOLDOWN_MS) || 5 * 60_000,
    newPoolWindowMs: Number(process.env.ANOMALY_NEW_POOL_WINDOW_MS) || 10 * 60_000,
    baselineMinCoverageMs:
      Number(process.env.ANOMALY_BASELINE_MIN_COVERAGE_MS) || 60 * 60_000,
  };
}
