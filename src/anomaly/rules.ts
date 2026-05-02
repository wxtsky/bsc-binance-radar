export type AnomalyRule = "vol_spike" | "combo";

export interface AnomalyMetrics {
  vol5minUsd?: number;
  vol24hAvg5minUsd?: number;
  volRatio?: number;
  fee5minUsd?: number;
  tvlUsd?: number;
  feeAprPct?: number;
}

export interface AnomalyTrigger {
  rule: AnomalyRule;
  tokenAddress: string;
  symbol: string;
  metrics: AnomalyMetrics;
  detectedAt: number;
}

export interface AnomalyThresholds {
  volSpikeRatio: number;        // ANOMALY_VOL_SPIKE_RATIO  vol_spike 阈值
  feeAprPct: number;            // ANOMALY_FEE_TVL_APR  combo 强信号需同时满足的 APR%
  detectIntervalMs: number;     // ANOMALY_DETECT_INTERVAL_MS
  cooldownMs: number;           // ANOMALY_COOLDOWN_MS
  baselineMinCoverageMs: number; // 数据覆盖少于此值时不触发 vol_spike，避免冷启动假阳
}

export function loadThresholds(): AnomalyThresholds {
  return {
    volSpikeRatio: Number(process.env.ANOMALY_VOL_SPIKE_RATIO) || 5,
    feeAprPct: Number(process.env.ANOMALY_FEE_TVL_APR) || 100,
    detectIntervalMs: Number(process.env.ANOMALY_DETECT_INTERVAL_MS) || 30_000,
    cooldownMs: Number(process.env.ANOMALY_COOLDOWN_MS) || 5 * 60_000,
    baselineMinCoverageMs:
      Number(process.env.ANOMALY_BASELINE_MIN_COVERAGE_MS) || 60 * 60_000,
  };
}
