import { getPool } from "../db/index.js";
import type { ChainId } from "../types/index.js";

const FIVE_MIN_MS = 5 * 60 * 1000;
const TWENTY_FOUR_H_MS = 24 * 60 * 60 * 1000;
const FIVE_MIN_BUCKETS_IN_24H = (24 * 60) / 5; // 288

export interface ActiveToken {
  tokenAddress: string;
  swapCount: number;
}

/** 找过去 5min 有 swap 的活跃 token（白名单已经在入库时过滤过了） */
export async function findActiveTokens(chain: ChainId = "bsc"): Promise<ActiveToken[]> {
  const cutoff = Date.now() - FIVE_MIN_MS;
  const res = await getPool().query<{ token_address: string; swap_count: number }>(
    `SELECT token_address, SUM(swap_count)::int AS swap_count
     FROM token_1min_stats
     WHERE chain = $1 AND bucket_start >= $2
     GROUP BY token_address
     HAVING SUM(swap_count) > 0`,
    [chain, cutoff]
  );
  return res.rows.map((r) => ({
    tokenAddress: r.token_address,
    swapCount: r.swap_count,
  }));
}

export interface TokenWindowStats {
  vol5minUsd: number;
  fee5minUsd: number;
  vol24hUsd: number;
  vol24hAvg5minUsd: number;
  baselineCoverageMs: number; // 最早数据点到现在的距离，用来判断 baseline 是否可信
}

/** 一次性拿 token 的 5min 滚动 + 24h baseline 指标 */
export async function getTokenWindowStats(
  tokenAddress: string,
  chain: ChainId = "bsc"
): Promise<TokenWindowStats> {
  const now = Date.now();
  const fiveMinAgo = now - FIVE_MIN_MS;
  const twentyFourHAgo = now - TWENTY_FOUR_H_MS;

  const res = await getPool().query<{
    vol5min: string | null;
    fee5min: string | null;
    vol24h: string | null;
    earliest_bucket: string | null;
  }>(
    `SELECT
       COALESCE(SUM(total_volume_usd) FILTER (WHERE bucket_start >= $2), 0) AS vol5min,
       COALESCE(SUM(total_fees_usd)   FILTER (WHERE bucket_start >= $2), 0) AS fee5min,
       COALESCE(SUM(total_volume_usd) FILTER (WHERE bucket_start >= $3), 0) AS vol24h,
       MIN(bucket_start) FILTER (WHERE bucket_start >= $3) AS earliest_bucket
     FROM token_1min_stats
     WHERE token_address = $1 AND chain = $4`,
    [tokenAddress, fiveMinAgo, twentyFourHAgo, chain]
  );

  const row = res.rows[0];
  const vol5min = Number(row?.vol5min) || 0;
  const fee5min = Number(row?.fee5min) || 0;
  const vol24h = Number(row?.vol24h) || 0;
  const earliest = row?.earliest_bucket ? Number(row.earliest_bucket) : null;
  const baselineCoverageMs = earliest != null ? Math.max(0, now - earliest) : 0;

  // 用实际数据覆盖的 5min 窗口数算 baseline，避免冷启动期间被 288 系数稀释
  const effectiveWindows =
    baselineCoverageMs > 0
      ? Math.min(baselineCoverageMs / FIVE_MIN_MS, FIVE_MIN_BUCKETS_IN_24H)
      : 0;
  const vol24hAvg5min = effectiveWindows > 0 ? vol24h / effectiveWindows : 0;

  return {
    vol5minUsd: vol5min,
    fee5minUsd: fee5min,
    vol24hUsd: vol24h,
    vol24hAvg5minUsd: vol24hAvg5min,
    baselineCoverageMs,
  };
}

/** 找 token 涉及的池（V3 / Pancake-V3，V4 不含）+ 最近 30min 是否活跃 */
export async function findTokenActivePools(
  tokenAddress: string,
  chain: ChainId = "bsc",
  activeWindowMs: number = 30 * 60_000
): Promise<string[]> {
  const cutoff = Date.now() - activeWindowMs;
  const res = await getPool().query<{ pool_address: string }>(
    `SELECT DISTINCT p.address AS pool_address
     FROM pools p
     JOIN pool_1min_stats s
       ON s.pool_address = p.address AND s.chain = p.chain
     WHERE p.chain = $1
       AND (LOWER(p.token0) = LOWER($2) OR LOWER(p.token1) = LOWER($2))
       AND s.bucket_start >= $3`,
    [chain, tokenAddress, cutoff]
  );
  return res.rows.map((r) => r.pool_address);
}

/** 查最近 cooldown 内同 token+rule 是否已经触发过 */
export async function isRecentlyAlerted(
  tokenAddress: string,
  rule: string,
  cooldownMs: number
): Promise<boolean> {
  const cutoff = Date.now() - cooldownMs;
  const res = await getPool().query<{ exists: boolean }>(
    `SELECT EXISTS (
       SELECT 1 FROM anomaly_events
       WHERE token_address = $1 AND rule = $2 AND detected_at >= $3
     ) AS exists`,
    [tokenAddress.toLowerCase(), rule, cutoff]
  );
  return res.rows[0]?.exists ?? false;
}
