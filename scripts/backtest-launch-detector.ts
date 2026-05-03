#!/usr/bin/env bun
/**
 * LAB 启动信号回测器 — 4 维 AND 门
 *
 * 设计文档：docs/superpowers/specs/2026-05-02-lab-launch-backtest-design.md
 *
 * 用法：
 *   bun scripts/backtest-launch-detector.ts A          # 阶段 A：LAB 单点验证
 *   bun scripts/backtest-launch-detector.ts B          # 阶段 B：全 303 token 假阳
 *   bun scripts/backtest-launch-detector.ts A 0xabc... # 任意 token 单点
 *
 * 阈值（vol 倍数 / 最小 swap / 价格涨幅 / 买入占比）通过环境变量覆盖：
 *   BT_VOL_RATIO=10 BT_MIN_SWAPS=5 BT_PRICE_PCT=1.0 BT_BUY_RATIO=0.8
 *
 * 容器内：
 *   docker compose run --rm --no-deps radar bun scripts/backtest-launch-detector.ts A
 */

import "dotenv/config";
import { writeFileSync } from "fs";
import { getPool, closeDatabase } from "../src/db/index.js";

const USDT_BSC = "0x55d398326f99059ff775485246999027b3197955";
const LAB_ADDR = "0x7ec43cf65f1663f820427c62a5780b8f2e25593a";

interface Thresholds {
  volRatio: number;
  minSwapCount: number;
  priceChangePct: number;
  buyRatio: number;
  baselineMinutes: number;
  cooldownMinutes: number; // 同 token 多久内不再告警（0 = 不限）
}

const DEFAULT_THRESHOLDS: Thresholds = {
  volRatio: Number(process.env.BT_VOL_RATIO) || 10,
  minSwapCount: Number(process.env.BT_MIN_SWAPS) || 5,
  priceChangePct: Number(process.env.BT_PRICE_PCT) || 1.0,
  buyRatio: Number(process.env.BT_BUY_RATIO) || 0.8,
  baselineMinutes: Number(process.env.BT_BASELINE_MIN) || 30,
  cooldownMinutes: Number(process.env.BT_COOLDOWN_MIN) || 0,
};

const ONE_MIN_MS = 60 * 1000;

interface Features {
  bucketStart: number; // ms, 1min bucket
  vol1min: number;
  swapCount1min: number;
  priceChangePct: number;
  buyRatio: number;
  vol30minAvg: number;
  baselineCoverageMin: number;
  usdtSwapCount: number;
}

interface Alert {
  cnTime: string;
  bucketStart: number;
  features: Features;
}

interface TokenReport {
  tokenAddress: string;
  symbol: string;
  scanFromCn: string;
  scanToCn: string;
  totalBuckets: number;
  alerts: Alert[];
  thresholds: Thresholds;
}

const cnFmt = new Intl.DateTimeFormat("zh-CN", {
  timeZone: "Asia/Shanghai",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});

function fmtCn(ms: number): string {
  return cnFmt.format(new Date(ms)).replace(/\//g, "-");
}

function absBigInt(x: bigint): bigint {
  return x < 0n ? -x : x;
}

/** 用 BigInt 缩放避免大数 → Number 丢精度。结果 6 位小数级。 */
function bigIntDiv(a: bigint, b: bigint): number {
  if (b === 0n) return 0;
  const SCALE = 1_000_000n;
  const aAbs = absBigInt(a);
  const bAbs = absBigInt(b);
  const scaled = (aAbs * SCALE) / bAbs;
  return Number(scaled) / Number(SCALE);
}

interface UsdtPool {
  address: string;
  usdtIsToken0: boolean; // true: token0=USDT, token1=target ; false: 反过来
}

async function getTokenSymbol(token: string): Promise<string> {
  const res = await getPool().query<{ symbol: string }>(
    `SELECT symbol FROM binance_bsc_tokens WHERE LOWER(contract_address) = LOWER($1)`,
    [token]
  );
  return res.rows[0]?.symbol || token.slice(0, 8);
}

async function getUsdtPools(token: string): Promise<UsdtPool[]> {
  const res = await getPool().query<{ address: string; token0: string; token1: string }>(
    `SELECT address, LOWER(token0) AS token0, LOWER(token1) AS token1
     FROM pools
     WHERE chain='bsc'
       AND (LOWER(token0)=LOWER($1) OR LOWER(token1)=LOWER($1))
       AND (LOWER(token0)=$2 OR LOWER(token1)=$2)`,
    [token, USDT_BSC]
  );
  return res.rows.map((r) => ({
    address: r.address.toLowerCase(),
    usdtIsToken0: r.token0 === USDT_BSC,
  }));
}

interface BucketStat {
  bucket: number;
  vol: number;
  swap: number;
}

async function getTokenBuckets(
  token: string,
  fromMs: number,
  toMs: number
): Promise<BucketStat[]> {
  const res = await getPool().query<{
    bucket_start: string;
    total_volume_usd: string;
    swap_count: number;
  }>(
    `SELECT bucket_start, total_volume_usd, swap_count
     FROM token_1min_stats
     WHERE LOWER(token_address) = LOWER($1)
       AND bucket_start BETWEEN $2 AND $3
     ORDER BY bucket_start`,
    [token, fromMs, toMs]
  );
  return res.rows.map((r) => ({
    bucket: Number(r.bucket_start),
    vol: Number(r.total_volume_usd) || 0,
    swap: Number(r.swap_count) || 0,
  }));
}

interface UsdtSwap {
  pool: string;
  ts: number;
  blockNum: number;
  id: number;
  amount0: bigint;
  amount1: bigint;
  volumeUsd: number;
  usdtIsToken0: boolean;
}

async function getUsdtPoolSwaps(
  token: string,
  pools: UsdtPool[],
  fromMs: number,
  toMs: number
): Promise<UsdtSwap[]> {
  if (pools.length === 0) return [];
  const addresses = pools.map((p) => p.address);
  const isToken0Map = new Map(pools.map((p) => [p.address, p.usdtIsToken0]));
  const res = await getPool().query<{
    pool_address: string;
    timestamp: string;
    block_number: string;
    id: string;
    amount0: string;
    amount1: string;
    volume_usd: string;
  }>(
    `SELECT pool_address, timestamp, block_number, id, amount0, amount1, volume_usd
     FROM swaps
     WHERE pool_address = ANY($1)
       AND timestamp BETWEEN $2 AND $3
     ORDER BY timestamp, block_number, id`,
    [addresses, fromMs, toMs]
  );
  return res.rows.map((r) => ({
    pool: r.pool_address.toLowerCase(),
    ts: Number(r.timestamp),
    blockNum: Number(r.block_number),
    id: Number(r.id),
    amount0: BigInt(r.amount0),
    amount1: BigInt(r.amount1),
    volumeUsd: Number(r.volume_usd) || 0,
    usdtIsToken0: isToken0Map.get(r.pool_address.toLowerCase()) ?? true,
  }));
}

/** 把 USDT 池 swaps 按 1min 桶切，计算 price_change_pct + buy_ratio。 */
function aggregateUsdtBuckets(swaps: UsdtSwap[]): Map<number, {
  priceChangePct: number;
  buyRatio: number;
  swapCount: number;
}> {
  const grouped = new Map<number, UsdtSwap[]>();
  for (const s of swaps) {
    const bucket = Math.floor(s.ts / ONE_MIN_MS) * ONE_MIN_MS;
    let arr = grouped.get(bucket);
    if (!arr) {
      arr = [];
      grouped.set(bucket, arr);
    }
    arr.push(s);
  }

  const out = new Map<number, { priceChangePct: number; buyRatio: number; swapCount: number }>();
  for (const [bucket, arr] of grouped) {
    arr.sort((a, b) => a.ts - b.ts || a.blockNum - b.blockNum || a.id - b.id);
    // 价格 = USDT amount / token amount（绝对值比）。两侧都是 18 decimals 时不需调整。
    const prices: number[] = [];
    let buy = 0;
    let sell = 0;
    for (const s of arr) {
      // 过滤 dust：volume_usd < $1 不计入价格/方向
      if (s.volumeUsd < 1) continue;
      const usdtRaw = s.usdtIsToken0 ? s.amount0 : s.amount1;
      const tokenRaw = s.usdtIsToken0 ? s.amount1 : s.amount0;
      if (tokenRaw === 0n) continue;
      const price = bigIntDiv(usdtRaw, tokenRaw);
      if (price > 0) prices.push(price);
      // 方向：token 侧 amount < 0 = 池吐 token = 用户买入
      if (tokenRaw < 0n) buy++;
      else if (tokenRaw > 0n) sell++;
    }

    const priceChangePct =
      prices.length >= 2 ? ((prices.at(-1)! - prices[0]) / prices[0]) * 100 : 0;
    const total = buy + sell;
    const buyRatio = total > 0 ? buy / total : 0;

    out.set(bucket, { priceChangePct, buyRatio, swapCount: total });
  }
  return out;
}

/** 30min rolling baseline：取该桶之前 N 个 1min 桶的 vol 均值，缺失桶记 0。 */
function buildFeatureSeries(
  bucketStats: BucketStat[],
  usdtBuckets: Map<number, { priceChangePct: number; buyRatio: number; swapCount: number }>,
  baselineMinutes: number
): Features[] {
  if (bucketStats.length === 0) return [];

  // 全时间序列（包含空桶），从首个有数据的桶开始
  const sorted = [...bucketStats].sort((a, b) => a.bucket - b.bucket);
  const firstBucket = sorted[0].bucket;
  const lastBucket = sorted[sorted.length - 1].bucket;

  const volByBucket = new Map<number, number>();
  const swapByBucket = new Map<number, number>();
  for (const b of sorted) {
    volByBucket.set(b.bucket, b.vol);
    swapByBucket.set(b.bucket, b.swap);
  }

  const series: Features[] = [];
  for (let t = firstBucket; t <= lastBucket; t += ONE_MIN_MS) {
    // 30min 滑动 baseline（不含当前桶）
    let sumVol = 0;
    for (let i = 1; i <= baselineMinutes; i++) {
      sumVol += volByBucket.get(t - i * ONE_MIN_MS) ?? 0;
    }
    const vol30minAvg = sumVol / baselineMinutes;
    const baselineCoverageMin = Math.floor((t - firstBucket) / ONE_MIN_MS);

    const u = usdtBuckets.get(t);
    series.push({
      bucketStart: t,
      vol1min: volByBucket.get(t) ?? 0,
      swapCount1min: swapByBucket.get(t) ?? 0,
      priceChangePct: u?.priceChangePct ?? 0,
      buyRatio: u?.buyRatio ?? 0,
      vol30minAvg,
      baselineCoverageMin,
      usdtSwapCount: u?.swapCount ?? 0,
    });
  }
  return series;
}

function decideAlert(f: Features, t: Thresholds): boolean {
  if (f.baselineCoverageMin < t.baselineMinutes) return false;
  if (f.vol30minAvg <= 0) return false;
  if (f.vol1min / f.vol30minAvg < t.volRatio) return false;
  if (f.swapCount1min < t.minSwapCount) return false;
  if (f.priceChangePct < t.priceChangePct) return false;
  if (f.buyRatio < t.buyRatio) return false;
  return true;
}

async function runForToken(
  token: string,
  fromMs: number,
  toMs: number,
  thresholds: Thresholds
): Promise<TokenReport> {
  const tokenLower = token.toLowerCase();
  const symbol = await getTokenSymbol(tokenLower);
  const pools = await getUsdtPools(tokenLower);
  const buckets = await getTokenBuckets(tokenLower, fromMs, toMs);
  const swaps = await getUsdtPoolSwaps(tokenLower, pools, fromMs, toMs);
  const usdtBuckets = aggregateUsdtBuckets(swaps);
  const series = buildFeatureSeries(buckets, usdtBuckets, thresholds.baselineMinutes);

  const alerts: Alert[] = [];
  const debugAroundMs = process.env.BT_DEBUG_AROUND_MS ? Number(process.env.BT_DEBUG_AROUND_MS) : 0;
  const cooldownMs = thresholds.cooldownMinutes * ONE_MIN_MS;
  let lastAlertMs = -Infinity;
  for (const f of series) {
    if (decideAlert(f, thresholds)) {
      // cooldown：同 token 上次告警以来未达冷静期 → 跳过
      if (cooldownMs > 0 && f.bucketStart - lastAlertMs < cooldownMs) {
        // skip
      } else {
        alerts.push({ cnTime: fmtCn(f.bucketStart), bucketStart: f.bucketStart, features: f });
        lastAlertMs = f.bucketStart;
      }
    }
    // 调试：dump 指定时间点 ± 10min 的所有桶特征
    if (debugAroundMs > 0 && Math.abs(f.bucketStart - debugAroundMs) <= 10 * ONE_MIN_MS) {
      const ratio = f.vol30minAvg > 0 ? (f.vol1min / f.vol30minAvg).toFixed(1) : "n/a";
      console.log(
        `  [DBG] ${fmtCn(f.bucketStart)} vol=$${f.vol1min.toFixed(2)} ×${ratio} | sc=${f.swapCount1min}(usdt=${f.usdtSwapCount}) | priceΔ=${f.priceChangePct.toFixed(2)}% | buy=${(f.buyRatio * 100).toFixed(0)}% | base30avg=$${f.vol30minAvg.toFixed(3)} cov=${f.baselineCoverageMin}min`
      );
    }
  }

  return {
    tokenAddress: tokenLower,
    symbol,
    scanFromCn: fmtCn(fromMs),
    scanToCn: fmtCn(toMs),
    totalBuckets: series.length,
    alerts,
    thresholds,
  };
}

async function runPhaseA(targetToken: string): Promise<void> {
  const thresholds = DEFAULT_THRESHOLDS;
  console.log(`\n=== Phase A: 单点验证 ===`);
  console.log(`Token: ${targetToken}`);
  console.log(`Thresholds:`, thresholds);

  // 拉该 token 全部历史时间范围
  const range = await getPool().query<{ min_ts: string; max_ts: string }>(
    `SELECT MIN(bucket_start) AS min_ts, MAX(bucket_start) AS max_ts
     FROM token_1min_stats
     WHERE LOWER(token_address) = LOWER($1)`,
    [targetToken]
  );
  const fromMs = Number(range.rows[0]?.min_ts);
  const toMs = Number(range.rows[0]?.max_ts);
  if (!fromMs || !toMs) {
    console.error(`没找到 token ${targetToken} 的桶数据`);
    return;
  }
  console.log(`数据范围: ${fmtCn(fromMs)} ~ ${fmtCn(toMs)}`);

  const report = await runForToken(targetToken, fromMs, toMs, thresholds);

  console.log(`\nSymbol: ${report.symbol}`);
  console.log(`Total minute buckets scanned: ${report.totalBuckets}`);
  console.log(`Total alerts: ${report.alerts.length}`);

  if (report.alerts.length > 0) {
    console.log(`\n首次告警: ${report.alerts[0].cnTime}`);
    console.log(`所有告警:`);
    for (const a of report.alerts) {
      const f = a.features;
      console.log(
        `  ${a.cnTime} | vol=$${f.vol1min.toFixed(1)} (×${(f.vol1min / f.vol30minAvg).toFixed(1)}) | swaps=${f.swapCount1min}(usdt=${f.usdtSwapCount}) | price=${f.priceChangePct.toFixed(2)}% | buy=${(f.buyRatio * 100).toFixed(0)}%`
      );
    }
  } else {
    console.log(`\n没有告警 — 阈值过严或者数据不足`);
  }

  // ground truth 对比
  if (targetToken.toLowerCase() === LAB_ADDR) {
    const gtMs = Date.parse("2026-05-01T18:37:00Z"); // CN 02:37 = UTC 18:37 前一天
    if (report.alerts.length > 0) {
      const firstMs = report.alerts[0].bucketStart;
      const lagMin = Math.round((firstMs - gtMs) / ONE_MIN_MS);
      console.log(
        `\n vs Ground truth (CN 02:37 = ${fmtCn(gtMs)}): 滞后 ${lagMin} 分钟 ${
          Math.abs(lagMin) <= 5 ? "✅ PASS" : "❌ FAIL（>5min）"
        }`
      );
    } else {
      console.log(`\n vs Ground truth: 没抓到 ❌ FAIL`);
    }
  }

  // 写完整 JSON
  const outFile = `backtest-phaseA-${targetToken.slice(0, 10)}.json`;
  writeFileSync(outFile, JSON.stringify(report, null, 2));
  console.log(`\n详细报告: ${outFile}`);
}

async function runPhaseB(): Promise<void> {
  const thresholds = DEFAULT_THRESHOLDS;
  console.log(`\n=== Phase B: 全 303 token 假阳测试 ===`);
  console.log(`Thresholds:`, thresholds);

  const range = await getPool().query<{ min_ts: string; max_ts: string }>(
    `SELECT MIN(bucket_start) AS min_ts, MAX(bucket_start) AS max_ts FROM token_1min_stats`
  );
  const fromMs = Number(range.rows[0]?.min_ts);
  const toMs = Number(range.rows[0]?.max_ts);
  console.log(`扫描时间范围: ${fmtCn(fromMs)} ~ ${fmtCn(toMs)}`);

  const tokens = await getPool().query<{ contract_address: string; symbol: string }>(
    `SELECT contract_address, symbol FROM binance_bsc_tokens ORDER BY symbol`
  );
  console.log(`Token 总数: ${tokens.rows.length}`);

  const allReports: TokenReport[] = [];
  let scanned = 0;
  for (const row of tokens.rows) {
    scanned++;
    const r = await runForToken(row.contract_address, fromMs, toMs, thresholds);
    if (r.alerts.length > 0) {
      allReports.push(r);
      console.log(
        `[${scanned}/${tokens.rows.length}] ${r.symbol.padEnd(10)} ${r.alerts.length} alerts | first=${r.alerts[0].cnTime}`
      );
    } else if (scanned % 50 === 0) {
      console.log(`[${scanned}/${tokens.rows.length}] ... (no alert so far for this batch)`);
    }
  }

  // 排序：按告警数倒序
  allReports.sort((a, b) => b.alerts.length - a.alerts.length);

  const totalAlerts = allReports.reduce((sum, r) => sum + r.alerts.length, 0);
  const labReport = allReports.find((r) => r.tokenAddress === LAB_ADDR);

  console.log(`\n=== 汇总 ===`);
  console.log(`阈值: vol×${thresholds.volRatio} | swaps≥${thresholds.minSwapCount} | price≥${thresholds.priceChangePct}% | buy≥${(thresholds.buyRatio * 100).toFixed(0)}%`);
  console.log(`触发 token 数: ${allReports.length}`);
  console.log(`总告警数: ${totalAlerts}`);
  console.log(`LAB 是否命中: ${labReport ? `✅ 首次 ${labReport.alerts[0].cnTime}` : "❌ 未命中"}`);
  console.log(`\nTop 告警 token:`);
  for (const r of allReports.slice(0, 20)) {
    console.log(
      `  ${r.symbol.padEnd(10)} ${String(r.alerts.length).padStart(3)} alerts | first=${r.alerts[0].cnTime}`
    );
  }

  const outFile = `backtest-phaseB.json`;
  writeFileSync(
    outFile,
    JSON.stringify(
      {
        thresholds,
        scanFromCn: fmtCn(fromMs),
        scanToCn: fmtCn(toMs),
        totalAlerts,
        triggerTokens: allReports.length,
        labHit: !!labReport,
        labFirstAlert: labReport?.alerts[0]?.cnTime ?? null,
        reports: allReports,
      },
      null,
      2
    )
  );
  console.log(`\n详细报告: ${outFile}`);
}

async function main() {
  const phase = (process.argv[2] || "A").toUpperCase();
  const argToken = process.argv[3];

  try {
    if (phase === "A") {
      await runPhaseA(argToken || LAB_ADDR);
    } else if (phase === "B") {
      await runPhaseB();
    } else {
      console.error("用法: bun scripts/backtest-launch-detector.ts A|B [token_address]");
      process.exit(1);
    }
  } finally {
    await closeDatabase();
  }
}

main().catch((err) => {
  console.error("[Backtest] Fatal:", err);
  process.exit(1);
});
