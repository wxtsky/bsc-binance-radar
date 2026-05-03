#!/usr/bin/env bun
/**
 * 把 backtest-phaseB.json 里的告警按 token 聚合，结合 swaps 表的 30d 价格曲线，
 * 输出"启动 Token Top 榜"：
 *   - 总告警次数
 *   - 首次/末次告警时间
 *   - 30d 价格涨幅（最高价 / 最低价）
 *   - 30d 总成交量
 *   - 启动后 24h 价格涨幅（首次告警起算）
 *
 * 用法：
 *   DATABASE_URL=... bun scripts/analyze-launch-tops.ts [phaseB.json] [min_alerts]
 *   默认读 ./backtest-phaseB.json，min_alerts=3
 */

import "dotenv/config";
import { readFileSync, writeFileSync } from "fs";
import { getPool, closeDatabase } from "../src/db/index.js";

const USDT = "0x55d398326f99059ff775485246999027b3197955";
const ONE_HOUR_MS = 60 * 60 * 1000;
const ONE_DAY_MS = 24 * ONE_HOUR_MS;

interface PhaseBReport {
  thresholds: any;
  totalAlerts: number;
  triggerTokens: number;
  reports: TokenReport[];
}
interface TokenReport {
  tokenAddress: string;
  symbol: string;
  alerts: Alert[];
}
interface Alert {
  cnTime: string;
  bucketStart: number;
  features: any;
}

interface PriceStats {
  firstPrice: number;
  lastPrice: number;
  minPrice: number;
  maxPrice: number;
  totalVol: number;
}

async function getTokenPriceStats(token: string): Promise<PriceStats | null> {
  const res = await getPool().query<{
    first_price: string | null;
    last_price: string | null;
    min_price: string | null;
    max_price: string | null;
    total_vol: string | null;
  }>(
    `WITH usdt_pools AS (
       SELECT address, LOWER(token0) AS t0, LOWER(token1) AS t1
       FROM pools
       WHERE chain='bsc'
         AND (LOWER(token0) = LOWER($1) OR LOWER(token1) = LOWER($1))
         AND (LOWER(token0) = $2 OR LOWER(token1) = $2)
     ),
     swap_prices AS (
       SELECT
         s.timestamp,
         CASE WHEN up.t0 = $2 THEN abs(s.amount0::numeric)/NULLIF(abs(s.amount1::numeric),0)
              ELSE abs(s.amount1::numeric)/NULLIF(abs(s.amount0::numeric),0) END AS price,
         s.volume_usd
       FROM swaps s
       JOIN usdt_pools up ON up.address = s.pool_address
       WHERE s.volume_usd > 1
     )
     SELECT
       (SELECT price::text FROM swap_prices ORDER BY timestamp ASC LIMIT 1) AS first_price,
       (SELECT price::text FROM swap_prices ORDER BY timestamp DESC LIMIT 1) AS last_price,
       MIN(price)::text AS min_price,
       MAX(price)::text AS max_price,
       SUM(volume_usd)::text AS total_vol
     FROM swap_prices`,
    [token, USDT]
  );
  const r = res.rows[0];
  if (!r || !r.first_price) return null;
  return {
    firstPrice: Number(r.first_price),
    lastPrice: Number(r.last_price),
    minPrice: Number(r.min_price),
    maxPrice: Number(r.max_price),
    totalVol: Number(r.total_vol) || 0,
  };
}

async function getPriceAt(token: string, ts: number): Promise<number | null> {
  const res = await getPool().query<{ price: string }>(
    `WITH usdt_pools AS (
       SELECT address, LOWER(token0) AS t0
       FROM pools
       WHERE chain='bsc'
         AND (LOWER(token0) = LOWER($1) OR LOWER(token1) = LOWER($1))
         AND (LOWER(token0) = $2 OR LOWER(token1) = $2)
     )
     SELECT
       (CASE WHEN up.t0 = $2 THEN abs(s.amount0::numeric)/NULLIF(abs(s.amount1::numeric),0)
             ELSE abs(s.amount1::numeric)/NULLIF(abs(s.amount0::numeric),0) END)::text AS price
     FROM swaps s
     JOIN usdt_pools up ON up.address = s.pool_address
     WHERE s.timestamp BETWEEN $3 AND $4 AND s.volume_usd > 1
     ORDER BY ABS(s.timestamp - $5) ASC
     LIMIT 1`,
    [token, USDT, ts - ONE_HOUR_MS, ts + ONE_HOUR_MS, ts]
  );
  return res.rows[0] ? Number(res.rows[0].price) : null;
}

interface AnalyzedToken {
  symbol: string;
  tokenAddress: string;
  alerts: number;
  firstAlertCn: string;
  lastAlertCn: string;
  firstAlertMs: number;
  totalVol30dUsd: number;
  minPrice: number;
  maxPrice: number;
  pumpPct: number; // 30d max/min - 1
  firstPrice: number;
  lastPrice: number;
  netReturnPct: number; // 30d 净涨幅
  priceAtFirstAlert: number | null;
  priceAfter24h: number | null;
  alert24hReturnPct: number | null;
}

async function main() {
  const file = process.argv[2] || "backtest-phaseB.json";
  const minAlerts = Number(process.argv[3]) || 3;
  console.log(`读取 ${file}，过滤 alerts >= ${minAlerts}`);

  const data: PhaseBReport = JSON.parse(readFileSync(file, "utf-8"));
  console.log(`总 token 触发: ${data.triggerTokens}, 总 alerts: ${data.totalAlerts}`);

  const candidates = data.reports.filter((r) => r.alerts.length >= minAlerts);
  console.log(`候选 token (alerts >= ${minAlerts}): ${candidates.length}`);

  const analyzed: AnalyzedToken[] = [];
  let n = 0;
  for (const r of candidates) {
    n++;
    const stats = await getTokenPriceStats(r.tokenAddress);
    const firstAlert = r.alerts[0];
    const priceAtAlert = await getPriceAt(r.tokenAddress, firstAlert.bucketStart);
    const priceAfter = await getPriceAt(r.tokenAddress, firstAlert.bucketStart + ONE_DAY_MS);
    const after24hReturn =
      priceAtAlert && priceAfter ? ((priceAfter - priceAtAlert) / priceAtAlert) * 100 : null;
    analyzed.push({
      symbol: r.symbol,
      tokenAddress: r.tokenAddress,
      alerts: r.alerts.length,
      firstAlertCn: r.alerts[0].cnTime,
      lastAlertCn: r.alerts.at(-1)!.cnTime,
      firstAlertMs: firstAlert.bucketStart,
      totalVol30dUsd: stats?.totalVol ?? 0,
      minPrice: stats?.minPrice ?? 0,
      maxPrice: stats?.maxPrice ?? 0,
      pumpPct: stats ? (stats.maxPrice / stats.minPrice - 1) * 100 : 0,
      firstPrice: stats?.firstPrice ?? 0,
      lastPrice: stats?.lastPrice ?? 0,
      netReturnPct: stats ? (stats.lastPrice / stats.firstPrice - 1) * 100 : 0,
      priceAtFirstAlert: priceAtAlert,
      priceAfter24h: priceAfter,
      alert24hReturnPct: after24hReturn,
    });
    if (n % 10 === 0) console.log(`  [${n}/${candidates.length}]`);
  }

  // 按 30d 最大涨幅排序（最值得关注的真启动）
  analyzed.sort((a, b) => b.pumpPct - a.pumpPct);

  // 输出
  console.log(`\n=== Top 启动 Token（按 30d 最大涨幅排）===`);
  console.log("symbol | alerts | 30d涨幅 | 净涨 | 24h-后涨幅 | 总vol | 首次告警(北京)");
  for (const t of analyzed.slice(0, 50)) {
    console.log(
      `${t.symbol.padEnd(10)} ${String(t.alerts).padStart(3)} | +${t.pumpPct.toFixed(0)}% | ${t.netReturnPct >= 0 ? "+" : ""}${t.netReturnPct.toFixed(0)}% | ${
        t.alert24hReturnPct === null
          ? "n/a"
          : (t.alert24hReturnPct >= 0 ? "+" : "") + t.alert24hReturnPct.toFixed(1) + "%"
      } | $${(t.totalVol30dUsd / 1e6).toFixed(2)}M | ${t.firstAlertCn}`
    );
  }

  writeFileSync(
    "launch-tops-analysis.json",
    JSON.stringify({ minAlerts, candidates: analyzed }, null, 2)
  );
  console.log(`\n详细报告: launch-tops-analysis.json`);
  await closeDatabase();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
