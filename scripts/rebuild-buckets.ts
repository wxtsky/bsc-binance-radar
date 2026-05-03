#!/usr/bin/env bun
/**
 * 从 swaps 表完整重建 pool_1min_stats / token_1min_stats。
 *
 * 用途：多 shard backfill 跑完后，所有 shard 都设了 BF_SKIP_REBUILD=1，
 * 用这个脚本统一重建一次（事务 + LOCK + ON CONFLICT，跟 stream 并发安全）。
 *
 * 用法：
 *   docker compose run --rm --no-deps radar bun scripts/rebuild-buckets.ts 90d
 *   docker compose run --rm --no-deps radar bun scripts/rebuild-buckets.ts 30d
 *   docker compose run --rm --no-deps radar bun scripts/rebuild-buckets.ts ALL  # 重建全表
 */

import "dotenv/config";
import { getPool, initSchema, closeDatabase } from "../src/db/index.js";

function parseDuration(s: string | undefined): number | null {
  if (!s) return 24;
  if (s.toUpperCase() === "ALL") return null;
  const m = /^(\d+)([hd])?$/i.exec(s);
  if (!m) throw new Error(`Invalid duration: ${s}（应为 24, 24h, 30d, ALL）`);
  const n = Number(m[1]);
  return (m[2] || "h").toLowerCase() === "d" ? n * 24 : n;
}

async function rebuildBucketsFromSwaps(rangeStartMs: number | null): Promise<void> {
  // 用事务 + LOCK + ON CONFLICT DO UPDATE 防止 stream 在 DELETE 与 INSERT 之间写入新 bucket 触发 PK 冲突
  const startCondPool = rangeStartMs !== null ? "WHERE bucket_start >= $1" : "";
  const startCondSwap = rangeStartMs !== null ? "WHERE timestamp >= $1" : "";
  const params = rangeStartMs !== null ? [rangeStartMs] : [];

  console.log(
    `[Rebuild] 重建 pool_1min_stats（事务 + LOCK） range=${rangeStartMs !== null ? new Date(rangeStartMs).toISOString() : "ALL"}`
  );
  const t0 = Date.now();
  const c1 = await getPool().connect();
  try {
    await c1.query("BEGIN");
    await c1.query("LOCK TABLE pool_1min_stats IN EXCLUSIVE MODE");
    if (rangeStartMs !== null) {
      await c1.query(`DELETE FROM pool_1min_stats ${startCondPool}`, params);
    } else {
      await c1.query(`TRUNCATE pool_1min_stats`);
    }
    const r1 = await c1.query(
      `INSERT INTO pool_1min_stats (pool_address, chain, bucket_start, total_fees_usd, total_volume_usd, swap_count)
       SELECT
         pool_address,
         chain,
         (timestamp / 60000) * 60000 AS bucket_start,
         COALESCE(SUM(fee_usd), 0),
         COALESCE(SUM(volume_usd), 0),
         COUNT(*)::int
       FROM swaps
       ${startCondSwap}
       GROUP BY pool_address, chain, (timestamp / 60000) * 60000
       ON CONFLICT (pool_address, chain, bucket_start) DO UPDATE SET
         total_fees_usd = EXCLUDED.total_fees_usd,
         total_volume_usd = EXCLUDED.total_volume_usd,
         swap_count = EXCLUDED.swap_count`,
      params
    );
    await c1.query("COMMIT");
    console.log(`[Rebuild]   pool_1min_stats inserted ${r1.rowCount} rows in ${((Date.now() - t0) / 1000).toFixed(0)}s`);
  } catch (e) {
    await c1.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    c1.release();
  }

  console.log("[Rebuild] 重建 token_1min_stats（事务 + LOCK）");
  const t1 = Date.now();
  const c2 = await getPool().connect();
  try {
    await c2.query("BEGIN");
    await c2.query("LOCK TABLE token_1min_stats IN EXCLUSIVE MODE");
    if (rangeStartMs !== null) {
      await c2.query(`DELETE FROM token_1min_stats WHERE bucket_start >= $1`, params);
    } else {
      await c2.query(`TRUNCATE token_1min_stats`);
    }
    const r2 = await c2.query(
      `WITH all_pools AS (
         SELECT address AS pool_id, chain, LOWER(token0) AS t0, LOWER(token1) AS t1 FROM pools
         UNION ALL
         SELECT pool_id, chain, LOWER(currency0), LOWER(currency1) FROM v4_pools
       ),
       pool_target AS (
         SELECT
           ap.pool_id,
           ap.chain,
           LOWER(bt.contract_address) AS target_token
         FROM all_pools ap
         JOIN binance_bsc_tokens bt
           ON ap.t0 = LOWER(bt.contract_address)
           OR ap.t1 = LOWER(bt.contract_address)
       )
       INSERT INTO token_1min_stats (token_address, chain, bucket_start, total_volume_usd, total_fees_usd, swap_count)
       SELECT
         pt.target_token,
         s.chain,
         (s.timestamp / 60000) * 60000 AS bucket_start,
         COALESCE(SUM(s.volume_usd), 0),
         COALESCE(SUM(s.fee_usd), 0),
         COUNT(*)::int
       FROM swaps s
       JOIN pool_target pt ON pt.pool_id = s.pool_address AND pt.chain = s.chain
       ${rangeStartMs !== null ? "WHERE s.timestamp >= $1" : ""}
       GROUP BY pt.target_token, s.chain, (s.timestamp / 60000) * 60000
       ON CONFLICT (token_address, chain, bucket_start) DO UPDATE SET
         total_volume_usd = EXCLUDED.total_volume_usd,
         total_fees_usd = EXCLUDED.total_fees_usd,
         swap_count = EXCLUDED.swap_count`,
      params
    );
    await c2.query("COMMIT");
    console.log(`[Rebuild]   token_1min_stats inserted ${r2.rowCount} rows in ${((Date.now() - t1) / 1000).toFixed(0)}s`);
  } catch (e) {
    await c2.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    c2.release();
  }
  console.log("[Rebuild] buckets 重建完成 ✅");
}

async function main() {
  const HOURS = parseDuration(process.argv[2]);
  await initSchema();

  let rangeStartMs: number | null = null;
  if (HOURS !== null) {
    // 用当前时间 - HOURS 算 rangeStartMs（不需要 latest block，因为 swaps 表的 timestamp 已是 ms）
    rangeStartMs = Date.now() - HOURS * 3600 * 1000 - 60_000;
  }
  await rebuildBucketsFromSwaps(rangeStartMs);
  await closeDatabase();
  process.exit(0);
}

main().catch((err) => {
  console.error("[Rebuild] Fatal:", err);
  process.exit(1);
});
