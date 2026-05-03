#!/usr/bin/env bun
/**
 * 单 worker 重跑指定 [fromBlock, toBlock] 区间，用于补救 backfill 失败的 batches。
 *
 * 用法：
 *   bun scripts/backfill-range.ts <fromBlock> <toBlock>
 *   bun scripts/backfill-range.ts 90613689 90614688
 *
 * 也可以一次传多组，逗号分隔：
 *   bun scripts/backfill-range.ts 90613689-90614688,91234000-91234999
 *
 * 容器内：
 *   docker compose run --rm --no-deps radar bun scripts/backfill-range.ts 90613689-90614688
 *
 * 单 worker 顺序跑，规避并发 UPSERT 死锁。swaps 表靠 uq_swaps_dedup 去重，
 * 重跑安全。跑完后建议再跑一次 backfill 30d 让 buckets 重建（或手动调用
 * rebuildBucketsFromSwaps）。
 */

import "dotenv/config";
import { createPublicClient, http, parseAbiItem, type Log } from "viem";
import { bsc } from "viem/chains";
import { initSchema, closeDatabase, getPool } from "../src/db/index.js";
import { CONTRACTS } from "../src/config/contracts.js";
import { initPriceService } from "../src/core/price-service.js";
import { startTokenTracker, stopTokenTracker } from "../src/token-tracker/tracker.js";
import {
  processV3SwapLog,
  processV4SwapLog,
  processPcsV4ClSwapLog,
  processV2BnbPriceSwap,
} from "../src/core/swap-listener.js";
import { newBatchBuffer, flushBatchBuffer } from "../src/db/queries.js";

const HTTP_URL = process.env.BSC_HTTP_URL || "http://151.123.172.62:81";

const V3_SWAP = parseAbiItem(
  "event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)"
);
const PANCAKE_V3_SWAP = parseAbiItem(
  "event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick, uint128 protocolFeesToken0, uint128 protocolFeesToken1)"
);
const V4_SWAP = parseAbiItem(
  "event Swap(bytes32 indexed id, address indexed sender, int128 amount0, int128 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick, uint24 fee)"
);
const PCS_V4_CL_SWAP = parseAbiItem(
  "event Swap(bytes32 indexed id, address indexed sender, int128 amount0, int128 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick, uint24 fee, uint16 protocolFee)"
);
const V2_SWAP = parseAbiItem(
  "event Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)"
);

const httpClient = createPublicClient({
  chain: bsc,
  transport: http(HTTP_URL, { timeout: 30_000, retryCount: 3 }),
  batch: { multicall: true },
});

interface Range {
  from: bigint;
  to: bigint;
}

function parseRanges(arg: string): Range[] {
  return arg.split(",").map((r) => {
    const m = r.trim().split("-");
    if (m.length === 2) return { from: BigInt(m[0]), to: BigInt(m[1]) };
    throw new Error(`Invalid range: ${r}`);
  });
}

async function processRange(range: Range, v4PoolManager: `0x${string}`): Promise<void> {
  const { from, to } = range;
  console.log(`[Retry] processing ${from}-${to}`);

  const [fb, tb] = await Promise.all([
    httpClient.getBlock({ blockNumber: from }),
    httpClient.getBlock({ blockNumber: to }),
  ]);
  const fromTs = Number(fb.timestamp) * 1000;
  const toTs = Number(tb.timestamp) * 1000;

  const pcsV4ClPoolManager = CONTRACTS.bsc.pancakeswapV4ClPoolManager as `0x${string}`;
  const bnbPricePool = CONTRACTS.bsc.bnbPricePool as `0x${string}`;
  const [v3, pancake, v4, pcsV4Cl, bnbV2] = await Promise.all([
    httpClient.getLogs({ fromBlock: from, toBlock: to, event: V3_SWAP }) as unknown as Promise<Log[]>,
    httpClient.getLogs({ fromBlock: from, toBlock: to, event: PANCAKE_V3_SWAP }) as unknown as Promise<Log[]>,
    httpClient.getLogs({
      address: v4PoolManager,
      fromBlock: from,
      toBlock: to,
      event: V4_SWAP,
    }) as unknown as Promise<Log[]>,
    httpClient.getLogs({
      address: pcsV4ClPoolManager,
      fromBlock: from,
      toBlock: to,
      event: PCS_V4_CL_SWAP,
    }) as unknown as Promise<Log[]>,
    httpClient.getLogs({
      address: bnbPricePool,
      fromBlock: from,
      toBlock: to,
      event: V2_SWAP,
    }) as unknown as Promise<Log[]>,
  ]);

  const buffer = newBatchBuffer();
  const tsForLog = (log: Log): number => {
    const blockNum = Number(log.blockNumber ?? from);
    const span = Number(to - from) || 1;
    const offset = blockNum - Number(from);
    return Math.round(fromTs + (offset / span) * (toTs - fromTs));
  };

  let processed = 0;
  let errors = 0;
  for (const log of v3) {
    try {
      await processV3SwapLog(log, "bsc", "uniswap-v3", tsForLog(log), buffer);
      processed++;
    } catch {
      errors++;
    }
  }
  for (const log of pancake) {
    try {
      await processV3SwapLog(log, "bsc", "pancakeswap-v3", tsForLog(log), buffer);
      processed++;
    } catch {
      errors++;
    }
  }
  for (const log of v4) {
    try {
      await processV4SwapLog(log, "bsc", tsForLog(log), buffer);
      processed++;
    } catch {
      errors++;
    }
  }
  for (const log of pcsV4Cl) {
    try {
      await processPcsV4ClSwapLog(log, "bsc", tsForLog(log), buffer);
      processed++;
    } catch {
      errors++;
    }
  }
  for (const log of bnbV2) {
    try {
      await processV2BnbPriceSwap(log, "bsc", tsForLog(log), buffer);
      processed++;
    } catch {
      errors++;
    }
  }

  // 单 worker，flush 不会和别人 deadlock；如果还失败就 retry 几次
  const MAX_RETRY = 3;
  for (let i = 0; i < MAX_RETRY; i++) {
    try {
      await flushBatchBuffer(buffer);
      break;
    } catch (err) {
      const msg = (err as Error).message;
      if (i === MAX_RETRY - 1) throw err;
      console.warn(`[Retry] flush attempt ${i + 1} failed (${msg}), retrying`);
      await new Promise((res) => setTimeout(res, 1000 * (i + 1)));
    }
  }

  console.log(
    `[Retry] ${from}-${to}: logs=${v3.length + pancake.length + v4.length} ok=${processed} err=${errors}`
  );
}

async function main() {
  const arg = process.argv[2];
  if (!arg) {
    console.error("用法: bun scripts/backfill-range.ts <fromBlock>-<toBlock>[,fromBlock-toBlock,...]");
    process.exit(1);
  }
  const ranges = parseRanges(arg);
  console.log(`[Retry] ${ranges.length} ranges to process`);

  await initSchema();
  await startTokenTracker();
  await initPriceService();

  const v4PoolManager = CONTRACTS.bsc.uniswapV4PoolManager as `0x${string}`;
  for (const r of ranges) {
    try {
      await processRange(r, v4PoolManager);
    } catch (err) {
      console.error(`[Retry] range ${r.from}-${r.to} failed:`, (err as Error).message);
    }
  }

  // 重建受影响时间范围的 buckets（事务 + LOCK + ON CONFLICT 防 stream 竞争）
  if (ranges.length > 0) {
    const minFrom = ranges.reduce((m, r) => (r.from < m ? r.from : m), ranges[0].from);
    const blkMin = await httpClient.getBlock({ blockNumber: minFrom });
    const rangeStartMs = Number(blkMin.timestamp) * 1000 - 60_000;
    console.log(`[Retry] 重建 buckets from ${new Date(rangeStartMs).toISOString()}`);

    const c1 = await getPool().connect();
    try {
      await c1.query("BEGIN");
      await c1.query("LOCK TABLE pool_1min_stats IN EXCLUSIVE MODE");
      await c1.query(`DELETE FROM pool_1min_stats WHERE bucket_start >= $1`, [rangeStartMs]);
      await c1.query(
        `INSERT INTO pool_1min_stats (pool_address, chain, bucket_start, total_fees_usd, total_volume_usd, swap_count)
         SELECT pool_address, chain, (timestamp/60000)*60000, COALESCE(SUM(fee_usd),0), COALESCE(SUM(volume_usd),0), COUNT(*)::int
         FROM swaps WHERE timestamp >= $1
         GROUP BY pool_address, chain, (timestamp/60000)*60000
         ON CONFLICT (pool_address, chain, bucket_start) DO UPDATE SET
           total_fees_usd = EXCLUDED.total_fees_usd,
           total_volume_usd = EXCLUDED.total_volume_usd,
           swap_count = EXCLUDED.swap_count`,
        [rangeStartMs]
      );
      await c1.query("COMMIT");
    } catch (e) {
      await c1.query("ROLLBACK").catch(() => {});
      throw e;
    } finally {
      c1.release();
    }

    const c2 = await getPool().connect();
    try {
      await c2.query("BEGIN");
      await c2.query("LOCK TABLE token_1min_stats IN EXCLUSIVE MODE");
      await c2.query(`DELETE FROM token_1min_stats WHERE bucket_start >= $1`, [rangeStartMs]);
      await c2.query(
        `WITH all_pools AS (
           SELECT address AS pool_id, chain, LOWER(token0) AS t0, LOWER(token1) AS t1 FROM pools
           UNION ALL
           SELECT pool_id, chain, LOWER(currency0), LOWER(currency1) FROM v4_pools
         ),
         pool_target AS (
           SELECT ap.pool_id, ap.chain, LOWER(bt.contract_address) AS target_token
           FROM all_pools ap
           JOIN binance_bsc_tokens bt
             ON ap.t0 = LOWER(bt.contract_address) OR ap.t1 = LOWER(bt.contract_address)
         )
         INSERT INTO token_1min_stats (token_address, chain, bucket_start, total_volume_usd, total_fees_usd, swap_count)
         SELECT pt.target_token, s.chain, (s.timestamp/60000)*60000,
                COALESCE(SUM(s.volume_usd),0), COALESCE(SUM(s.fee_usd),0), COUNT(*)::int
         FROM swaps s
         JOIN pool_target pt ON pt.pool_id = s.pool_address AND pt.chain = s.chain
         WHERE s.timestamp >= $1
         GROUP BY pt.target_token, s.chain, (s.timestamp/60000)*60000
         ON CONFLICT (token_address, chain, bucket_start) DO UPDATE SET
           total_volume_usd = EXCLUDED.total_volume_usd,
           total_fees_usd = EXCLUDED.total_fees_usd,
           swap_count = EXCLUDED.swap_count`,
        [rangeStartMs]
      );
      await c2.query("COMMIT");
    } catch (e) {
      await c2.query("ROLLBACK").catch(() => {});
      throw e;
    } finally {
      c2.release();
    }
    console.log("[Retry] buckets 重建完成 ✅");
  }

  stopTokenTracker();
  await closeDatabase();
  process.exit(0);
}

main().catch((err) => {
  console.error("[Retry] Fatal:", err);
  process.exit(1);
});
