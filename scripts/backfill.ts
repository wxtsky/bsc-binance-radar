#!/usr/bin/env bun
/**
 * 历史 swap 回补，让 detector 立刻有 baseline。
 *
 * 优化版：
 * - 批量 PG upsert（同 batch 内 (token, bucket) merge 后一次性 multi-row VALUES）
 * - batch 间并发 fetch + process（CONCURRENCY 个 worker）
 * - 块时间动态 probe，timestamp 线性插值
 *
 * 用法：
 *   bun scripts/backfill.ts [hours] [concurrency]
 *   bun scripts/backfill.ts 24 6
 *
 * 容器内：
 *   docker compose run --rm --no-deps radar bun scripts/backfill.ts 24
 */

import "dotenv/config";
import { createPublicClient, http, parseAbiItem, type Log } from "viem";
import { bsc } from "viem/chains";
import { getPool, initSchema, closeDatabase } from "../src/db/index.js";
import { CONTRACTS } from "../src/config/contracts.js";
import { initPriceService } from "../src/core/price-service.js";
import { startTokenTracker, stopTokenTracker } from "../src/token-tracker/tracker.js";
import { processV3SwapLog, processV4SwapLog } from "../src/core/swap-listener.js";
import { newBatchBuffer, flushBatchBuffer } from "../src/db/queries.js";

const HOURS = Number(process.argv[2]) || 24;
const CONCURRENCY = Number(process.argv[3]) || 6;
const BLOCKS_PER_BATCH = 1000n;

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

const httpClient = createPublicClient({
  chain: bsc,
  transport: http(HTTP_URL, { timeout: 30_000, retryCount: 2 }),
  batch: { multicall: true },
});

interface BatchJob {
  from: bigint;
  to: bigint;
  index: number;
}

async function processOneBatch(
  job: BatchJob,
  v4PoolManager: `0x${string}`
): Promise<{ logs: number; processed: number; errors: number }> {
  const { from, to } = job;

  let fromTs: number;
  let toTs: number;
  try {
    const [fb, tb] = await Promise.all([
      httpClient.getBlock({ blockNumber: from }),
      httpClient.getBlock({ blockNumber: to }),
    ]);
    fromTs = Number(fb.timestamp) * 1000;
    toTs = Number(tb.timestamp) * 1000;
  } catch (err) {
    throw new Error(`getBlock ${from}/${to}: ${(err as Error).message}`);
  }

  let v3: Log[] = [];
  let pancake: Log[] = [];
  let v4: Log[] = [];
  try {
    [v3, pancake, v4] = await Promise.all([
      httpClient.getLogs({ fromBlock: from, toBlock: to, event: V3_SWAP }) as unknown as Promise<Log[]>,
      httpClient.getLogs({ fromBlock: from, toBlock: to, event: PANCAKE_V3_SWAP }) as unknown as Promise<Log[]>,
      httpClient.getLogs({
        address: v4PoolManager,
        fromBlock: from,
        toBlock: to,
        event: V4_SWAP,
      }) as unknown as Promise<Log[]>,
    ]);
  } catch (err) {
    throw new Error(`getLogs ${from}-${to}: ${(err as Error).message}`);
  }

  const logCount = v3.length + pancake.length + v4.length;
  const buffer = newBatchBuffer();

  const tsForLog = (log: Log): number => {
    const blockNum = Number(log.blockNumber ?? from);
    const range = Number(to - from) || 1;
    const offset = blockNum - Number(from);
    return Math.round(fromTs + (offset / range) * (toTs - fromTs));
  };

  let processed = 0;
  let errors = 0;

  // process 每条 log（getPoolInfo/getV4PoolInfo cache 受益），写到内存 buffer
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

  // 批量 flush 到 PG
  await flushBatchBuffer(buffer);

  return { logs: logCount, processed, errors };
}

async function main() {
  console.log(`[Backfill] ${HOURS}h, concurrency=${CONCURRENCY}, batch=${BLOCKS_PER_BATCH} blocks via ${HTTP_URL}`);

  await initSchema();
  await startTokenTracker();
  await initPriceService();

  const latest = await httpClient.getBlockNumber();

  // probe 真实块时间
  const PROBE_DISTANCE = 1000n;
  const [latestBlk, probeBlk] = await Promise.all([
    httpClient.getBlock({ blockNumber: latest }),
    httpClient.getBlock({ blockNumber: latest - PROBE_DISTANCE }),
  ]);
  const realBlockTimeS =
    (Number(latestBlk.timestamp) - Number(probeBlk.timestamp)) / Number(PROBE_DISTANCE);
  const TOTAL_BLOCKS = BigInt(Math.ceil((HOURS * 3600) / realBlockTimeS));
  const start = latest - TOTAL_BLOCKS;
  console.log(
    `[Backfill] block time ${realBlockTimeS.toFixed(3)}s; ${TOTAL_BLOCKS} blocks; range ${start} → ${latest}`
  );

  // 幂等保护：清掉同时间范围已存在的桶 / swap
  const latestTsMs = Number(latestBlk.timestamp) * 1000;
  const rangeStartMs = latestTsMs - HOURS * 3600 * 1000 - 60_000;
  console.log(`[Backfill] Clearing buckets/swaps from ${new Date(rangeStartMs).toISOString()}`);
  await getPool().query(`DELETE FROM token_1min_stats WHERE bucket_start >= $1`, [rangeStartMs]);
  await getPool().query(`DELETE FROM pool_1min_stats WHERE bucket_start >= $1`, [rangeStartMs]);
  await getPool().query(`DELETE FROM swaps WHERE timestamp >= $1`, [rangeStartMs]);

  // 切 jobs
  const jobs: BatchJob[] = [];
  let idx = 0;
  for (let from = start; from < latest; from += BLOCKS_PER_BATCH) {
    const to = from + BLOCKS_PER_BATCH - 1n > latest ? latest : from + BLOCKS_PER_BATCH - 1n;
    jobs.push({ from, to, index: idx++ });
  }
  console.log(`[Backfill] ${jobs.length} batches`);

  const v4PoolManager = CONTRACTS.bsc.uniswapV4PoolManager as `0x${string}`;

  // worker pool
  const queue = [...jobs];
  const t0 = Date.now();
  let totalLogs = 0;
  let totalProcessed = 0;
  let totalErrors = 0;
  let completedBatches = 0;

  async function worker() {
    while (queue.length > 0) {
      const job = queue.shift();
      if (!job) break;
      try {
        const r = await processOneBatch(job, v4PoolManager);
        totalLogs += r.logs;
        totalProcessed += r.processed;
        totalErrors += r.errors;
      } catch (err) {
        console.error(`[Backfill] batch ${job.index} (${job.from}-${job.to}) failed:`, (err as Error).message);
        totalErrors++;
      }
      completedBatches++;
      if (completedBatches % 20 === 0 || completedBatches === jobs.length) {
        const pct = Math.round((completedBatches * 100) / jobs.length);
        const elapsedS = ((Date.now() - t0) / 1000).toFixed(0);
        const rate = (completedBatches / parseFloat(elapsedS)).toFixed(2);
        console.log(
          `[Backfill] ${pct}% ${completedBatches}/${jobs.length} | logs=${totalLogs} ok=${totalProcessed} err=${totalErrors} | ${elapsedS}s ${rate}batch/s`
        );
      }
    }
  }

  await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));

  const elapsedS = ((Date.now() - t0) / 1000).toFixed(0);
  console.log(
    `[Backfill] Done in ${elapsedS}s — fetched ${totalLogs}, processed ${totalProcessed}, errors ${totalErrors}`
  );

  stopTokenTracker();
  await closeDatabase();
  process.exit(0);
}

main().catch((err) => {
  console.error("[Backfill] Fatal:", err);
  process.exit(1);
});
