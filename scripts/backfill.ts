#!/usr/bin/env bun
/**
 * 历史 swap 回补，让 detector 立刻有 baseline。
 *
 * 优化版：
 * - 批量 PG upsert（同 batch 内 (token, bucket) merge 后一次性 multi-row VALUES）
 * - batch 间并发 fetch + process（CONCURRENCY 个 worker）
 * - 块时间动态 probe，timestamp 线性插值
 * - swaps 表 ON CONFLICT DO NOTHING（uq_swaps_dedup）防 stream + backfill 双写重复
 * - backfill 跑完后从 swaps 表重建 token_1min_stats / pool_1min_stats，避免双累加
 *
 * 用法：
 *   bun scripts/backfill.ts [duration] [concurrency]
 *   bun scripts/backfill.ts 24       # 24 小时
 *   bun scripts/backfill.ts 24h      # 24 小时
 *   bun scripts/backfill.ts 30d      # 30 天 = 720 小时
 *   bun scripts/backfill.ts 30d 8    # 30 天，8 并发 worker
 *
 * 容器内：
 *   docker compose run --rm --no-deps radar bun scripts/backfill.ts 30d
 */

import "dotenv/config";
import { createPublicClient, http, parseAbiItem, type Log } from "viem";
import { bsc } from "viem/chains";
import { getPool, initSchema, closeDatabase } from "../src/db/index.js";
import { CONTRACTS } from "../src/config/contracts.js";
import { initPriceService, prewarmMetadataCache } from "../src/core/price-service.js";
import { startTokenTracker, stopTokenTracker } from "../src/token-tracker/tracker.js";
import {
  processV3SwapLog,
  processV4SwapLog,
  processPcsV4ClSwapLog,
  processV2BnbPriceSwap,
  prefetchPcsV4ClPoolInfo,
  prefetchV3PoolInfo,
  prefetchV4PoolInfo,
} from "../src/core/swap-listener.js";
import { newBatchBuffer, flushBatchBuffer, flushBatchBufferSwapsOnly } from "../src/db/queries.js";

function parseDuration(s: string | undefined): number {
  if (!s) return 24;
  const m = /^(\d+)([hd])?$/i.exec(s);
  if (!m) throw new Error(`Invalid duration: ${s}（应为 24, 24h, 30d 这样）`);
  const n = Number(m[1]);
  return (m[2] || "h").toLowerCase() === "d" ? n * 24 : n;
}

const HOURS = parseDuration(process.argv[2]);
// 默认 8 worker（节点限 ~3-4 并发 / 单 IP，8 worker × 5 getLogs ≈ 节点甜点）。
// 实测：16 worker 节点排队反而慢。
const CONCURRENCY = Number(process.argv[3]) || Number(process.env.BF_CONCURRENCY) || 8;
// 默认 1000 blocks/batch（30d 测过最稳）。BF_BATCH_SIZE 可覆盖。
const BLOCKS_PER_BATCH = BigInt(Number(process.env.BF_BATCH_SIZE) || 1000);

// 多 RPC 节点 sharding 用：BF_RPC_URL 覆盖默认节点（如 NodeReal 公网）；
// BF_FROM_BLOCK / BF_TO_BLOCK 显式指定 block 范围（覆盖 HOURS 算的 latest-90d）。
// 跑完不重建 buckets：BF_SKIP_REBUILD=1（多 shard 跑时只让最后一个 shard 重建）。
const HTTP_URL = process.env.BF_RPC_URL || process.env.BSC_HTTP_URL || "http://151.123.172.62:81";
const FROM_BLOCK_OVERRIDE = process.env.BF_FROM_BLOCK ? BigInt(process.env.BF_FROM_BLOCK) : null;
const TO_BLOCK_OVERRIDE = process.env.BF_TO_BLOCK ? BigInt(process.env.BF_TO_BLOCK) : null;
const SKIP_REBUILD = process.env.BF_SKIP_REBUILD === "1";
const SHARD_LABEL = process.env.BF_SHARD_LABEL || "main";

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
  // 关闭 transport batch（不合并多个 RPC 到 1 个 HTTP）。
  // 单 batch 内 5 个 getLogs 同时 in-flight 时，合并会让节点串行处理这 N 个 RPC，
  // 其中 V3 / PCS V3 全链 getLogs 单次 12-16s（无 address filter），
  // 跟轻量的 V4 / PCS V4 / BNB getLogs 合并 = 整体被慢的拖死。
  // 不合并 → 节点能真并发处理，单 batch 时长 ≈ max(getLogs) 而不是 sum。
  transport: http(HTTP_URL, {
    timeout: 60_000,
    retryCount: 2,
  }),
  batch: { multicall: true },
});

interface BatchJob {
  from: bigint;
  to: bigint;
  index: number;
}

interface BatchTiming {
  fetchMs: number;
  prefetchMs: number;
  processMs: number;
  flushMs: number;
}

async function processOneBatch(
  job: BatchJob,
  v4PoolManager: `0x${string}`,
  pcsV4ClPoolManager: `0x${string}`,
  bnbPricePool: `0x${string}`,
  timing: BatchTiming
): Promise<{ logs: number; processed: number; errors: number }> {
  const { from, to } = job;

  const fetchT0 = Date.now();
  let fromTs: number;
  let toTs: number;
  let v3: Log[] = [];
  let pancake: Log[] = [];
  let v4: Log[] = [];
  let pcsV4Cl: Log[] = [];
  let bnbV2: Log[] = [];
  try {
    // getBlock + 5 个 getLogs 全部并发 in-flight（不再分两步）→ 节点真并行处理。
    const [fb, tb, l1, l2, l3, l4, l5] = await Promise.all([
      httpClient.getBlock({ blockNumber: from }),
      httpClient.getBlock({ blockNumber: to }),
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
    fromTs = Number(fb.timestamp) * 1000;
    toTs = Number(tb.timestamp) * 1000;
    v3 = l1;
    pancake = l2;
    v4 = l3;
    pcsV4Cl = l4;
    bnbV2 = l5;
  } catch (err) {
    throw new Error(`fetch ${from}-${to}: ${(err as Error).message}`);
  }
  timing.fetchMs += Date.now() - fetchT0;

  const logCount = v3.length + pancake.length + v4.length + pcsV4Cl.length + bnbV2.length;
  const buffer = newBatchBuffer();

  // Prefetch pool info using viem multicall — 把 batch 内所有 unique pool 的元数据
  // 一次性 batch RPC 拿回；不然每个 cache miss 都触发独立 readContract，很慢。
  // 90d 前的 V3/V4/PCS V4 CL 池子大量都是新的（30d cache 不命中）。
  const prefetchT0 = Date.now();
  await Promise.all([
    prefetchV3PoolInfo(
      v3.map((l) => l.address),
      "bsc",
      "uniswap-v3"
    ),
    prefetchV3PoolInfo(
      pancake.map((l) => l.address),
      "bsc",
      "pancakeswap-v3"
    ),
    prefetchV4PoolInfo(
      v4.map((l) => l.topics[1]).filter((id): id is `0x${string}` => typeof id === "string"),
      "bsc"
    ),
    prefetchPcsV4ClPoolInfo(
      pcsV4Cl.map((l) => l.topics[1]).filter((id): id is `0x${string}` => typeof id === "string"),
      "bsc"
    ),
  ]);
  timing.prefetchMs += Date.now() - prefetchT0;

  const tsForLog = (log: Log): number => {
    const blockNum = Number(log.blockNumber ?? from);
    const range = Number(to - from) || 1;
    const offset = blockNum - Number(from);
    return Math.round(fromTs + (offset / range) * (toTs - fromTs));
  };

  let processed = 0;
  let errors = 0;

  const processT0 = Date.now();
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

  timing.processMs += Date.now() - processT0;

  // 批量 flush 到 PG —— 只写 swaps + bnb_prices。
  // pool_1min_stats / token_1min_stats 跑完后 rebuildBucketsFromSwaps 重建覆盖，
  // 中间累加 buckets 既浪费 IO 又是 8 worker deadlock 根源。
  const flushT0 = Date.now();
  await flushBatchBufferSwapsOnly(buffer);
  timing.flushMs += Date.now() - flushT0;

  return { logs: logCount, processed, errors };
}

async function main() {
  console.log(
    `[Backfill][${SHARD_LABEL}] ${HOURS}h, concurrency=${CONCURRENCY}, batch=${BLOCKS_PER_BATCH} blocks via ${HTTP_URL}`
  );

  await initSchema();
  await startTokenTracker();
  await initPriceService();
  // Prewarm metadata cache from DB（白名单 303 + base tokens 4），让 process 函数 0 RPC for metadata
  await prewarmMetadataCache();

  const latest = await httpClient.getBlockNumber();

  // probe 真实块时间（rangeStartMs 用于 rebuild 时；shard 模式下 shard0 不重建，
  // 让 main shard 跑完再做整体 rebuild）。
  const PROBE_DISTANCE = 1000n;
  const [latestBlk, probeBlk] = await Promise.all([
    httpClient.getBlock({ blockNumber: latest }),
    httpClient.getBlock({ blockNumber: latest - PROBE_DISTANCE }),
  ]);
  const realBlockTimeS =
    (Number(latestBlk.timestamp) - Number(probeBlk.timestamp)) / Number(PROBE_DISTANCE);
  const TOTAL_BLOCKS = BigInt(Math.ceil((HOURS * 3600) / realBlockTimeS));

  // 选 range：override 优先，否则 latest-90d → latest
  const start = FROM_BLOCK_OVERRIDE ?? latest - TOTAL_BLOCKS;
  const end = TO_BLOCK_OVERRIDE ?? latest;
  console.log(
    `[Backfill][${SHARD_LABEL}] block time ${realBlockTimeS.toFixed(3)}s; range ${start} → ${end} (${end - start} blocks)`
  );

  // 幂等保护：
  //   swaps 表靠 uq_swaps_dedup 索引 + ON CONFLICT DO NOTHING（不再 DELETE）
  //   pool/token 1min_stats 跑完后从 swaps 重建（不再依赖 backfill 中间累加状态）
  const latestTsMs = Number(latestBlk.timestamp) * 1000;
  const rangeStartMs = latestTsMs - HOURS * 3600 * 1000 - 60_000;
  console.log(
    `[Backfill][${SHARD_LABEL}] 时间范围 ${new Date(rangeStartMs).toISOString()} → ${new Date(latestTsMs).toISOString()}`
  );

  // 切 jobs（end 替代 latest，支持 BF_TO_BLOCK override）
  const jobs: BatchJob[] = [];
  let idx = 0;
  for (let from = start; from < end; from += BLOCKS_PER_BATCH) {
    const to = from + BLOCKS_PER_BATCH - 1n > end ? end : from + BLOCKS_PER_BATCH - 1n;
    jobs.push({ from, to, index: idx++ });
  }
  console.log(`[Backfill][${SHARD_LABEL}] ${jobs.length} batches`);

  const v4PoolManager = CONTRACTS.bsc.uniswapV4PoolManager as `0x${string}`;
  const pcsV4ClPoolManager = CONTRACTS.bsc.pancakeswapV4ClPoolManager as `0x${string}`;
  const bnbPricePool = CONTRACTS.bsc.bnbPricePool as `0x${string}`;

  // worker pool
  const queue = [...jobs];
  const t0 = Date.now();
  let totalLogs = 0;
  let totalProcessed = 0;
  let totalErrors = 0;
  let completedBatches = 0;
  // 阶段累计时长（worker 维度，跨 worker 求和）。除以 worker 数得出每 worker 平均阶段时长。
  const timing: BatchTiming = { fetchMs: 0, prefetchMs: 0, processMs: 0, flushMs: 0 };

  async function worker() {
    while (queue.length > 0) {
      const job = queue.shift();
      if (!job) break;
      try {
        const r = await processOneBatch(job, v4PoolManager, pcsV4ClPoolManager, bnbPricePool, timing);
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
        // 每 batch 平均各阶段（毫秒），按 worker 维度归一（总时长/总 batch）。
        const avgFetch = (timing.fetchMs / completedBatches).toFixed(0);
        const avgPrefetch = (timing.prefetchMs / completedBatches).toFixed(0);
        const avgProcess = (timing.processMs / completedBatches).toFixed(0);
        const avgFlush = (timing.flushMs / completedBatches).toFixed(0);
        const remainBatches = jobs.length - completedBatches;
        const etaH = remainBatches / parseFloat(rate) / 3600;
        console.log(
          `[Backfill][${SHARD_LABEL}] ${pct}% ${completedBatches}/${jobs.length} | logs=${totalLogs} ok=${totalProcessed} err=${totalErrors} | ${elapsedS}s ${rate}batch/s ETA=${etaH.toFixed(1)}h | avg ms: fetch=${avgFetch} prefetch=${avgPrefetch} process=${avgProcess} flush=${avgFlush}`
        );
      }
    }
  }

  await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));

  const elapsedS = ((Date.now() - t0) / 1000).toFixed(0);
  console.log(
    `[Backfill][${SHARD_LABEL}] swap 抓取完成 ${elapsedS}s — fetched ${totalLogs}, processed ${totalProcessed}, errors ${totalErrors}`
  );

  // 跑完从 swaps 重建 buckets（彻底防 stream/backfill 双累加）。
  // 多 shard 时只让 main shard 重建（其他 shard 设 BF_SKIP_REBUILD=1）。
  if (SKIP_REBUILD) {
    console.log(`[Backfill][${SHARD_LABEL}] skip rebuild (BF_SKIP_REBUILD=1)`);
  } else {
    await rebuildBucketsFromSwaps(rangeStartMs);
  }

  stopTokenTracker();
  await closeDatabase();
  process.exit(0);
}

/**
 * 从 swaps 表完整重建 [rangeStartMs, +∞) 区间的 token_1min_stats / pool_1min_stats。
 *
 * - DELETE 该区间内已有 buckets（不动更早的）
 * - GROUP BY 1min 桶，按 swaps.fee_usd / volume_usd 汇总
 * - token-level：通过 pools / v4_pools JOIN binance_bsc_tokens 找出每个池子的 target token
 *   （swap-listener 只把 swap 入库时已经过滤过白名单，这里直接 JOIN 即可）
 */
async function rebuildBucketsFromSwaps(rangeStartMs: number): Promise<void> {
  // 用事务 + LOCK + ON CONFLICT DO UPDATE 防止 stream 在 DELETE 与 INSERT 之间写入新 bucket 触发 PK 冲突
  console.log("[Backfill] 重建 pool_1min_stats（事务 + LOCK）");
  const c1 = await getPool().connect();
  try {
    await c1.query("BEGIN");
    await c1.query("LOCK TABLE pool_1min_stats IN EXCLUSIVE MODE");
    await c1.query(`DELETE FROM pool_1min_stats WHERE bucket_start >= $1`, [rangeStartMs]);
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
       WHERE timestamp >= $1
       GROUP BY pool_address, chain, (timestamp / 60000) * 60000
       ON CONFLICT (pool_address, chain, bucket_start) DO UPDATE SET
         total_fees_usd = EXCLUDED.total_fees_usd,
         total_volume_usd = EXCLUDED.total_volume_usd,
         swap_count = EXCLUDED.swap_count`,
      [rangeStartMs]
    );
    await c1.query("COMMIT");
    console.log(`[Backfill]   pool_1min_stats inserted ${r1.rowCount}`);
  } catch (e) {
    await c1.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    c1.release();
  }

  console.log("[Backfill] 重建 token_1min_stats（事务 + LOCK）");
  const c2 = await getPool().connect();
  try {
    await c2.query("BEGIN");
    await c2.query("LOCK TABLE token_1min_stats IN EXCLUSIVE MODE");
    await c2.query(`DELETE FROM token_1min_stats WHERE bucket_start >= $1`, [rangeStartMs]);
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
       WHERE s.timestamp >= $1
       GROUP BY pt.target_token, s.chain, (s.timestamp / 60000) * 60000
       ON CONFLICT (token_address, chain, bucket_start) DO UPDATE SET
         total_volume_usd = EXCLUDED.total_volume_usd,
         total_fees_usd = EXCLUDED.total_fees_usd,
         swap_count = EXCLUDED.swap_count`,
      [rangeStartMs]
    );
    await c2.query("COMMIT");
    console.log(`[Backfill]   token_1min_stats inserted ${r2.rowCount}`);
  } catch (e) {
    await c2.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    c2.release();
  }
  console.log("[Backfill] buckets 重建完成 ✅");
}

main().catch((err) => {
  console.error("[Backfill] Fatal:", err);
  process.exit(1);
});
