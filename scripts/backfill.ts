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
import { CHAIN_STATIC } from "../src/config/chains.js";
import type { PoolInfo } from "../src/types/index.js";
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
import {
  bulkUpsertPools,
  bulkUpsertV4Pools,
  getAllBinanceBscTokens,
  newBatchBuffer,
  flushBatchBuffer,
  flushBatchBufferSwapsOnly,
  flushBatchBufferToStaging,
  createStagingTable,
  migrateStagingToSwaps,
  type BatchBuffer,
} from "../src/db/queries.js";

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
// 双 shard 跑时必须设 BF_SKIP_MIGRATE=1：避免一个 shard 先跑完触发 migrate（含 TRUNCATE staging），
// 把另一个 shard 还在写的 staging 数据清掉。所有 shard 跑完后手动 migrate。
const SKIP_MIGRATE = process.env.BF_SKIP_MIGRATE === "1";
const SHARD_LABEL = process.env.BF_SHARD_LABEL || "main";

// Staging 模式：fetcher 写到无索引 staging 表，跑完 migrate 到 swaps。
// 默认开启（解耦 fetch / flush，跨过 hypertable INSERT 慢的瓶颈）。
// BF_STAGING=0 关闭走老的 hypertable 直写。
const USE_STAGING = process.env.BF_STAGING !== "0";
// flush worker 数量（独立于 fetch worker）。staging 写入很快，少量 worker 即可。
const FLUSH_WORKERS = Number(process.env.BF_FLUSH_WORKERS) || 4;
// 内部 channel 队列上限（防 OOM；每个 buffer ~30k swap × ~150 bytes ≈ 5MB，16 个 = 80MB）
const QUEUE_MAX = Number(process.env.BF_QUEUE_MAX) || 16;

// V3 / PCS V3 PoolCreated（启动时 discover 用，拿到 token0/token1/fee 直接判断白名单）
const V3_POOL_CREATED = parseAbiItem(
  "event PoolCreated(address indexed token0, address indexed token1, uint24 indexed fee, int24 tickSpacing, address pool)"
);

// V4 PoolManager Initialize（V4 的「PoolCreated」）
// 顺序：id, currency0, currency1, fee, tickSpacing, hooks, sqrtPriceX96, tick
const V4_INITIALIZE = parseAbiItem(
  "event Initialize(bytes32 indexed id, address indexed currency0, address indexed currency1, uint24 fee, int24 tickSpacing, address hooks, uint160 sqrtPriceX96, int24 tick)"
);

// PCS V4 CL Initialize：顺序不同（hooks/fee/parameters）
const PCS_V4_CL_INITIALIZE = parseAbiItem(
  "event Initialize(bytes32 indexed id, address indexed currency0, address indexed currency1, address hooks, uint24 fee, bytes32 parameters, uint160 sqrtPriceX96, int24 tick)"
);

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

// Discovery RPC（archive node）：扫 V3 PoolCreated / V4 Initialize 全历史用。
// 自建节点 prune 了 ~1y 老 logs（HANDOFF § 13），扫 V3 deploy block (~26M) 必失败，
// 必须用公网 archive。NodeReal 50K blocks/call 上限，BF_DISCOVERY_RPC 可覆盖。
const DISCOVERY_RPC_URL =
  process.env.BF_DISCOVERY_RPC ||
  "https://bsc-mainnet.nodereal.io/v1/b13fcff9775e4d1bb28a0735292a1819";

const discoveryClient = createPublicClient({
  chain: bsc,
  transport: http(DISCOVERY_RPC_URL, { timeout: 60_000, retryCount: 3 }),
});

const DISCOVERY_STEP = 49_999n; // NodeReal eth_getLogs 50K blocks 上限（含端点）
const DISCOVERY_CONCURRENCY = Number(process.env.BF_DISCOVERY_CONCURRENCY) || 8;

// V3 / PCS V3 部署在 BSC ~26.9M (2023-03)；V4 deploy ~46.5M (2025-01)。保守起点。
const V3_BSC_DEPLOY_BLOCK = 26_000_000n;
const V4_BSC_DEPLOY_BLOCK = 45_000_000n;


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

interface BlockRange {
  from: bigint;
  to: bigint;
}

function buildRanges(fromBlock: bigint, toBlock: bigint, step: bigint): BlockRange[] {
  const ranges: BlockRange[] = [];
  for (let f = fromBlock; f <= toBlock; f += step + 1n) {
    const t = f + step > toBlock ? toBlock : f + step;
    ranges.push({ from: f, to: t });
  }
  return ranges;
}

/**
 * 并发扫多个 block range；fail-safe（单 range 失败 log warn 继续，不阻塞整体）。
 */
async function concurrentScan<T>(
  ranges: BlockRange[],
  concurrency: number,
  scan: (r: BlockRange) => Promise<T[]>,
  onProgress?: (done: number, total: number, found: number, elapsedMs: number) => void
): Promise<T[]> {
  const results: T[] = [];
  const t0 = Date.now();
  let cursor = 0;
  let done = 0;
  async function worker() {
    while (true) {
      const idx = cursor++;
      if (idx >= ranges.length) break;
      const r = ranges[idx];
      try {
        const found = await scan(r);
        results.push(...found);
      } catch (err) {
        console.warn(`[scan] range ${r.from}-${r.to} fail: ${(err as Error).message}`);
      }
      done++;
      if (onProgress && (done % 20 === 0 || done === ranges.length)) {
        onProgress(done, ranges.length, results.length, Date.now() - t0);
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, ranges.length) }, worker));
  return results;
}

/**
 * 扫 V3 / PCS V3 Factory 全历史 PoolCreated → 按白名单 token0/token1 过滤 → 写入 pools 表。
 * 配合 dump restore 做"双保险并集"：dump 兜底死池子（节点漏 logs），扫描兜底新池子。
 * bulkUpsertPools 用 ON CONFLICT (address, chain) DO UPDATE，dump 池子被同样数据覆盖（no-op）。
 */
async function discoverWhitelistedPools(
  toBlock: bigint,
  targetTokens: Set<string>,
  baseTokens: Set<string>
): Promise<{ v3: `0x${string}`[]; pcsV3: `0x${string}`[] }> {
  const t0 = Date.now();

  // 第 1 步：先看 PG 已有（dump 兜底）
  const dumpV3Res = await getPool().query<{ address: string }>(
    `SELECT address FROM pools WHERE chain='bsc' AND dex='uniswap-v3'`
  );
  const dumpPcsRes = await getPool().query<{ address: string }>(
    `SELECT address FROM pools WHERE chain='bsc' AND dex='pancakeswap-v3'`
  );
  const dumpV3 = dumpV3Res.rows.length;
  const dumpPcs = dumpPcsRes.rows.length;
  console.log(`[Discover V3] PG 已有: V3=${dumpV3}, PCS V3=${dumpPcs}（dump 兜底）`);

  // 第 2 步：扫链 PoolCreated（V3 + PCS V3 Factory，并发 N 个 range）
  // 收录规则：(token0 ∈ target) || (token1 ∈ target) || (base/base)
  // target = 303 个币安永续白名单 token；base = USDT/USDC/USD1/WBNB
  // 不收 USDT/meme 这种（meme 不在 target，仅 USDT 在 base，按 base/base 也不算）
  const v3Factory = CONTRACTS.bsc.uniswapV3Factory as `0x${string}`;
  const pcsV3Factory = CONTRACTS.bsc.pancakeswapV3Factory as `0x${string}`;
  const fromBlock = V3_BSC_DEPLOY_BLOCK;
  const totalBlocks = toBlock - fromBlock;
  console.log(
    `[Discover V3] 扫 V3/PCS V3 PoolCreated (${fromBlock} → ${toBlock}, ${totalBlocks} blocks, ${DISCOVERY_CONCURRENCY} workers)...`
  );

  const ranges = buildRanges(fromBlock, toBlock, DISCOVERY_STEP);

  const found = await concurrentScan<PoolInfo>(
    ranges,
    DISCOVERY_CONCURRENCY,
    async (r) => {
      const [v3Logs, pcsLogs] = await Promise.all([
        discoveryClient.getLogs({
          address: v3Factory,
          fromBlock: r.from,
          toBlock: r.to,
          event: V3_POOL_CREATED,
        }),
        discoveryClient.getLogs({
          address: pcsV3Factory,
          fromBlock: r.from,
          toBlock: r.to,
          event: V3_POOL_CREATED,
        }),
      ]);
      const out: PoolInfo[] = [];
      const handle = (log: Log, dex: "uniswap-v3" | "pancakeswap-v3") => {
        const a = log.args as { token0?: string; token1?: string; fee?: number; pool?: string };
        if (!a.token0 || !a.token1 || !a.pool) return;
        const t0L = a.token0.toLowerCase();
        const t1L = a.token1.toLowerCase();
        const t0InTarget = targetTokens.has(t0L);
        const t1InTarget = targetTokens.has(t1L);
        const baseBase = baseTokens.has(t0L) && baseTokens.has(t1L);
        if (t0InTarget || t1InTarget || baseBase) {
          out.push({
            address: a.pool.toLowerCase(),
            chain: "bsc",
            dex,
            token0: t0L,
            token1: t1L,
            feeTier: Number(a.fee ?? 0),
          });
        }
      };
      for (const log of v3Logs) handle(log as unknown as Log, "uniswap-v3");
      for (const log of pcsLogs) handle(log as unknown as Log, "pancakeswap-v3");
      return out;
    },
    (done, total, foundN, elapsedMs) => {
      const pct = ((done / total) * 100).toFixed(0);
      console.log(`[Discover V3] ${pct}% (${done}/${total}) | found=${foundN} | ${(elapsedMs / 1000).toFixed(0)}s`);
    }
  );

  // 第 3 步：dedup（同一 pool 不会被两个 factory emit，但保险）→ bulkUpsertPools
  const seen = new Map<string, PoolInfo>();
  for (const p of found) seen.set(`${p.dex}:${p.address}`, p);
  const dedup = Array.from(seen.values());
  if (dedup.length > 0) await bulkUpsertPools(dedup);

  // 第 4 步：重新 SELECT 拿 union 后的全集（dump ∪ scan）
  const v3Final = await getPool().query<{ address: string }>(
    `SELECT address FROM pools WHERE chain='bsc' AND dex='uniswap-v3'`
  );
  const pcsFinal = await getPool().query<{ address: string }>(
    `SELECT address FROM pools WHERE chain='bsc' AND dex='pancakeswap-v3'`
  );
  const v3 = v3Final.rows.map((r) => r.address as `0x${string}`);
  const pcsV3 = pcsFinal.rows.map((r) => r.address as `0x${string}`);

  const newV3 = v3.length - dumpV3;
  const newPcs = pcsV3.length - dumpPcs;
  console.log(
    `[Discover V3] 完成 ${((Date.now() - t0) / 1000).toFixed(0)}s — V3=${v3.length}(+${newV3}), PCS V3=${pcsV3.length}(+${newPcs})`
  );
  return { v3, pcsV3 };
}

/**
 * V4 / PCS V4 CL discovery：从 PoolManager Initialize 事件拿所有 poolId + token0/token1/fee/hooks，
 * upsert v4_pools 表 → backfill 主循环 prefetch 全 cache hit（不走 multicall）。
 *
 * V4 BSC deploy ~2025-01（block ~46.5M），PCS V4 CL deploy ~2024 中。保险起点 45M。
 * 范围 ~51M blocks，并发 8 worker × 49999 step ≈ 2-4 分钟。
 */
type V4Row = {
  poolId: string;
  chain: "bsc";
  info: { currency0: string; currency1: string; fee: number; tickSpacing: number; hooks: string };
};

async function discoverV4Pools(
  toBlock: bigint,
  targetTokens: Set<string>,
  baseTokens: Set<string>
): Promise<{ v4Ids: `0x${string}`[]; pcsV4ClIds: `0x${string}`[] }> {
  const fromBlock = V4_BSC_DEPLOY_BLOCK;
  const v4PoolManager = CONTRACTS.bsc.uniswapV4PoolManager as `0x${string}`;
  const pcsV4ClPoolManager = CONTRACTS.bsc.pancakeswapV4ClPoolManager as `0x${string}`;
  const totalBlocks = toBlock - fromBlock;
  console.log(
    `[Discover V4] 扫 V4/PCS V4 CL Initialize (${fromBlock} → ${toBlock}, ${totalBlocks} blocks, ${DISCOVERY_CONCURRENCY} workers)...`
  );
  const t0 = Date.now();

  const ranges = buildRanges(fromBlock, toBlock, DISCOVERY_STEP);

  const matches = (c0: string, c1: string): boolean => {
    return targetTokens.has(c0) || targetTokens.has(c1) || (baseTokens.has(c0) && baseTokens.has(c1));
  };

  const all = await concurrentScan<V4Row>(
    ranges,
    DISCOVERY_CONCURRENCY,
    async (r) => {
      const [v4Logs, pcsLogs] = await Promise.all([
        discoveryClient.getLogs({
          address: v4PoolManager,
          fromBlock: r.from,
          toBlock: r.to,
          event: V4_INITIALIZE,
        }),
        discoveryClient.getLogs({
          address: pcsV4ClPoolManager,
          fromBlock: r.from,
          toBlock: r.to,
          event: PCS_V4_CL_INITIALIZE,
        }),
      ]);
      const out: V4Row[] = [];
      for (const log of v4Logs) {
        const a = log.args as { id?: string; currency0?: string; currency1?: string; fee?: number; tickSpacing?: number; hooks?: string };
        if (!a.id || !a.currency0 || !a.currency1) continue;
        const c0 = a.currency0.toLowerCase();
        const c1 = a.currency1.toLowerCase();
        if (matches(c0, c1)) {
          out.push({
            poolId: a.id,
            chain: "bsc",
            info: {
              currency0: c0,
              currency1: c1,
              fee: Number(a.fee ?? 0),
              tickSpacing: Number(a.tickSpacing ?? 0),
              hooks: (a.hooks ?? "0x0000000000000000000000000000000000000000").toLowerCase(),
            },
          });
        }
      }
      for (const log of pcsLogs) {
        const a = log.args as { id?: string; currency0?: string; currency1?: string; fee?: number; hooks?: string };
        if (!a.id || !a.currency0 || !a.currency1) continue;
        const c0 = a.currency0.toLowerCase();
        const c1 = a.currency1.toLowerCase();
        if (matches(c0, c1)) {
          out.push({
            poolId: `pcsv4cl:${a.id}`,
            chain: "bsc",
            info: {
              currency0: c0,
              currency1: c1,
              fee: Number(a.fee ?? 0),
              tickSpacing: 0,
              hooks: (a.hooks ?? "0x0000000000000000000000000000000000000000").toLowerCase(),
            },
          });
        }
      }
      return out;
    },
    (done, total, foundN, elapsedMs) => {
      const pct = ((done / total) * 100).toFixed(0);
      console.log(`[Discover V4] ${pct}% (${done}/${total}) | found=${foundN} | ${(elapsedMs / 1000).toFixed(0)}s`);
    }
  );

  // dedup by poolId（同一 poolId 在两个 factory 不会重复，但保险）
  const seen = new Map<string, V4Row>();
  for (const r of all) seen.set(r.poolId, r);
  const dedup = Array.from(seen.values());

  if (dedup.length > 0) await bulkUpsertV4Pools(dedup);

  // 从 PG 拿 union 全集（dump ∪ scan），返回 poolIds 给 swap getLogs args.id 过滤
  const v4Res = await getPool().query<{ pool_id: string }>(
    `SELECT pool_id FROM v4_pools WHERE chain='bsc' AND pool_id NOT LIKE 'pcsv4cl:%'`
  );
  const pcsRes = await getPool().query<{ pool_id: string }>(
    `SELECT pool_id FROM v4_pools WHERE chain='bsc' AND pool_id LIKE 'pcsv4cl:%'`
  );
  const v4Ids = v4Res.rows.map((r) => r.pool_id as `0x${string}`);
  const pcsV4ClIds = pcsRes.rows.map(
    (r) => r.pool_id.replace(/^pcsv4cl:/, "") as `0x${string}`
  );

  console.log(
    `[Discover V4] 完成 ${((Date.now() - t0) / 1000).toFixed(0)}s — V4=${v4Ids.length} 池, PCS V4 CL=${pcsV4ClIds.length} 池`
  );
  return { v4Ids, pcsV4ClIds };
}

/**
 * V4 swap getLogs 拆 chunk + topic filter（args.id）。
 * 节点上限 1000 topic，超了节点 reject "Invalid parameters"。
 * V4 ~21k 池子 + PCS V4 CL ~8k 池子 → 必须拆。
 *
 * 收益：避免全链拉 V4 swap（90d 早期可能 30k+ logs/batch），节点端只返回白名单 swap，
 * 后续 prefetch / process / flush 都只处理白名单数据，整体 ETA 下降。
 *
 * 单 worker 内 chunks 限 4 并发：避免与同 batch 的 V3/PCS V3/BNB getLogs + 多 fetcher worker
 * 一起把节点打爆。8 fetcher × 4 chunk = 32 V4 RPC concurrent + 5 其他 = 37 system concurrent。
 */
const TOPIC_FILTER_CHUNK_SIZE = 1000;
const V4_CHUNK_CONCURRENCY = 4;

async function getLogsV4Chunked(
  managerAddr: `0x${string}`,
  poolIds: `0x${string}`[],
  fromBlock: bigint,
  toBlock: bigint,
  event: ReturnType<typeof parseAbiItem>
): Promise<Log[]> {
  if (poolIds.length === 0) {
    // 没池子（比如 PCS V4 CL dump=0 + scan=0 极端情况）→ 全链拉，process 阶段过滤
    return (await httpClient.getLogs({
      address: managerAddr,
      fromBlock,
      toBlock,
      // @ts-expect-error event union
      event,
    })) as unknown as Log[];
  }
  if (poolIds.length <= TOPIC_FILTER_CHUNK_SIZE) {
    return (await httpClient.getLogs({
      address: managerAddr,
      fromBlock,
      toBlock,
      // @ts-expect-error event union
      event,
      args: { id: poolIds },
    })) as unknown as Log[];
  }
  const chunks: `0x${string}`[][] = [];
  for (let i = 0; i < poolIds.length; i += TOPIC_FILTER_CHUNK_SIZE) {
    chunks.push(poolIds.slice(i, i + TOPIC_FILTER_CHUNK_SIZE));
  }
  const results: Log[][] = [];
  let cursor = 0;
  async function chunkWorker() {
    while (true) {
      const idx = cursor++;
      if (idx >= chunks.length) break;
      const logs = (await httpClient.getLogs({
        address: managerAddr,
        fromBlock,
        toBlock,
        // @ts-expect-error event union
        event,
        args: { id: chunks[idx] },
      })) as unknown as Log[];
      results.push(logs);
    }
  }
  await Promise.all(
    Array.from({ length: Math.min(V4_CHUNK_CONCURRENCY, chunks.length) }, chunkWorker)
  );
  return results.flat();
}

/**
 * Fetch + process（不 flush）。返回 BatchBuffer 给独立 flush worker 处理。
 * 拆 fetch / flush 解耦：fetcher 持续跑节点，flush 后台串行写 PG，互不阻塞。
 */
async function fetchAndProcessBatch(
  job: BatchJob,
  v4PoolManager: `0x${string}`,
  pcsV4ClPoolManager: `0x${string}`,
  bnbPricePool: `0x${string}`,
  v3PoolList: `0x${string}`[],
  pcsV3PoolList: `0x${string}`[],
  v4PoolIds: `0x${string}`[],
  pcsV4ClPoolIds: `0x${string}`[],
  timing: BatchTiming
): Promise<{ buffer: BatchBuffer; logs: number; processed: number; errors: number }> {
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
    // V3/PCS V3：address: [...whitelistedPools] 让节点直接过滤
    // V4/PCS V4 CL：PoolManager singleton，不能 address filter，但 swap event `id` 是 indexed
    //   topic filter 节点上限 1000，所以拆 chunk + 内部 4 并发跑（getLogsV4Chunked）
    //   收益：节点端只返回白名单 swap，prefetch / process / flush 都受益
    const [fb, tb, l1, l2, l3, l4, l5] = await Promise.all([
      httpClient.getBlock({ blockNumber: from }),
      httpClient.getBlock({ blockNumber: to }),
      httpClient.getLogs({ address: v3PoolList, fromBlock: from, toBlock: to, event: V3_SWAP }) as unknown as Promise<Log[]>,
      httpClient.getLogs({ address: pcsV3PoolList, fromBlock: from, toBlock: to, event: PANCAKE_V3_SWAP }) as unknown as Promise<Log[]>,
      getLogsV4Chunked(v4PoolManager, v4PoolIds, from, to, V4_SWAP),
      getLogsV4Chunked(pcsV4ClPoolManager, pcsV4ClPoolIds, from, to, PCS_V4_CL_SWAP),
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

  // 不在这里 flush（拆出独立 flush worker）
  return { buffer, logs: logCount, processed, errors };
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

  // 准备 staging 表（如果用 staging 模式）
  if (USE_STAGING) {
    await createStagingTable();
    console.log(`[Backfill][${SHARD_LABEL}] staging mode ON: fetcher 写 swaps_staging（无 index），跑完 migrate 到 swaps`);
  }

  // === Pool whitelist discovery（启动时一次性扫 V3/PCS V3 全历史 PoolCreated）===
  // 让 V3/PCS V3 swap getLogs 带 address filter，节点直接过滤掉非白名单池子的 swap。
  // BF_SKIP_DISCOVERY=1 跳过（仅 V4 单跑或调试用）
  let v3PoolList: `0x${string}`[] = [];
  let pcsV3PoolList: `0x${string}`[] = [];
  let v4PoolIds: `0x${string}`[] = [];
  let pcsV4ClPoolIds: `0x${string}`[] = [];
  if (process.env.BF_SKIP_DISCOVERY !== "1") {
    const tokens = await getAllBinanceBscTokens();
    // 拆 target / base：
    //   target = 303 个币安永续 token（核心监控目标）
    //   base = USDT/USDC/USD1/WBNB（仅作为 quote / base/base 监控基准）
    // 收录规则：(t0 ∈ target || t1 ∈ target) || (base/base)
    // 不收 USDT/meme（meme 不在 target，单边 base 不算）
    const targetTokens = new Set<string>();
    for (const t of tokens) targetTokens.add(t.contractAddress.toLowerCase());
    const baseTokens = new Set<string>(CHAIN_STATIC.bsc.baseTokens);
    console.log(`[Backfill][${SHARD_LABEL}] 白名单 target=${targetTokens.size}, base=${baseTokens.size}`);

    const discovered = await discoverWhitelistedPools(end, targetTokens, baseTokens);
    v3PoolList = discovered.v3;
    pcsV3PoolList = discovered.pcsV3;

    // V4 / PCS V4 CL discovery：pre-fill v4_pools 表 + 拿 poolIds 给 swap getLogs topic filter
    const v4Discovered = await discoverV4Pools(end, targetTokens, baseTokens);
    v4PoolIds = v4Discovered.v4Ids;
    pcsV4ClPoolIds = v4Discovered.pcsV4ClIds;
  } else {
    console.log(`[Backfill][${SHARD_LABEL}] BF_SKIP_DISCOVERY=1, V3/PCS V3 走全链扫描 + V4 走链上 multicall（慢）`);
  }

  // 调试用：discovery 跑完立刻退出，验证扫链不跑 backfill 主循环
  if (process.env.BF_DISCOVER_ONLY === "1") {
    console.log(`[Backfill][${SHARD_LABEL}] BF_DISCOVER_ONLY=1，discovery 已完成，跳过 backfill 主循环`);
    stopTokenTracker();
    await closeDatabase();
    return;
  }

  // ============== 拆 fetch 和 flush 两个 worker pool ==============
  const fetchQueue = [...jobs];
  const flushQueue: Array<{ buffer: BatchBuffer; jobIndex: number }> = [];
  let fetchersDone = false;
  const t0 = Date.now();
  let totalLogs = 0;
  let totalProcessed = 0;
  let totalErrors = 0;
  let completedBatches = 0; // 已被 flush worker 处理完成的 batch
  let fetchedBatches = 0;
  const timing: BatchTiming = { fetchMs: 0, prefetchMs: 0, processMs: 0, flushMs: 0 };

  // Fetch worker：拉 + process → 写 channel；channel 满则 backpressure 等
  async function fetchWorker() {
    while (fetchQueue.length > 0) {
      const job = fetchQueue.shift();
      if (!job) break;
      // backpressure: 队列满则等 flush worker 消化
      while (flushQueue.length >= QUEUE_MAX) {
        await new Promise((r) => setTimeout(r, 50));
      }
      try {
        const r = await fetchAndProcessBatch(job, v4PoolManager, pcsV4ClPoolManager, bnbPricePool, v3PoolList, pcsV3PoolList, v4PoolIds, pcsV4ClPoolIds, timing);
        totalLogs += r.logs;
        totalProcessed += r.processed;
        totalErrors += r.errors;
        flushQueue.push({ buffer: r.buffer, jobIndex: job.index });
        fetchedBatches++;
      } catch (err) {
        console.error(`[Backfill][${SHARD_LABEL}] batch ${job.index} (${job.from}-${job.to}) fetch fail:`, (err as Error).message);
        totalErrors++;
      }
    }
  }

  // Flush worker：从 channel 拿 buffer → 写 PG（staging 或 swaps）
  async function flushWorker() {
    while (!fetchersDone || flushQueue.length > 0) {
      const item = flushQueue.shift();
      if (!item) {
        await new Promise((r) => setTimeout(r, 50));
        continue;
      }
      const flushT0 = Date.now();
      try {
        if (USE_STAGING) {
          await flushBatchBufferToStaging(item.buffer);
        } else {
          await flushBatchBufferSwapsOnly(item.buffer);
        }
      } catch (err) {
        console.error(`[Backfill][${SHARD_LABEL}] batch ${item.jobIndex} flush fail:`, (err as Error).message);
        totalErrors++;
      }
      timing.flushMs += Date.now() - flushT0;
      completedBatches++;
      if (completedBatches % 20 === 0 || completedBatches === jobs.length) {
        const pct = Math.round((completedBatches * 100) / jobs.length);
        const elapsedS = ((Date.now() - t0) / 1000).toFixed(0);
        const rate = (completedBatches / parseFloat(elapsedS)).toFixed(2);
        const avgFetch = (timing.fetchMs / Math.max(fetchedBatches, 1)).toFixed(0);
        const avgPrefetch = (timing.prefetchMs / Math.max(fetchedBatches, 1)).toFixed(0);
        const avgProcess = (timing.processMs / Math.max(fetchedBatches, 1)).toFixed(0);
        const avgFlush = (timing.flushMs / completedBatches).toFixed(0);
        const remainBatches = jobs.length - completedBatches;
        const etaH = remainBatches / parseFloat(rate) / 3600;
        console.log(
          `[Backfill][${SHARD_LABEL}] ${pct}% ${completedBatches}/${jobs.length} (fetched=${fetchedBatches} q=${flushQueue.length}) | logs=${totalLogs} ok=${totalProcessed} err=${totalErrors} | ${elapsedS}s ${rate}batch/s ETA=${etaH.toFixed(1)}h | avg ms: fetch=${avgFetch} prefetch=${avgPrefetch} process=${avgProcess} flush=${avgFlush}`
        );
      }
    }
  }

  // 启 fetch + flush worker pool
  const fetchTasks = Array.from({ length: CONCURRENCY }, () => fetchWorker());
  const flushTasks = Array.from({ length: FLUSH_WORKERS }, () => flushWorker());

  await Promise.all(fetchTasks);
  fetchersDone = true;
  await Promise.all(flushTasks);
  // ============== worker pool 结束 ==============

  const elapsedS = ((Date.now() - t0) / 1000).toFixed(0);
  console.log(
    `[Backfill][${SHARD_LABEL}] swap 抓取+flush 完成 ${elapsedS}s — fetched ${totalLogs}, processed ${totalProcessed}, errors ${totalErrors}`
  );

  // Staging 模式：把 staging 数据 migrate 到 swaps（hash semi-join 比逐行 ON CONFLICT 快）
  // 双 shard 跑时设 BF_SKIP_MIGRATE=1 跳过，所有 shard 跑完后单独 migrate。
  if (USE_STAGING && SKIP_MIGRATE) {
    console.log(`[Backfill][${SHARD_LABEL}] skip migrate (BF_SKIP_MIGRATE=1)；所有 shard 跑完后手动 migrate`);
  } else if (USE_STAGING) {
    console.log(`[Backfill][${SHARD_LABEL}] migrating staging → swaps（这步可能 1-2h，PG hash semi-join）`);
    const migT0 = Date.now();
    const r = await migrateStagingToSwaps();
    console.log(
      `[Backfill][${SHARD_LABEL}] migrate done ${((Date.now() - migT0) / 1000).toFixed(0)}s — staged=${r.staged}, inserted=${r.inserted}`
    );
  }

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
