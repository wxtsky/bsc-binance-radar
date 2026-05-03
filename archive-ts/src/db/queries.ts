import { getPool } from "./index.js";
import type { ChainId, SwapRecord, PoolInfo, V4PoolTokenInfo, BinanceBscToken } from "../types/index.js";

// ============================================================================
// Batch helpers (backfill 用)
// ============================================================================

export interface BucketDelta {
  key: string; // poolOrToken
  chain: ChainId;
  bucketStart: number;
  feeUsd: number;
  volumeUsd: number;
  swapCount: number;
}

export interface BatchBuffer {
  swaps: SwapRecord[];
  poolDeltas: Map<string, BucketDelta>; // key: `${pool}|${chain}|${bucket}`
  tokenDeltas: Map<string, BucketDelta>;
  bnbPrices: import("./queries.js").BnbPriceRecord[];
}

export function newBatchBuffer(): BatchBuffer {
  return { swaps: [], poolDeltas: new Map(), tokenDeltas: new Map(), bnbPrices: [] };
}

export function bufferAddSwap(buf: BatchBuffer, swap: SwapRecord): void {
  buf.swaps.push(swap);
}

export function bufferAddBnbPrice(
  buf: BatchBuffer,
  rec: import("./queries.js").BnbPriceRecord
): void {
  buf.bnbPrices.push(rec);
}

export function bufferAddPoolBucket(
  buf: BatchBuffer,
  poolAddress: string,
  chain: ChainId,
  bucketStart: number,
  feeUsd: number,
  volumeUsd: number
): void {
  const key = `${poolAddress}|${chain}|${bucketStart}`;
  const cur = buf.poolDeltas.get(key);
  if (cur) {
    cur.feeUsd += feeUsd;
    cur.volumeUsd += volumeUsd;
    cur.swapCount += 1;
  } else {
    buf.poolDeltas.set(key, {
      key: poolAddress,
      chain,
      bucketStart,
      feeUsd,
      volumeUsd,
      swapCount: 1,
    });
  }
}

export function bufferAddTokenBucket(
  buf: BatchBuffer,
  tokenAddress: string,
  chain: ChainId,
  bucketStart: number,
  volumeUsd: number,
  feeUsd: number
): void {
  const key = `${tokenAddress}|${chain}|${bucketStart}`;
  const cur = buf.tokenDeltas.get(key);
  if (cur) {
    cur.feeUsd += feeUsd;
    cur.volumeUsd += volumeUsd;
    cur.swapCount += 1;
  } else {
    buf.tokenDeltas.set(key, {
      key: tokenAddress,
      chain,
      bucketStart,
      feeUsd,
      volumeUsd,
      swapCount: 1,
    });
  }
}

// pg-node bind 协议有 16-bit (≤32767) 参数上限。多行 placeholder 写法
// (row × col) 在 30k row × 10 col 时撞限。改用 unnest(arr1, arr2, ...) —
// 固定 N 个 array 参数（N=列数），单次 INSERT 可一口气塞数十万行，避免
// 拆 chunk 多 round-trip。
const SWAPS_UNNEST_CHUNK = 30000;
const BUCKETS_CHUNK = 2000;

async function bulkInsertSwaps(swaps: SwapRecord[]): Promise<void> {
  for (let i = 0; i < swaps.length; i += SWAPS_UNNEST_CHUNK) {
    const slice = swaps.slice(i, i + SWAPS_UNNEST_CHUNK);
    const poolAddrs: string[] = [];
    const chains: string[] = [];
    const dexes: string[] = [];
    const txHashes: string[] = [];
    const amount0s: string[] = [];
    const amount1s: string[] = [];
    const feeUsds: number[] = [];
    const volumeUsds: number[] = [];
    const timestamps: string[] = [];
    const blockNumbers: string[] = [];
    for (const s of slice) {
      poolAddrs.push(s.poolAddress);
      chains.push(s.chain);
      dexes.push(s.dex);
      txHashes.push(s.txHash);
      amount0s.push(s.amount0);
      amount1s.push(s.amount1);
      feeUsds.push(s.feeUsd);
      volumeUsds.push(s.volumeUsd);
      // bigint 列用 string 传，避免 pg-node Number 精度丢失
      timestamps.push(String(s.timestamp));
      blockNumbers.push(String(s.blockNumber));
    }
    await getPool().query(
      `INSERT INTO swaps (pool_address, chain, dex, tx_hash, amount0, amount1, fee_usd, volume_usd, timestamp, block_number)
       SELECT * FROM unnest(
         $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
         $7::float8[], $8::float8[], $9::bigint[], $10::bigint[]
       )
       ON CONFLICT (tx_hash, pool_address, amount0, amount1, timestamp) DO NOTHING`,
      [poolAddrs, chains, dexes, txHashes, amount0s, amount1s, feeUsds, volumeUsds, timestamps, blockNumbers]
    );
  }
}

async function bulkUpsertPool1minStats(deltas: BucketDelta[]): Promise<void> {
  for (let i = 0; i < deltas.length; i += BUCKETS_CHUNK) {
    const slice = deltas.slice(i, i + BUCKETS_CHUNK);
    const params: unknown[] = [];
    const placeholders: string[] = [];
    for (const d of slice) {
      const base = params.length;
      placeholders.push(
        `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6})`
      );
      params.push(d.key, d.chain, d.bucketStart, d.feeUsd, d.volumeUsd, d.swapCount);
    }
    await getPool().query(
      `INSERT INTO pool_1min_stats (pool_address, chain, bucket_start, total_fees_usd, total_volume_usd, swap_count)
       VALUES ${placeholders.join(",")}
       ON CONFLICT (pool_address, chain, bucket_start) DO UPDATE SET
         total_fees_usd = pool_1min_stats.total_fees_usd + EXCLUDED.total_fees_usd,
         total_volume_usd = pool_1min_stats.total_volume_usd + EXCLUDED.total_volume_usd,
         swap_count = pool_1min_stats.swap_count + EXCLUDED.swap_count`,
      params
    );
  }
}

async function bulkUpsertToken1minStats(deltas: BucketDelta[]): Promise<void> {
  for (let i = 0; i < deltas.length; i += BUCKETS_CHUNK) {
    const slice = deltas.slice(i, i + BUCKETS_CHUNK);
    const params: unknown[] = [];
    const placeholders: string[] = [];
    for (const d of slice) {
      const base = params.length;
      placeholders.push(
        `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6})`
      );
      params.push(d.key, d.chain, d.bucketStart, d.volumeUsd, d.feeUsd, d.swapCount);
    }
    await getPool().query(
      `INSERT INTO token_1min_stats (token_address, chain, bucket_start, total_volume_usd, total_fees_usd, swap_count)
       VALUES ${placeholders.join(",")}
       ON CONFLICT (token_address, chain, bucket_start) DO UPDATE SET
         total_volume_usd = token_1min_stats.total_volume_usd + EXCLUDED.total_volume_usd,
         total_fees_usd = token_1min_stats.total_fees_usd + EXCLUDED.total_fees_usd,
         swap_count = token_1min_stats.swap_count + EXCLUDED.swap_count`,
      params
    );
  }
}

export async function flushBatchBuffer(buf: BatchBuffer): Promise<void> {
  await Promise.all([
    buf.swaps.length ? bulkInsertSwaps(buf.swaps) : Promise.resolve(),
    buf.poolDeltas.size ? bulkUpsertPool1minStats([...buf.poolDeltas.values()]) : Promise.resolve(),
    buf.tokenDeltas.size ? bulkUpsertToken1minStats([...buf.tokenDeltas.values()]) : Promise.resolve(),
    buf.bnbPrices.length ? bulkInsertBnbPrice(buf.bnbPrices) : Promise.resolve(),
  ]);
}

/**
 * 只 flush swaps + bnb_prices，跳过 pool_1min_stats / token_1min_stats。
 * Backfill 专用：跑完后 rebuildBucketsFromSwaps 会从 swaps 重建 buckets，
 * 中间累加 buckets 完全是浪费 —— 这两表占 flush 大头（multi-row UPSERT 触发
 * ON CONFLICT DO UPDATE，多 worker 并发还互相 deadlock）。
 */
export async function flushBatchBufferSwapsOnly(buf: BatchBuffer): Promise<void> {
  await Promise.all([
    buf.swaps.length ? bulkInsertSwaps(buf.swaps) : Promise.resolve(),
    buf.bnbPrices.length ? bulkInsertBnbPrice(buf.bnbPrices) : Promise.resolve(),
  ]);
}

/**
 * Backfill staging 模式：写到 swaps_staging（无 index，无 PK，无 unique）。
 * - 极快：每 batch 30k swap 单 INSERT，0 conflict check, 0 B-tree update
 * - 跑完 backfill 后调用 migrateStagingToSwaps() 一次性 INSERT INTO swaps
 *   （PG hash semi-join，比逐行 ON CONFLICT 快 10x+）
 */
export async function bulkInsertSwapsStaging(swaps: SwapRecord[]): Promise<void> {
  for (let i = 0; i < swaps.length; i += 30000) {
    const slice = swaps.slice(i, i + 30000);
    const poolAddrs: string[] = [];
    const chains: string[] = [];
    const dexes: string[] = [];
    const txHashes: string[] = [];
    const amount0s: string[] = [];
    const amount1s: string[] = [];
    const feeUsds: number[] = [];
    const volumeUsds: number[] = [];
    const timestamps: string[] = [];
    const blockNumbers: string[] = [];
    for (const s of slice) {
      poolAddrs.push(s.poolAddress);
      chains.push(s.chain);
      dexes.push(s.dex);
      txHashes.push(s.txHash);
      amount0s.push(s.amount0);
      amount1s.push(s.amount1);
      feeUsds.push(s.feeUsd);
      volumeUsds.push(s.volumeUsd);
      timestamps.push(String(s.timestamp));
      blockNumbers.push(String(s.blockNumber));
    }
    await getPool().query(
      `INSERT INTO swaps_staging (pool_address, chain, dex, tx_hash, amount0, amount1, fee_usd, volume_usd, timestamp, block_number)
       SELECT * FROM unnest(
         $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
         $7::float8[], $8::float8[], $9::bigint[], $10::bigint[]
       )`,
      [poolAddrs, chains, dexes, txHashes, amount0s, amount1s, feeUsds, volumeUsds, timestamps, blockNumbers]
    );
  }
}

export async function flushBatchBufferToStaging(buf: BatchBuffer): Promise<void> {
  await Promise.all([
    buf.swaps.length ? bulkInsertSwapsStaging(buf.swaps) : Promise.resolve(),
    buf.bnbPrices.length ? bulkInsertBnbPrice(buf.bnbPrices) : Promise.resolve(),
  ]);
}

/** 创建 swaps_staging 表（无 index 无 PK，纯 append 写）。Idempotent。 */
export async function createStagingTable(): Promise<void> {
  await getPool().query(`
    CREATE TABLE IF NOT EXISTS swaps_staging (
      pool_address TEXT NOT NULL,
      chain TEXT NOT NULL,
      dex TEXT NOT NULL,
      tx_hash TEXT NOT NULL,
      amount0 TEXT NOT NULL,
      amount1 TEXT NOT NULL,
      fee_usd DOUBLE PRECISION NOT NULL,
      volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
      timestamp BIGINT NOT NULL,
      block_number BIGINT NOT NULL
    )
  `);
}

/**
 * 把 swaps_staging 的数据合并到 swaps（hypertable）。
 * - WHERE NOT EXISTS 配合 hash semi-join 比逐行 ON CONFLICT 快得多
 * - DISTINCT ON 防 staging 内部重复（多 worker 同 batch 跑出的极小可能性）
 * - 完成后清空 staging
 */
export async function migrateStagingToSwaps(): Promise<{ inserted: number; staged: number }> {
  const stagedRes = await getPool().query<{ n: string }>(`SELECT COUNT(*)::text AS n FROM swaps_staging`);
  const staged = Number(stagedRes.rows[0]?.n ?? 0);
  if (staged === 0) return { inserted: 0, staged: 0 };

  // 单大事务 INSERT；PG 应用 hash semi-join
  const res = await getPool().query(`
    INSERT INTO swaps (pool_address, chain, dex, tx_hash, amount0, amount1, fee_usd, volume_usd, timestamp, block_number)
    SELECT DISTINCT ON (s.tx_hash, s.pool_address, s.amount0, s.amount1, s."timestamp")
      s.pool_address, s.chain, s.dex, s.tx_hash, s.amount0, s.amount1, s.fee_usd, s.volume_usd, s."timestamp", s.block_number
    FROM swaps_staging s
    WHERE NOT EXISTS (
      SELECT 1 FROM swaps w
      WHERE w.tx_hash = s.tx_hash
        AND w.pool_address = s.pool_address
        AND w.amount0 = s.amount0
        AND w.amount1 = s.amount1
        AND w."timestamp" = s."timestamp"
    )
    ON CONFLICT DO NOTHING
  `);
  const inserted = res.rowCount ?? 0;

  await getPool().query(`TRUNCATE swaps_staging`);
  return { inserted, staged };
}

// ============================================================================
// Swap / pool persistence
// ============================================================================

export async function insertSwap(swap: SwapRecord): Promise<void> {
  await getPool().query(
    `INSERT INTO swaps (pool_address, chain, dex, tx_hash, amount0, amount1, fee_usd, volume_usd, timestamp, block_number)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
     ON CONFLICT (tx_hash, pool_address, amount0, amount1, timestamp) DO NOTHING`,
    [
      swap.poolAddress,
      swap.chain,
      swap.dex,
      swap.txHash,
      swap.amount0,
      swap.amount1,
      swap.feeUsd,
      swap.volumeUsd,
      swap.timestamp,
      swap.blockNumber,
    ]
  );
}

export async function upsertPool(pool: PoolInfo): Promise<void> {
  await getPool().query(
    `INSERT INTO pools (address, chain, dex, token0, token1, fee_tier)
     VALUES ($1, $2, $3, $4, $5, $6)
     ON CONFLICT (address, chain) DO UPDATE SET
       dex = EXCLUDED.dex,
       token0 = EXCLUDED.token0,
       token1 = EXCLUDED.token1,
       fee_tier = EXCLUDED.fee_tier`,
    [pool.address, pool.chain, pool.dex, pool.token0, pool.token1, pool.feeTier]
  );
}

export async function upsertV4Pool(poolId: string, chain: ChainId, info: V4PoolTokenInfo): Promise<void> {
  await getPool().query(
    `INSERT INTO v4_pools (pool_id, chain, currency0, currency1, fee, tick_spacing, hooks)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (pool_id, chain) DO UPDATE SET
       currency0 = EXCLUDED.currency0,
       currency1 = EXCLUDED.currency1,
       fee = EXCLUDED.fee,
       tick_spacing = EXCLUDED.tick_spacing,
       hooks = EXCLUDED.hooks`,
    [poolId, chain, info.currency0, info.currency1, info.fee, info.tickSpacing, info.hooks]
  );
}

export async function getV4Pool(poolId: string, chain: ChainId): Promise<V4PoolTokenInfo | null> {
  const res = await getPool().query<{
    currency0: string;
    currency1: string;
    fee: number;
    tickSpacing: number;
    hooks: string;
  }>(
    `SELECT currency0, currency1, fee, tick_spacing AS "tickSpacing", hooks
     FROM v4_pools WHERE pool_id = $1 AND chain = $2`,
    [poolId, chain]
  );
  return res.rows[0] ?? null;
}

export async function getPoolRecord(address: string, chain: ChainId): Promise<PoolInfo | null> {
  const res = await getPool().query<PoolInfo>(
    `SELECT address, chain, dex, token0, token1, fee_tier AS "feeTier"
     FROM pools WHERE address = $1 AND chain = $2`,
    [address, chain]
  );
  return res.rows[0] ?? null;
}

/**
 * 批量查 pool 元信息。返回 Map<lowercase address, PoolInfo>，缺失的不在 map 里。
 * 用于 backfill prefetch 阶段单次 SQL 替代 N+1 SELECT，显著降低 PG round-trip。
 */
export async function getPoolRecordsBatch(
  addresses: string[],
  chain: ChainId
): Promise<Map<string, PoolInfo>> {
  const out = new Map<string, PoolInfo>();
  if (addresses.length === 0) return out;
  const lowered = [...new Set(addresses.map((a) => a.toLowerCase()))];
  // PG 单 IN list 上限 ~32k，分块保险
  const CHUNK = 5000;
  for (let i = 0; i < lowered.length; i += CHUNK) {
    const slice = lowered.slice(i, i + CHUNK);
    const res = await getPool().query<PoolInfo>(
      `SELECT address, chain, dex, token0, token1, fee_tier AS "feeTier"
       FROM pools WHERE chain = $1 AND address = ANY($2::text[])`,
      [chain, slice]
    );
    for (const row of res.rows) out.set(row.address.toLowerCase(), row);
  }
  return out;
}

/**
 * 批量查 V4 pool 元信息。返回 Map<pool_id, V4PoolTokenInfo>。
 */
export async function getV4PoolsBatch(
  poolIds: string[],
  chain: ChainId
): Promise<Map<string, V4PoolTokenInfo>> {
  const out = new Map<string, V4PoolTokenInfo>();
  if (poolIds.length === 0) return out;
  const unique = [...new Set(poolIds)];
  const CHUNK = 5000;
  for (let i = 0; i < unique.length; i += CHUNK) {
    const slice = unique.slice(i, i + CHUNK);
    const res = await getPool().query<{
      pool_id: string;
      currency0: string;
      currency1: string;
      fee: number;
      tickSpacing: number;
      hooks: string;
    }>(
      `SELECT pool_id, currency0, currency1, fee, tick_spacing AS "tickSpacing", hooks
       FROM v4_pools WHERE chain = $1 AND pool_id = ANY($2::text[])`,
      [chain, slice]
    );
    for (const row of res.rows) {
      out.set(row.pool_id, {
        currency0: row.currency0,
        currency1: row.currency1,
        fee: row.fee,
        tickSpacing: row.tickSpacing,
        hooks: row.hooks,
      });
    }
  }
  return out;
}

/**
 * 批量 UPSERT pools。替代 prefetch 阶段 N 次 upsertPool 单条 INSERT。
 */
export async function bulkUpsertPools(pools: PoolInfo[]): Promise<void> {
  if (pools.length === 0) return;
  const CHUNK = 1000;
  for (let i = 0; i < pools.length; i += CHUNK) {
    const slice = pools.slice(i, i + CHUNK);
    const params: unknown[] = [];
    const placeholders: string[] = [];
    for (const p of slice) {
      const base = params.length;
      placeholders.push(
        `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6})`
      );
      params.push(p.address, p.chain, p.dex, p.token0, p.token1, p.feeTier);
    }
    await getPool().query(
      `INSERT INTO pools (address, chain, dex, token0, token1, fee_tier)
       VALUES ${placeholders.join(",")}
       ON CONFLICT (address, chain) DO UPDATE SET
         dex = EXCLUDED.dex,
         token0 = EXCLUDED.token0,
         token1 = EXCLUDED.token1,
         fee_tier = EXCLUDED.fee_tier`,
      params
    );
  }
}

/**
 * 批量 UPSERT v4_pools。替代 prefetch 阶段 N 次 upsertV4Pool 单条 INSERT。
 */
export async function bulkUpsertV4Pools(
  rows: Array<{ poolId: string; chain: ChainId; info: V4PoolTokenInfo }>
): Promise<void> {
  if (rows.length === 0) return;
  const CHUNK = 1000;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const slice = rows.slice(i, i + CHUNK);
    const params: unknown[] = [];
    const placeholders: string[] = [];
    for (const r of slice) {
      const base = params.length;
      placeholders.push(
        `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6},$${base + 7})`
      );
      params.push(
        r.poolId,
        r.chain,
        r.info.currency0,
        r.info.currency1,
        r.info.fee,
        r.info.tickSpacing,
        r.info.hooks
      );
    }
    await getPool().query(
      `INSERT INTO v4_pools (pool_id, chain, currency0, currency1, fee, tick_spacing, hooks)
       VALUES ${placeholders.join(",")}
       ON CONFLICT (pool_id, chain) DO UPDATE SET
         currency0 = EXCLUDED.currency0,
         currency1 = EXCLUDED.currency1,
         fee = EXCLUDED.fee,
         tick_spacing = EXCLUDED.tick_spacing,
         hooks = EXCLUDED.hooks`,
      params
    );
  }
}

// ============================================================================
// 1-minute bucket aggregation (pool-level)
// ============================================================================

export async function upsertPool1minStat(
  poolAddress: string,
  chain: ChainId,
  bucketStart: number,
  feeUsd: number,
  volumeUsd: number
): Promise<void> {
  await getPool().query(
    `INSERT INTO pool_1min_stats (pool_address, chain, bucket_start, total_fees_usd, total_volume_usd, swap_count)
     VALUES ($1, $2, $3, $4, $5, 1)
     ON CONFLICT (pool_address, chain, bucket_start) DO UPDATE SET
       total_fees_usd = pool_1min_stats.total_fees_usd + EXCLUDED.total_fees_usd,
       total_volume_usd = pool_1min_stats.total_volume_usd + EXCLUDED.total_volume_usd,
       swap_count = pool_1min_stats.swap_count + 1`,
    [poolAddress, chain, bucketStart, feeUsd, volumeUsd]
  );
}

// ============================================================================
// 1-minute bucket aggregation (token-level, used by anomaly engine)
// ============================================================================

export async function upsertToken1minStat(
  tokenAddress: string,
  chain: ChainId,
  bucketStart: number,
  volumeUsd: number,
  feeUsd: number
): Promise<void> {
  await getPool().query(
    `INSERT INTO token_1min_stats (token_address, chain, bucket_start, total_volume_usd, total_fees_usd, swap_count)
     VALUES ($1, $2, $3, $4, $5, 1)
     ON CONFLICT (token_address, chain, bucket_start) DO UPDATE SET
       total_volume_usd = token_1min_stats.total_volume_usd + EXCLUDED.total_volume_usd,
       total_fees_usd = token_1min_stats.total_fees_usd + EXCLUDED.total_fees_usd,
       swap_count = token_1min_stats.swap_count + 1`,
    [tokenAddress, chain, bucketStart, volumeUsd, feeUsd]
  );
}

// ============================================================================
// Binance whitelist
// ============================================================================

export async function upsertBinanceBscToken(token: BinanceBscToken): Promise<void> {
  await getPool().query(
    `INSERT INTO binance_bsc_tokens (contract_address, symbol, base_asset, decimals, updated_at)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (contract_address) DO UPDATE SET
       symbol = EXCLUDED.symbol,
       base_asset = EXCLUDED.base_asset,
       decimals = EXCLUDED.decimals,
       updated_at = EXCLUDED.updated_at`,
    [
      token.contractAddress.toLowerCase(),
      token.symbol,
      token.baseAsset,
      token.decimals,
      token.updatedAt,
    ]
  );
}

export async function getAllBinanceBscTokens(): Promise<BinanceBscToken[]> {
  const res = await getPool().query<BinanceBscToken>(
    `SELECT contract_address AS "contractAddress", symbol, base_asset AS "baseAsset",
            decimals, updated_at AS "updatedAt"
     FROM binance_bsc_tokens`
  );
  return res.rows;
}

// ============================================================================
// BNB price history (from PancakeV2 WBNB/USDT pool swaps)
// ============================================================================

export interface BnbPriceRecord {
  timestamp: number;
  priceUsd: number;
  blockNumber: number;
  txHash: string;
  logIndex: number;
}

export async function insertBnbPrice(rec: BnbPriceRecord): Promise<void> {
  await getPool().query(
    `INSERT INTO bnb_price_history (timestamp, price_usd, block_number, tx_hash, log_index)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (tx_hash, log_index) DO NOTHING`,
    [rec.timestamp, rec.priceUsd, rec.blockNumber, rec.txHash, rec.logIndex]
  );
}

export async function bulkInsertBnbPrice(recs: BnbPriceRecord[]): Promise<void> {
  if (recs.length === 0) return;
  const CHUNK = 1000;
  for (let i = 0; i < recs.length; i += CHUNK) {
    const slice = recs.slice(i, i + CHUNK);
    const params: unknown[] = [];
    const placeholders: string[] = [];
    for (const r of slice) {
      const base = params.length;
      placeholders.push(`($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5})`);
      params.push(r.timestamp, r.priceUsd, r.blockNumber, r.txHash, r.logIndex);
    }
    await getPool().query(
      `INSERT INTO bnb_price_history (timestamp, price_usd, block_number, tx_hash, log_index)
       VALUES ${placeholders.join(",")}
       ON CONFLICT (tx_hash, log_index) DO NOTHING`,
      params
    );
  }
}

/** 给定 timestamp，返回最近的 BNB/USD 价格（前后 1h 内最近一条；找不到返回 null）。 */
export async function getBnbPriceAt(ts: number): Promise<number | null> {
  const ONE_HOUR_MS = 60 * 60 * 1000;
  const res = await getPool().query<{ price_usd: string }>(
    `SELECT price_usd::text
     FROM bnb_price_history
     WHERE timestamp BETWEEN $1 AND $2
     ORDER BY ABS(timestamp - $3) ASC
     LIMIT 1`,
    [ts - ONE_HOUR_MS, ts + ONE_HOUR_MS, ts]
  );
  return res.rows[0] ? Number(res.rows[0].price_usd) : null;
}

// ============================================================================
// Anomaly events
// ============================================================================

export async function insertAnomalyEvent(
  tokenAddress: string,
  symbol: string | null,
  rule: string,
  metrics: object,
  detectedAt: number = Date.now()
): Promise<void> {
  await getPool().query(
    `INSERT INTO anomaly_events (token_address, symbol, rule, metrics, detected_at)
     VALUES ($1, $2, $3, $4::jsonb, $5)`,
    [tokenAddress.toLowerCase(), symbol, rule, JSON.stringify(metrics), detectedAt]
  );
}
