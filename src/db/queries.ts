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
}

export function newBatchBuffer(): BatchBuffer {
  return { swaps: [], poolDeltas: new Map(), tokenDeltas: new Map() };
}

export function bufferAddSwap(buf: BatchBuffer, swap: SwapRecord): void {
  buf.swaps.push(swap);
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

const SWAPS_CHUNK = 1000;
const BUCKETS_CHUNK = 2000;

async function bulkInsertSwaps(swaps: SwapRecord[]): Promise<void> {
  for (let i = 0; i < swaps.length; i += SWAPS_CHUNK) {
    const slice = swaps.slice(i, i + SWAPS_CHUNK);
    const params: unknown[] = [];
    const placeholders: string[] = [];
    for (const s of slice) {
      const base = params.length;
      placeholders.push(
        `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6},$${base + 7},$${base + 8},$${base + 9},$${base + 10})`
      );
      params.push(s.poolAddress, s.chain, s.dex, s.txHash, s.amount0, s.amount1, s.feeUsd, s.volumeUsd, s.timestamp, s.blockNumber);
    }
    await getPool().query(
      `INSERT INTO swaps (pool_address, chain, dex, tx_hash, amount0, amount1, fee_usd, volume_usd, timestamp, block_number)
       VALUES ${placeholders.join(",")}
       ON CONFLICT (tx_hash, pool_address, amount0, amount1) DO NOTHING`,
      params
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
  ]);
}

// ============================================================================
// Swap / pool persistence
// ============================================================================

export async function insertSwap(swap: SwapRecord): Promise<void> {
  await getPool().query(
    `INSERT INTO swaps (pool_address, chain, dex, tx_hash, amount0, amount1, fee_usd, volume_usd, timestamp, block_number)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
     ON CONFLICT (tx_hash, pool_address, amount0, amount1) DO NOTHING`,
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
