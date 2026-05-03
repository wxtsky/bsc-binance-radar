import { type Log, slice, toEventHash } from "viem";
import { getClient } from "../clients/viem-clients.js";
import { CONTRACTS } from "../config/contracts.js";
import { CHAIN_STATIC, hasBaseToken, areBothBaseTokens } from "../config/chains.js";
import {
  UNISWAP_V3_POOL_ABI,
  PANCAKESWAP_V3_POOL_ABI,
  UNISWAP_V4_POOL_MANAGER_ABI,
  UNISWAP_V4_POSITION_MANAGER_ABI,
  PANCAKE_V4_CL_POOL_MANAGER_ABI,
  PANCAKE_V4_CL_POSITION_MANAGER_ABI,
  UNISWAP_V2_PAIR_ABI,
} from "../config/abis.js";
import {
  insertSwap,
  upsertPool,
  upsertV4Pool,
  getPoolRecord,
  getV4Pool,
  getPoolRecordsBatch,
  getV4PoolsBatch,
  bulkUpsertPools,
  bulkUpsertV4Pools,
  upsertPool1minStat,
  upsertToken1minStat,
  bufferAddSwap,
  bufferAddPoolBucket,
  bufferAddTokenBucket,
  bufferAddBnbPrice,
  insertBnbPrice,
  type BatchBuffer,
} from "../db/queries.js";
import { WRAPPED_NATIVE } from "../config/contracts.js";
import { getTokenPrices, getCacheKey } from "./price-service.js";
import { isWatchedToken } from "../token-tracker/watchlist.js";
import type { ChainId, DexType, SwapRecord, V4PoolTokenInfo } from "../types/index.js";
import { EventEmitter } from "events";
import { TTLCache, TTLSet } from "../utils/ttlCache.js";

export const swapEvents = new EventEmitter();
swapEvents.setMaxListeners(100);

const activeUnwatchers: Array<() => void> = [];
const unwatchersByChain = new Map<ChainId, Array<() => void>>();

const UNISWAP_V3_SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
const PANCAKESWAP_V3_SWAP_TOPIC = "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83";
// V2 (PancakeV2 / UniswapV2) Pair Swap event topic
export const UNISWAP_V2_SWAP_TOPIC = toEventHash(
  "Swap(address,uint256,uint256,uint256,uint256,address)"
);
// PancakeV4 (Infinity) CL Swap event topic
export const PANCAKE_V4_CL_SWAP_TOPIC = toEventHash(
  "Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24,uint16)"
);

const MAX_POOL_CACHE_SIZE = 10000;
const MAX_INVALID_CACHE_SIZE = 50000;

const poolInfoCache = new TTLCache<string, { token0: string; token1: string; feeTier: number }>({
  max: MAX_POOL_CACHE_SIZE,
});
const v4PoolInfoCache = new TTLCache<string, V4PoolTokenInfo>({ max: MAX_POOL_CACHE_SIZE });
const invalidPoolCache = new TTLSet<string>({ max: MAX_INVALID_CACHE_SIZE });
const invalidV4PoolCache = new TTLSet<string>({ max: MAX_INVALID_CACHE_SIZE });

async function getV4PoolInfo(poolId: string, chain: ChainId): Promise<V4PoolTokenInfo | null> {
  const cacheKey = `${chain}:${poolId}`;
  if (v4PoolInfoCache.has(cacheKey)) return v4PoolInfoCache.get(cacheKey)!;
  if (invalidV4PoolCache.has(cacheKey)) return null;

  const dbPool = await getV4Pool(poolId, chain);
  if (dbPool) {
    v4PoolInfoCache.set(cacheKey, dbPool);
    return dbPool;
  }

  try {
    const client = getClient(chain);
    const positionManager = CONTRACTS[chain].uniswapV4PositionManager;
    const poolIdBytes25 = slice(poolId as `0x${string}`, 0, 25);

    const result = await client.readContract({
      address: positionManager as `0x${string}`,
      abi: UNISWAP_V4_POSITION_MANAGER_ABI,
      functionName: "poolKeys",
      args: [poolIdBytes25],
    });

    const [currency0, currency1, fee, tickSpacing, hooks] = result as [
      string,
      string,
      number,
      number,
      string
    ];

    const ZERO = "0x0000000000000000000000000000000000000000";
    if (currency0.toLowerCase() === ZERO && currency1.toLowerCase() === ZERO) {
      invalidV4PoolCache.add(cacheKey);
      return null;
    }

    const poolInfo: V4PoolTokenInfo = {
      currency0,
      currency1,
      fee: Number(fee),
      tickSpacing: Number(tickSpacing),
      hooks,
    };
    v4PoolInfoCache.set(cacheKey, poolInfo);
    await upsertV4Pool(poolId, chain, poolInfo);
    return poolInfo;
  } catch {
    invalidV4PoolCache.add(cacheKey);
    return null;
  }
}

async function verifyPoolFactory(poolAddress: string, chain: ChainId, dex: DexType): Promise<boolean> {
  const cacheKey = `${chain}:${poolAddress.toLowerCase()}`;
  if (invalidPoolCache.has(cacheKey)) return false;

  try {
    const client = getClient(chain);
    const abi = dex === "pancakeswap-v3" ? PANCAKESWAP_V3_POOL_ABI : UNISWAP_V3_POOL_ABI;

    const factory = (await client.readContract({
      address: poolAddress as `0x${string}`,
      abi,
      functionName: "factory",
    })) as string;

    const expectedFactory =
      dex === "pancakeswap-v3"
        ? CONTRACTS[chain].pancakeswapV3Factory
        : CONTRACTS[chain].uniswapV3Factory;

    if (factory.toLowerCase() !== expectedFactory.toLowerCase()) {
      invalidPoolCache.add(cacheKey);
      return false;
    }
    return true;
  } catch {
    invalidPoolCache.add(cacheKey);
    return false;
  }
}

async function getPoolInfo(
  poolAddress: string,
  chain: ChainId,
  dex: DexType
): Promise<{ token0: string; token1: string; feeTier: number } | null> {
  const cacheKey = `${chain}:${poolAddress.toLowerCase()}`;
  if (poolInfoCache.has(cacheKey)) return poolInfoCache.get(cacheKey)!;
  if (invalidPoolCache.has(cacheKey)) return null;

  const dbPool = await getPoolRecord(poolAddress, chain);
  if (dbPool) {
    const info = { token0: dbPool.token0, token1: dbPool.token1, feeTier: dbPool.feeTier };
    poolInfoCache.set(cacheKey, info);
    return info;
  }

  const isValid = await verifyPoolFactory(poolAddress, chain, dex);
  if (!isValid) return null;

  try {
    const client = getClient(chain);
    const abi = dex === "pancakeswap-v3" ? PANCAKESWAP_V3_POOL_ABI : UNISWAP_V3_POOL_ABI;
    const poolAddr = poolAddress as `0x${string}`;

    const results = await client.multicall({
      contracts: [
        { address: poolAddr, abi, functionName: "token0" },
        { address: poolAddr, abi, functionName: "token1" },
        { address: poolAddr, abi, functionName: "fee" },
      ],
    });

    if (
      results[0].status !== "success" ||
      results[1].status !== "success" ||
      results[2].status !== "success"
    ) {
      return null;
    }

    const info = {
      token0: results[0].result as string,
      token1: results[1].result as string,
      feeTier: Number(results[2].result),
    };
    poolInfoCache.set(cacheKey, info);
    await upsertPool({
      address: poolAddress,
      chain,
      dex,
      token0: info.token0,
      token1: info.token1,
      feeTier: info.feeTier,
    });
    return info;
  } catch (error) {
    console.error(`[Radar] Error fetching pool info for ${poolAddress}:`, error);
    return null;
  }
}

/** 批量 prefetch V3 / Pancake-V3 pool info（用 viem multicall 一次拿一批 pool 的 token0/token1/fee）
 *  跳过 verifyPoolFactory（90d 历史里 fake pool 概率极低）。
 */
export async function prefetchV3PoolInfo(
  poolAddresses: string[],
  chain: ChainId,
  dex: "uniswap-v3" | "pancakeswap-v3"
): Promise<void> {
  if (poolAddresses.length === 0) return;
  const unique = [...new Set(poolAddresses.map((a) => a.toLowerCase()))];
  const newAddrs = unique.filter((addr) => {
    const k = `${chain}:${addr}`;
    return !poolInfoCache.has(k) && !invalidPoolCache.has(k);
  });
  if (newAddrs.length === 0) return;

  // 单 SQL 批量查（替代 N+1 SELECT）—— PG round-trip 从 N 次降到 1 次。
  const dbMap = await getPoolRecordsBatch(newAddrs, chain).catch(() => new Map());
  const stillMissing: string[] = [];
  for (const addr of newAddrs) {
    const dbPool = dbMap.get(addr);
    if (dbPool) {
      poolInfoCache.set(`${chain}:${addr}`, {
        token0: dbPool.token0,
        token1: dbPool.token1,
        feeTier: dbPool.feeTier,
      });
    } else {
      stillMissing.push(addr);
    }
  }
  if (stillMissing.length === 0) return;

  const abi = dex === "pancakeswap-v3" ? PANCAKESWAP_V3_POOL_ABI : UNISWAP_V3_POOL_ABI;
  const client = getClient(chain);
  const contracts = stillMissing.flatMap((addr) => [
    { address: addr as `0x${string}`, abi, functionName: "token0" as const },
    { address: addr as `0x${string}`, abi, functionName: "token1" as const },
    { address: addr as `0x${string}`, abi, functionName: "fee" as const },
  ]);

  let results: { status: string; result?: unknown }[];
  try {
    results = (await client.multicall({ contracts: contracts as unknown as never })) as {
      status: string;
      result?: unknown;
    }[];
  } catch (err) {
    console.warn(`[Radar] prefetchV3PoolInfo multicall failed (n=${stillMissing.length}): ${(err as Error).message}`);
    return;
  }

  // 收集成 batch UPSERT，替代 N 次单条 INSERT。
  const upsertRows: import("../types/index.js").PoolInfo[] = [];
  for (let i = 0; i < stillMissing.length; i++) {
    const addr = stillMissing[i];
    const r0 = results[i * 3];
    const r1 = results[i * 3 + 1];
    const r2 = results[i * 3 + 2];
    if (
      !r0 || !r1 || !r2 ||
      r0.status !== "success" || r1.status !== "success" || r2.status !== "success"
    ) {
      invalidPoolCache.add(`${chain}:${addr}`);
      continue;
    }
    const info = {
      token0: r0.result as string,
      token1: r1.result as string,
      feeTier: Number(r2.result),
    };
    poolInfoCache.set(`${chain}:${addr}`, info);
    upsertRows.push({ address: addr, chain, dex, token0: info.token0, token1: info.token1, feeTier: info.feeTier });
  }
  await bulkUpsertPools(upsertRows);
}

/** 批量 prefetch UniV4 pool info（同 PancakeV4 CL，但用 UniV4 PositionManager） */
export async function prefetchV4PoolInfo(
  poolIds: string[],
  chain: ChainId
): Promise<void> {
  if (poolIds.length === 0) return;
  const unique = [...new Set(poolIds)];
  const newIds = unique.filter((id) => {
    const k = `${chain}:${id}`;
    return !v4PoolInfoCache.has(k) && !invalidV4PoolCache.has(k);
  });
  if (newIds.length === 0) return;

  const dbMap = await getV4PoolsBatch(newIds, chain).catch(() => new Map());
  const stillMissing: string[] = [];
  for (const id of newIds) {
    const dbPool = dbMap.get(id);
    if (dbPool) {
      v4PoolInfoCache.set(`${chain}:${id}`, dbPool);
    } else {
      stillMissing.push(id);
    }
  }
  if (stillMissing.length === 0) return;

  const positionManager = CONTRACTS[chain].uniswapV4PositionManager as `0x${string}`;
  const client = getClient(chain);
  const contracts = stillMissing.map((id) => ({
    address: positionManager,
    abi: UNISWAP_V4_POSITION_MANAGER_ABI,
    functionName: "poolKeys" as const,
    args: [slice(id as `0x${string}`, 0, 25)] as const,
  }));

  let results: { status: string; result?: unknown }[];
  try {
    results = (await client.multicall({ contracts: contracts as unknown as never })) as {
      status: string;
      result?: unknown;
    }[];
  } catch (err) {
    console.warn(`[Radar] prefetchV4PoolInfo multicall failed (n=${stillMissing.length}): ${(err as Error).message}`);
    return;
  }

  const ZERO = "0x0000000000000000000000000000000000000000";
  const upsertRows: Array<{ poolId: string; chain: ChainId; info: V4PoolTokenInfo }> = [];
  for (let i = 0; i < stillMissing.length; i++) {
    const id = stillMissing[i];
    const r = results[i];
    if (!r || r.status !== "success") {
      invalidV4PoolCache.add(`${chain}:${id}`);
      continue;
    }
    const tup = r.result as [string, string, number, number, string];
    const [c0, c1, fee, tickSpacing, hooks] = tup;
    if ((c0 || ZERO).toLowerCase() === ZERO && (c1 || ZERO).toLowerCase() === ZERO) {
      invalidV4PoolCache.add(`${chain}:${id}`);
      continue;
    }
    const info: V4PoolTokenInfo = {
      currency0: c0,
      currency1: c1,
      fee: Number(fee),
      tickSpacing: Number(tickSpacing),
      hooks,
    };
    v4PoolInfoCache.set(`${chain}:${id}`, info);
    upsertRows.push({ poolId: id, chain, info });
  }
  await bulkUpsertV4Pools(upsertRows);
}

function parseint256(hex: string): bigint {
  const value = BigInt(`0x${hex}`);
  const MAX_INT256 = 2n ** 255n;
  if (value >= MAX_INT256) return value - 2n ** 256n;
  return value;
}

function parseint128(hex: string): bigint {
  const value = BigInt(`0x${hex}`);
  const MAX_INT128 = 2n ** 127n;
  if (value >= MAX_INT128) return value - 2n ** 128n;
  return value;
}

function nonBaseToken(token0: string, token1: string, chain: ChainId): string | null {
  const set = CHAIN_STATIC[chain].baseTokens;
  const t0 = token0.toLowerCase();
  const t1 = token1.toLowerCase();
  if (!set.has(t0)) return t0;
  if (!set.has(t1)) return t1;
  return null;
}

export async function processV3SwapLog(
  log: Log,
  chain: ChainId,
  dex: DexType,
  overrideTimestamp?: number,
  buffer?: BatchBuffer
): Promise<void> {
  try {
    const poolAddress = log.address.toLowerCase();
    const poolInfo = await getPoolInfo(poolAddress, chain, dex);
    if (!poolInfo) return;

    const filters = CHAIN_STATIC[chain].filters;
    if (poolInfo.feeTier <= filters.minFeeTier) return;

    if (!hasBaseToken(poolInfo.token0, poolInfo.token1, chain)) return;
    if (areBothBaseTokens(poolInfo.token0, poolInfo.token1, chain)) return;

    // Filter: only watch pools whose non-base token is in Binance/BSC whitelist
    const target = nonBaseToken(poolInfo.token0, poolInfo.token1, chain);
    if (!target || !isWatchedToken(target)) return;

    const amount0 = parseint256(log.data.slice(2, 66));
    const amount1 = parseint256(log.data.slice(66, 130));

    const prices = await getTokenPrices([
      { chain, address: poolInfo.token0 },
      { chain, address: poolInfo.token1 },
    ]);

    const cacheKey0 = getCacheKey(poolInfo.token0, chain);
    const cacheKey1 = getCacheKey(poolInfo.token1, chain);

    const price0 = prices.get(cacheKey0)?.price || 0;
    const price1 = prices.get(cacheKey1)?.price || 0;
    const decimals0 = prices.get(cacheKey0)?.decimals || 18;
    const decimals1 = prices.get(cacheKey1)?.decimals || 18;

    let totalFeeUsd = 0;
    let volumeUsd = 0;
    const feeTier = poolInfo.feeTier;

    if (amount0 > 0n && price0 > 0) {
      volumeUsd = (Number(amount0) / 10 ** decimals0) * price0;
      totalFeeUsd = (volumeUsd * feeTier) / 1_000_000;
    } else if (amount1 > 0n && price1 > 0) {
      volumeUsd = (Number(amount1) / 10 ** decimals1) * price1;
      totalFeeUsd = (volumeUsd * feeTier) / 1_000_000;
    } else if (amount0 < 0n && price0 > 0) {
      volumeUsd = (Number(-amount0) / 10 ** decimals0) * price0;
      totalFeeUsd = (volumeUsd * feeTier) / (1_000_000 - feeTier);
    } else if (amount1 < 0n && price1 > 0) {
      volumeUsd = (Number(-amount1) / 10 ** decimals1) * price1;
      totalFeeUsd = (volumeUsd * feeTier) / (1_000_000 - feeTier);
    }

    if (dex === "pancakeswap-v3") {
      totalFeeUsd = (totalFeeUsd * 2) / 3;
    }

    if (!Number.isFinite(totalFeeUsd) || totalFeeUsd <= 0 || totalFeeUsd > filters.maxSingleSwapFeeUsd) {
      return;
    }

    const timestamp = overrideTimestamp ?? Date.now();
    const safeVolume = Number.isFinite(volumeUsd) ? volumeUsd : 0;
    const swap: SwapRecord = {
      poolAddress,
      chain,
      dex,
      txHash: log.transactionHash || "",
      amount0: amount0.toString(),
      amount1: amount1.toString(),
      feeUsd: totalFeeUsd,
      volumeUsd: safeVolume,
      timestamp,
      blockNumber: Number(log.blockNumber),
    };

    const bucketStart = Math.floor(timestamp / 60_000) * 60_000;
    if (buffer) {
      bufferAddSwap(buffer, swap);
      bufferAddPoolBucket(buffer, poolAddress, chain, bucketStart, totalFeeUsd, safeVolume);
      bufferAddTokenBucket(buffer, target, chain, bucketStart, safeVolume, totalFeeUsd);
    } else {
      await Promise.all([
        insertSwap(swap),
        upsertPool1minStat(poolAddress, chain, bucketStart, totalFeeUsd, safeVolume),
        upsertToken1minStat(target, chain, bucketStart, safeVolume, totalFeeUsd),
      ]);
    }
    if (overrideTimestamp === undefined) {
      swapEvents.emit("swap", { chain, token: target });
      const symbol0 = prices.get(cacheKey0)?.symbol || "UNKNOWN";
      const symbol1 = prices.get(cacheKey1)?.symbol || "UNKNOWN";
      console.log(`[Radar] [${chain}] ${dex} ${symbol0}/${symbol1} fee=$${totalFeeUsd.toFixed(2)} vol=$${safeVolume.toFixed(0)}`);
    }
  } catch (error) {
    console.error("[Radar] Error processing V3 swap log:", error);
  }
}

export async function processV4SwapLog(
  log: Log,
  chain: ChainId,
  overrideTimestamp?: number,
  buffer?: BatchBuffer
): Promise<void> {
  try {
    const poolId = log.topics[1];
    if (!poolId) return;

    const poolInfo = await getV4PoolInfo(poolId, chain);
    if (!poolInfo) return;

    const amount0 = parseint128(log.data.slice(34, 66));
    const amount1 = parseint128(log.data.slice(98, 130));
    const feeTier = parseInt(log.data.slice(380, 386), 16);

    const filters = CHAIN_STATIC[chain].filters;
    if (feeTier <= filters.minFeeTier) return;

    if (!hasBaseToken(poolInfo.currency0, poolInfo.currency1, chain)) return;
    if (areBothBaseTokens(poolInfo.currency0, poolInfo.currency1, chain)) return;

    const target = nonBaseToken(poolInfo.currency0, poolInfo.currency1, chain);
    if (!target || !isWatchedToken(target)) return;

    const prices = await getTokenPrices([
      { chain, address: poolInfo.currency0 },
      { chain, address: poolInfo.currency1 },
    ]);

    const cacheKey0 = getCacheKey(poolInfo.currency0, chain);
    const cacheKey1 = getCacheKey(poolInfo.currency1, chain);

    const price0 = prices.get(cacheKey0)?.price || 0;
    const price1 = prices.get(cacheKey1)?.price || 0;
    const decimals0 = prices.get(cacheKey0)?.decimals || 18;
    const decimals1 = prices.get(cacheKey1)?.decimals || 18;

    let totalFeeUsd = 0;
    let volumeUsd = 0;

    if (amount0 > 0n && price0 > 0) {
      volumeUsd = (Number(amount0) / 10 ** decimals0) * price0;
      totalFeeUsd = (volumeUsd * feeTier) / 1_000_000;
    } else if (amount1 > 0n && price1 > 0) {
      volumeUsd = (Number(amount1) / 10 ** decimals1) * price1;
      totalFeeUsd = (volumeUsd * feeTier) / 1_000_000;
    } else if (amount0 < 0n && price0 > 0) {
      volumeUsd = (Number(-amount0) / 10 ** decimals0) * price0;
      totalFeeUsd = (volumeUsd * feeTier) / (1_000_000 - feeTier);
    } else if (amount1 < 0n && price1 > 0) {
      volumeUsd = (Number(-amount1) / 10 ** decimals1) * price1;
      totalFeeUsd = (volumeUsd * feeTier) / (1_000_000 - feeTier);
    }

    if (!Number.isFinite(totalFeeUsd) || totalFeeUsd <= 0 || totalFeeUsd > filters.maxSingleSwapFeeUsd) {
      return;
    }

    const timestamp = overrideTimestamp ?? Date.now();
    const safeVolume = Number.isFinite(volumeUsd) ? volumeUsd : 0;
    const swap: SwapRecord = {
      poolAddress: poolId,
      chain,
      dex: "uniswap-v4",
      txHash: log.transactionHash || "",
      amount0: amount0.toString(),
      amount1: amount1.toString(),
      feeUsd: totalFeeUsd,
      volumeUsd: safeVolume,
      timestamp,
      blockNumber: Number(log.blockNumber),
    };

    const bucketStart = Math.floor(timestamp / 60_000) * 60_000;
    if (buffer) {
      bufferAddSwap(buffer, swap);
      bufferAddPoolBucket(buffer, poolId, chain, bucketStart, totalFeeUsd, safeVolume);
      bufferAddTokenBucket(buffer, target, chain, bucketStart, safeVolume, totalFeeUsd);
    } else {
      await Promise.all([
        insertSwap(swap),
        upsertPool1minStat(poolId, chain, bucketStart, totalFeeUsd, safeVolume),
        upsertToken1minStat(target, chain, bucketStart, safeVolume, totalFeeUsd),
      ]);
    }
    if (overrideTimestamp === undefined) {
      swapEvents.emit("swap", { chain, token: target });
      const symbol0 = prices.get(cacheKey0)?.symbol || "UNKNOWN";
      const symbol1 = prices.get(cacheKey1)?.symbol || "UNKNOWN";
      console.log(`[Radar] [${chain}] uniswap-v4 ${symbol0}/${symbol1} fee=$${totalFeeUsd.toFixed(2)} vol=$${safeVolume.toFixed(0)}`);
    }
  } catch (error) {
    console.error("[Radar] Error processing V4 swap log:", error);
  }
}

// =============================================================================
// PancakeV2 WBNB/USDT 池：仅记录 BNB 历史价（不入 swaps 表，不参与异动检测）
// =============================================================================

/** PancakeV2 BNB price pool 在 BSC 上 token0=BUSD/USDT/WBNB ?，启动时探测决定方向。
 *  这里硬编码：池 0x16b9a82891338f9bA80E2D6970FddA79D1eb0daE 在 PancakeV2 中
 *  token0=USDT (0x55d3...), token1=WBNB (0xbb4c...)。如果以后换池子要重新确认。
 */
let bnbPoolWbnbIs1: boolean | null = null;

async function ensureBnbPoolDirection(chain: ChainId): Promise<boolean> {
  if (bnbPoolWbnbIs1 !== null) return bnbPoolWbnbIs1;
  const wbnb = WRAPPED_NATIVE[chain].toLowerCase();
  const poolAddr = CONTRACTS[chain].bnbPricePool as `0x${string}`;
  try {
    const client = getClient(chain);
    const [t0, t1] = await Promise.all([
      client.readContract({ address: poolAddr, abi: UNISWAP_V2_PAIR_ABI, functionName: "token0" }),
      client.readContract({ address: poolAddr, abi: UNISWAP_V2_PAIR_ABI, functionName: "token1" }),
    ]);
    const token0 = (t0 as string).toLowerCase();
    const token1 = (t1 as string).toLowerCase();
    if (token0 !== wbnb && token1 !== wbnb) {
      throw new Error(`bnbPricePool ${poolAddr} 不含 WBNB（token0=${token0}, token1=${token1}）`);
    }
    bnbPoolWbnbIs1 = token1 === wbnb;
    console.log(
      `[Radar] BNB price pool direction: WBNB is ${bnbPoolWbnbIs1 ? "token1" : "token0"}`
    );
    return bnbPoolWbnbIs1;
  } catch (err) {
    console.error("[Radar] ensureBnbPoolDirection failed:", err);
    throw err;
  }
}

/** 处理 V2 Swap 事件，反算 BNB/USD 价并入 bnb_price_history。
 *  V2 Swap log.data: amount0In, amount1In, amount0Out, amount1Out (各 32 字节 uint)
 */
export async function processV2BnbPriceSwap(
  log: Log,
  chain: ChainId,
  overrideTimestamp?: number,
  buffer?: BatchBuffer
): Promise<void> {
  try {
    const wbnbIs1 = await ensureBnbPoolDirection(chain);
    const data = log.data.slice(2); // strip 0x
    if (data.length < 256) return; // 不足 4 个 uint256
    const amt0In = BigInt("0x" + data.slice(0, 64));
    const amt1In = BigInt("0x" + data.slice(64, 128));
    const amt0Out = BigInt("0x" + data.slice(128, 192));
    const amt1Out = BigInt("0x" + data.slice(192, 256));

    // V2 一笔 swap 通常一个 In 一个 Out 非零
    // 用户买 WBNB → amount{stable}In > 0, amount{wbnb}Out > 0 → price = stableIn / wbnbOut
    // 用户卖 WBNB → amount{wbnb}In > 0, amount{stable}Out > 0 → price = stableOut / wbnbIn
    const wbnbIn = wbnbIs1 ? amt1In : amt0In;
    const wbnbOut = wbnbIs1 ? amt1Out : amt0Out;
    const stableIn = wbnbIs1 ? amt0In : amt1In;
    const stableOut = wbnbIs1 ? amt0Out : amt1Out;

    // 用 BigInt 缩放避免大数 → Number 丢精度（USDT amount 1e18+ > 2^53 ≈ 9e15）
    const SCALE = 1_000_000n;
    let priceScaled = 0n;
    if (wbnbOut > 0n && stableIn > 0n) {
      priceScaled = (stableIn * SCALE) / wbnbOut;
    } else if (wbnbIn > 0n && stableOut > 0n) {
      priceScaled = (stableOut * SCALE) / wbnbIn;
    }
    const priceUsd = Number(priceScaled) / Number(SCALE);

    // BSC USDT 18 decimals, WBNB 18 decimals → ratio 不需 decimals 调整
    if (!Number.isFinite(priceUsd) || priceUsd <= 0 || priceUsd > 100_000) return;

    const timestamp = overrideTimestamp ?? Date.now();
    const txHash = log.transactionHash || "";
    const logIndex = Number(log.logIndex ?? 0);
    const blockNumber = Number(log.blockNumber ?? 0);
    const rec = { timestamp, priceUsd, blockNumber, txHash, logIndex };

    if (buffer) {
      bufferAddBnbPrice(buffer, rec);
    } else {
      await insertBnbPrice(rec);
    }

    if (overrideTimestamp === undefined && Math.random() < 0.005) {
      // 实时模式偶尔打日志确认存活，避免刷屏
      console.log(`[Radar] BNB price tick: $${priceUsd.toFixed(2)}`);
    }
  } catch (error) {
    console.error("[Radar] Error processing V2 BNB swap log:", error);
  }
}

// =============================================================================
// PancakeSwap V4 (Infinity) CL — concentrated liquidity 池
// 跟 Uniswap V4 architecture 一样：singleton PoolManager + bytes32 PoolId
// =============================================================================

const pcsV4PoolInfoCache = new TTLCache<string, V4PoolTokenInfo>({ max: MAX_POOL_CACHE_SIZE });
const invalidPcsV4PoolCache = new TTLSet<string>({ max: MAX_INVALID_CACHE_SIZE });

async function getPcsV4ClPoolInfo(
  poolId: string,
  chain: ChainId
): Promise<V4PoolTokenInfo | null> {
  const cacheKey = `pcsv4cl:${chain}:${poolId}`;
  if (pcsV4PoolInfoCache.has(cacheKey)) return pcsV4PoolInfoCache.get(cacheKey)!;
  if (invalidPcsV4PoolCache.has(cacheKey)) return null;

  // 复用 v4_pools 表存（用 'pcsv4cl:' 前缀区分 namespace，避免与 UniV4 PoolId 撞）
  const dbPool = await getV4Pool(`pcsv4cl:${poolId}`, chain);
  if (dbPool) {
    pcsV4PoolInfoCache.set(cacheKey, dbPool);
    return dbPool;
  }

  try {
    const client = getClient(chain);
    const positionManager = CONTRACTS[chain].pancakeswapV4ClPositionManager;
    const poolIdBytes25 = slice(poolId as `0x${string}`, 0, 25);

    const result = await client.readContract({
      address: positionManager as `0x${string}`,
      abi: PANCAKE_V4_CL_POSITION_MANAGER_ABI,
      functionName: "poolKeys",
      args: [poolIdBytes25],
    });

    const [currency0, currency1, hooks, _poolManager, fee, _parameters] = result as [
      string,
      string,
      string,
      string,
      number,
      string
    ];

    const ZERO = "0x0000000000000000000000000000000000000000";
    if ((currency0 || ZERO).toLowerCase() === ZERO && (currency1 || ZERO).toLowerCase() === ZERO) {
      invalidPcsV4PoolCache.add(cacheKey);
      return null;
    }

    // PancakeV4 PoolKey 没有 tickSpacing 字段（在 parameters bytes32 里），这里塞 0
    const info: V4PoolTokenInfo = {
      currency0,
      currency1,
      fee: Number(fee),
      tickSpacing: 0,
      hooks,
    };
    pcsV4PoolInfoCache.set(cacheKey, info);
    await upsertV4Pool(`pcsv4cl:${poolId}`, chain, info);
    return info;
  } catch {
    invalidPcsV4PoolCache.add(cacheKey);
    return null;
  }
}

/** 批量 prefetch PancakeV4 CL pool info（用 viem multicall 一次性 fetch 多个 poolKeys）
 *  backfill 时 cache miss 多，此函数先把一批 logs 的 poolIds 一次 multicall 写入 cache + DB，
 *  之后 processPcsV4ClSwapLog 全 cache hit 极快。
 */
export async function prefetchPcsV4ClPoolInfo(
  poolIds: string[],
  chain: ChainId
): Promise<void> {
  if (poolIds.length === 0) return;
  const unique = [...new Set(poolIds)];
  const newIds = unique.filter((id) => {
    const k = `pcsv4cl:${chain}:${id}`;
    return !pcsV4PoolInfoCache.has(k) && !invalidPcsV4PoolCache.has(k);
  });
  if (newIds.length === 0) return;

  // 单 SQL 批量查（v4_pools 表的 pcsv4cl 加 prefix namespace）
  const prefixedIds = newIds.map((id) => `pcsv4cl:${id}`);
  const dbMap = await getV4PoolsBatch(prefixedIds, chain).catch(() => new Map());
  const stillMissing: string[] = [];
  for (const id of newIds) {
    const dbPool = dbMap.get(`pcsv4cl:${id}`);
    if (dbPool) {
      pcsV4PoolInfoCache.set(`pcsv4cl:${chain}:${id}`, dbPool);
    } else {
      stillMissing.push(id);
    }
  }
  if (stillMissing.length === 0) return;

  // 链上 multicall 拿剩下的 poolKeys
  const positionManager = CONTRACTS[chain].pancakeswapV4ClPositionManager as `0x${string}`;
  const client = getClient(chain);
  const contracts = stillMissing.map((id) => ({
    address: positionManager,
    abi: PANCAKE_V4_CL_POSITION_MANAGER_ABI,
    functionName: "poolKeys" as const,
    args: [slice(id as `0x${string}`, 0, 25)] as const,
  }));

  // viem multicall 自动按 batch_size 拆分，一次 RPC 解决（typed as MulticallContracts ABI 固定）
  let results: { status: string; result?: unknown }[];
  try {
    results = (await client.multicall({ contracts: contracts as unknown as never })) as {
      status: string;
      result?: unknown;
    }[];
  } catch (err) {
    console.warn(`[Radar] prefetchPcsV4ClPoolInfo multicall failed (n=${stillMissing.length}): ${(err as Error).message}`);
    return;
  }

  const ZERO = "0x0000000000000000000000000000000000000000";
  const upsertRows: Array<{ poolId: string; chain: ChainId; info: V4PoolTokenInfo }> = [];
  for (let i = 0; i < stillMissing.length; i++) {
    const id = stillMissing[i];
    const cacheKey = `pcsv4cl:${chain}:${id}`;
    const r = results[i];
    if (!r || r.status !== "success") {
      invalidPcsV4PoolCache.add(cacheKey);
      continue;
    }
    const tup = r.result as [string, string, string, string, number, string];
    const [c0, c1, hooks, _pm, fee, _params] = tup;
    if ((c0 || ZERO).toLowerCase() === ZERO && (c1 || ZERO).toLowerCase() === ZERO) {
      invalidPcsV4PoolCache.add(cacheKey);
      continue;
    }
    const info: V4PoolTokenInfo = {
      currency0: c0,
      currency1: c1,
      fee: Number(fee),
      tickSpacing: 0,
      hooks,
    };
    pcsV4PoolInfoCache.set(cacheKey, info);
    upsertRows.push({ poolId: `pcsv4cl:${id}`, chain, info });
  }
  await bulkUpsertV4Pools(upsertRows);
}

/** PancakeV4 CL Swap log data: amount0(int128) | amount1(int128) | sqrtPriceX96(uint160)
 *  | liquidity(uint128) | tick(int24) | fee(uint24) | protocolFee(uint16)
 *  与 UniV4 前 6 字段布局相同（每字段 32 字节 ABI 编码），fee 偏移在 [380:386] 不变。
 */
export async function processPcsV4ClSwapLog(
  log: Log,
  chain: ChainId,
  overrideTimestamp?: number,
  buffer?: BatchBuffer
): Promise<void> {
  try {
    const poolId = log.topics[1];
    if (!poolId) return;

    const poolInfo = await getPcsV4ClPoolInfo(poolId, chain);
    if (!poolInfo) return;

    const amount0 = parseint128(log.data.slice(34, 66));
    const amount1 = parseint128(log.data.slice(98, 130));
    const feeTier = parseInt(log.data.slice(380, 386), 16);

    const filters = CHAIN_STATIC[chain].filters;
    if (feeTier <= filters.minFeeTier) return;

    if (!hasBaseToken(poolInfo.currency0, poolInfo.currency1, chain)) return;
    if (areBothBaseTokens(poolInfo.currency0, poolInfo.currency1, chain)) return;

    const target = nonBaseToken(poolInfo.currency0, poolInfo.currency1, chain);
    if (!target || !isWatchedToken(target)) return;

    const prices = await getTokenPrices([
      { chain, address: poolInfo.currency0 },
      { chain, address: poolInfo.currency1 },
    ]);

    const cacheKey0 = getCacheKey(poolInfo.currency0, chain);
    const cacheKey1 = getCacheKey(poolInfo.currency1, chain);

    const price0 = prices.get(cacheKey0)?.price || 0;
    const price1 = prices.get(cacheKey1)?.price || 0;
    const decimals0 = prices.get(cacheKey0)?.decimals || 18;
    const decimals1 = prices.get(cacheKey1)?.decimals || 18;

    let totalFeeUsd = 0;
    let volumeUsd = 0;

    if (amount0 > 0n && price0 > 0) {
      volumeUsd = (Number(amount0) / 10 ** decimals0) * price0;
      totalFeeUsd = (volumeUsd * feeTier) / 1_000_000;
    } else if (amount1 > 0n && price1 > 0) {
      volumeUsd = (Number(amount1) / 10 ** decimals1) * price1;
      totalFeeUsd = (volumeUsd * feeTier) / 1_000_000;
    } else if (amount0 < 0n && price0 > 0) {
      volumeUsd = (Number(-amount0) / 10 ** decimals0) * price0;
      totalFeeUsd = (volumeUsd * feeTier) / (1_000_000 - feeTier);
    } else if (amount1 < 0n && price1 > 0) {
      volumeUsd = (Number(-amount1) / 10 ** decimals1) * price1;
      totalFeeUsd = (volumeUsd * feeTier) / (1_000_000 - feeTier);
    }

    // PancakeV3 收 1/3 协议费，CL 暂按官方文档同样按 2/3 给 LP；如有出入再调
    totalFeeUsd = (totalFeeUsd * 2) / 3;

    if (
      !Number.isFinite(totalFeeUsd) ||
      totalFeeUsd <= 0 ||
      totalFeeUsd > filters.maxSingleSwapFeeUsd
    )
      return;

    const timestamp = overrideTimestamp ?? Date.now();
    const safeVolume = Number.isFinite(volumeUsd) ? volumeUsd : 0;
    // 用 'pcsv4cl:' 前缀作为 swaps.pool_address，跟 v4_pools.pool_id 一致
    // → rebuildBucketsFromSwaps 的 JOIN（pool_id = swaps.pool_address）能匹配上
    const namespacedPoolId = `pcsv4cl:${poolId}`;
    const swap: SwapRecord = {
      poolAddress: namespacedPoolId,
      chain,
      dex: "pancakeswap-v4-cl",
      txHash: log.transactionHash || "",
      amount0: amount0.toString(),
      amount1: amount1.toString(),
      feeUsd: totalFeeUsd,
      volumeUsd: safeVolume,
      timestamp,
      blockNumber: Number(log.blockNumber),
    };

    const bucketStart = Math.floor(timestamp / 60_000) * 60_000;
    if (buffer) {
      bufferAddSwap(buffer, swap);
      bufferAddPoolBucket(buffer, namespacedPoolId, chain, bucketStart, totalFeeUsd, safeVolume);
      bufferAddTokenBucket(buffer, target, chain, bucketStart, safeVolume, totalFeeUsd);
    } else {
      await Promise.all([
        insertSwap(swap),
        upsertPool1minStat(namespacedPoolId, chain, bucketStart, totalFeeUsd, safeVolume),
        upsertToken1minStat(target, chain, bucketStart, safeVolume, totalFeeUsd),
      ]);
    }
    if (overrideTimestamp === undefined) {
      swapEvents.emit("swap", { chain, token: target });
      const symbol0 = prices.get(cacheKey0)?.symbol || "UNKNOWN";
      const symbol1 = prices.get(cacheKey1)?.symbol || "UNKNOWN";
      console.log(
        `[Radar] [${chain}] pancakeswap-v4-cl ${symbol0}/${symbol1} fee=$${totalFeeUsd.toFixed(2)} vol=$${safeVolume.toFixed(0)}`
      );
    }
  } catch (error) {
    console.error("[Radar] Error processing PancakeV4 CL swap log:", error);
  }
}

export async function startSwapListener(chain: ChainId, dex: DexType): Promise<void> {
  const client = getClient(chain);

  if (dex === "uniswap-v4") {
    const poolManager = CONTRACTS[chain].uniswapV4PoolManager;

    const unwatch = client.watchContractEvent({
      address: poolManager as `0x${string}`,
      abi: UNISWAP_V4_POOL_MANAGER_ABI,
      eventName: "Swap",
      onLogs: (logs) => {
        Promise.allSettled(logs.map((log) => processV4SwapLog(log, chain))).then((results) => {
          const failures = results.filter((r) => r.status === "rejected");
          if (failures.length > 0) {
            console.error(`[Radar] [${chain}] ${failures.length}/${logs.length} V4 swap logs failed`);
          }
        });
      },
      onError: (error) => {
        console.warn(`[Radar] [${chain}] V4 watch error:`, error.message);
      },
    });
    activeUnwatchers.push(unwatch);
    (unwatchersByChain.get(chain) ?? unwatchersByChain.set(chain, []).get(chain)!).push(unwatch);

    console.log(`[Radar] [${chain}] Started V4 listener on ${poolManager.slice(0, 10)}...`);
  } else if (dex === "pancakeswap-v4-cl") {
    const poolManager = CONTRACTS[chain].pancakeswapV4ClPoolManager;

    const unwatch = client.watchContractEvent({
      address: poolManager as `0x${string}`,
      abi: PANCAKE_V4_CL_POOL_MANAGER_ABI,
      eventName: "Swap",
      onLogs: (logs) => {
        Promise.allSettled(logs.map((log) => processPcsV4ClSwapLog(log, chain))).then((results) => {
          const failures = results.filter((r) => r.status === "rejected");
          if (failures.length > 0) {
            console.error(
              `[Radar] [${chain}] ${failures.length}/${logs.length} PancakeV4 CL swap logs failed`
            );
          }
        });
      },
      onError: (error) => {
        console.warn(`[Radar] [${chain}] PancakeV4 CL watch error:`, error.message);
      },
    });
    activeUnwatchers.push(unwatch);
    (unwatchersByChain.get(chain) ?? unwatchersByChain.set(chain, []).get(chain)!).push(unwatch);

    console.log(
      `[Radar] [${chain}] Started PancakeV4 CL listener on ${poolManager.slice(0, 10)}...`
    );
  } else {
    const topic = dex === "pancakeswap-v3" ? PANCAKESWAP_V3_SWAP_TOPIC : UNISWAP_V3_SWAP_TOPIC;

    const unwatch = client.watchEvent({
      topics: [topic],
      onLogs: (logs) => {
        const relevantLogs = logs.filter((log) => log.topics[0] === topic);
        if (relevantLogs.length > 0) {
          Promise.allSettled(relevantLogs.map((log) => processV3SwapLog(log, chain, dex))).then(
            (results) => {
              const failures = results.filter((r) => r.status === "rejected");
              if (failures.length > 0) {
                console.error(
                  `[Radar] [${chain}] ${failures.length}/${relevantLogs.length} ${dex} swap logs failed`
                );
              }
            }
          );
        }
      },
      onError: (error: Error) => {
        console.warn(`[Radar] [${chain}] ${dex} watch error:`, error.message);
      },
    } as Parameters<typeof client.watchEvent>[0]);
    activeUnwatchers.push(unwatch);
    (unwatchersByChain.get(chain) ?? unwatchersByChain.set(chain, []).get(chain)!).push(unwatch);

    console.log(`[Radar] [${chain}] Started ${dex} listener`);
  }
}

/** 单池监听：PancakeV2 WBNB/USDT 池，仅记录 BNB/USD 历史价 */
export async function startBnbPriceListener(chain: ChainId): Promise<void> {
  await ensureBnbPoolDirection(chain); // 启动时验证池方向（fail 早暴露）
  const poolAddr = CONTRACTS[chain].bnbPricePool as `0x${string}`;
  const client = getClient(chain);

  const unwatch = client.watchEvent({
    address: poolAddr,
    event: {
      anonymous: false,
      inputs: [
        { indexed: true, name: "sender", type: "address" },
        { indexed: false, name: "amount0In", type: "uint256" },
        { indexed: false, name: "amount1In", type: "uint256" },
        { indexed: false, name: "amount0Out", type: "uint256" },
        { indexed: false, name: "amount1Out", type: "uint256" },
        { indexed: true, name: "to", type: "address" },
      ],
      name: "Swap",
      type: "event",
    },
    onLogs: (logs) => {
      Promise.allSettled(logs.map((log) => processV2BnbPriceSwap(log, chain))).then((results) => {
        const failures = results.filter((r) => r.status === "rejected");
        if (failures.length > 0) {
          console.error(
            `[Radar] [${chain}] ${failures.length}/${logs.length} BNB price swap logs failed`
          );
        }
      });
    },
    onError: (error) => {
      console.warn(`[Radar] [${chain}] BNB price watch error:`, error.message);
    },
  });
  activeUnwatchers.push(unwatch);
  (unwatchersByChain.get(chain) ?? unwatchersByChain.set(chain, []).get(chain)!).push(unwatch);

  console.log(`[Radar] [${chain}] Started BNB price listener on ${poolAddr.slice(0, 10)}...`);
}

export function stopListenersForChain(chain: ChainId): void {
  const unwatchers = unwatchersByChain.get(chain);
  if (!unwatchers) return;
  for (const unwatch of unwatchers) {
    try {
      unwatch();
    } catch {
      /* ignore */
    }
  }
  unwatchersByChain.delete(chain);
  const stopped = new Set(unwatchers);
  for (let i = activeUnwatchers.length - 1; i >= 0; i--) {
    if (stopped.has(activeUnwatchers[i])) activeUnwatchers.splice(i, 1);
  }
  console.log(`[Radar] [${chain}] All listeners stopped (${unwatchers.length})`);
}

export function stopAllListeners(): void {
  for (const unwatch of activeUnwatchers) {
    unwatch();
  }
  activeUnwatchers.length = 0;
  console.log("[Radar] All listeners stopped");
}
