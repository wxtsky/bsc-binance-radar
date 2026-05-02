import { calculateV3PoolTVL } from "../core/tvl-calculator.js";
import { findTokenActivePools } from "./aggregator.js";
import { TTLCache } from "../utils/ttlCache.js";
import type { ChainId } from "../types/index.js";

const TVL_CACHE_TTL_MS = 5 * 60 * 1000;

const tokenTvlCache = new TTLCache<string, number>({ max: 500, ttl: TVL_CACHE_TTL_MS });

/**
 * Token TVL = sum(其涉及的活跃 V3 / Pancake-V3 pool 的 TVL)
 * - 缓存 5min（避免每次 detector 循环都打链）
 * - V4 池不算（无法直接 balanceOf 拿，需要 PositionManager 累加，本项目暂不实现）
 */
export async function getTokenTVL(
  tokenAddress: string,
  chain: ChainId = "bsc"
): Promise<number> {
  const cacheKey = `${chain}:${tokenAddress.toLowerCase()}`;
  const cached = tokenTvlCache.get(cacheKey);
  if (cached !== undefined) return cached;

  const pools = await findTokenActivePools(tokenAddress, chain);
  if (pools.length === 0) {
    tokenTvlCache.set(cacheKey, 0);
    return 0;
  }

  const tvls = await Promise.all(pools.map((addr) => calculateV3PoolTVL(addr, chain)));
  const total = tvls.reduce((a, b) => a + b, 0);
  tokenTvlCache.set(cacheKey, total);
  return total;
}
