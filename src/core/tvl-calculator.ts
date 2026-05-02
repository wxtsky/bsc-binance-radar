import { getClient } from "../clients/viem-clients.js";
import { UNISWAP_V3_POOL_ABI, ERC20_ABI } from "../config/abis.js";
import { getTokenPrices, getCacheKey } from "./price-service.js";
import type { ChainId } from "../types/index.js";

function calculatePriceFromSqrtPriceX96(
  sqrtPriceX96: bigint,
  decimals0: number,
  decimals1: number,
  knownPrice: number,
  knownIsToken1: boolean
): number {
  const sqrtPriceX96Num = Number(sqrtPriceX96);
  const priceRatio = (sqrtPriceX96Num / 2 ** 96) ** 2;
  const decimalAdjustment = 10 ** (decimals0 - decimals1);

  if (knownIsToken1) {
    return priceRatio * decimalAdjustment * knownPrice;
  }

  if (priceRatio * decimalAdjustment === 0) return 0;
  return knownPrice / (priceRatio * decimalAdjustment);
}

/**
 * 算 V3 / Pancake-V3 风格池子的 TVL（USD）
 * V4 池子拿不到 balanceOf，本函数不适用，caller 自行处理
 */
export async function calculateV3PoolTVL(poolAddress: string, chain: ChainId): Promise<number> {
  try {
    const client = getClient(chain);
    const poolAddr = poolAddress as `0x${string}`;

    const poolResults = await client.multicall({
      contracts: [
        { address: poolAddr, abi: UNISWAP_V3_POOL_ABI, functionName: "token0" },
        { address: poolAddr, abi: UNISWAP_V3_POOL_ABI, functionName: "token1" },
        { address: poolAddr, abi: UNISWAP_V3_POOL_ABI, functionName: "slot0" },
      ],
    });

    if (poolResults[0].status !== "success" || poolResults[1].status !== "success") return 0;

    const token0 = poolResults[0].result as `0x${string}`;
    const token1 = poolResults[1].result as `0x${string}`;
    const slot0 =
      poolResults[2].status === "success"
        ? (poolResults[2].result as readonly [bigint, number, number, number, number, number, boolean])
        : null;
    const sqrtPriceX96 = slot0 ? slot0[0] : 0n;

    const dataResults = await client.multicall({
      contracts: [
        { address: token0, abi: ERC20_ABI, functionName: "balanceOf", args: [poolAddr] },
        { address: token1, abi: ERC20_ABI, functionName: "balanceOf", args: [poolAddr] },
        { address: token0, abi: ERC20_ABI, functionName: "decimals" },
        { address: token1, abi: ERC20_ABI, functionName: "decimals" },
      ],
    });

    const balance0 = dataResults[0].status === "success" ? (dataResults[0].result as bigint) : 0n;
    const balance1 = dataResults[1].status === "success" ? (dataResults[1].result as bigint) : 0n;
    const decimals0 = dataResults[2].status === "success" ? (dataResults[2].result as number) : 18;
    const decimals1 = dataResults[3].status === "success" ? (dataResults[3].result as number) : 18;

    const prices = await getTokenPrices([
      { chain, address: token0 },
      { chain, address: token1 },
    ]);

    let price0 = prices.get(getCacheKey(token0, chain))?.price || 0;
    let price1 = prices.get(getCacheKey(token1, chain))?.price || 0;

    if (sqrtPriceX96 > 0n) {
      if (price0 > 0 && price1 === 0) {
        price1 = calculatePriceFromSqrtPriceX96(sqrtPriceX96, decimals0, decimals1, price0, false);
      } else if (price1 > 0 && price0 === 0) {
        price0 = calculatePriceFromSqrtPriceX96(sqrtPriceX96, decimals0, decimals1, price1, true);
      }
    }

    const tvl0 = (Number(balance0) / 10 ** decimals0) * price0;
    const tvl1 = (Number(balance1) / 10 ** decimals1) * price1;
    return tvl0 + tvl1;
  } catch (error) {
    console.error(`[Radar] Error calculating V3 TVL for ${poolAddress}:`, error);
    return 0;
  }
}
