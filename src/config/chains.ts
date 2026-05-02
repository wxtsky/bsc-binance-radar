import type { ChainConfig, ChainId } from "../types/index.js";
import { NATIVE_TOKEN_ADDRESS, WRAPPED_NATIVE } from "./contracts.js";

export const SUPPORTED_CHAINS: ChainId[] = ["bsc"];

export const CHAIN_CONFIGS: Record<ChainId, ChainConfig> = {
  bsc: {
    id: 56,
    name: "bsc",
    wssUrl: process.env.BSC_WSS_URL || "",
    chainIndex: "56",
  },
};

export interface ChainStaticConfig {
  baseTokens: Set<string>;
  stableCoins: Set<string>;
  priceAnchor: string;
  filters: {
    minFeeTier: number;
    maxSingleSwapFeeUsd: number;
    poolTvlMaxUsd: number;
  };
}

const COMMON_FILTERS = {
  minFeeTier: 0,                  // 放开所有 fee tier（含 0.01% 稳定币池）
  maxSingleSwapFeeUsd: 10_000_000, // 仅拦明显脏数据（价格故障导致的天文数字）
  poolTvlMaxUsd: 100_000_000,      // 占位，swap-listener 未使用
};

export const CHAIN_STATIC: Record<ChainId, ChainStaticConfig> = {
  bsc: {
    baseTokens: new Set([
      NATIVE_TOKEN_ADDRESS.toLowerCase(),
      WRAPPED_NATIVE.bsc.toLowerCase(),
      "0x55d398326f99059ff775485246999027b3197955", // USDT
      "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d", // USDC
      "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d", // USD1
    ]),
    stableCoins: new Set([
      "0x55d398326f99059ff775485246999027b3197955", // USDT
      "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d", // USDC
      "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d", // USD1
    ]),
    priceAnchor: WRAPPED_NATIVE.bsc.toLowerCase(),
    filters: COMMON_FILTERS,
  },
};

export function hasBaseToken(token0: string, token1: string, chain: ChainId): boolean {
  const set = CHAIN_STATIC[chain]?.baseTokens;
  if (!set) return false;
  return set.has(token0.toLowerCase()) || set.has(token1.toLowerCase());
}

export function areBothBaseTokens(token0: string, token1: string, chain: ChainId): boolean {
  const set = CHAIN_STATIC[chain]?.baseTokens;
  if (!set) return false;
  return set.has(token0.toLowerCase()) && set.has(token1.toLowerCase());
}

export function isStableCoin(address: string, chain: ChainId): boolean {
  return CHAIN_STATIC[chain]?.stableCoins.has(address.toLowerCase()) ?? false;
}
