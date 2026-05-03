import crypto from "crypto";
import { CHAIN_CONFIGS, CHAIN_STATIC } from "../config/chains.js";
import { NATIVE_TOKEN_ADDRESS, WRAPPED_NATIVE } from "../config/contracts.js";
import { ERC20_ABI } from "../config/abis.js";
import { getClient } from "../clients/viem-clients.js";
import type { ChainId, TokenPriceInfo } from "../types/index.js";

const OKX_API_BASE = "https://web3.okx.com";

let nativePrices: Record<ChainId, number> = { bsc: 0 };
let nativePriceExpiry = 0;
const NATIVE_PRICE_TTL = 10 * 60 * 1000;

const TOKEN_METADATA_CACHE = new Map<string, { symbol: string; decimals: number }>();
const TOKEN_PRICE_CACHE = new Map<string, { price: number; expiry: number }>();
const TOKEN_PRICE_TTL = 30 * 1000;

function generateSign(method: string, path: string, body: unknown, timestamp: string): string {
  const secret = process.env.OKX_SECRET_KEY || process.env.OKX_API_SECRET || "";
  const bodyStr = body ? JSON.stringify(body) : "";
  const preSign = timestamp + method.toUpperCase() + path + bodyStr;
  return crypto.createHmac("sha256", secret).update(preSign).digest("base64");
}

function getHeaders(method: string, path: string, body: unknown): HeadersInit {
  const timestamp = new Date().toISOString();
  return {
    "Content-Type": "application/json",
    "OK-ACCESS-KEY": process.env.OKX_API_KEY || "",
    "OK-ACCESS-SIGN": generateSign(method, path, body, timestamp),
    "OK-ACCESS-PASSPHRASE": process.env.OKX_PASSPHRASE || "",
    "OK-ACCESS-TIMESTAMP": timestamp,
  };
}

function normalizeAddress(address: string, chain: ChainId): string {
  if (address.toLowerCase() === NATIVE_TOKEN_ADDRESS.toLowerCase()) {
    return WRAPPED_NATIVE[chain].toLowerCase();
  }
  return address.toLowerCase();
}

export function getCacheKey(address: string, chain: ChainId): string {
  return `${chain}:${normalizeAddress(address, chain)}`;
}

async function fetchNativePrices(): Promise<void> {
  if (Date.now() < nativePriceExpiry) return;

  try {
    const body = [
      { chainIndex: CHAIN_CONFIGS.bsc.chainIndex, tokenContractAddress: WRAPPED_NATIVE.bsc.toLowerCase() },
    ];

    const path = "/api/v6/dex/market/price-info";
    const res = await fetch(`${OKX_API_BASE}${path}`, {
      method: "POST",
      headers: getHeaders("POST", path, body),
      body: JSON.stringify(body),
    });

    const data = await res.json();

    if (data.code === "0" && data.data) {
      for (const item of data.data) {
        if (item.chainIndex === CHAIN_CONFIGS.bsc.chainIndex) {
          nativePrices.bsc = parseFloat(item.price) || 0;
        }
      }
      nativePriceExpiry = Date.now() + NATIVE_PRICE_TTL;
      console.log(`[Radar] Native price updated: BNB=$${nativePrices.bsc.toFixed(2)}`);
    }
  } catch (error) {
    console.error("[Radar] Error fetching native prices:", error);
  }
}

function isStablecoin(address: string, chain: ChainId): boolean {
  return CHAIN_STATIC[chain]?.stableCoins.has(address.toLowerCase()) ?? false;
}

function isWrappedNative(address: string, chain: ChainId): boolean {
  return normalizeAddress(address, chain) === WRAPPED_NATIVE[chain].toLowerCase();
}

function getSimplifiedPrice(address: string, chain: ChainId): number {
  const normalized = normalizeAddress(address, chain);
  if (isStablecoin(normalized, chain)) return 1;
  if (isWrappedNative(normalized, chain)) return nativePrices[chain];
  return 0;
}

async function fetchTokenMetadataFromChain(
  tokens: Array<{ chain: ChainId; address: string }>
): Promise<void> {
  const tokensByChain = new Map<ChainId, string[]>();

  for (const { chain, address } of tokens) {
    const normalized = normalizeAddress(address, chain);
    const cacheKey = getCacheKey(address, chain);
    if (TOKEN_METADATA_CACHE.has(cacheKey)) continue;

    if (address.toLowerCase() === NATIVE_TOKEN_ADDRESS.toLowerCase()) {
      TOKEN_METADATA_CACHE.set(cacheKey, { symbol: "BNB", decimals: 18 });
      continue;
    }

    if (!tokensByChain.has(chain)) tokensByChain.set(chain, []);
    tokensByChain.get(chain)!.push(normalized);
  }

  for (const [chain, addresses] of tokensByChain) {
    if (addresses.length === 0) continue;
    try {
      const client = getClient(chain);
      const contracts = addresses.flatMap((address) => [
        { address: address as `0x${string}`, abi: ERC20_ABI, functionName: "symbol" },
        { address: address as `0x${string}`, abi: ERC20_ABI, functionName: "decimals" },
      ]);
      const results = await client.multicall({ contracts });

      for (let i = 0; i < addresses.length; i++) {
        const address = addresses[i];
        const symbolResult = results[i * 2];
        const decimalsResult = results[i * 2 + 1];
        if (symbolResult.status === "success" && decimalsResult.status === "success") {
          TOKEN_METADATA_CACHE.set(`${chain}:${address}`, {
            symbol: symbolResult.result as string,
            decimals: Number(decimalsResult.result),
          });
        }
      }
    } catch (error) {
      console.error(`[Radar] Error fetching token metadata from ${chain}:`, error);
    }
  }
}

/** 一次性预热 metadata cache：
 *  - 从 binance_bsc_tokens DB 表拿 303 个白名单 token 的 decimals + symbol（已知字段）
 *  - 加上 BSC 的 base tokens（USDT/USDC/USD1/WBNB），全部直接写入 cache
 *  这样 swap-listener 处理任何 swap 时 metadata 全 cache hit，不用每次 multicall。
 */
export async function prewarmMetadataCache(): Promise<void> {
  const { getAllBinanceBscTokens } = await import("../db/queries.js");
  const tokens = await getAllBinanceBscTokens();
  for (const t of tokens) {
    const cacheKey = `bsc:${t.contractAddress.toLowerCase()}`;
    TOKEN_METADATA_CACHE.set(cacheKey, {
      symbol: t.symbol,
      decimals: t.decimals || 18,
    });
  }
  // BSC base tokens（USDT/USDC/USD1 都是 18 decimals，WBNB 18）
  const baseTokens: Array<{ addr: string; symbol: string }> = [
    { addr: "0x55d398326f99059ff775485246999027b3197955", symbol: "USDT" },
    { addr: "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d", symbol: "USDC" },
    { addr: "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d", symbol: "USD1" },
    { addr: "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", symbol: "WBNB" },
  ];
  for (const { addr, symbol } of baseTokens) {
    TOKEN_METADATA_CACHE.set(`bsc:${addr}`, { symbol, decimals: 18 });
  }
  console.log(`[Radar] Metadata cache prewarmed: ${TOKEN_METADATA_CACHE.size} tokens`);
}

export async function getTokenPrices(
  tokens: Array<{ chain: ChainId; address: string }>
): Promise<Map<string, TokenPriceInfo>> {
  await fetchNativePrices();

  const result = new Map<string, TokenPriceInfo>();
  const tokensToFetchMetadata: Array<{ chain: ChainId; address: string }> = [];

  for (const token of tokens) {
    const cacheKey = getCacheKey(token.address, token.chain);
    if (!TOKEN_METADATA_CACHE.has(cacheKey)) tokensToFetchMetadata.push(token);
  }

  if (tokensToFetchMetadata.length > 0) {
    await fetchTokenMetadataFromChain(tokensToFetchMetadata);
  }

  for (const token of tokens) {
    const cacheKey = getCacheKey(token.address, token.chain);
    const metadata = TOKEN_METADATA_CACHE.get(cacheKey);
    const simplifiedPrice = getSimplifiedPrice(token.address, token.chain);

    const cachedPrice = TOKEN_PRICE_CACHE.get(cacheKey);
    let price = simplifiedPrice;
    if (simplifiedPrice === 0 && cachedPrice && cachedPrice.expiry > Date.now()) {
      price = cachedPrice.price;
    } else if (simplifiedPrice !== 0) {
      TOKEN_PRICE_CACHE.set(cacheKey, { price: simplifiedPrice, expiry: Date.now() + TOKEN_PRICE_TTL });
    }

    result.set(cacheKey, {
      address: normalizeAddress(token.address, token.chain),
      chain: token.chain,
      price,
      symbol: metadata?.symbol || "UNKNOWN",
      decimals: metadata?.decimals || 18,
    });
  }

  return result;
}

export async function initPriceService(): Promise<void> {
  await fetchNativePrices();
}
