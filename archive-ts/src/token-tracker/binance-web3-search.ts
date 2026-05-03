/**
 * 用 binance 官方 web3 token search 反查 BSC 链上合约地址
 * （美国 IP 可访问，覆盖 fapi+capital 取交集后漏掉的代币如 LAB）
 *
 * 数据源：https://web3.binance.com/bapi/defi/v5/public/wallet-direct/buw/wallet/market/token/search/ai
 */

const SEARCH_API =
  "https://web3.binance.com/bapi/defi/v5/public/wallet-direct/buw/wallet/market/token/search/ai";

interface SearchEntry {
  chainId: string;
  contractAddress: string;
  symbol: string;
  name: string;
  marketCap?: string;
  liquidity?: string;
}

interface SearchResp {
  code: string;
  data?: SearchEntry[];
}

export interface Web3SearchHit {
  contractAddress: string;
  symbol: string;
  name: string;
  marketCapUsd: number;
  liquidityUsd: number;
}

/**
 * 按 symbol 在 BSC (chainId=56) 反查，返回市值最高且符合阈值的候选
 *
 * @param symbol 待查询的 baseAsset symbol（区分大小写敏感比对）
 * @param minMarketCap 最低市值过滤，防止匹配到垃圾仿盘
 * @param minLiquidity 最低流动性过滤
 */
export async function searchBscTokenBySymbol(
  symbol: string,
  minMarketCap = 10_000,
  minLiquidity = 1_000
): Promise<Web3SearchHit | null> {
  const url = `${SEARCH_API}?keyword=${encodeURIComponent(symbol)}&chainId=56`;
  const res = await fetch(url, {
    signal: AbortSignal.timeout(8000),
    headers: { "User-Agent": "bsc-binance-radar/1.0" },
  });
  if (!res.ok) {
    throw new Error(`web3 search HTTP ${res.status}`);
  }
  const data = (await res.json()) as SearchResp;
  if (!data.data) return null;

  const candidates: Web3SearchHit[] = data.data
    .filter((t) => t.chainId === "56" && t.symbol?.toUpperCase() === symbol.toUpperCase() && t.contractAddress)
    .map((t) => ({
      contractAddress: t.contractAddress.toLowerCase(),
      symbol: t.symbol,
      name: t.name ?? "",
      marketCapUsd: parseFloat(t.marketCap ?? "0") || 0,
      liquidityUsd: parseFloat(t.liquidity ?? "0") || 0,
    }))
    .sort((a, b) => b.marketCapUsd - a.marketCapUsd);

  for (const c of candidates) {
    if (c.marketCapUsd >= minMarketCap && c.liquidityUsd >= minLiquidity) {
      return c;
    }
  }
  return null;
}
