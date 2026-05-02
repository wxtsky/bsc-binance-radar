import { fetchActivePerpetualBaseAssets } from "./binance-futures.js";
import { fetchBscDepositCoins } from "./binance-coin-info.js";
import { upsertBinanceBscToken } from "../db/queries.js";
import { loadWatchlist } from "./watchlist.js";

export interface BuildResult {
  perpetualCount: number;
  bscDepositCount: number;
  intersectionCount: number;
  unmatchedSymbols: string[];
}

/**
 * 取交集：币安 USDT-M 永续合约 baseAsset ∩ 币安 BSC 充提合约地址
 * 把交集 (symbol → BSC 合约地址) 写入 binance_bsc_tokens 表，重新 load watchlist
 */
export async function buildWatchlist(): Promise<BuildResult> {
  const [perpetuals, bscCoins] = await Promise.all([
    fetchActivePerpetualBaseAssets(),
    fetchBscDepositCoins(),
  ]);

  const intersection: string[] = [];
  const unmatched: string[] = [];
  const now = Date.now();
  const upserts: Promise<void>[] = [];

  for (const baseAsset of perpetuals) {
    const bsc = bscCoins.get(baseAsset);
    if (!bsc) {
      unmatched.push(baseAsset);
      continue;
    }
    intersection.push(baseAsset);
    upserts.push(
      upsertBinanceBscToken({
        symbol: baseAsset,
        baseAsset,
        contractAddress: bsc.contractAddress,
        decimals: 18, // 占位，定价时由 ERC20.decimals 链上读取覆盖
        updatedAt: now,
      })
    );
  }
  await Promise.all(upserts);
  await loadWatchlist();

  return {
    perpetualCount: perpetuals.size,
    bscDepositCount: bscCoins.size,
    intersectionCount: intersection.length,
    unmatchedSymbols: unmatched,
  };
}
