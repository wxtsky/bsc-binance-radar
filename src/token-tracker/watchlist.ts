import { getAllBinanceBscTokens } from "../db/queries.js";

const watchedAddresses = new Set<string>();
let loaded = false;

export async function loadWatchlist(): Promise<void> {
  watchedAddresses.clear();
  const tokens = await getAllBinanceBscTokens();
  for (const token of tokens) {
    watchedAddresses.add(token.contractAddress.toLowerCase());
  }
  loaded = true;
  console.log(`[Radar] Watchlist loaded: ${watchedAddresses.size} Binance/BSC tokens`);
}

export function isWatchedToken(address: string): boolean {
  // Bypass for Phase 1 dev / smoke test (records every swap, ignores whitelist)
  if (process.env.WATCHLIST_BYPASS === "true") return true;
  if (!loaded) return false;
  return watchedAddresses.has(address.toLowerCase());
}

export function getWatchlistSize(): number {
  return watchedAddresses.size;
}

export function getWatchlist(): string[] {
  return Array.from(watchedAddresses);
}
