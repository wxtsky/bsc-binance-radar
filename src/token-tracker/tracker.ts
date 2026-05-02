import { buildWatchlist } from "./builder.js";
import { getWatchlistSize, loadWatchlist } from "./watchlist.js";

const REFRESH_INTERVAL_MS = 6 * 60 * 60 * 1000; // 6 hours

let timer: ReturnType<typeof setInterval> | null = null;

async function refreshOnce(): Promise<void> {
  try {
    const result = await buildWatchlist();
    console.log(
      `[Tracker] Watchlist refreshed: ${result.intersectionCount} tokens ` +
        `(perpetuals=${result.perpetualCount}, bsc-deposit=${result.bscDepositCount})`
    );
    if (result.intersectionCount === 0) {
      console.warn("[Tracker] Watchlist is empty after refresh — check Binance API connectivity");
    }
  } catch (err) {
    console.error("[Tracker] Refresh failed (keeping previous watchlist):", err);
  }
}

/** 启动时调用：先 load 一份 DB 缓存（如有），然后异步触发首次刷新 */
export async function startTokenTracker(): Promise<void> {
  await loadWatchlist();
  const cachedSize = getWatchlistSize();
  console.log(`[Tracker] Loaded ${cachedSize} cached tokens from DB`);

  // 第一次刷新：DB 为空时阻塞，否则异步后台跑
  if (cachedSize === 0) {
    await refreshOnce();
  } else {
    refreshOnce().catch(() => {});
  }

  if (!timer) {
    timer = setInterval(() => {
      refreshOnce().catch((err) => console.error("[Tracker] Periodic refresh error:", err));
    }, REFRESH_INTERVAL_MS);
    if (typeof timer.unref === "function") timer.unref();
  }
}

export function stopTokenTracker(): void {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
}
