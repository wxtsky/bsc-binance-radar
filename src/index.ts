import "dotenv/config";
import { initSchema, closeDatabase } from "./db/index.js";
import { initPriceService } from "./core/price-service.js";
import {
  startSwapListener,
  startBnbPriceListener,
  stopAllListeners,
} from "./core/swap-listener.js";
import { startLivenessProbe, stopLivenessProbe } from "./core/livenessProbe.js";
import { CHAIN_CONFIGS, SUPPORTED_CHAINS } from "./config/chains.js";
import { getWatchlistSize } from "./token-tracker/watchlist.js";
import { startTokenTracker, stopTokenTracker } from "./token-tracker/tracker.js";
import { startAnomalyDetector, stopAnomalyDetector } from "./anomaly/detector.js";
import { startNotifier, stopNotifier } from "./notifier/index.js";
import type { DexType } from "./types/index.js";

const SUPPORTED_DEXES: DexType[] = [
  "uniswap-v3",
  "uniswap-v4",
  "pancakeswap-v3",
  "pancakeswap-v4-cl",
];

async function main() {
  if (!CHAIN_CONFIGS.bsc.wssUrl) {
    throw new Error("BSC_WSS_URL is required. Set it in .env or environment.");
  }

  if (!process.env.OKX_API_KEY || !process.env.OKX_PASSPHRASE) {
    console.warn("[Radar] OKX_API_KEY / OKX_PASSPHRASE missing — native price (BNB) will be 0 and most swaps will be skipped.");
  }

  await initSchema();
  console.log("[Radar] Database ready");

  await startTokenTracker();
  if (getWatchlistSize() === 0 && process.env.WATCHLIST_BYPASS !== "true") {
    console.warn(
      "[Radar] Watchlist is empty after refresh — Binance API may be unreachable. " +
        "Set WATCHLIST_BYPASS=true to bypass the filter."
    );
  }

  console.log("[Radar] Initializing price service...");
  await initPriceService();

  console.log("[Radar] Starting swap listeners...");
  for (const chain of SUPPORTED_CHAINS) {
    for (const dex of SUPPORTED_DEXES) {
      try {
        await startSwapListener(chain, dex);
      } catch (err) {
        console.error(`[Radar] Failed to start ${chain} ${dex} listener:`, err);
      }
    }
    // 单池 BNB 价基准监听
    try {
      await startBnbPriceListener(chain);
    } catch (err) {
      console.error(`[Radar] Failed to start BNB price listener for ${chain}:`, err);
    }
  }

  startLivenessProbe();
  startNotifier();
  startAnomalyDetector();
  console.log("[Radar] Ready");
}

async function shutdown(signal: string) {
  console.log(`[Radar] Received ${signal}, shutting down...`);
  stopAnomalyDetector();
  stopNotifier();
  stopLivenessProbe();
  stopAllListeners();
  stopTokenTracker();
  await closeDatabase();
  process.exit(0);
}

process.once("SIGINT", () => { shutdown("SIGINT"); });
process.once("SIGTERM", () => { shutdown("SIGTERM"); });

main().catch((err) => {
  console.error("[Radar] Fatal error during startup:", err);
  process.exit(1);
});
