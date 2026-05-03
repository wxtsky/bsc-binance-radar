#!/usr/bin/env bun
/**
 * 本地同步脚本：从 fapi.binance.com 拉 USDT-M 永续合约 baseAsset 列表，
 * 写到 seed/binance-perpetuals.json，供服务器（geo-blocked 美国 IP）通过
 * GitHub raw 镜像读取。
 *
 * 使用：
 *   bun run sync-perpetuals      # 拉一次写文件
 *   git add seed/binance-perpetuals.json && git commit + push  # 同步到服务器
 *
 * 或者用 launchd / cron 每天自动跑：
 *   0 * * * * cd ~/code/bsc-binance-radar && bun run sync-perpetuals && git add seed/ && git commit -m "chore: sync binance perpetuals" && git push
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { normalizeBinanceBaseAsset } from "../src/token-tracker/binance-futures.js";

const FAPI = "https://fapi.binance.com/fapi/v1/exchangeInfo";

interface FuturesSymbol {
  symbol: string;
  contractType: string;
  status: string;
  quoteAsset: string;
  baseAsset: string;
}

interface FuturesExchangeInfo {
  symbols: FuturesSymbol[];
}

async function main() {
  console.log(`Fetching ${FAPI} ...`);
  const res = await fetch(FAPI);
  if (!res.ok) {
    console.error(`fapi error: HTTP ${res.status} ${res.statusText}`);
    console.error(
      "如果你也在 geo-blocked 区域（美国 IP），这个脚本无法在这台机器上运行。"
    );
    process.exit(1);
  }
  const data = (await res.json()) as FuturesExchangeInfo;

  const baseAssets = new Set<string>();
  for (const s of data.symbols) {
    if (
      s.contractType === "PERPETUAL" &&
      s.quoteAsset === "USDT" &&
      s.status === "TRADING"
    ) {
      baseAssets.add(normalizeBinanceBaseAsset(s.baseAsset));
    }
  }

  const out = {
    source: "fapi.binance.com/fapi/v1/exchangeInfo",
    syncedAt: new Date().toISOString(),
    count: baseAssets.size,
    baseAssets: Array.from(baseAssets).sort(),
  };

  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  const outPath = path.resolve(__dirname, "..", "seed", "binance-perpetuals.json");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2) + "\n");

  console.log(`✓ Wrote ${out.count} baseAssets to ${path.relative(process.cwd(), outPath)}`);
  console.log(`  Synced at: ${out.syncedAt}`);
  console.log(`  Sample: ${out.baseAssets.slice(0, 10).join(", ")} ...`);
  console.log("");
  console.log("Next: git add seed/ && git commit -m 'sync binance perpetuals' && git push");
}

main().catch((err) => {
  console.error("Sync failed:", err);
  process.exit(1);
});
