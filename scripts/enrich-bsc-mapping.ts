#!/usr/bin/env bun
/**
 * 补全白名单：给 fapi 永续上有但 capital BSC 充提没开 通道 的代币（如 LAB），
 * 通过 binance web3 token search 反查 BSC 合约地址，写入 binance_bsc_tokens 表
 *
 * 用法：
 *   bun scripts/enrich-bsc-mapping.ts            # 默认市值阈值 $10k / 流动性 $1k
 *   bun scripts/enrich-bsc-mapping.ts 50000      # 提高市值阈值到 $50k
 *
 * 容器内：
 *   docker compose run --rm --no-deps radar bun scripts/enrich-bsc-mapping.ts
 */

import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { fetchBscDepositCoins } from "../src/token-tracker/binance-coin-info.js";
import { searchBscTokenBySymbol } from "../src/token-tracker/binance-web3-search.js";
import { upsertBinanceBscToken, getAllBinanceBscTokens } from "../src/db/queries.js";
import { initSchema, closeDatabase } from "../src/db/index.js";

const MIN_MC = Number(process.argv[2]) || 10_000;
const MIN_LIQ = Number(process.argv[3]) || 1_000;
const RATE_LIMIT_MS = 300; // 100 calls/min, web3 search 比较温和

interface SeedFile {
  baseAssets: string[];
}

async function main() {
  console.log(`[Enrich] threshold mc=$${MIN_MC} liq=$${MIN_LIQ}`);

  // fapi seed baseAsset
  const __filename = fileURLToPath(import.meta.url);
  const seedPath = path.resolve(path.dirname(__filename), "..", "seed", "binance-perpetuals.json");
  const seed = JSON.parse(fs.readFileSync(seedPath, "utf-8")) as SeedFile;
  const baseAssets = seed.baseAssets.map((b) => b.toUpperCase());
  console.log(`[Enrich] fapi seed: ${baseAssets.length} baseAssets`);

  // capital BSC list
  const bscCoins = await fetchBscDepositCoins();
  console.log(`[Enrich] capital BSC: ${bscCoins.size} coins`);

  // 已在白名单（DB）的合约地址 — 用于跳过已知项
  await initSchema();
  const existing = await getAllBinanceBscTokens();
  const existingSymbols = new Set(existing.map((t) => t.symbol.toUpperCase()));
  console.log(`[Enrich] DB binance_bsc_tokens: ${existing.length}`);

  // 找 fapi 上有但 capital 没 BSC 通道，且 DB 还没补进过的
  const missing = baseAssets.filter((b) => !bscCoins.has(b) && !existingSymbols.has(b));
  console.log(`[Enrich] need to enrich: ${missing.length}`);

  let found = 0;
  let skipped = 0;
  let failed = 0;
  const t0 = Date.now();

  for (let i = 0; i < missing.length; i++) {
    const sym = missing[i];
    try {
      const hit = await searchBscTokenBySymbol(sym, MIN_MC, MIN_LIQ);
      if (!hit) {
        skipped++;
      } else {
        await upsertBinanceBscToken({
          symbol: sym,
          baseAsset: sym,
          contractAddress: hit.contractAddress,
          decimals: 18,
          updatedAt: Date.now(),
        });
        found++;
        console.log(
          `  [${i + 1}/${missing.length}] ✓ ${sym} → ${hit.contractAddress.slice(0, 10)}… mc=$${hit.marketCapUsd.toFixed(0)} liq=$${hit.liquidityUsd.toFixed(0)} name="${hit.name}"`
        );
      }
    } catch (err) {
      failed++;
      console.warn(`  [${i + 1}/${missing.length}] ✗ ${sym}:`, (err as Error).message);
    }
    if (i < missing.length - 1) {
      await new Promise((r) => setTimeout(r, RATE_LIMIT_MS));
    }
  }

  const elapsedS = ((Date.now() - t0) / 1000).toFixed(0);
  console.log(
    `[Enrich] Done in ${elapsedS}s — found ${found}, skipped (no BSC token / below threshold) ${skipped}, failed ${failed}`
  );
  await closeDatabase();
  process.exit(0);
}

main().catch((err) => {
  console.error("[Enrich] Fatal:", err);
  process.exit(1);
});
