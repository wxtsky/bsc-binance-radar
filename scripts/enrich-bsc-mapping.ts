#!/usr/bin/env bun
/**
 * 补全白名单：fapi 永续上有但 capital BSC 充提没开 通道 的代币（如 LAB），
 * web3 token search 反查 BSC 合约地址 + **链上 ERC20.symbol() 严格校验**
 *
 * 校验规则：拿到候选合约后调链上 symbol()，必须跟 fapi baseAsset 一致才入白名单。
 * 解决 SAFE 选了 SAFE(AnWang) 这种仿盘风险。中文 token 也支持。
 *
 * 用法：
 *   bun scripts/enrich-bsc-mapping.ts                    # 默认 mc=$10k / liq=$1k
 *   bun scripts/enrich-bsc-mapping.ts 50000              # 提阈值
 *   bun scripts/enrich-bsc-mapping.ts --reverify         # 重新校验所有非 capital 的已存在白名单项
 *
 * 容器内：
 *   docker compose run --rm --no-deps radar bun scripts/enrich-bsc-mapping.ts
 */

import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { createPublicClient, http } from "viem";
import { bsc } from "viem/chains";
import { fetchBscDepositCoins } from "../src/token-tracker/binance-coin-info.js";
import { searchBscTokenBySymbol } from "../src/token-tracker/binance-web3-search.js";
import {
  upsertBinanceBscToken,
  getAllBinanceBscTokens,
} from "../src/db/queries.js";
import { ERC20_ABI } from "../src/config/abis.js";
import { initSchema, getPool, closeDatabase } from "../src/db/index.js";

const argv = process.argv.slice(2);
const REVERIFY = argv.includes("--reverify");
const numericArgs = argv.filter((a) => !a.startsWith("--"));
const MIN_MC = Number(numericArgs[0]) || 10_000;
const MIN_LIQ = Number(numericArgs[1]) || 1_000;
const RATE_LIMIT_MS = 300;
const HTTP_URL = process.env.BSC_HTTP_URL || "http://151.123.172.62:81";

const httpClient = createPublicClient({
  chain: bsc,
  transport: http(HTTP_URL, { timeout: 10_000, retryCount: 1 }),
});

interface SeedFile {
  baseAssets: string[];
}

/** 链上读 ERC20.symbol，失败或不一致返回 null */
async function readOnChainSymbol(addr: string): Promise<string | null> {
  try {
    const result = await httpClient.readContract({
      address: addr as `0x${string}`,
      abi: ERC20_ABI,
      functionName: "symbol",
    });
    return String(result);
  } catch {
    return null;
  }
}

function symbolsEqual(onChain: string, expected: string): boolean {
  const a = onChain.trim().toUpperCase();
  const b = expected.trim().toUpperCase();
  if (a === b) return true;
  // 容忍 wrapped 前缀（IP→wIP, ROSE→wROSE）
  if (a === "W" + b) return true;
  return false;
}

async function reverifyMode() {
  console.log("[Enrich] --reverify mode: re-checking on-chain symbol for non-capital entries");
  await initSchema();
  const all = await getAllBinanceBscTokens();
  const bscCoins = await fetchBscDepositCoins();
  const candidates = all.filter((t) => !bscCoins.has(t.baseAsset.toUpperCase()));
  console.log(`[Enrich] ${candidates.length} non-capital entries to verify`);

  let kept = 0;
  let removed = 0;
  for (let i = 0; i < candidates.length; i++) {
    const t = candidates[i];
    const onChain = await readOnChainSymbol(t.contractAddress);
    if (onChain && symbolsEqual(onChain, t.baseAsset)) {
      kept++;
    } else {
      removed++;
      await getPool().query("DELETE FROM binance_bsc_tokens WHERE contract_address = $1", [
        t.contractAddress.toLowerCase(),
      ]);
      console.log(
        `  [${i + 1}/${candidates.length}] ✗ ${t.baseAsset} (${t.contractAddress.slice(0, 10)}…) on-chain symbol="${onChain ?? "<read failed>"}" — DELETED`
      );
    }
  }
  console.log(`[Enrich] reverify done: kept ${kept}, removed ${removed}`);
  await closeDatabase();
  process.exit(0);
}

async function enrichMode() {
  console.log(`[Enrich] threshold mc=$${MIN_MC} liq=$${MIN_LIQ}, on-chain symbol() verify ON`);

  const __filename = fileURLToPath(import.meta.url);
  const seedPath = path.resolve(path.dirname(__filename), "..", "seed", "binance-perpetuals.json");
  const seed = JSON.parse(fs.readFileSync(seedPath, "utf-8")) as SeedFile;
  const baseAssets = seed.baseAssets.map((b) => b.toUpperCase());
  console.log(`[Enrich] fapi seed: ${baseAssets.length} baseAssets`);

  const bscCoins = await fetchBscDepositCoins();
  console.log(`[Enrich] capital BSC: ${bscCoins.size} coins`);

  await initSchema();
  const existing = await getAllBinanceBscTokens();
  const existingSymbols = new Set(existing.map((t) => t.symbol.toUpperCase()));
  console.log(`[Enrich] DB binance_bsc_tokens: ${existing.length}`);

  const missing = baseAssets.filter((b) => !bscCoins.has(b) && !existingSymbols.has(b));
  console.log(`[Enrich] need to enrich: ${missing.length}`);

  let found = 0;
  let mismatch = 0;
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
        const onChain = await readOnChainSymbol(hit.contractAddress);
        if (!onChain || !symbolsEqual(onChain, sym)) {
          mismatch++;
          console.log(
            `  [${i + 1}/${missing.length}] ✗ ${sym} → ${hit.contractAddress.slice(0, 10)}… on-chain symbol="${onChain ?? "<read failed>"}" mismatch (web3 said "${hit.symbol}" name="${hit.name}")`
          );
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
            `  [${i + 1}/${missing.length}] ✓ ${sym} → ${hit.contractAddress.slice(0, 10)}… (mc=$${hit.marketCapUsd.toFixed(0)} liq=$${hit.liquidityUsd.toFixed(0)})`
          );
        }
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
    `[Enrich] Done in ${elapsedS}s — accepted ${found}, on-chain mismatch ${mismatch}, no candidate ${skipped}, failed ${failed}`
  );
  await closeDatabase();
  process.exit(0);
}

const main = REVERIFY ? reverifyMode : enrichMode;
main().catch((err) => {
  console.error("[Enrich] Fatal:", err);
  process.exit(1);
});
