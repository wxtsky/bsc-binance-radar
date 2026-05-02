#!/usr/bin/env bun
/**
 * 回补历史 swap 数据，让 detector 立刻有 baseline
 *
 * 用法：
 *   bun scripts/backfill.ts [hours]      # 默认 24，可传 4 / 6 / 12 等
 *
 * 容器内：
 *   docker compose run --rm --no-deps radar bun scripts/backfill.ts 4
 */

import "dotenv/config";
import type { Log } from "viem";
import { initSchema, closeDatabase } from "../src/db/index.js";
import { getClient } from "../src/clients/viem-clients.js";
import { CONTRACTS } from "../src/config/contracts.js";
import { UNISWAP_V4_POOL_MANAGER_ABI } from "../src/config/abis.js";
import { initPriceService } from "../src/core/price-service.js";
import { startTokenTracker, stopTokenTracker } from "../src/token-tracker/tracker.js";
import { processV3SwapLog, processV4SwapLog } from "../src/core/swap-listener.js";

const HOURS = Number(process.argv[2]) || 24;
const BSC_BLOCK_TIME_S = 3;
const BLOCKS_PER_BATCH = 5000n;
const TOTAL_BLOCKS = BigInt(Math.floor((HOURS * 3600) / BSC_BLOCK_TIME_S));

const V3_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67" as const;
const PANCAKE_V3_TOPIC =
  "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83" as const;

async function main() {
  console.log(`[Backfill] Starting backfill of ~${HOURS}h swap history...`);

  await initSchema();
  await startTokenTracker();
  await initPriceService();

  const client = getClient("bsc");
  const latest = await client.getBlockNumber();
  const start = latest - TOTAL_BLOCKS;
  console.log(
    `[Backfill] Block range ${start} → ${latest} (${TOTAL_BLOCKS.toString()} blocks, batch=${BLOCKS_PER_BATCH})`
  );

  let totalLogs = 0;
  let totalProcessed = 0;
  let totalErrors = 0;
  const t0 = Date.now();

  const v4PoolManager = CONTRACTS.bsc.uniswapV4PoolManager as `0x${string}`;

  for (let from = start; from < latest; from += BLOCKS_PER_BATCH) {
    const to = from + BLOCKS_PER_BATCH - 1n > latest ? latest : from + BLOCKS_PER_BATCH - 1n;

    let v3: Log[] = [];
    let pancake: Log[] = [];
    let v4: Log[] = [];
    try {
      [v3, pancake, v4] = await Promise.all([
        client.getLogs({ fromBlock: from, toBlock: to, topics: [V3_TOPIC] }) as Promise<Log[]>,
        client.getLogs({ fromBlock: from, toBlock: to, topics: [PANCAKE_V3_TOPIC] }) as Promise<Log[]>,
        client.getLogs({
          address: v4PoolManager,
          fromBlock: from,
          toBlock: to,
          event: UNISWAP_V4_POOL_MANAGER_ABI[0],
        }) as unknown as Promise<Log[]>,
      ]);
    } catch (err) {
      console.error(`[Backfill] getLogs failed for ${from}-${to}:`, (err as Error).message);
      continue;
    }

    const batchTotal = v3.length + pancake.length + v4.length;
    totalLogs += batchTotal;

    for (const log of v3) {
      try {
        await processV3SwapLog(log, "bsc", "uniswap-v3");
        totalProcessed++;
      } catch {
        totalErrors++;
      }
    }
    for (const log of pancake) {
      try {
        await processV3SwapLog(log, "bsc", "pancakeswap-v3");
        totalProcessed++;
      } catch {
        totalErrors++;
      }
    }
    for (const log of v4) {
      try {
        await processV4SwapLog(log, "bsc");
        totalProcessed++;
      } catch {
        totalErrors++;
      }
    }

    const pct = Number(((to - start) * 100n) / (latest - start));
    const elapsedS = ((Date.now() - t0) / 1000).toFixed(0);
    console.log(
      `[Backfill] ${pct}% (block ${to}) batch ${batchTotal} | total fetched=${totalLogs} ok=${totalProcessed} err=${totalErrors} | ${elapsedS}s`
    );
  }

  const elapsedS = ((Date.now() - t0) / 1000).toFixed(0);
  console.log(
    `[Backfill] Done in ${elapsedS}s — fetched ${totalLogs}, processed ${totalProcessed}, errors ${totalErrors}`
  );

  stopTokenTracker();
  await closeDatabase();
  process.exit(0);
}

main().catch((err) => {
  console.error("[Backfill] Fatal:", err);
  process.exit(1);
});
