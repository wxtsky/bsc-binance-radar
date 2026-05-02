#!/usr/bin/env bun
/**
 * 回补历史 swap 数据，让 detector 立刻有 baseline
 *
 * 用法：
 *   bun scripts/backfill.ts [hours]      # 默认 24
 *
 * 容器内：
 *   docker compose run --rm --no-deps radar bun scripts/backfill.ts 4
 *
 * 注：WSS 不适合大范围 eth_getLogs（容易超时），用 HTTP RPC（BSC_HTTP_URL）。
 */

import "dotenv/config";
import { createPublicClient, http, parseAbiItem, type Log } from "viem";
import { bsc } from "viem/chains";
import { initSchema, closeDatabase } from "../src/db/index.js";
import { CONTRACTS } from "../src/config/contracts.js";
import { initPriceService } from "../src/core/price-service.js";
import { startTokenTracker, stopTokenTracker } from "../src/token-tracker/tracker.js";
import { processV3SwapLog, processV4SwapLog } from "../src/core/swap-listener.js";

const HOURS = Number(process.argv[2]) || 24;
const BSC_BLOCK_TIME_S = 3;
const BLOCKS_PER_BATCH = 1000n; // 保守，避免节点 timeout
const TOTAL_BLOCKS = BigInt(Math.floor((HOURS * 3600) / BSC_BLOCK_TIME_S));

const HTTP_URL = process.env.BSC_HTTP_URL || "http://151.123.172.62:81";

// 三种 swap 事件 ABI（viem 用这个自动算 topic[0]）
const V3_SWAP = parseAbiItem(
  "event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)"
);
const PANCAKE_V3_SWAP = parseAbiItem(
  "event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick, uint128 protocolFeesToken0, uint128 protocolFeesToken1)"
);
const V4_SWAP = parseAbiItem(
  "event Swap(bytes32 indexed id, address indexed sender, int128 amount0, int128 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick, uint24 fee)"
);

async function main() {
  console.log(`[Backfill] Starting ~${HOURS}h backfill via HTTP RPC ${HTTP_URL}`);

  await initSchema();
  await startTokenTracker();
  await initPriceService();

  const httpClient = createPublicClient({
    chain: bsc,
    transport: http(HTTP_URL, { timeout: 30_000, retryCount: 2 }),
    batch: { multicall: true },
  });

  const latest = await httpClient.getBlockNumber();
  const start = latest - TOTAL_BLOCKS;
  console.log(
    `[Backfill] Block ${start} → ${latest} (${TOTAL_BLOCKS} blocks, batch ${BLOCKS_PER_BATCH})`
  );

  let totalLogs = 0;
  let totalProcessed = 0;
  let totalErrors = 0;
  const t0 = Date.now();

  const v4PoolManager = CONTRACTS.bsc.uniswapV4PoolManager as `0x${string}`;

  for (let from = start; from < latest; from += BLOCKS_PER_BATCH) {
    const to = from + BLOCKS_PER_BATCH - 1n > latest ? latest : from + BLOCKS_PER_BATCH - 1n;

    // 拿 batch 起始 block 的真实时间，按 BSC 3s/block 推每条 log 的 timestamp
    let fromBlockTs = 0;
    try {
      const blk = await httpClient.getBlock({ blockNumber: from });
      fromBlockTs = Number(blk.timestamp) * 1000;
    } catch (err) {
      console.error(`[Backfill] getBlock ${from} failed:`, (err as Error).message);
      continue;
    }

    let v3: Log[] = [];
    let pancake: Log[] = [];
    let v4: Log[] = [];
    try {
      [v3, pancake, v4] = await Promise.all([
        httpClient.getLogs({ fromBlock: from, toBlock: to, event: V3_SWAP }) as unknown as Promise<Log[]>,
        httpClient.getLogs({ fromBlock: from, toBlock: to, event: PANCAKE_V3_SWAP }) as unknown as Promise<Log[]>,
        httpClient.getLogs({
          address: v4PoolManager,
          fromBlock: from,
          toBlock: to,
          event: V4_SWAP,
        }) as unknown as Promise<Log[]>,
      ]);
    } catch (err) {
      console.error(`[Backfill] getLogs ${from}-${to} failed:`, (err as Error).message);
      continue;
    }

    const batchTotal = v3.length + pancake.length + v4.length;
    totalLogs += batchTotal;

    const tsForLog = (log: Log): number => {
      const blockOffset = Number(log.blockNumber ?? from) - Number(from);
      return fromBlockTs + blockOffset * BSC_BLOCK_TIME_S * 1000;
    };

    for (const log of v3) {
      try {
        await processV3SwapLog(log, "bsc", "uniswap-v3", tsForLog(log));
        totalProcessed++;
      } catch {
        totalErrors++;
      }
    }
    for (const log of pancake) {
      try {
        await processV3SwapLog(log, "bsc", "pancakeswap-v3", tsForLog(log));
        totalProcessed++;
      } catch {
        totalErrors++;
      }
    }
    for (const log of v4) {
      try {
        await processV4SwapLog(log, "bsc", tsForLog(log));
        totalProcessed++;
      } catch {
        totalErrors++;
      }
    }

    const pct = Number(((to - start) * 100n) / (latest - start));
    const elapsedS = ((Date.now() - t0) / 1000).toFixed(0);
    console.log(
      `[Backfill] ${pct}% block ${to} batch ${batchTotal} | total ${totalLogs} ok ${totalProcessed} err ${totalErrors} | ${elapsedS}s`
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
