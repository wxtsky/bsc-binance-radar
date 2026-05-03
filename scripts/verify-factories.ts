/**
 * 一次性 verify 脚本：扫 4 个合约最近一段 block，确认 (a) 地址命中 (b) 事件签名解码正确。
 *
 * 用 NodeReal archive node（公网，不受自建节点 prune 影响）。
 *
 * 跑：
 *   bun scripts/verify-factories.ts
 *   bun scripts/verify-factories.ts --rpc http://151.123.172.62:81   # 自建节点
 */

import { createPublicClient, http, parseAbiItem } from "viem";
import { bsc } from "viem/chains";
import { CONTRACTS } from "../src/config/contracts.js";

const RPC =
  process.argv.find((a) => a.startsWith("--rpc"))?.split("=")[1] ??
  "https://bsc-mainnet.nodereal.io/v1/b13fcff9775e4d1bb28a0735292a1819";

const SCAN_RANGE = 49_999n; // NodeReal 50K blocks/call 上限（HANDOFF § 13）

const V3_POOL_CREATED = parseAbiItem(
  "event PoolCreated(address indexed token0, address indexed token1, uint24 indexed fee, int24 tickSpacing, address pool)"
);
const V4_INITIALIZE = parseAbiItem(
  "event Initialize(bytes32 indexed id, address indexed currency0, address indexed currency1, uint24 fee, int24 tickSpacing, address hooks, uint160 sqrtPriceX96, int24 tick)"
);
const PCS_V4_CL_INITIALIZE = parseAbiItem(
  "event Initialize(bytes32 indexed id, address indexed currency0, address indexed currency1, address hooks, uint24 fee, bytes32 parameters, uint160 sqrtPriceX96, int24 tick)"
);

interface VerifyTarget {
  name: string;
  address: `0x${string}`;
  event: ReturnType<typeof parseAbiItem>;
  expectedFields: string[];
}

const targets: VerifyTarget[] = [
  {
    name: "UniswapV3 Factory",
    address: CONTRACTS.bsc.uniswapV3Factory as `0x${string}`,
    event: V3_POOL_CREATED,
    expectedFields: ["token0", "token1", "fee", "tickSpacing", "pool"],
  },
  {
    name: "PancakeSwap V3 Factory",
    address: CONTRACTS.bsc.pancakeswapV3Factory as `0x${string}`,
    event: V3_POOL_CREATED,
    expectedFields: ["token0", "token1", "fee", "tickSpacing", "pool"],
  },
  {
    name: "UniswapV4 PoolManager",
    address: CONTRACTS.bsc.uniswapV4PoolManager as `0x${string}`,
    event: V4_INITIALIZE,
    expectedFields: ["id", "currency0", "currency1", "fee", "tickSpacing", "hooks"],
  },
  {
    name: "PancakeSwap V4 CL PoolManager",
    address: CONTRACTS.bsc.pancakeswapV4ClPoolManager as `0x${string}`,
    event: PCS_V4_CL_INITIALIZE,
    expectedFields: ["id", "currency0", "currency1", "hooks", "fee", "parameters"],
  },
];

async function main() {
  const client = createPublicClient({ chain: bsc, transport: http(RPC, { timeout: 60_000 }) });
  console.log(`[verify] RPC = ${RPC}`);
  const latest = await client.getBlockNumber();
  const fromBlock = latest - SCAN_RANGE;
  console.log(`[verify] 扫范围 ${fromBlock} → ${latest} (${SCAN_RANGE} blocks)\n`);

  let allOk = true;
  for (const t of targets) {
    process.stdout.write(`[${t.name}] (${t.address})\n`);
    try {
      const logs = await client.getLogs({
        address: t.address,
        fromBlock,
        toBlock: latest,
        // @ts-expect-error event union
        event: t.event,
      });
      if (logs.length === 0) {
        console.log(`  ❌ 0 events in last ${SCAN_RANGE} blocks → 地址或事件签名可能错`);
        allOk = false;
        continue;
      }
      const sample = logs[logs.length - 1] as unknown as { args: Record<string, unknown>; blockNumber: bigint; transactionHash: string };
      const args = sample.args ?? {};
      const missing = t.expectedFields.filter((f) => args[f] === undefined || args[f] === null);
      if (missing.length > 0) {
        console.log(`  ❌ ${logs.length} events, but sample missing fields: ${missing.join(", ")}`);
        console.log(`     sample.args =`, args);
        allOk = false;
        continue;
      }
      console.log(`  ✅ ${logs.length} events, sample @ block ${sample.blockNumber}`);
      console.log(`     tx: ${sample.transactionHash}`);
      const preview: Record<string, string> = {};
      for (const f of t.expectedFields) {
        const v = args[f];
        preview[f] = typeof v === "bigint" ? v.toString() : String(v);
      }
      console.log(`     args:`, preview);
    } catch (err) {
      console.log(`  ❌ 错: ${(err as Error).message}`);
      allOk = false;
    }
    console.log();
  }

  if (allOk) {
    console.log("✅ 全部 verify 通过 — factory 地址 + 事件签名都正确，可以用于扫历史 PoolCreated。");
    process.exit(0);
  } else {
    console.log("❌ 部分 verify 失败，需要修地址或事件签名后再扫。");
    process.exit(1);
  }
}

main();
