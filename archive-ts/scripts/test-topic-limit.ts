// 二分搜索节点对 topics OR 数组的上限
import { createPublicClient, http, parseAbiItem } from "viem";
import { bsc } from "viem/chains";

const PCS_V4_CL_SWAP = parseAbiItem(
  "event Swap(bytes32 indexed id, address indexed sender, int128 amount0, int128 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick, uint24 fee, uint16 protocolFee)"
);

const client = createPublicClient({
  chain: bsc,
  transport: http(process.argv[2] || "http://151.123.172.62:81", { timeout: 30_000 }),
});

const sample: `0x${string}`[] = Array.from(
  { length: 10000 },
  (_, i) => `0x${i.toString(16).padStart(64, "0")}` as `0x${string}`
);

const latest = await client.getBlockNumber();
const fromBlock = latest - 100n;
const toBlock = latest;

for (const n of [10, 100, 500, 1000, 2000, 4000, 8000]) {
  try {
    await client.getLogs({
      address: "0xa0FfB9c1CE1Fe56963B0321B32E7A0302114058b",
      fromBlock, toBlock, event: PCS_V4_CL_SWAP,
      args: { id: sample.slice(0, n) },
    });
    console.log(`✅ N=${n} OK`);
  } catch (err) {
    console.log(`❌ N=${n} FAIL: ${(err as Error).message.split("\n")[0]}`);
    break;
  }
}
