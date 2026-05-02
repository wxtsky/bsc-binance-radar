#!/usr/bin/env bun
/**
 * 测试飞书 webhook 推送：分别发 4 种 anomaly 信号样本到群里看效果
 *
 * 使用：
 *   NOTIFY_FEISHU_WEBHOOK=<your-url> bun scripts/test-feishu.ts
 *   NOTIFY_FEISHU_SECRET=<secret> NOTIFY_FEISHU_WEBHOOK=<url> bun scripts/test-feishu.ts
 */

import "dotenv/config";
import { sendFeishuAlert } from "../src/notifier/feishu.js";
import type { AnomalyTrigger } from "../src/anomaly/rules.js";

const webhook = process.env.NOTIFY_FEISHU_WEBHOOK;
if (!webhook) {
  console.error("Set NOTIFY_FEISHU_WEBHOOK env first");
  process.exit(1);
}
const secret = process.env.NOTIFY_FEISHU_SECRET || undefined;

const samples: AnomalyTrigger[] = [
  {
    rule: "vol_spike",
    tokenAddress: "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82",
    symbol: "CAKE",
    detectedAt: Date.now(),
    metrics: {
      vol5minUsd: 152340,
      vol24hAvg5minUsd: 18920,
      volRatio: 8.05,
      fee5minUsd: 380,
      tvlUsd: 4280000,
      feeAprPct: 9.3,
    },
  },
  {
    rule: "fee_tvl_apr",
    tokenAddress: "0xff7d6a96ae471bbcd7713af9cb1feeb16cf56b41",
    symbol: "BR",
    detectedAt: Date.now(),
    metrics: {
      vol5minUsd: 24,
      vol24hAvg5minUsd: 199,
      volRatio: 0.12,
      fee5minUsd: 0.067,
      tvlUsd: 2179,
      feeAprPct: 322.8,
    },
  },
  {
    rule: "combo",
    tokenAddress: "0x6d5ad1592ed9d6d1df9b93c793ab759573ed6714",
    symbol: "Broccoli",
    detectedAt: Date.now(),
    metrics: {
      vol5minUsd: 3175,
      vol24hAvg5minUsd: 412,
      volRatio: 7.7,
      fee5minUsd: 21.17,
      tvlUsd: 1698000,
      feeAprPct: 131.0,
    },
  },
  {
    rule: "new_pool",
    tokenAddress: "0x000ae314e2a2172a039b26378814c252734f556a",
    symbol: "ASTER",
    detectedAt: Date.now(),
    metrics: {
      newPoolAddress: "0xad26ebf44e6f57db393a054f2164fb7ac54609a4",
      newPoolDex: "pancakeswap-v3",
      newPoolFeeTier: 2500,
      newPoolAgeMs: 8421,
    },
  },
];

console.log(`Sending ${samples.length} sample alerts to Feishu${secret ? " (signed)" : ""}...`);
for (const t of samples) {
  try {
    await sendFeishuAlert(t, { webhook, secret });
    console.log(`✓ [${t.rule}] ${t.symbol}`);
  } catch (err) {
    console.error(`✗ [${t.rule}] ${t.symbol}:`, err instanceof Error ? err.message : err);
  }
  // 飞书机器人对同一 webhook 限速 (~5 msg/s)，sleep 一下
  await new Promise((r) => setTimeout(r, 600));
}
console.log("Done. Check your Feishu group.");
