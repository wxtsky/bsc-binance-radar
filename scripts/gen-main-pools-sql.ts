#!/usr/bin/env bun
/**
 * 从 /tmp/main-pools-v3.json 读 183 主池 → 输出 INSERT SQL
 *
 * 用法：
 *   bun scripts/gen-main-pools-sql.ts > /tmp/main-pools-insert.sql
 *
 * 输出可直接灌入远程 PG：
 *   psql ... -f /tmp/main-pools-insert.sql
 */
const j = await Bun.file("/tmp/main-pools-v3.json").json();

const DEX_ENUM: Record<string, number> = {
  "uniswap-v3": 1,
  "pancakeswap-v3": 2,
  "uniswap-v4": 3,
  "pancakeswap-v4-cl": 4,
  "pancakeswap-v2": 5,
};

function feeBps(feePct: string): number | null {
  // 统一 V3 标准 hundredths-of-bps：fee_usd = volume_usd * fee_value / 1_000_000
  // 0.01% → 100, 0.05% → 500, 0.25% → 2500, 0.30% → 3000, 1.00% → 10000
  // V4 OKX 显示 99.59% 这种带 hooks 池：→ 995900（也直接走 /1e6 公式，结果异常但不崩）
  const num = parseFloat(feePct.replace("%", ""));
  if (Number.isNaN(num)) return null;
  return Math.round(num * 10000);  // pct → hundredths-of-bps
}

function bytea(hex: string): string {
  return `'\\x${hex.replace(/^0x/, "").toLowerCase()}'`;
}

console.log("-- M2: 183 个主池 INSERT");
console.log(`-- 生成时间: ${new Date().toISOString()}`);
console.log(`-- 数据源: /tmp/main-pools-v3.json (probe-main-pools.ts 输出)`);
console.log("");
console.log("BEGIN;");
console.log("TRUNCATE main_pools;");
console.log("");

const skipped: string[] = [];
const inserts: string[] = [];

for (const r of j.rows) {
  const dexInt = DEX_ENUM[r.dex];
  if (!dexInt) { skipped.push(`${r.coin} (dex=${r.dex})`); continue; }
  const fee = feeBps(r.fee);
  const poolSize = r.poolAddress.length === 66 ? 32 : 20;
  const tvl = isFinite(r.tvl) ? r.tvl.toFixed(2) : "NULL";
  inserts.push(
    `INSERT INTO main_pools(coin, token_addr, pool_addr, pool_addr_size, dex, base_addr, base_sym, fee_bps, tvl_usd, is_native_bnb) ` +
    `VALUES ('${r.coin.replace(/'/g, "''")}', ${bytea(r.tokenAddr)}, ${bytea(r.poolAddress)}, ${poolSize}, ${dexInt}, ${bytea(r.baseAddr)}, '${r.baseSym}', ${fee ?? "NULL"}, ${tvl}, ${r.isNativeBnb});`,
  );
}

console.log(inserts.join("\n"));
console.log("");
console.log("COMMIT;");
console.log("");
console.log(`-- 共 INSERT ${inserts.length} 行 (skipped ${skipped.length}: ${skipped.slice(0, 5).join(", ")})`);
console.log("-- 验证：SELECT dex, COUNT(*) FROM main_pools GROUP BY dex ORDER BY dex;");
