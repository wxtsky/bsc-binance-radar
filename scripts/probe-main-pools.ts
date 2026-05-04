#!/usr/bin/env bun
/**
 * 探测 v3：305 主池 + 链上 factory() 决议 V2/V3
 *
 * 步骤：
 *   1. fapi 永续 baseAssets (528)
 *   2. bapi 现货 BSC 充提合约 (191 hits)
 *   3. CoinGecko platforms 补全 (127 hits)
 *   4. 1000-prefix normalize (7 hits)
 *   5. OKX top-liquidity → 选第一个支持 dex 的 base 池
 *   6. 对 PancakeSwap fee=0.25% 的池调链上 factory() 决议 V2/V3
 *   7. 标记 V4 native BNB 池（base=BNB 而非 WBNB）
 *   8. dump 完整主池 JSON
 */
import crypto from "node:crypto";

const env = await Bun.file(".env").text();
const cfg = Object.fromEntries(
  env.split("\n").filter((l) => l.includes("=")).map((l) => {
    const i = l.indexOf("=");
    return [l.slice(0, i).trim(), l.slice(i + 1).trim()];
  }),
);
const OKX_KEY = cfg.OKX_API_KEY!;
const OKX_SECRET = cfg.OKX_SECRET_KEY!;
const OKX_PASS = cfg.OKX_PASSPHRASE!;
const RPC_URL = cfg.BSC_HTTP_URL || "https://bsc-mainnet.nodereal.io/v1/b13fcff9775e4d1bb28a0735292a1819";

// 已知 BSC 上的 V2 / V3 factory 地址（lowercase）
const PCS_V2_FACTORY = "0xca143ce32fe78f1f7019d7d551a6402fc5350c73";
const PCS_V3_FACTORY = "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865";
const UNI_V3_FACTORY = "0xdb1d10011ad0ff90774d0c6bb92e5c5c8b4461f7";

const FACTORY_SELECTOR = "0xc45a0155"; // bytes4(keccak256("factory()"))
const NATIVE_BNB_ADDR = "0x0000000000000000000000000000000000000000";
const WBNB = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c";
const USDT = "0x55d398326f99059ff775485246999027b3197955";
const USDC = "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d";
const SYM_TO_ADDR: Record<string, string> = {
  WBNB, BNB: NATIVE_BNB_ADDR, USDT, USDC,
};

function sign(ts: string, method: string, path: string): string {
  return crypto.createHmac("sha256", OKX_SECRET).update(ts + method + path).digest("base64");
}
async function okxTopLiquidity(token: string, retries = 3) {
  const path = `/api/v6/dex/market/token/top-liquidity?chainIndex=56&tokenContractAddress=${token}`;
  let lastErr: any;
  for (let i = 0; i < retries; i++) {
    try {
      const ts = new Date().toISOString().replace(/\.\d+Z$/, ".000Z");
      const r = await fetch(`https://web3.okx.com${path}`, {
        headers: {
          "OK-ACCESS-KEY": OKX_KEY,
          "OK-ACCESS-SIGN": sign(ts, "GET", path),
          "OK-ACCESS-TIMESTAMP": ts,
          "OK-ACCESS-PASSPHRASE": OKX_PASS,
        },
      });
      return (await r.json()) as { code: string; msg: string; data?: any[] };
    } catch (e) {
      lastErr = e;
      await new Promise((r) => setTimeout(r, 500 * (i + 1)));
    }
  }
  throw lastErr;
}

async function ethCall(to: string, data: string, retries = 3): Promise<string | null> {
  let lastErr: any;
  for (let i = 0; i < retries; i++) {
    try {
      const r = await fetch(RPC_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          jsonrpc: "2.0", id: 1, method: "eth_call",
          params: [{ to, data }, "latest"],
        }),
      });
      const j = await r.json() as any;
      if (j.error || !j.result || j.result === "0x" || j.result.length < 66) return null;
      return j.result;
    } catch (e) {
      lastErr = e;
      await new Promise((r) => setTimeout(r, 500 * (i + 1)));
    }
  }
  console.error("ethCall failed:", lastErr?.message);
  return null;
}
async function poolFactory(poolAddr: string): Promise<string | null> {
  const r = await ethCall(poolAddr, FACTORY_SELECTOR);
  if (!r) return null;
  return "0x" + r.slice(-40).toLowerCase();
}

console.log("=== fapi + bapi + binance Alpha 三源拉取 ===");
const t0 = Date.now();
const [fapiResp, bapiResp, alphaResp] = await Promise.all([
  fetch("https://fapi.binance.com/fapi/v1/exchangeInfo").then((r) => r.json()),
  fetch("https://www.binance.com/bapi/capital/v1/public/capital/getNetworkCoinAll").then((r) => r.json()),
  fetch("https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list").then((r) => r.json()),
]) as [any, any, any];
console.log(`pulled in ${Date.now() - t0}ms`);

const perp = new Set<string>(
  fapiResp.symbols
    .filter((s: any) => s.contractType === "PERPETUAL" && s.status === "TRADING" && s.quoteAsset === "USDT")
    .map((s: any) => s.baseAsset.toUpperCase()),
);

// bapi: 现货 BSC 充提合约（最权威，先用）
const bapiMap = new Map<string, string>();
for (const c of bapiResp.data || []) {
  const e = c.networkList.find((n: any) => n.network === "BSC");
  if (e?.contractAddress && (e.depositEnable || e.withdrawEnable)) {
    bapiMap.set(c.coin.toUpperCase(), e.contractAddress.toLowerCase());
  }
}

// Alpha: 补永续 only 的 BSC 合约（cexCoinName 优先，回退 symbol）
const alphaCex = new Map<string, string>();
const alphaSym = new Map<string, string>();
for (const t of alphaResp.data || []) {
  if (t.chainName !== "BSC" || !t.contractAddress) continue;
  const addr = t.contractAddress.toLowerCase();
  if (t.cexCoinName) alphaCex.set(t.cexCoinName.toUpperCase(), addr);
  alphaSym.set(t.symbol.toUpperCase(), addr);
}
console.log(`sources: bapi=${bapiMap.size}, alphaCex=${alphaCex.size}, alphaSym=${alphaSym.size}`);

// 排除稳定币 perp（USDC/TUSD/USDP 等，监控无意义）
const STABLE_SKIP = new Set(["USDC", "TUSD", "USDP", "USDA", "USTC", "DAI", "FDUSD", "BUSD"]);

function normCands(sym: string): string[] {
  const out = [sym];
  if (sym.startsWith("1000")) out.push(sym.slice(4));
  if (sym.startsWith("1MBABY")) out.push("BABY" + sym.slice(6));
  if (sym.startsWith("1M")) out.push(sym.slice(2));
  return out;
}

interface Resolved { coin: string; addr: string; source: string }
const resolved: Resolved[] = [];
const skippedStable: string[] = [];
const bySource = { bapi: 0, alphaCex: 0, alphaSym: 0 };

for (const coin of perp) {
  if (STABLE_SKIP.has(coin)) { skippedStable.push(coin); continue; }
  const cands = normCands(coin);
  let hit: { addr: string; source: string } | null = null;
  for (const c of cands) {
    const a = bapiMap.get(c);
    if (a) { hit = { addr: a, source: c === coin ? "bapi" : `bapi:${c}` }; bySource.bapi++; break; }
  }
  if (!hit) for (const c of cands) {
    const a = alphaCex.get(c);
    if (a) { hit = { addr: a, source: c === coin ? "alphaCex" : `alphaCex:${c}` }; bySource.alphaCex++; break; }
  }
  if (!hit) for (const c of cands) {
    const a = alphaSym.get(c);
    if (a) { hit = { addr: a, source: c === coin ? "alphaSym" : `alphaSym:${c}` }; bySource.alphaSym++; break; }
  }
  if (hit) resolved.push({ coin, addr: hit.addr, source: hit.source });
}
console.log(`perp=${perp.size}, stable-skip=${skippedStable.length} (${skippedStable.join(", ")})`);
console.log(`resolved=${resolved.length}  bySource=`, bySource);

console.log(`\n=== OKX top-liquidity 选主池 (${resolved.length} token) ===`);
interface MainPoolRow {
  coin: string; tokenAddr: string; source: string;
  protocol: string; pool: string; baseSym: string; baseAddr: string;
  poolAddress: string; fee: string; tvl: number;
  isV4: boolean; isNativeBnb: boolean;
  dex: string; // resolved 之后的最终 dex 标识
  factoryDecided?: string; // 链上 factory() 决议结果
}
const rows: MainPoolRow[] = [];
const noBasePool: string[] = [];
const noSupportedDex: { coin: string; topProto: string }[] = [];

const SUPPORTED_PROTOS = new Set(["Uniswap V4", "PancakeSwap", "Uniswap"]);

const CONCURRENCY = 6;
let cursor = 0;
async function w(id: number) {
  while (cursor < resolved.length) {
    const i = cursor++;
    const t = resolved[i];
    const r = await okxTopLiquidity(t.addr);
    if (r.code !== "0") continue;
    let chosen: any = null;
    let firstBase: any = null;
    for (const c of r.data || []) {
      const symbols = c.pool.toUpperCase().split("/");
      // OKX 给的 pool symbol 是 token 自己的（如 $BANANA），跟 perp baseAsset (如 BANANAS31) 可能不一致
      // 所以不用 perp coin symbol 匹配，直接找池子里的 base symbol
      const otherSym = symbols.find((s: string) => ["BNB", "WBNB", "USDT", "USDC"].includes(s));
      if (!otherSym) continue;
      if (!firstBase) firstBase = { c, otherSym };
      if (SUPPORTED_PROTOS.has(c.protocolName)) { chosen = { c, otherSym }; break; }
    }
    if (!firstBase) { noBasePool.push(t.coin); continue; }
    if (!chosen) { noSupportedDex.push({ coin: t.coin, topProto: firstBase.c.protocolName }); continue; }
    const c = chosen.c;
    const isV4 = c.poolAddress.length > 42;
    const isNativeBnb = chosen.otherSym === "BNB";
    let dex = "";
    if (c.protocolName === "Uniswap V4") dex = "uniswap-v4";
    else if (c.protocolName === "PancakeSwap" && isV4) dex = "pancakeswap-v4-cl";
    else if (c.protocolName === "Uniswap") dex = "uniswap-v3";
    else if (c.protocolName === "PancakeSwap") {
      dex = c.liquidityProviderFeePercent === "0.25%" ? "pancakeswap-v2-or-v3" : "pancakeswap-v3";
    }
    rows.push({
      coin: t.coin, tokenAddr: t.addr, source: t.source,
      protocol: c.protocolName, pool: c.pool,
      baseSym: chosen.otherSym, baseAddr: SYM_TO_ADDR[chosen.otherSym] || "?",
      poolAddress: c.poolAddress.toLowerCase(),
      fee: c.liquidityProviderFeePercent, tvl: Number(c.liquidityUsd),
      isV4, isNativeBnb, dex,
    });
    if ((i + 1) % 50 === 0) console.error(`[w${id}] ${i + 1}/${resolved.length}`);
  }
}
const t1 = Date.now();
await Promise.all(Array.from({ length: CONCURRENCY }, (_, i) => w(i)));
console.log(`OKX 完成 ${((Date.now() - t1) / 1000).toFixed(1)}s, 主池 ${rows.length} 个`);

// === M1.1 链上 factory() 决议 PCS v2-or-v3 ===
const ambig = rows.filter((r) => r.dex === "pancakeswap-v2-or-v3");
console.log(`\n=== 链上 factory() 决议 ${ambig.length} 个歧义池 ===`);
const t2 = Date.now();
let cursor2 = 0;
async function fw(id: number) {
  while (cursor2 < ambig.length) {
    const i = cursor2++;
    const row = ambig[i];
    const fac = await poolFactory(row.poolAddress);
    row.factoryDecided = fac || "?";
    if (fac === PCS_V2_FACTORY) row.dex = "pancakeswap-v2";
    else if (fac === PCS_V3_FACTORY) row.dex = "pancakeswap-v3";
    else row.dex = `pcs-unknown(${fac})`;
    if ((i + 1) % 20 === 0) console.error(`[fw${id}] ${i + 1}/${ambig.length}`);
    await new Promise((r) => setTimeout(r, 20));
  }
}
await Promise.all(Array.from({ length: 5 }, (_, i) => fw(i)));
console.log(`factory() 完成 ${((Date.now() - t2) / 1000).toFixed(1)}s`);

// === 统计 ===
const byDex: Record<string, number> = {};
const byBase: Record<string, number> = {};
for (const r of rows) {
  byDex[r.dex] = (byDex[r.dex] || 0) + 1;
  byBase[r.baseSym] = (byBase[r.baseSym] || 0) + 1;
}
const nativeBnb = rows.filter((r) => r.isNativeBnb);

console.log(`\n=== 最终 dex 分布 (${rows.length} 主池) ===`);
for (const [k, v] of Object.entries(byDex).sort((a, b) => b[1] - a[1])) {
  console.log(`  ${k.padEnd(28)} ${v}`);
}
console.log(`\n=== base 分布 ===`);
for (const [k, v] of Object.entries(byBase).sort((a, b) => b[1] - a[1])) {
  console.log(`  ${k.padEnd(10)} ${v}`);
}
console.log(`\n=== V4 native BNB 池 (${nativeBnb.length}) ===`);
for (const r of nativeBnb) {
  console.log(`  ${r.coin.padEnd(15)} ${r.protocol.padEnd(12)} ${r.dex.padEnd(20)} TVL=$${Math.round(r.tvl).toLocaleString().padStart(12)} pool=${r.poolAddress}`);
}
console.log(`\n=== 排除 ===`);
console.log(`  没 base 池: ${noBasePool.length} (${noBasePool.join(", ")})`);
console.log(`  top dex 不支持: ${noSupportedDex.length}`);
for (const x of noSupportedDex) console.log(`    ${x.coin.padEnd(15)} top=${x.topProto}`);

// dump
const out = {
  generated_at: new Date().toISOString(),
  perp_count: perp.size, resolved_count: resolved.length, main_pool_count: rows.length,
  by_dex: byDex, by_base: byBase,
  rows,
  excluded: { no_base_pool: noBasePool, no_supported_dex: noSupportedDex },
};
await Bun.write("/tmp/main-pools-v3.json", JSON.stringify(out, null, 2));
console.log(`\n详情写入 /tmp/main-pools-v3.json`);
