const FAPI_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo";

/**
 * Mac 本地用 `bun run sync-perpetuals` 拉 fapi 数据写到 seed/binance-perpetuals.json
 * 并提交到 git。服务器在 fapi 被 451 时从 GitHub raw 读这份镜像。
 *
 * 数据源 100% 是 binance fapi 官方，只是经过 git 中转给 geo-blocked 机器。
 */
const SEED_RAW_URL =
  process.env.BINANCE_PERPETUALS_SEED_URL ||
  "https://raw.githubusercontent.com/wxtsky/bsc-binance-radar/main/seed/binance-perpetuals.json";

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

interface SeedFile {
  source: string;
  syncedAt: string;
  count: number;
  baseAssets: string[];
}

/**
 * fapi 用 1000PEPE/1000SHIB/1000000MOG 等前缀表示精度归一化报价，
 * 但 capital getNetworkCoinAll 的 coin 字段是 PEPE/SHIB/MOG（无前缀）。
 * 取交集前必须剥前缀，否则会漏掉所有 1000xxx 系列 meme token。
 *
 * 注意：仅匹配 ^1000+，避免破坏真实以 1 开头的 token（如 1INCH）。
 */
export function normalizeBinanceBaseAsset(base: string): string {
  return base.toUpperCase().replace(/^1000+/, "");
}

async function fetchFromFapiDirect(): Promise<Set<string>> {
  const res = await fetch(FAPI_EXCHANGE_INFO);
  if (!res.ok) {
    throw new Error(`fapi exchangeInfo HTTP ${res.status}`);
  }
  const data = (await res.json()) as FuturesExchangeInfo;
  const set = new Set<string>();
  for (const s of data.symbols) {
    if (
      s.contractType === "PERPETUAL" &&
      s.quoteAsset === "USDT" &&
      s.status === "TRADING"
    ) {
      set.add(normalizeBinanceBaseAsset(s.baseAsset));
    }
  }
  return set;
}

async function fetchFromGitHubSeed(): Promise<Set<string>> {
  const res = await fetch(SEED_RAW_URL, {
    headers: { "User-Agent": "bsc-binance-radar/1.0" },
  });
  if (!res.ok) {
    throw new Error(`Seed mirror HTTP ${res.status} (${SEED_RAW_URL})`);
  }
  const seed = (await res.json()) as SeedFile;
  if (!Array.isArray(seed.baseAssets) || seed.baseAssets.length === 0) {
    throw new Error("Seed file invalid or empty");
  }
  const ageHours = (Date.now() - new Date(seed.syncedAt).getTime()) / 3_600_000;
  console.log(
    `[Tracker] Loaded ${seed.baseAssets.length} baseAssets from seed mirror ` +
      `(synced ${ageHours.toFixed(1)}h ago, source=${seed.source})`
  );
  return new Set(seed.baseAssets.map((b) => b.toUpperCase()));
}

/**
 * 拉「币安 USDT-M 永续合约 baseAsset 列表」。
 *
 * 路径：
 *   1. 直接 fapi.binance.com (非美机器走这条，最实时)
 *   2. fapi 失败 fallback 到 GitHub raw 上的 seed JSON
 *      (mac 本地用 `bun run sync-perpetuals` 同步进 git)
 *
 * 数据源全程是 binance fapi 官方，仅在 geo-block 时通过 git 镜像中转。
 */
export async function fetchActivePerpetualBaseAssets(): Promise<Set<string>> {
  try {
    return await fetchFromFapiDirect();
  } catch (err) {
    console.warn(
      `[Tracker] fapi direct failed (${(err as Error).message}); falling back to GitHub seed`
    );
    return await fetchFromGitHubSeed();
  }
}
