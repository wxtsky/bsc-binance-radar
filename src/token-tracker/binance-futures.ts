const FAPI_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo";

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

/**
 * 拉取所有正在交易的 USDT-M 永续合约的 baseAsset 集合
 * (BTC、ETH、PEPE、CAKE 等 ~500 个)
 */
export async function fetchActivePerpetualBaseAssets(): Promise<Set<string>> {
  const res = await fetch(FAPI_EXCHANGE_INFO);
  if (!res.ok) {
    throw new Error(`Failed to fetch fapi exchangeInfo: ${res.status} ${res.statusText}`);
  }
  const data = (await res.json()) as FuturesExchangeInfo;

  const baseAssets = new Set<string>();
  for (const s of data.symbols) {
    if (
      s.contractType === "PERPETUAL" &&
      s.quoteAsset === "USDT" &&
      s.status === "TRADING"
    ) {
      baseAssets.add(s.baseAsset.toUpperCase());
    }
  }
  return baseAssets;
}
