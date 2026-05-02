const BAPI_NETWORK_COIN_ALL = "https://www.binance.com/bapi/capital/v1/public/capital/getNetworkCoinAll";

interface NetworkEntry {
  network: string;
  coin: string;
  contractAddress: string | null;
  withdrawEnable: boolean;
  depositEnable: boolean;
}

interface CoinEntry {
  coin: string;
  name: string;
  networkList: NetworkEntry[];
}

interface BapiResp {
  code: string;
  data: CoinEntry[];
}

export interface BscDepositCoin {
  coin: string;
  contractAddress: string;
}

/**
 * 拉取所有有 BSC 充提网络配置的币种 + 其 BSC 合约地址
 * (~100 个左右)
 */
export async function fetchBscDepositCoins(): Promise<Map<string, BscDepositCoin>> {
  const res = await fetch(BAPI_NETWORK_COIN_ALL);
  if (!res.ok) {
    throw new Error(`Failed to fetch bapi getNetworkCoinAll: ${res.status} ${res.statusText}`);
  }
  const data = (await res.json()) as BapiResp;
  if (data.code !== "000000") {
    throw new Error(`bapi getNetworkCoinAll returned non-zero code: ${data.code}`);
  }

  const out = new Map<string, BscDepositCoin>();
  for (const c of data.data) {
    const bsc = c.networkList.find((n) => n.network === "BSC");
    if (!bsc || !bsc.contractAddress) continue;
    // 仅留下当前可正常 deposit 或 withdraw 的（避免被下架的旧合约）
    if (!bsc.depositEnable && !bsc.withdrawEnable) continue;
    out.set(c.coin.toUpperCase(), {
      coin: c.coin.toUpperCase(),
      contractAddress: bsc.contractAddress.toLowerCase(),
    });
  }
  return out;
}
