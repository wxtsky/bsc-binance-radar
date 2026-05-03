export type ChainId = "bsc";
export type DexType =
  | "uniswap-v3"
  | "uniswap-v4"
  | "pancakeswap-v3"
  | "pancakeswap-v4-cl";

export interface PoolInfo {
  address: string;
  chain: ChainId;
  dex: DexType;
  token0: string;
  token1: string;
  feeTier: number;
}

export interface SwapRecord {
  id?: number;
  poolAddress: string;
  chain: ChainId;
  dex: DexType;
  txHash: string;
  amount0: string;
  amount1: string;
  feeUsd: number;
  volumeUsd: number;
  timestamp: number;
  blockNumber: number;
}

export interface TokenPriceInfo {
  address: string;
  chain: ChainId;
  price: number;
  symbol: string;
  decimals: number;
}

export interface V4PoolTokenInfo {
  currency0: string;
  currency1: string;
  fee: number;
  tickSpacing: number;
  hooks: string;
}

export interface ChainConfig {
  id: number;
  name: ChainId;
  wssUrl: string;
  chainIndex: string;
}

export interface ContractAddresses {
  uniswapV3Factory: string;
  uniswapV4PoolManager: string;
  uniswapV4PositionManager: string;
  pancakeswapV3Factory: string;
  pancakeswapV4ClPoolManager: string;
  pancakeswapV4ClPositionManager: string;
  // 价格基准池（base-pair pool），仅作 BNB/USD 历史价的数据源
  bnbPricePool: string;
}

export interface BinanceBscToken {
  symbol: string;
  baseAsset: string;
  contractAddress: string;
  decimals: number;
  updatedAt: number;
}
