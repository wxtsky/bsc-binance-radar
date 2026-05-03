import type { ChainId, ContractAddresses } from "../types/index.js";

export const CONTRACTS: Record<ChainId, ContractAddresses> = {
  bsc: {
    uniswapV3Factory: "0xdB1d10011AD0Ff90774D0C6Bb92e5C5c8b4461F7",
    uniswapV4PoolManager: "0x28e2ea090877bf75740558f6bfb36a5ffee9e9df",
    uniswapV4PositionManager: "0x7a4a5c919ae2541aed11041a1aeee68f1287f95b",
    pancakeswapV3Factory: "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
    // PancakeSwap Infinity / V4 CL（concentrated liquidity）
    pancakeswapV4ClPoolManager: "0xa0FfB9c1CE1Fe56963B0321B32E7A0302114058b",
    pancakeswapV4ClPositionManager: "0x55f4c8abA71A1e923edC303eb4fEfF14608cC226",
    // PancakeSwap V2 WBNB/USDT 池（最深 TVL $35M），仅作 BNB/USD 历史价数据源
    bnbPricePool: "0x16b9a82891338f9bA80E2D6970FddA79D1eb0daE",
  },
};

export const NATIVE_TOKEN_ADDRESS = "0x0000000000000000000000000000000000000000";

export const WRAPPED_NATIVE: Record<ChainId, string> = {
  bsc: "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
};
