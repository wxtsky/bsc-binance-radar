// Uniswap V3 Pool ABI (Swap event + pool info)
export const UNISWAP_V3_POOL_ABI = [
  {
    anonymous: false,
    inputs: [
      { indexed: true, name: "sender", type: "address" },
      { indexed: true, name: "recipient", type: "address" },
      { indexed: false, name: "amount0", type: "int256" },
      { indexed: false, name: "amount1", type: "int256" },
      { indexed: false, name: "sqrtPriceX96", type: "uint160" },
      { indexed: false, name: "liquidity", type: "uint128" },
      { indexed: false, name: "tick", type: "int24" },
    ],
    name: "Swap",
    type: "event",
  },
  {
    inputs: [],
    name: "token0",
    outputs: [{ name: "", type: "address" }],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [],
    name: "token1",
    outputs: [{ name: "", type: "address" }],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [],
    name: "fee",
    outputs: [{ name: "", type: "uint24" }],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [],
    name: "slot0",
    outputs: [
      { name: "sqrtPriceX96", type: "uint160" },
      { name: "tick", type: "int24" },
      { name: "observationIndex", type: "uint16" },
      { name: "observationCardinality", type: "uint16" },
      { name: "observationCardinalityNext", type: "uint16" },
      { name: "feeProtocol", type: "uint8" },
      { name: "unlocked", type: "bool" },
    ],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [],
    name: "factory",
    outputs: [{ name: "", type: "address" }],
    stateMutability: "view",
    type: "function",
  },
] as const;

// PancakeSwap V3 Pool ABI (Swap event with protocolFees)
export const PANCAKESWAP_V3_POOL_ABI = [
  {
    anonymous: false,
    inputs: [
      { indexed: true, name: "sender", type: "address" },
      { indexed: true, name: "recipient", type: "address" },
      { indexed: false, name: "amount0", type: "int256" },
      { indexed: false, name: "amount1", type: "int256" },
      { indexed: false, name: "sqrtPriceX96", type: "uint160" },
      { indexed: false, name: "liquidity", type: "uint128" },
      { indexed: false, name: "tick", type: "int24" },
      { indexed: false, name: "protocolFeesToken0", type: "uint128" },
      { indexed: false, name: "protocolFeesToken1", type: "uint128" },
    ],
    name: "Swap",
    type: "event",
  },
  {
    inputs: [],
    name: "token0",
    outputs: [{ name: "", type: "address" }],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [],
    name: "token1",
    outputs: [{ name: "", type: "address" }],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [],
    name: "fee",
    outputs: [{ name: "", type: "uint24" }],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [],
    name: "slot0",
    outputs: [
      { name: "sqrtPriceX96", type: "uint160" },
      { name: "tick", type: "int24" },
      { name: "observationIndex", type: "uint16" },
      { name: "observationCardinality", type: "uint16" },
      { name: "observationCardinalityNext", type: "uint16" },
      { name: "feeProtocol", type: "uint32" },
      { name: "unlocked", type: "bool" },
    ],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [],
    name: "factory",
    outputs: [{ name: "", type: "address" }],
    stateMutability: "view",
    type: "function",
  },
] as const;

// Uniswap V4 PoolManager ABI
export const UNISWAP_V4_POOL_MANAGER_ABI = [
  {
    anonymous: false,
    inputs: [
      { indexed: true, name: "id", type: "bytes32" },
      { indexed: true, name: "sender", type: "address" },
      { indexed: false, name: "amount0", type: "int128" },
      { indexed: false, name: "amount1", type: "int128" },
      { indexed: false, name: "sqrtPriceX96", type: "uint160" },
      { indexed: false, name: "liquidity", type: "uint128" },
      { indexed: false, name: "tick", type: "int24" },
      { indexed: false, name: "fee", type: "uint24" },
    ],
    name: "Swap",
    type: "event",
  },
] as const;

// Uniswap V4 PositionManager ABI (for poolKeys)
export const UNISWAP_V4_POSITION_MANAGER_ABI = [
  {
    inputs: [{ name: "poolId", type: "bytes25" }],
    name: "poolKeys",
    outputs: [
      { name: "currency0", type: "address" },
      { name: "currency1", type: "address" },
      { name: "fee", type: "uint24" },
      { name: "tickSpacing", type: "int24" },
      { name: "hooks", type: "address" },
    ],
    stateMutability: "view",
    type: "function",
  },
] as const;

// PancakeSwap V4 (Infinity) CL PoolManager ABI
// Swap(PoolId indexed id, address indexed sender,
//      int128 amount0, int128 amount1,
//      uint160 sqrtPriceX96, uint128 liquidity, int24 tick,
//      uint24 fee, uint16 protocolFee)
export const PANCAKE_V4_CL_POOL_MANAGER_ABI = [
  {
    anonymous: false,
    inputs: [
      { indexed: true, name: "id", type: "bytes32" },
      { indexed: true, name: "sender", type: "address" },
      { indexed: false, name: "amount0", type: "int128" },
      { indexed: false, name: "amount1", type: "int128" },
      { indexed: false, name: "sqrtPriceX96", type: "uint160" },
      { indexed: false, name: "liquidity", type: "uint128" },
      { indexed: false, name: "tick", type: "int24" },
      { indexed: false, name: "fee", type: "uint24" },
      { indexed: false, name: "protocolFee", type: "uint16" },
    ],
    name: "Swap",
    type: "event",
  },
] as const;

// PancakeSwap V4 CLPositionManager ABI (mapping(bytes25 poolId => PoolKey))
// PancakeV4 PoolKey 多一个 parameters bytes32 字段（编码 tickSpacing/hookFlags）
export const PANCAKE_V4_CL_POSITION_MANAGER_ABI = [
  {
    inputs: [{ name: "poolId", type: "bytes25" }],
    name: "poolKeys",
    outputs: [
      { name: "currency0", type: "address" },
      { name: "currency1", type: "address" },
      { name: "hooks", type: "address" },
      { name: "poolManager", type: "address" },
      { name: "fee", type: "uint24" },
      { name: "parameters", type: "bytes32" },
    ],
    stateMutability: "view",
    type: "function",
  },
] as const;

// UniswapV2 / PancakeV2 Pool Swap event ABI
// Swap(address indexed sender,
//      uint amount0In, uint amount1In,
//      uint amount0Out, uint amount1Out,
//      address indexed to)
export const UNISWAP_V2_PAIR_ABI = [
  {
    anonymous: false,
    inputs: [
      { indexed: true, name: "sender", type: "address" },
      { indexed: false, name: "amount0In", type: "uint256" },
      { indexed: false, name: "amount1In", type: "uint256" },
      { indexed: false, name: "amount0Out", type: "uint256" },
      { indexed: false, name: "amount1Out", type: "uint256" },
      { indexed: true, name: "to", type: "address" },
    ],
    name: "Swap",
    type: "event",
  },
  {
    inputs: [],
    name: "token0",
    outputs: [{ name: "", type: "address" }],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [],
    name: "token1",
    outputs: [{ name: "", type: "address" }],
    stateMutability: "view",
    type: "function",
  },
] as const;

// ERC20 ABI
export const ERC20_ABI = [
  {
    inputs: [{ name: "account", type: "address" }],
    name: "balanceOf",
    outputs: [{ name: "", type: "uint256" }],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [],
    name: "decimals",
    outputs: [{ name: "", type: "uint8" }],
    stateMutability: "view",
    type: "function",
  },
  {
    inputs: [],
    name: "symbol",
    outputs: [{ name: "", type: "string" }],
    stateMutability: "view",
    type: "function",
  },
] as const;
