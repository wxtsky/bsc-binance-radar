use alloy::primitives::{address, Address};

/// BSC Factory / PoolManager 地址（已 verify，scripts/verify-factories）
pub struct BscContracts;

impl BscContracts {
    pub const UNISWAP_V3_FACTORY: Address = address!("dB1d10011AD0Ff90774D0C6Bb92e5C5c8b4461F7");
    pub const UNISWAP_V4_POOL_MANAGER: Address = address!("28e2ea090877bf75740558f6bfb36a5ffee9e9df");
    pub const PANCAKESWAP_V3_FACTORY: Address = address!("0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865");
    pub const PANCAKESWAP_V4_CL_POOL_MANAGER: Address =
        address!("a0FfB9c1CE1Fe56963B0321B32E7A0302114058b");

    /// PancakeSwap V2 WBNB/USDT 池（仅作 BNB/USD 历史价数据源）
    pub const BNB_PRICE_POOL: Address = address!("16b9a82891338f9bA80E2D6970FddA79D1eb0daE");
}

/// V3 / PCS V3 部署在 BSC ~26.9M (2023-03)；V4 deploy ~46.5M (2025-01)。保守起点。
pub const V3_BSC_DEPLOY_BLOCK: u64 = 26_000_000;
pub const V4_BSC_DEPLOY_BLOCK: u64 = 45_000_000;
