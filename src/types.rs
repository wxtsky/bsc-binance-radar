use serde::{Deserialize, Serialize};

pub type ChainId = String; // "bsc"（暂时只 BSC）

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DexType {
    UniswapV3,
    UniswapV4,
    PancakeswapV3,
    PancakeswapV4Cl,
    PancakeswapV2BnbPrice,
}

impl DexType {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            DexType::UniswapV3 => "uniswap-v3",
            DexType::UniswapV4 => "uniswap-v4",
            DexType::PancakeswapV3 => "pancakeswap-v3",
            DexType::PancakeswapV4Cl => "pancakeswap-v4-cl",
            DexType::PancakeswapV2BnbPrice => "pancakeswap-v2",
        }
    }
}

/// V3 / PCS V3 池子记录（pools 表）
#[derive(Debug, Clone)]
pub struct PoolInfo {
    pub address: String,    // lowercase 0x...
    pub chain: String,
    pub dex: DexType,
    pub token0: String,     // lowercase
    pub token1: String,     // lowercase
    pub fee_tier: i32,
}

/// V4 / PCS V4 CL 池子记录（v4_pools 表）
/// pool_id 是 PoolKey 的 keccak256 hash（bytes32）。
/// PCS V4 CL 在表里 pool_id 加 `pcsv4cl:` 前缀做 namespace。
#[derive(Debug, Clone)]
pub struct V4PoolInfo {
    pub pool_id: String,    // 带 namespace 前缀（pcsv4cl: for PCS）
    pub chain: String,
    pub currency0: String,  // lowercase
    pub currency1: String,
    pub fee: i32,
    pub tick_spacing: i32,
    pub hooks: String,      // lowercase
}

/// 单笔 swap 记录（swaps 表 / swaps_staging 表）
#[derive(Debug, Clone)]
pub struct SwapRecord {
    pub pool_address: String,    // V3: lowercase pool addr; V4: "pcsv4cl:hash" or "hash"
    pub chain: String,
    pub dex: DexType,
    pub tx_hash: String,
    pub amount0: String,         // signed decimal as string（避免精度丢失）
    pub amount1: String,
    pub fee_usd: f64,
    pub volume_usd: f64,
    pub timestamp: i64,          // ms
    pub block_number: i64,
}

/// BNB/USDT V2 池的价格记录（bnb_price_history 表）
#[derive(Debug, Clone)]
pub struct BnbPricePoint {
    pub timestamp: i64,
    pub price_usd: f64,
    pub block_number: i64,
    pub tx_hash: String,
    pub log_index: i32,
}

/// 白名单 token（binance_bsc_tokens 表）
#[derive(Debug, Clone)]
pub struct BinanceBscToken {
    pub contract_address: String,  // lowercase
    pub symbol: String,
    pub base_asset: String,
    pub decimals: i32,
    pub updated_at: i64,
}
