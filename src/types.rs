use alloy::primitives::{Address, B256, I256};
use serde::{Deserialize, Serialize};

pub type ChainId = i16; // SMALLINT

pub const CHAIN_BSC: i16 = 1;

/// dex 枚举（跟 db/init.sql 对应）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i16)]
pub enum DexType {
    UniswapV3 = 1,
    PancakeswapV3 = 2,
    UniswapV4 = 3,
    PancakeswapV4Cl = 4,
    PancakeswapV2BnbPrice = 5,
}

impl DexType {
    pub fn as_db_smallint(&self) -> i16 {
        *self as i16
    }

    pub fn from_db_smallint(v: i16) -> Option<Self> {
        match v {
            1 => Some(DexType::UniswapV3),
            2 => Some(DexType::PancakeswapV3),
            3 => Some(DexType::UniswapV4),
            4 => Some(DexType::PancakeswapV4Cl),
            5 => Some(DexType::PancakeswapV2BnbPrice),
            _ => None,
        }
    }

    pub fn is_v3_like(&self) -> bool {
        matches!(self, DexType::UniswapV3 | DexType::PancakeswapV3)
    }

    pub fn is_v4_like(&self) -> bool {
        matches!(self, DexType::UniswapV4 | DexType::PancakeswapV4Cl)
    }
}

/// V3 / PCS V3 池子记录（pools 表）
#[derive(Debug, Clone)]
pub struct PoolInfo {
    pub address: Address,    // 20 bytes
    pub chain: ChainId,
    pub dex: DexType,
    pub token0: Address,
    pub token1: Address,
    pub fee_tier: i32,
}

/// V4 / PCS V4 CL 池子记录（v4_pools 表）
/// pool_id 是 PoolKey 的 keccak256 hash（32 bytes）。dex 列区分 V4 / PCS V4 CL。
#[derive(Debug, Clone)]
pub struct V4PoolInfo {
    pub pool_id: B256,       // 32 bytes
    pub chain: ChainId,
    pub dex: DexType,        // UniswapV4 or PancakeswapV4Cl
    pub currency0: Address,
    pub currency1: Address,
    pub fee: i32,
    pub tick_spacing: i32,
    pub hooks: Address,
}

/// 单笔 swap 记录（swaps 表 / swaps_staging 表）
/// pool_address：V3 是 20 bytes，V4 是 32 bytes hash（变长 BYTEA 列存）
#[derive(Debug, Clone)]
pub struct SwapRecord {
    pub pool_address: Vec<u8>,   // 20 bytes (V3) or 32 bytes (V4)
    pub chain: ChainId,
    pub dex: DexType,
    pub tx_hash: B256,           // 32 bytes
    pub amount0: I256,           // signed decimal
    pub amount1: I256,
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
    pub tx_hash: B256,
    pub log_index: i32,
}

/// 白名单 token（binance_bsc_tokens 表）
#[derive(Debug, Clone)]
pub struct BinanceBscToken {
    pub contract_address: Address,
    pub symbol: String,
    pub base_asset: String,
    pub decimals: i32,
    pub updated_at: i64,
}
