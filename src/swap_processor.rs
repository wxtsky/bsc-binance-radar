//! Swap log → SwapRecord / BnbPricePoint 解码 + USD volume/fee 计算
//!
//! 业务逻辑：
//! - V3 / PCS V3: indexed sender, recipient + amount0, amount1 (int256), fee 来自 pool 元信息
//! - V4 / PCS V4 CL: indexed id (PoolKey hash), amount0/1 (int128), fee 在 event 内
//! - V2 BNB price pool: amount0In/Out, amount1In/Out (uint256) - WBNB/USDT 池反算 BNB 价
//!
//! USD 计算：
//! - 如果池子 base 是 USDT/USDC: volume_usd = abs(USD-amount) / 10^18
//! - 如果池子 base 是 WBNB: volume_usd = abs(WBNB-amount) / 10^18 × bnb_price
//! - fee_usd = volume_usd × fee_tier / 10^6  (V3 fee_tier = 100/500/3000/10000)
//! - V4 fee 在 event 内（每池可变），按 event 拿

use crate::abis::{PancakeV3Swap, PcsV4ClSwap, V2Swap, V3Swap, V4Swap};
use crate::chain::{USDC_ADDRESS, USDT_ADDRESS, WBNB_ADDRESS};
use crate::types::{BnbPricePoint, DexType, SwapRecord};
use alloy::primitives::{I256, U256};
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use anyhow::{anyhow, Result};
use parking_lot::RwLock;
use std::sync::Arc;

/// 当前 BNB/USD 价（近似，由 BNB price pool 实时更新）
pub struct BnbPriceCache {
    pub price: RwLock<f64>,
}

impl BnbPriceCache {
    pub fn new(initial: f64) -> Self {
        Self {
            price: RwLock::new(initial),
        }
    }

    pub fn get(&self) -> f64 {
        *self.price.read()
    }

    pub fn set(&self, price: f64) {
        *self.price.write() = price;
    }
}

#[derive(Debug, Clone)]
pub struct PoolMeta {
    pub token0: String,
    pub token1: String,
    pub fee_tier: u32, // basis points * 100；V3 uniswap fee 标度
}

fn token_norm(addr: alloy::primitives::Address) -> String {
    format!("{:?}", addr).to_lowercase()
}

fn signed_amount_i256_to_f64(v: I256) -> f64 {
    // V3 amount0/amount1 是 int256 signed，正负代表方向
    // 取绝对值除 10^18
    let abs = if v.is_negative() {
        // I256::wrapping_neg
        I256::ZERO.wrapping_sub(v)
    } else {
        v
    };
    let s = abs.to_string();
    let parsed: f64 = s.parse().unwrap_or(0.0);
    parsed / 1e18
}

fn signed_amount_i128_to_f64(v: i128) -> f64 {
    (v.unsigned_abs() as f64) / 1e18
}

fn u256_to_f64(v: U256) -> f64 {
    let s = v.to_string();
    s.parse::<f64>().unwrap_or(0.0) / 1e18
}

/// 已知池子元信息时计算 USD 值。
fn calc_usd_amount(
    token0: &str,
    token1: &str,
    abs_amount0: f64,
    abs_amount1: f64,
    bnb_price: f64,
) -> f64 {
    // 优先稳定币（USDT / USDC）侧
    if token0 == USDT_ADDRESS || token0 == USDC_ADDRESS {
        return abs_amount0;
    }
    if token1 == USDT_ADDRESS || token1 == USDC_ADDRESS {
        return abs_amount1;
    }
    if token0 == WBNB_ADDRESS {
        return abs_amount0 * bnb_price;
    }
    if token1 == WBNB_ADDRESS {
        return abs_amount1 * bnb_price;
    }
    0.0
}

pub fn process_v3_swap(
    log: &Log,
    chain: &str,
    dex: DexType,
    pool: &PoolMeta,
    pool_addr: &str,
    timestamp_ms: i64,
    bnb_price: f64,
) -> Result<SwapRecord> {
    let tx_hash = log
        .transaction_hash
        .ok_or_else(|| anyhow!("missing tx_hash"))?;
    let block_number = log
        .block_number
        .ok_or_else(|| anyhow!("missing block_number"))?;

    // V3 / PCS V3 共用 amount0 / amount1 字段，topic 不同（PCS 多 protocolFees）
    let (amount0, amount1) = if dex == DexType::PancakeswapV3 {
        let parsed = PancakeV3Swap::decode_log(&log.inner, true)?;
        (parsed.amount0, parsed.amount1)
    } else {
        let parsed = V3Swap::decode_log(&log.inner, true)?;
        (parsed.amount0, parsed.amount1)
    };

    let abs0 = signed_amount_i256_to_f64(amount0);
    let abs1 = signed_amount_i256_to_f64(amount1);
    let volume_usd = calc_usd_amount(&pool.token0, &pool.token1, abs0, abs1, bnb_price);
    // V3 fee_tier 单位 1e-6（如 3000 = 0.3%）
    let fee_usd = volume_usd * (pool.fee_tier as f64) / 1_000_000.0;

    Ok(SwapRecord {
        pool_address: pool_addr.to_string(),
        chain: chain.to_string(),
        dex,
        tx_hash: format!("{:?}", tx_hash),
        amount0: amount0.to_string(),
        amount1: amount1.to_string(),
        fee_usd,
        volume_usd,
        timestamp: timestamp_ms,
        block_number: block_number as i64,
    })
}

pub fn process_v4_swap(
    log: &Log,
    chain: &str,
    pool: &PoolMeta,
    pool_id: &str,
    timestamp_ms: i64,
    bnb_price: f64,
) -> Result<SwapRecord> {
    let tx_hash = log
        .transaction_hash
        .ok_or_else(|| anyhow!("missing tx_hash"))?;
    let block_number = log
        .block_number
        .ok_or_else(|| anyhow!("missing block_number"))?;

    let parsed = V4Swap::decode_log(&log.inner, true)?;
    let amount0 = parsed.amount0;
    let amount1 = parsed.amount1;
    let fee_event = parsed.fee.try_into().unwrap_or(pool.fee_tier);

    let abs0 = signed_amount_i128_to_f64(amount0 as i128);
    let abs1 = signed_amount_i128_to_f64(amount1 as i128);
    let volume_usd = calc_usd_amount(&pool.token0, &pool.token1, abs0, abs1, bnb_price);
    let fee_usd = volume_usd * (fee_event as f64) / 1_000_000.0;

    Ok(SwapRecord {
        pool_address: pool_id.to_string(),
        chain: chain.to_string(),
        dex: DexType::UniswapV4,
        tx_hash: format!("{:?}", tx_hash),
        amount0: amount0.to_string(),
        amount1: amount1.to_string(),
        fee_usd,
        volume_usd,
        timestamp: timestamp_ms,
        block_number: block_number as i64,
    })
}

pub fn process_pcs_v4_cl_swap(
    log: &Log,
    chain: &str,
    pool: &PoolMeta,
    pool_id_with_prefix: &str,
    timestamp_ms: i64,
    bnb_price: f64,
) -> Result<SwapRecord> {
    let tx_hash = log
        .transaction_hash
        .ok_or_else(|| anyhow!("missing tx_hash"))?;
    let block_number = log
        .block_number
        .ok_or_else(|| anyhow!("missing block_number"))?;

    let parsed = PcsV4ClSwap::decode_log(&log.inner, true)?;
    let amount0 = parsed.amount0;
    let amount1 = parsed.amount1;
    let fee_event: u32 = parsed.fee.try_into().unwrap_or(pool.fee_tier);

    let abs0 = signed_amount_i128_to_f64(amount0 as i128);
    let abs1 = signed_amount_i128_to_f64(amount1 as i128);
    let volume_usd = calc_usd_amount(&pool.token0, &pool.token1, abs0, abs1, bnb_price);
    let fee_usd = volume_usd * (fee_event as f64) / 1_000_000.0;

    Ok(SwapRecord {
        pool_address: pool_id_with_prefix.to_string(),
        chain: chain.to_string(),
        dex: DexType::PancakeswapV4Cl,
        tx_hash: format!("{:?}", tx_hash),
        amount0: amount0.to_string(),
        amount1: amount1.to_string(),
        fee_usd,
        volume_usd,
        timestamp: timestamp_ms,
        block_number: block_number as i64,
    })
}

/// V2 BNB price pool（WBNB/USDT 单池）：从 swap reserves delta 反算 BNB/USD 价
/// 池子 token0=WBNB(0xbb4..), token1=USDT(0x55d..)（实测从 PoolCreated 顺序）
/// 有些 V2 池 WBNB 在 token1，需要按池子配置反推。
/// 简化：假设我们的 pool token0=USDT, token1=WBNB（DB 标记 priceDirection 字段更稳）
/// 这里用 amount0 / amount1 比例反推 BNB 价。
pub fn process_v2_bnb_swap(
    log: &Log,
    timestamp_ms: i64,
    wbnb_is_token0: bool,
) -> Result<(BnbPricePoint, f64)> {
    let tx_hash = log
        .transaction_hash
        .ok_or_else(|| anyhow!("missing tx_hash"))?;
    let block_number = log
        .block_number
        .ok_or_else(|| anyhow!("missing block_number"))?;
    let log_index: i32 = log
        .log_index
        .map(|i| i as i32)
        .unwrap_or(0);

    let parsed = V2Swap::decode_log(&log.inner, true)?;
    // 净 WBNB / USDT 流出量
    let (wbnb_amt, usdt_amt) = if wbnb_is_token0 {
        let wbnb = parsed.amount0In + parsed.amount0Out;
        let usdt = parsed.amount1In + parsed.amount1Out;
        (wbnb, usdt)
    } else {
        let wbnb = parsed.amount1In + parsed.amount1Out;
        let usdt = parsed.amount0In + parsed.amount0Out;
        (wbnb, usdt)
    };
    // 都是 1e18 decimals → 比例就是 USD/WBNB
    if wbnb_amt.is_zero() {
        return Err(anyhow!("v2 BNB swap with zero WBNB amount"));
    }
    // 用 BigUint 高精度比例：price = usdt / wbnb （都 18 decimals 同 scale）
    let usdt_str = usdt_amt.to_string();
    let wbnb_str = wbnb_amt.to_string();
    let usdt_f: f64 = usdt_str.parse().unwrap_or(0.0);
    let wbnb_f: f64 = wbnb_str.parse().unwrap_or(0.0);
    let price = if wbnb_f > 0.0 { usdt_f / wbnb_f } else { 0.0 };

    Ok((
        BnbPricePoint {
            timestamp: timestamp_ms,
            price_usd: price,
            block_number: block_number as i64,
            tx_hash: format!("{:?}", tx_hash),
            log_index,
        },
        price,
    ))
}

/// 把 block range 内的 fromTs/toTs 线性插值得到 log 的 timestamp（ms）
pub fn interpolate_log_ts(log: &Log, from_block: u64, to_block: u64, from_ts_ms: i64, to_ts_ms: i64) -> i64 {
    let blk = log.block_number.unwrap_or(from_block);
    let range = (to_block - from_block).max(1) as i64;
    let offset = (blk as i64 - from_block as i64).max(0);
    from_ts_ms + (offset * (to_ts_ms - from_ts_ms)) / range
}

pub type SharedBnbPriceCache = Arc<BnbPriceCache>;
