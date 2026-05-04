//! Swap log → SwapRecord / BnbPricePoint 解码 + USD volume/fee 计算

use crate::abis::{PancakeV3Swap, PcsV4ClSwap, V2Swap, V3Swap, V4Swap};
use crate::chain::{NATIVE_BNB, USDC, USDT, WBNB};
use crate::types::{BnbPricePoint, ChainId, DexType, SwapRecord};
use alloy::primitives::{Address, B256, I256};
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use anyhow::{anyhow, Result};
use parking_lot::RwLock;

/// 当前 BNB/USD 价（近似，由 BNB price pool 实时更新）
pub struct BnbPriceCache {
    pub price: RwLock<f64>,
}

impl BnbPriceCache {
    pub fn new(initial: f64) -> Self {
        Self { price: RwLock::new(initial) }
    }
    pub fn get(&self) -> f64 { *self.price.read() }
    pub fn set(&self, price: f64) { *self.price.write() = price; }
}

#[derive(Debug, Clone, Copy)]
pub struct PoolMeta {
    pub token0: Address,
    pub token1: Address,
    pub fee_tier: u32,
}

fn signed_amount_i256_to_f64(v: I256) -> f64 {
    let abs = if v.is_negative() {
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

fn calc_usd_amount(token0: Address, token1: Address, abs_amount0: f64, abs_amount1: f64, bnb_price: f64) -> f64 {
    if token0 == USDT || token0 == USDC {
        return abs_amount0;
    }
    if token1 == USDT || token1 == USDC {
        return abs_amount1;
    }
    // WBNB / V4 native BNB（PoolKey.currency0 = 0x000…000）等价
    if token0 == WBNB || token0 == NATIVE_BNB {
        return abs_amount0 * bnb_price;
    }
    if token1 == WBNB || token1 == NATIVE_BNB {
        return abs_amount1 * bnb_price;
    }
    0.0
}

pub fn process_v3_swap(
    log: &Log,
    chain: ChainId,
    dex: DexType,
    pool: &PoolMeta,
    pool_addr: Address,
    timestamp_ms: i64,
    bnb_price: f64,
) -> Result<SwapRecord> {
    let tx_hash = log.transaction_hash.ok_or_else(|| anyhow!("missing tx_hash"))?;
    let block_number = log.block_number.ok_or_else(|| anyhow!("missing block_number"))?;

    let (amount0, amount1) = if dex == DexType::PancakeswapV3 {
        let parsed = PancakeV3Swap::decode_log(&log.inner, true)?;
        (parsed.amount0, parsed.amount1)
    } else {
        let parsed = V3Swap::decode_log(&log.inner, true)?;
        (parsed.amount0, parsed.amount1)
    };

    let abs0 = signed_amount_i256_to_f64(amount0);
    let abs1 = signed_amount_i256_to_f64(amount1);
    let volume_usd = calc_usd_amount(pool.token0, pool.token1, abs0, abs1, bnb_price);
    let fee_usd = volume_usd * (pool.fee_tier as f64) / 1_000_000.0;

    Ok(SwapRecord {
        pool_address: pool_addr.as_slice().to_vec(),
        chain,
        dex,
        tx_hash,
        amount0,
        amount1,
        fee_usd,
        volume_usd,
        timestamp: timestamp_ms,
        block_number: block_number as i64,
    })
}

pub fn process_v4_swap(
    log: &Log,
    chain: ChainId,
    pool: &PoolMeta,
    pool_id: B256,
    timestamp_ms: i64,
    bnb_price: f64,
) -> Result<SwapRecord> {
    let tx_hash = log.transaction_hash.ok_or_else(|| anyhow!("missing tx_hash"))?;
    let block_number = log.block_number.ok_or_else(|| anyhow!("missing block_number"))?;

    let parsed = V4Swap::decode_log(&log.inner, true)?;
    let amount0 = parsed.amount0;
    let amount1 = parsed.amount1;
    let fee_event: u32 = parsed.fee.try_into().unwrap_or(pool.fee_tier);

    let abs0 = signed_amount_i128_to_f64(amount0 as i128);
    let abs1 = signed_amount_i128_to_f64(amount1 as i128);
    let volume_usd = calc_usd_amount(pool.token0, pool.token1, abs0, abs1, bnb_price);
    let fee_usd = volume_usd * (fee_event as f64) / 1_000_000.0;

    Ok(SwapRecord {
        pool_address: pool_id.as_slice().to_vec(),
        chain,
        dex: DexType::UniswapV4,
        tx_hash,
        amount0: I256::try_from(amount0 as i128).unwrap_or(I256::ZERO),
        amount1: I256::try_from(amount1 as i128).unwrap_or(I256::ZERO),
        fee_usd,
        volume_usd,
        timestamp: timestamp_ms,
        block_number: block_number as i64,
    })
}

pub fn process_pcs_v4_cl_swap(
    log: &Log,
    chain: ChainId,
    pool: &PoolMeta,
    pool_id: B256,
    timestamp_ms: i64,
    bnb_price: f64,
) -> Result<SwapRecord> {
    let tx_hash = log.transaction_hash.ok_or_else(|| anyhow!("missing tx_hash"))?;
    let block_number = log.block_number.ok_or_else(|| anyhow!("missing block_number"))?;

    let parsed = PcsV4ClSwap::decode_log(&log.inner, true)?;
    let amount0 = parsed.amount0;
    let amount1 = parsed.amount1;
    let fee_event: u32 = parsed.fee.try_into().unwrap_or(pool.fee_tier);

    let abs0 = signed_amount_i128_to_f64(amount0 as i128);
    let abs1 = signed_amount_i128_to_f64(amount1 as i128);
    let volume_usd = calc_usd_amount(pool.token0, pool.token1, abs0, abs1, bnb_price);
    let fee_usd = volume_usd * (fee_event as f64) / 1_000_000.0;

    Ok(SwapRecord {
        pool_address: pool_id.as_slice().to_vec(),
        chain,
        dex: DexType::PancakeswapV4Cl,
        tx_hash,
        amount0: I256::try_from(amount0 as i128).unwrap_or(I256::ZERO),
        amount1: I256::try_from(amount1 as i128).unwrap_or(I256::ZERO),
        fee_usd,
        volume_usd,
        timestamp: timestamp_ms,
        block_number: block_number as i64,
    })
}

/// V3 BNB price pool 反算 BNB/USD 价（PCS V3 WBNB/USDT 0.01% 池）
/// 直接用 Swap event 自带的 sqrtPriceX96 算 spot price，无成交滑点干扰
/// PCS V3 0x172fcD41E0913e95784454622d1c3724f546f849:
///   token0 = USDT (18 decimals), token1 = WBNB (18 decimals)
///   price_raw = (sqrtPriceX96 / 2^96)^2 = WBNB/USDT
///   BNB/USD = USDT/WBNB = 1 / price_raw
pub fn process_v3_bnb_swap(
    log: &Log,
    timestamp_ms: i64,
    wbnb_is_token0: bool,
) -> Result<(BnbPricePoint, f64)> {
    let tx_hash = log.transaction_hash.ok_or_else(|| anyhow!("missing tx_hash"))?;
    let block_number = log.block_number.ok_or_else(|| anyhow!("missing block_number"))?;
    let log_index: i32 = log.log_index.map(|i| i as i32).unwrap_or(0);

    let parsed = PancakeV3Swap::decode_log(&log.inner, true)?;
    let sqrt_str = parsed.sqrtPriceX96.to_string();
    let sqrt_f: f64 = sqrt_str.parse().unwrap_or(0.0);
    if sqrt_f <= 0.0 {
        return Err(anyhow!("v3 BNB swap with zero sqrtPriceX96"));
    }
    // (sqrtPriceX96 / 2^96)^2 = token1/token0 ratio (raw decimal units)
    // USDT 18 / WBNB 18 → no decimal adjust
    let q96 = 2f64.powi(96);
    let ratio = (sqrt_f / q96).powi(2);  // token1/token0
    let price = if wbnb_is_token0 {
        // token0=WBNB, token1=USDT → ratio = USDT/WBNB = BNB/USD
        ratio
    } else {
        // token0=USDT, token1=WBNB → ratio = WBNB/USDT, BNB/USD = 1/ratio
        if ratio > 0.0 { 1.0 / ratio } else { 0.0 }
    };

    Ok((
        BnbPricePoint {
            timestamp: timestamp_ms,
            price_usd: price,
            block_number: block_number as i64,
            tx_hash,
            log_index,
        },
        price,
    ))
}

/// 通用 V2 池 swap 处理（PCS V2 主池监控，输出 SwapRecord）
/// V2 Swap event: (sender, amount0In, amount1In, amount0Out, amount1Out, to)
/// amount0/1 都是 unsigned，每边只一个非零（in 或 out）
/// 转换为 V3 角度（池子角度）：amount = In - Out（正=池子收到=用户支付，负=池子付出=用户收到）
pub fn process_v2_swap(
    log: &Log,
    chain: ChainId,
    pool: &PoolMeta,
    pool_addr: Address,
    timestamp_ms: i64,
    bnb_price: f64,
) -> Result<SwapRecord> {
    let tx_hash = log.transaction_hash.ok_or_else(|| anyhow!("missing tx_hash"))?;
    let block_number = log.block_number.ok_or_else(|| anyhow!("missing block_number"))?;

    let parsed = V2Swap::decode_log(&log.inner, true)?;
    // V3 风格符号：In 进池子=正, Out 出池子=负 → amount = In - Out
    let amount0_signed = i256_from_u256_diff(parsed.amount0In, parsed.amount0Out);
    let amount1_signed = i256_from_u256_diff(parsed.amount1In, parsed.amount1Out);

    let abs0 = signed_amount_i256_to_f64(amount0_signed);
    let abs1 = signed_amount_i256_to_f64(amount1_signed);
    let volume_usd = calc_usd_amount(pool.token0, pool.token1, abs0, abs1, bnb_price);
    // 跟 V3 一致用 hundredths-of-bps（PCS V2 标准 0.25% = 2500）
    let fee_usd = volume_usd * (pool.fee_tier as f64) / 1_000_000.0;

    Ok(SwapRecord {
        pool_address: pool_addr.as_slice().to_vec(),
        chain,
        dex: DexType::PancakeswapV2,
        tx_hash,
        amount0: amount0_signed,
        amount1: amount1_signed,
        fee_usd,
        volume_usd,
        timestamp: timestamp_ms,
        block_number: block_number as i64,
    })
}

fn i256_from_u256_diff(a: alloy::primitives::U256, b: alloy::primitives::U256) -> I256 {
    if a >= b {
        I256::from_raw(a - b)
    } else {
        I256::ZERO.wrapping_sub(I256::from_raw(b - a))
    }
}

pub fn interpolate_log_ts(log: &Log, from_block: u64, to_block: u64, from_ts_ms: i64, to_ts_ms: i64) -> i64 {
    let blk = log.block_number.unwrap_or(from_block);
    let range = (to_block - from_block).max(1) as i64;
    let offset = (blk as i64 - from_block as i64).max(0);
    from_ts_ms + (offset * (to_ts_ms - from_ts_ms)) / range
}
