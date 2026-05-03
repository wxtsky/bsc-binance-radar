//! 实时 stream listener：BSC WSS subscribe 4 dex Swap + V2 BNB 价池
//!
//! 收到 swap log 直接走单条 INSERT（吞吐低，不需 staging）。
//! 同时实时累加 token_1min_stats（INSERT ON CONFLICT UPDATE）让 detector 看到最新数据。
//!
//! Liveness probe：60s 无 swap 触发警告（remount 由 docker restart 兜底）。

use crate::abis::{PancakeV3Swap, PcsV4ClSwap, V2Swap, V3Swap, V4Swap};
use crate::clients::bsc::{build_ws_client, default_ws_url};
use crate::contracts::BscContracts;
use crate::db::get_pool;
use crate::swap_processor::{
    interpolate_log_ts, process_pcs_v4_cl_swap, process_v2_bnb_swap, process_v3_swap,
    process_v4_swap, BnbPriceCache, PoolMeta,
};
use crate::types::{BnbPricePoint, DexType, SwapRecord};
use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use anyhow::Result;
use chrono::Utc;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

pub struct PoolCache {
    pub v3: HashMap<Address, PoolMeta>,
    pub pcs_v3: HashMap<Address, PoolMeta>,
    pub v4: HashMap<B256, PoolMeta>,
    pub pcs_v4_cl: HashMap<B256, PoolMeta>,
}

pub async fn load_pool_cache_from_db() -> Result<PoolCache> {
    let pool = get_pool().get().await?;
    let v3_rows = pool.query(
        "SELECT address, token0, token1, fee_tier FROM pools WHERE chain='bsc' AND dex='uniswap-v3'",
        &[],
    ).await?;
    let mut v3 = HashMap::with_capacity(v3_rows.len());
    for r in &v3_rows {
        if let Ok(addr) = r.get::<_, String>(0).parse::<Address>() {
            v3.insert(addr, PoolMeta {
                token0: r.get::<_, String>(1).to_lowercase(),
                token1: r.get::<_, String>(2).to_lowercase(),
                fee_tier: r.get::<_, i32>(3) as u32,
            });
        }
    }

    let pcs_rows = pool.query(
        "SELECT address, token0, token1, fee_tier FROM pools WHERE chain='bsc' AND dex='pancakeswap-v3'",
        &[],
    ).await?;
    let mut pcs_v3 = HashMap::with_capacity(pcs_rows.len());
    for r in &pcs_rows {
        if let Ok(addr) = r.get::<_, String>(0).parse::<Address>() {
            pcs_v3.insert(addr, PoolMeta {
                token0: r.get::<_, String>(1).to_lowercase(),
                token1: r.get::<_, String>(2).to_lowercase(),
                fee_tier: r.get::<_, i32>(3) as u32,
            });
        }
    }

    let v4_rows = pool.query(
        "SELECT pool_id, currency0, currency1, fee FROM v4_pools WHERE chain='bsc' AND pool_id NOT LIKE 'pcsv4cl:%'",
        &[],
    ).await?;
    let mut v4 = HashMap::with_capacity(v4_rows.len());
    for r in &v4_rows {
        if let Ok(id) = r.get::<_, String>(0).parse::<B256>() {
            v4.insert(id, PoolMeta {
                token0: r.get::<_, String>(1).to_lowercase(),
                token1: r.get::<_, String>(2).to_lowercase(),
                fee_tier: r.get::<_, i32>(3) as u32,
            });
        }
    }

    let pcs_v4_rows = pool.query(
        "SELECT pool_id, currency0, currency1, fee FROM v4_pools WHERE chain='bsc' AND pool_id LIKE 'pcsv4cl:%'",
        &[],
    ).await?;
    let mut pcs_v4_cl = HashMap::with_capacity(pcs_v4_rows.len());
    for r in &pcs_v4_rows {
        let id_str: String = r.get(0);
        if let Ok(id) = id_str.trim_start_matches("pcsv4cl:").parse::<B256>() {
            pcs_v4_cl.insert(id, PoolMeta {
                token0: r.get::<_, String>(1).to_lowercase(),
                token1: r.get::<_, String>(2).to_lowercase(),
                fee_tier: r.get::<_, i32>(3) as u32,
            });
        }
    }

    info!(
        "[Stream] PoolCache loaded V3={} PCS V3={} V4={} PCS V4 CL={}",
        v3.len(), pcs_v3.len(), v4.len(), pcs_v4_cl.len()
    );
    Ok(PoolCache { v3, pcs_v3, v4, pcs_v4_cl })
}

async fn insert_swap(swap: &SwapRecord) -> Result<()> {
    let client = get_pool().get().await?;
    let dex_str = swap.dex.as_db_str();
    client.execute(
        "INSERT INTO swaps (pool_address, chain, dex, tx_hash, amount0, amount1, fee_usd, volume_usd, timestamp, block_number)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
         ON CONFLICT (tx_hash, pool_address, amount0, amount1, timestamp) DO NOTHING",
        &[
            &swap.pool_address, &swap.chain, &dex_str, &swap.tx_hash,
            &swap.amount0, &swap.amount1, &swap.fee_usd, &swap.volume_usd,
            &swap.timestamp, &swap.block_number,
        ],
    ).await?;

    // 实时累加 token_1min_stats（detector 实时看到 baseline 更新）
    // 找 pool 对应的 target token（白名单），累加到该 token 的 bucket
    let bucket_start = (swap.timestamp / 60000) * 60000;
    // 通过 pool_address 查 pool 的 token，再 join binance_bsc_tokens 拿 target
    client.execute(
        "INSERT INTO token_1min_stats (token_address, chain, bucket_start, total_volume_usd, total_fees_usd, swap_count)
         SELECT LOWER(bt.contract_address), $1, $2, $3, $4, 1
         FROM (
             SELECT token0, token1 FROM pools WHERE address = $5 AND chain = $1
             UNION ALL
             SELECT currency0 AS token0, currency1 AS token1 FROM v4_pools WHERE pool_id = $5 AND chain = $1
         ) p
         JOIN binance_bsc_tokens bt
           ON LOWER(bt.contract_address) IN (LOWER(p.token0), LOWER(p.token1))
         ON CONFLICT (token_address, chain, bucket_start) DO UPDATE SET
           total_volume_usd = token_1min_stats.total_volume_usd + EXCLUDED.total_volume_usd,
           total_fees_usd = token_1min_stats.total_fees_usd + EXCLUDED.total_fees_usd,
           swap_count = token_1min_stats.swap_count + EXCLUDED.swap_count",
        &[&swap.chain, &bucket_start, &swap.volume_usd, &swap.fee_usd, &swap.pool_address],
    ).await?;
    Ok(())
}

async fn insert_bnb_price(p: &BnbPricePoint) -> Result<()> {
    let client = get_pool().get().await?;
    client.execute(
        "INSERT INTO bnb_price_history (timestamp, price_usd, block_number, tx_hash, log_index)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (tx_hash, log_index) DO NOTHING",
        &[&p.timestamp, &p.price_usd, &p.block_number, &p.tx_hash, &p.log_index],
    ).await?;
    Ok(())
}

pub async fn run_stream_listener(
    pool_cache: Arc<PoolCache>,
    bnb_cache: Arc<BnbPriceCache>,
    last_swap_ms: Arc<RwLock<i64>>,
) -> Result<()> {
    let ws_url = default_ws_url()?;
    info!("[Stream] connecting WSS {}", ws_url);
    let provider = build_ws_client(&ws_url).await?;

    // V3 / PCS V3 用 pool_cache 地址做 filter
    let v3_addrs: Vec<Address> = pool_cache.v3.keys().copied().collect();
    let pcs_v3_addrs: Vec<Address> = pool_cache.pcs_v3.keys().copied().collect();

    let v3_filter = Filter::new()
        .address(v3_addrs.clone())
        .event_signature(V3Swap::SIGNATURE_HASH);
    let pcs_v3_filter = Filter::new()
        .address(pcs_v3_addrs.clone())
        .event_signature(PancakeV3Swap::SIGNATURE_HASH);
    let v4_filter = Filter::new()
        .address(BscContracts::UNISWAP_V4_POOL_MANAGER)
        .event_signature(V4Swap::SIGNATURE_HASH);
    let pcs_v4_cl_filter = Filter::new()
        .address(BscContracts::PANCAKESWAP_V4_CL_POOL_MANAGER)
        .event_signature(PcsV4ClSwap::SIGNATURE_HASH);
    let bnb_filter = Filter::new()
        .address(BscContracts::BNB_PRICE_POOL)
        .event_signature(V2Swap::SIGNATURE_HASH);

    let v3_sub = provider.subscribe_logs(&v3_filter).await?;
    let pcs_v3_sub = provider.subscribe_logs(&pcs_v3_filter).await?;
    let v4_sub = provider.subscribe_logs(&v4_filter).await?;
    let pcs_v4_sub = provider.subscribe_logs(&pcs_v4_cl_filter).await?;
    let bnb_sub = provider.subscribe_logs(&bnb_filter).await?;

    info!("[Stream] subscribed 5 logs (V3 / PCS V3 / V4 / PCS V4 CL / BNB price)");

    let mut v3_stream = v3_sub.into_stream();
    let mut pcs_v3_stream = pcs_v3_sub.into_stream();
    let mut v4_stream = v4_sub.into_stream();
    let mut pcs_v4_stream = pcs_v4_sub.into_stream();
    let mut bnb_stream = bnb_sub.into_stream();

    let bnb_pool_wbnb_is_token0 = false; // PCS V2 WBNB/USDT: token0=USDT, token1=WBNB

    loop {
        tokio::select! {
            Some(log) = v3_stream.next() => {
                let pool_addr = log.address();
                if let Some(meta) = pool_cache.v3.get(&pool_addr) {
                    let now_ms = Utc::now().timestamp_millis();
                    let pool_addr_str = format!("{:?}", pool_addr).to_lowercase();
                    let bnb_now = bnb_cache.get();
                    if let Ok(rec) = process_v3_swap(&log, "bsc", DexType::UniswapV3, meta, &pool_addr_str, now_ms, bnb_now) {
                        if let Err(e) = insert_swap(&rec).await {
                            warn!("[Stream] V3 insert fail: {}", e);
                        }
                        *last_swap_ms.write().await = now_ms;
                    }
                }
            }
            Some(log) = pcs_v3_stream.next() => {
                let pool_addr = log.address();
                if let Some(meta) = pool_cache.pcs_v3.get(&pool_addr) {
                    let now_ms = Utc::now().timestamp_millis();
                    let pool_addr_str = format!("{:?}", pool_addr).to_lowercase();
                    let bnb_now = bnb_cache.get();
                    if let Ok(rec) = process_v3_swap(&log, "bsc", DexType::PancakeswapV3, meta, &pool_addr_str, now_ms, bnb_now) {
                        if let Err(e) = insert_swap(&rec).await {
                            warn!("[Stream] PCS V3 insert fail: {}", e);
                        }
                        *last_swap_ms.write().await = now_ms;
                    }
                }
            }
            Some(log) = v4_stream.next() => {
                let id = match log.topics().get(1) {
                    Some(t) => *t,
                    None => continue,
                };
                if let Some(meta) = pool_cache.v4.get(&id) {
                    let now_ms = Utc::now().timestamp_millis();
                    let pool_id_str = format!("{:?}", id);
                    let bnb_now = bnb_cache.get();
                    if let Ok(rec) = process_v4_swap(&log, "bsc", meta, &pool_id_str, now_ms, bnb_now) {
                        if let Err(e) = insert_swap(&rec).await {
                            warn!("[Stream] V4 insert fail: {}", e);
                        }
                        *last_swap_ms.write().await = now_ms;
                    }
                }
            }
            Some(log) = pcs_v4_stream.next() => {
                let id = match log.topics().get(1) {
                    Some(t) => *t,
                    None => continue,
                };
                if let Some(meta) = pool_cache.pcs_v4_cl.get(&id) {
                    let now_ms = Utc::now().timestamp_millis();
                    let pool_id_str = format!("pcsv4cl:{:?}", id);
                    let bnb_now = bnb_cache.get();
                    if let Ok(rec) = process_pcs_v4_cl_swap(&log, "bsc", meta, &pool_id_str, now_ms, bnb_now) {
                        if let Err(e) = insert_swap(&rec).await {
                            warn!("[Stream] PCS V4 CL insert fail: {}", e);
                        }
                        *last_swap_ms.write().await = now_ms;
                    }
                }
            }
            Some(log) = bnb_stream.next() => {
                let now_ms = Utc::now().timestamp_millis();
                if let Ok((point, price)) = process_v2_bnb_swap(&log, now_ms, bnb_pool_wbnb_is_token0) {
                    bnb_cache.set(price);
                    if let Err(e) = insert_bnb_price(&point).await {
                        warn!("[Stream] BNB insert fail: {}", e);
                    }
                }
            }
            else => break,
        }
    }
    warn!("[Stream] all subscriptions ended");
    Ok(())
}

pub async fn liveness_probe(last_swap_ms: Arc<RwLock<i64>>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
    loop {
        interval.tick().await;
        let now_ms = Utc::now().timestamp_millis();
        let last = *last_swap_ms.read().await;
        let silence_s = (now_ms - last) / 1000;
        if last > 0 && silence_s >= 60 {
            warn!("[Liveness] {} 秒无 swap，可能 WSS 已断（容器 restart 兜底）", silence_s);
        }
    }
}

fn _ignore() {
    // 抑制未使用 import 警告
    let _ = interpolate_log_ts;
}
