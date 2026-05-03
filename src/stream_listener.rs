//! 实时 stream listener：BSC WSS subscribe 4 dex Swap + V2 BNB 价池

use crate::abis::{PancakeV3Swap, PcsV4ClSwap, V2Swap, V3Swap, V4Swap};
use crate::clients::bsc::{build_ws_client, default_ws_url};
use crate::contracts::BscContracts;
use crate::db::get_pool;
use crate::db::queries::{load_v3_pool_cache, load_v4_pool_cache};
use crate::swap_processor::{
    process_pcs_v4_cl_swap, process_v2_bnb_swap, process_v3_swap, process_v4_swap, BnbPriceCache,
    PoolMeta,
};
use crate::types::{BnbPricePoint, ChainId, DexType, SwapRecord, CHAIN_BSC};
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
    let v3_raw = load_v3_pool_cache(CHAIN_BSC, DexType::UniswapV3).await?;
    let pcs_v3_raw = load_v3_pool_cache(CHAIN_BSC, DexType::PancakeswapV3).await?;
    let v4_raw = load_v4_pool_cache(CHAIN_BSC, DexType::UniswapV4).await?;
    let pcs_v4_cl_raw = load_v4_pool_cache(CHAIN_BSC, DexType::PancakeswapV4Cl).await?;

    let v3: HashMap<Address, PoolMeta> = v3_raw.into_iter().map(|(addr, (t0, t1, fee))| {
        (addr, PoolMeta { token0: t0, token1: t1, fee_tier: fee })
    }).collect();
    let pcs_v3: HashMap<Address, PoolMeta> = pcs_v3_raw.into_iter().map(|(addr, (t0, t1, fee))| {
        (addr, PoolMeta { token0: t0, token1: t1, fee_tier: fee })
    }).collect();
    let v4: HashMap<B256, PoolMeta> = v4_raw.into_iter().map(|(id, (c0, c1, fee))| {
        (id, PoolMeta { token0: c0, token1: c1, fee_tier: fee })
    }).collect();
    let pcs_v4_cl: HashMap<B256, PoolMeta> = pcs_v4_cl_raw.into_iter().map(|(id, (c0, c1, fee))| {
        (id, PoolMeta { token0: c0, token1: c1, fee_tier: fee })
    }).collect();

    info!(
        "[Stream] PoolCache loaded V3={} PCS V3={} V4={} PCS V4 CL={}",
        v3.len(), pcs_v3.len(), v4.len(), pcs_v4_cl.len()
    );
    Ok(PoolCache { v3, pcs_v3, v4, pcs_v4_cl })
}

async fn insert_swap(swap: &SwapRecord) -> Result<()> {
    let client = get_pool().get().await?;
    let pool_addr_slice = swap.pool_address.as_slice();
    let tx_hash_slice = swap.tx_hash.as_slice();
    let dex_smallint = swap.dex.as_db_smallint();
    let amount0_str = swap.amount0.to_string();
    let amount1_str = swap.amount1.to_string();
    client.execute(
        "INSERT INTO swaps (pool_address, chain, dex, tx_hash, amount0, amount1, fee_usd, volume_usd, timestamp, block_number)
         VALUES ($1, $2, $3, $4, $5::numeric, $6::numeric, $7, $8, $9, $10)
         ON CONFLICT (tx_hash, pool_address, amount0, amount1, timestamp) DO NOTHING",
        &[
            &pool_addr_slice, &swap.chain, &dex_smallint, &tx_hash_slice,
            &amount0_str, &amount1_str, &swap.fee_usd, &swap.volume_usd,
            &swap.timestamp, &swap.block_number,
        ],
    ).await?;

    // 实时累加 token_1min_stats（让 detector 实时看到 baseline 更新）
    let bucket_start = (swap.timestamp / 60000) * 60000;
    client.execute(
        "INSERT INTO token_1min_stats (token_address, chain, bucket_start, total_volume_usd, total_fees_usd, swap_count)
         SELECT bt.contract_address, $1, $2, $3, $4, 1
         FROM (
             SELECT token0, token1 FROM pools WHERE address = $5 AND chain = $1
             UNION ALL
             SELECT currency0 AS token0, currency1 AS token1 FROM v4_pools WHERE pool_id = $5 AND chain = $1
         ) p
         JOIN binance_bsc_tokens bt
           ON bt.contract_address IN (p.token0, p.token1)
         ON CONFLICT (token_address, chain, bucket_start) DO UPDATE SET
           total_volume_usd = token_1min_stats.total_volume_usd + EXCLUDED.total_volume_usd,
           total_fees_usd = token_1min_stats.total_fees_usd + EXCLUDED.total_fees_usd,
           swap_count = token_1min_stats.swap_count + EXCLUDED.swap_count",
        &[&swap.chain, &bucket_start, &swap.volume_usd, &swap.fee_usd, &pool_addr_slice],
    ).await?;
    Ok(())
}

async fn insert_bnb_price(p: &BnbPricePoint) -> Result<()> {
    let client = get_pool().get().await?;
    let tx_hash_slice = p.tx_hash.as_slice();
    client.execute(
        "INSERT INTO bnb_price_history (timestamp, price_usd, block_number, tx_hash, log_index)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (tx_hash, log_index) DO NOTHING",
        &[&p.timestamp, &p.price_usd, &p.block_number, &tx_hash_slice, &p.log_index],
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

    let bnb_pool_wbnb_is_token0 = false;

    loop {
        tokio::select! {
            Some(log) = v3_stream.next() => {
                let pool_addr = log.address();
                if let Some(meta) = pool_cache.v3.get(&pool_addr) {
                    let now_ms = Utc::now().timestamp_millis();
                    let bnb_now = bnb_cache.get();
                    if let Ok(rec) = process_v3_swap(&log, CHAIN_BSC, DexType::UniswapV3, meta, pool_addr, now_ms, bnb_now) {
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
                    let bnb_now = bnb_cache.get();
                    if let Ok(rec) = process_v3_swap(&log, CHAIN_BSC, DexType::PancakeswapV3, meta, pool_addr, now_ms, bnb_now) {
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
                    let bnb_now = bnb_cache.get();
                    if let Ok(rec) = process_v4_swap(&log, CHAIN_BSC, meta, id, now_ms, bnb_now) {
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
                    let bnb_now = bnb_cache.get();
                    if let Ok(rec) = process_pcs_v4_cl_swap(&log, CHAIN_BSC, meta, id, now_ms, bnb_now) {
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
