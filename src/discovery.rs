//! V3/V4 pool discovery：扫 Factory PoolCreated / PoolManager Initialize 事件，
//! 按 (target, base) 配对规则过滤，写入 pools / v4_pools 表。

use crate::abis::{PcsV4ClInitialize, PoolCreated, V4Initialize};
use crate::chain::{bsc_base_tokens, pool_includes_pair};
use crate::clients::BscHttpClient;
use crate::contracts::{BscContracts, V3_BSC_DEPLOY_BLOCK, V4_BSC_DEPLOY_BLOCK};
use crate::db::queries::{
    bulk_upsert_pools, bulk_upsert_v4_pools, select_pool_addresses_by_dex, select_v4_pool_ids,
};
use crate::types::{DexType, PoolInfo, V4PoolInfo, CHAIN_BSC};
use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use anyhow::Result;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{info, warn};

pub const DISCOVERY_STEP: u64 = 49_999;

pub fn discovery_concurrency() -> usize {
    std::env::var("BF_DISCOVERY_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8)
}

#[derive(Debug, Clone, Copy)]
pub struct BlockRange {
    pub from: u64,
    pub to: u64,
}

pub fn build_ranges(from_block: u64, to_block: u64, step: u64) -> Vec<BlockRange> {
    let mut ranges = Vec::new();
    let mut f = from_block;
    while f <= to_block {
        let t = (f + step).min(to_block);
        ranges.push(BlockRange { from: f, to: t });
        if t == to_block {
            break;
        }
        f = t + 1;
    }
    ranges
}

pub struct DiscoverV3Result {
    pub v3: Vec<Address>,
    pub pcs_v3: Vec<Address>,
}

pub async fn discover_whitelisted_pools(
    client: Arc<BscHttpClient>,
    to_block: u64,
    target_tokens: HashSet<Address>,
) -> Result<DiscoverV3Result> {
    let t0 = std::time::Instant::now();

    let dump_v3 = select_pool_addresses_by_dex(CHAIN_BSC, DexType::UniswapV3).await?.len();
    let dump_pcs = select_pool_addresses_by_dex(CHAIN_BSC, DexType::PancakeswapV3).await?.len();
    info!("[Discover V3] PG 已有: V3={}, PCS V3={}（dump 兜底）", dump_v3, dump_pcs);

    let from_block = V3_BSC_DEPLOY_BLOCK;
    let total_blocks = to_block.saturating_sub(from_block);
    let concurrency = discovery_concurrency();
    let ranges = build_ranges(from_block, to_block, DISCOVERY_STEP);
    info!(
        "[Discover V3] 扫 V3/PCS V3 PoolCreated ({} → {}, {} blocks, {} workers, {} ranges)...",
        from_block, to_block, total_blocks, concurrency, ranges.len()
    );

    let bases = Arc::new(bsc_base_tokens());
    let targets = Arc::new(target_tokens);
    let v3_factory = BscContracts::UNISWAP_V3_FACTORY;
    let pcs_v3_factory = BscContracts::PANCAKESWAP_V3_FACTORY;

    let semaphore = Arc::new(Semaphore::new(concurrency));
    let total_ranges = ranges.len();
    let done_counter = Arc::new(AtomicUsize::new(0));
    let found_counter = Arc::new(AtomicUsize::new(0));

    let mut futures: FuturesUnordered<_> = ranges
        .into_iter()
        .map(|r| {
            let client = Arc::clone(&client);
            let semaphore = Arc::clone(&semaphore);
            let bases = Arc::clone(&bases);
            let targets = Arc::clone(&targets);
            let done_counter = Arc::clone(&done_counter);
            let found_counter = Arc::clone(&found_counter);
            async move {
                let _permit = semaphore.acquire_owned().await.expect("semaphore");
                let filter_v3 = Filter::new()
                    .address(v3_factory)
                    .from_block(r.from)
                    .to_block(r.to)
                    .event_signature(PoolCreated::SIGNATURE_HASH);
                let filter_pcs = Filter::new()
                    .address(pcs_v3_factory)
                    .from_block(r.from)
                    .to_block(r.to)
                    .event_signature(PoolCreated::SIGNATURE_HASH);
                let f1 = client.get_logs(&filter_v3);
                let f2 = client.get_logs(&filter_pcs);
                let (v3_logs, pcs_logs) = tokio::join!(f1, f2);

                let mut out: Vec<PoolInfo> = Vec::new();
                let mut handle = |logs_res: Result<Vec<alloy::rpc::types::Log>, _>, dex: DexType| match logs_res {
                    Ok(logs) => {
                        for log in logs {
                            let parsed = match PoolCreated::decode_log(&log.inner, true) {
                                Ok(e) => e,
                                Err(_) => continue,
                            };
                            if pool_includes_pair(parsed.token0, parsed.token1, &targets, &bases) {
                                out.push(PoolInfo {
                                    address: parsed.pool,
                                    chain: CHAIN_BSC,
                                    dex,
                                    token0: parsed.token0,
                                    token1: parsed.token1,
                                    fee_tier: parsed.fee.try_into().unwrap_or(0),
                                });
                            }
                        }
                    }
                    Err(e) => warn!("[Discover V3] range {}-{} fail: {}", r.from, r.to, e),
                };
                handle(v3_logs.map_err(|e| e), DexType::UniswapV3);
                handle(pcs_logs.map_err(|e| e), DexType::PancakeswapV3);

                let done = done_counter.fetch_add(1, Ordering::Relaxed) + 1;
                let found_now = found_counter.fetch_add(out.len(), Ordering::Relaxed) + out.len();
                if done % 20 == 0 || done == total_ranges {
                    let pct = (done * 100) / total_ranges;
                    info!(
                        "[Discover V3] {}% ({}/{}) | found={} | {}s",
                        pct, done, total_ranges, found_now, t0.elapsed().as_secs()
                    );
                }
                out
            }
        })
        .collect();

    let mut all: Vec<PoolInfo> = Vec::new();
    while let Some(batch) = futures.next().await {
        all.extend(batch);
    }

    let mut seen: HashMap<(DexType, Address), PoolInfo> = HashMap::new();
    for p in all {
        seen.insert((p.dex, p.address), p);
    }
    let dedup: Vec<PoolInfo> = seen.into_values().collect();
    if !dedup.is_empty() {
        bulk_upsert_pools(&dedup).await?;
    }

    let v3 = select_pool_addresses_by_dex(CHAIN_BSC, DexType::UniswapV3).await?;
    let pcs_v3 = select_pool_addresses_by_dex(CHAIN_BSC, DexType::PancakeswapV3).await?;

    let new_v3 = v3.len() as i64 - dump_v3 as i64;
    let new_pcs = pcs_v3.len() as i64 - dump_pcs as i64;
    info!(
        "[Discover V3] 完成 {}s — V3={}(+{}), PCS V3={}(+{})",
        t0.elapsed().as_secs(),
        v3.len(),
        new_v3,
        pcs_v3.len(),
        new_pcs
    );
    Ok(DiscoverV3Result { v3, pcs_v3 })
}

pub struct DiscoverV4Result {
    pub v4_ids: Vec<B256>,
    pub pcs_v4_cl_ids: Vec<B256>,
}

pub async fn discover_v4_pools(
    client: Arc<BscHttpClient>,
    to_block: u64,
    target_tokens: HashSet<Address>,
) -> Result<DiscoverV4Result> {
    let t0 = std::time::Instant::now();
    let from_block = V4_BSC_DEPLOY_BLOCK;
    let total_blocks = to_block.saturating_sub(from_block);
    let concurrency = discovery_concurrency();
    let ranges = build_ranges(from_block, to_block, DISCOVERY_STEP);
    info!(
        "[Discover V4] 扫 V4/PCS V4 CL Initialize ({} → {}, {} blocks, {} workers, {} ranges)...",
        from_block, to_block, total_blocks, concurrency, ranges.len()
    );

    let bases = Arc::new(bsc_base_tokens());
    let targets = Arc::new(target_tokens);
    let v4_pm = BscContracts::UNISWAP_V4_POOL_MANAGER;
    let pcs_v4_cl_pm = BscContracts::PANCAKESWAP_V4_CL_POOL_MANAGER;

    let semaphore = Arc::new(Semaphore::new(concurrency));
    let total_ranges = ranges.len();
    let done_counter = Arc::new(AtomicUsize::new(0));
    let found_counter = Arc::new(AtomicUsize::new(0));

    let mut futures: FuturesUnordered<_> = ranges
        .into_iter()
        .map(|r| {
            let client = Arc::clone(&client);
            let semaphore = Arc::clone(&semaphore);
            let bases = Arc::clone(&bases);
            let targets = Arc::clone(&targets);
            let done_counter = Arc::clone(&done_counter);
            let found_counter = Arc::clone(&found_counter);
            async move {
                let _permit = semaphore.acquire_owned().await.expect("semaphore");
                let filter_v4 = Filter::new()
                    .address(v4_pm)
                    .from_block(r.from)
                    .to_block(r.to)
                    .event_signature(V4Initialize::SIGNATURE_HASH);
                let filter_pcs = Filter::new()
                    .address(pcs_v4_cl_pm)
                    .from_block(r.from)
                    .to_block(r.to)
                    .event_signature(PcsV4ClInitialize::SIGNATURE_HASH);
                let f_v4 = client.get_logs(&filter_v4);
                let f_pcs = client.get_logs(&filter_pcs);
                let (v4_logs, pcs_logs) = tokio::join!(f_v4, f_pcs);

                let mut out: Vec<V4PoolInfo> = Vec::new();
                if let Ok(logs) = v4_logs {
                    for log in logs {
                        let parsed = match V4Initialize::decode_log(&log.inner, true) {
                            Ok(e) => e,
                            Err(_) => continue,
                        };
                        if pool_includes_pair(parsed.currency0, parsed.currency1, &targets, &bases) {
                            out.push(V4PoolInfo {
                                pool_id: parsed.id,
                                chain: CHAIN_BSC,
                                dex: DexType::UniswapV4,
                                currency0: parsed.currency0,
                                currency1: parsed.currency1,
                                fee: parsed.fee.try_into().unwrap_or(0),
                                tick_spacing: parsed.tickSpacing.try_into().unwrap_or(0),
                                hooks: parsed.hooks,
                            });
                        }
                    }
                }
                if let Ok(logs) = pcs_logs {
                    for log in logs {
                        let parsed = match PcsV4ClInitialize::decode_log(&log.inner, true) {
                            Ok(e) => e,
                            Err(_) => continue,
                        };
                        if pool_includes_pair(parsed.currency0, parsed.currency1, &targets, &bases) {
                            out.push(V4PoolInfo {
                                pool_id: parsed.id,
                                chain: CHAIN_BSC,
                                dex: DexType::PancakeswapV4Cl,
                                currency0: parsed.currency0,
                                currency1: parsed.currency1,
                                fee: parsed.fee.try_into().unwrap_or(0),
                                tick_spacing: 0,
                                hooks: parsed.hooks,
                            });
                        }
                    }
                }

                let done = done_counter.fetch_add(1, Ordering::Relaxed) + 1;
                let found_now = found_counter.fetch_add(out.len(), Ordering::Relaxed) + out.len();
                if done % 20 == 0 || done == total_ranges {
                    let pct = (done * 100) / total_ranges;
                    info!(
                        "[Discover V4] {}% ({}/{}) | found={} | {}s",
                        pct, done, total_ranges, found_now, t0.elapsed().as_secs()
                    );
                }
                out
            }
        })
        .collect();

    let mut all: Vec<V4PoolInfo> = Vec::new();
    while let Some(batch) = futures.next().await {
        all.extend(batch);
    }

    let mut seen: HashMap<(DexType, B256), V4PoolInfo> = HashMap::new();
    for p in all {
        seen.insert((p.dex, p.pool_id), p);
    }
    let dedup: Vec<V4PoolInfo> = seen.into_values().collect();
    if !dedup.is_empty() {
        bulk_upsert_v4_pools(&dedup).await?;
    }

    let v4_ids = select_v4_pool_ids(CHAIN_BSC, DexType::UniswapV4).await?;
    let pcs_v4_cl_ids = select_v4_pool_ids(CHAIN_BSC, DexType::PancakeswapV4Cl).await?;

    info!(
        "[Discover V4] 完成 {}s — V4={} 池, PCS V4 CL={} 池",
        t0.elapsed().as_secs(),
        v4_ids.len(),
        pcs_v4_cl_ids.len()
    );
    Ok(DiscoverV4Result { v4_ids, pcs_v4_cl_ids })
}
