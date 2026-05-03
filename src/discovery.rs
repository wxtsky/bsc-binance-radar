//! V3/V4 pool discovery：扫 Factory PoolCreated / PoolManager Initialize 事件，
//! 按 (target, base) 配对规则过滤，写入 pools / v4_pools 表。

use crate::abis::{PcsV4ClInitialize, PoolCreated, V4Initialize};
use crate::chain::{bsc_base_tokens, pool_includes_pair};
use crate::clients::BscHttpClient;
use crate::contracts::{BscContracts, V3_BSC_DEPLOY_BLOCK, V4_BSC_DEPLOY_BLOCK};
use crate::db::queries::{
    bulk_upsert_pools, bulk_upsert_v4_pools, select_pool_addresses_by_dex,
    select_v4_pool_ids_by_namespace,
};
use crate::types::{DexType, PoolInfo, V4PoolInfo};
use alloy::primitives::Address;
use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use anyhow::Result;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Semaphore;
use tracing::{info, warn};

/// NodeReal eth_getLogs 50K blocks/call 上限
pub const DISCOVERY_STEP: u64 = 49_999;

/// Discovery 并发：默认 8 worker，可被 BF_DISCOVERY_CONCURRENCY 覆盖
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

/// 扫 V3/PCS V3 全历史 PoolCreated → 按 (target, base) 配对过滤 → bulk_upsert_pools
/// 配合 dump 兜底做"双保险并集"：dump 兜底死池子（节点漏 logs），扫描兜底新池子。
pub async fn discover_whitelisted_pools(
    client: Arc<BscHttpClient>,
    to_block: u64,
    target_tokens: HashSet<String>,
) -> Result<DiscoverV3Result> {
    let t0 = std::time::Instant::now();

    // 第 1 步：先看 PG 已有（dump 兜底）
    let dump_v3 = select_pool_addresses_by_dex("bsc", DexType::UniswapV3.as_db_str())
        .await?
        .len();
    let dump_pcs = select_pool_addresses_by_dex("bsc", DexType::PancakeswapV3.as_db_str())
        .await?
        .len();
    info!("[Discover V3] PG 已有: V3={}, PCS V3={}（dump 兜底）", dump_v3, dump_pcs);

    let from_block = V3_BSC_DEPLOY_BLOCK;
    let total_blocks = to_block.saturating_sub(from_block);
    let concurrency = discovery_concurrency();
    let ranges = build_ranges(from_block, to_block, DISCOVERY_STEP);
    info!(
        "[Discover V3] 扫 V3/PCS V3 PoolCreated ({} → {}, {} blocks, {} workers, {} ranges)...",
        from_block, to_block, total_blocks, concurrency, ranges.len()
    );

    let bases = bsc_base_tokens();
    let bases = Arc::new(bases);
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

                let mut handle = |logs_res: Result<Vec<alloy::rpc::types::Log>, _>, dex: DexType| {
                    match logs_res {
                        Ok(logs) => {
                            for log in logs {
                                let parsed = match PoolCreated::decode_log(&log.inner, true) {
                                    Ok(e) => e,
                                    Err(_) => continue,
                                };
                                let t0_str = format!("{:?}", parsed.token0).to_lowercase();
                                let t1_str = format!("{:?}", parsed.token1).to_lowercase();
                                if pool_includes_pair(&t0_str, &t1_str, &targets, &bases) {
                                    out.push(PoolInfo {
                                        address: format!("{:?}", parsed.pool).to_lowercase(),
                                        chain: "bsc".to_string(),
                                        dex,
                                        token0: t0_str,
                                        token1: t1_str,
                                        fee_tier: parsed.fee.try_into().unwrap_or(0),
                                    });
                                }
                            }
                        }
                        Err(e) => warn!("[Discover V3] range {}-{} fail: {}", r.from, r.to, e),
                    }
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

    // dedup by (dex, address)
    let mut seen: HashMap<(DexType, String), PoolInfo> = HashMap::new();
    for p in all {
        seen.insert((p.dex, p.address.clone()), p);
    }
    let dedup: Vec<PoolInfo> = seen.into_values().collect();
    if !dedup.is_empty() {
        bulk_upsert_pools(&dedup).await?;
    }

    // 重新 SELECT 拿 union（dump ∪ scan）
    let v3_addrs = select_pool_addresses_by_dex("bsc", DexType::UniswapV3.as_db_str()).await?;
    let pcs_addrs = select_pool_addresses_by_dex("bsc", DexType::PancakeswapV3.as_db_str()).await?;
    let v3: Vec<Address> = v3_addrs
        .iter()
        .filter_map(|a| a.parse().ok())
        .collect();
    let pcs_v3: Vec<Address> = pcs_addrs
        .iter()
        .filter_map(|a| a.parse().ok())
        .collect();

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
    pub v4_ids: Vec<alloy::primitives::B256>,
    pub pcs_v4_cl_ids: Vec<alloy::primitives::B256>,
}

/// 扫 V4 / PCS V4 CL PoolManager Initialize → 按 (target, base) 配对 filter → bulk_upsert_v4_pools
pub async fn discover_v4_pools(
    client: Arc<BscHttpClient>,
    to_block: u64,
    target_tokens: HashSet<String>,
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
                        let c0 = format!("{:?}", parsed.currency0).to_lowercase();
                        let c1 = format!("{:?}", parsed.currency1).to_lowercase();
                        if pool_includes_pair(&c0, &c1, &targets, &bases) {
                            out.push(V4PoolInfo {
                                pool_id: format!("{:?}", parsed.id),
                                chain: "bsc".to_string(),
                                currency0: c0,
                                currency1: c1,
                                fee: parsed.fee.try_into().unwrap_or(0),
                                tick_spacing: parsed.tickSpacing.try_into().unwrap_or(0),
                                hooks: format!("{:?}", parsed.hooks).to_lowercase(),
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
                        let c0 = format!("{:?}", parsed.currency0).to_lowercase();
                        let c1 = format!("{:?}", parsed.currency1).to_lowercase();
                        if pool_includes_pair(&c0, &c1, &targets, &bases) {
                            out.push(V4PoolInfo {
                                pool_id: format!("pcsv4cl:{:?}", parsed.id),
                                chain: "bsc".to_string(),
                                currency0: c0,
                                currency1: c1,
                                fee: parsed.fee.try_into().unwrap_or(0),
                                tick_spacing: 0,
                                hooks: format!("{:?}", parsed.hooks).to_lowercase(),
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

    let mut seen: HashMap<String, V4PoolInfo> = HashMap::new();
    for p in all {
        seen.insert(p.pool_id.clone(), p);
    }
    let dedup: Vec<V4PoolInfo> = seen.into_values().collect();
    if !dedup.is_empty() {
        bulk_upsert_v4_pools(&dedup).await?;
    }

    // 从 PG 拿 union 全集，返回 poolIds 给 swap getLogs args.id 过滤
    let v4_ids_str = select_v4_pool_ids_by_namespace("bsc", false).await?;
    let pcs_ids_str = select_v4_pool_ids_by_namespace("bsc", true).await?;
    let v4_ids: Vec<alloy::primitives::B256> = v4_ids_str
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect();
    let pcs_v4_cl_ids: Vec<alloy::primitives::B256> = pcs_ids_str
        .iter()
        .filter_map(|s| s.trim_start_matches("pcsv4cl:").parse().ok())
        .collect();

    info!(
        "[Discover V4] 完成 {}s — V4={} 池, PCS V4 CL={} 池",
        t0.elapsed().as_secs(),
        v4_ids.len(),
        pcs_v4_cl_ids.len()
    );
    Ok(DiscoverV4Result { v4_ids, pcs_v4_cl_ids })
}
