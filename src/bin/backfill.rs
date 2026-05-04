//! 历史 swap 回补（90d/30d）。
//!
//! 流程：
//!   1. init PG / watchlist
//!   2. discovery（V3 PoolCreated + V4/PCS V4 CL Initialize）→ pools / v4_pools 表
//!   3. 预加载 pool meta cache 到内存（消除 prefetch SQL）
//!   4. fetch worker × N → 拉 swap logs → process → channel
//!   5. flush worker × M → binary COPY 写 swaps_staging
//!   6. migrate staging → swaps（hypertable，DISTINCT ON + ON CONFLICT 兜底）
//!   7. rebuild buckets
//!
//! 命令行：
//!   backfill 90d
//!   BF_FROM_BLOCK=... BF_TO_BLOCK=... BF_SHARD_LABEL=main BF_SKIP_REBUILD=1 backfill 90d

use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use bsc_binance_radar::abis::{PancakeV3Swap, PcsV4ClSwap, V3Swap, V4Swap};
use bsc_binance_radar::clients::bsc::{
    build_http_client, default_http_url, discovery_http_url,
};
use bsc_binance_radar::contracts::BscContracts;
use bsc_binance_radar::db::queries::{
    bulk_insert_bnb_prices, bulk_insert_swaps_staging, create_staging_table, load_main_pool_cache,
    migrate_staging_to_swaps, rebuild_buckets_from_swaps, select_all_binance_bsc_tokens,
};
use bsc_binance_radar::db::{ensure_schema, init_pool};
use bsc_binance_radar::abis::V2Swap;
use bsc_binance_radar::swap_processor::{
    interpolate_log_ts, process_pcs_v4_cl_swap, process_v2_swap, process_v3_bnb_swap,
    process_v3_swap, process_v4_swap, BnbPriceCache, PoolMeta,
};
use bsc_binance_radar::token_tracker::{init_watchlist, watchlist_addresses};
use bsc_binance_radar::types::{BnbPricePoint, DexType, SwapRecord};
use clap::Parser;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "backfill", about = "BSC binance radar 历史 swap 回补")]
struct Cli {
    /// 时长：24（小时）/ 24h / 30d / 90d
    #[arg(default_value = "90d")]
    duration: String,

    /// fetch worker 数（默认 BF_CONCURRENCY env 或 8）
    #[arg(short = 'c', long)]
    concurrency: Option<usize>,
}

fn parse_duration_hours(s: &str) -> Result<u64> {
    let (num_part, unit) = if let Some(stripped) = s.strip_suffix('d') {
        (stripped, "d")
    } else if let Some(stripped) = s.strip_suffix('h') {
        (stripped, "h")
    } else {
        (s, "h")
    };
    let n: u64 = num_part.parse().with_context(|| format!("invalid duration: {}", s))?;
    Ok(if unit == "d" { n * 24 } else { n })
}

#[derive(Debug, Clone)]
struct Job {
    from: u64,
    to: u64,
    index: usize,
}

#[derive(Debug, Default)]
struct BatchBuffer {
    swaps: Vec<SwapRecord>,
    bnb_prices: Vec<BnbPricePoint>,
}

#[derive(Debug, Default)]
struct Timing {
    fetch_ms: AtomicU64,
    process_ms: AtomicU64,
    flush_ms: AtomicU64,
}

struct PoolCache {
    v3: HashMap<Address, PoolMeta>,
    pcs_v3: HashMap<Address, PoolMeta>,
    v2: HashMap<Address, PoolMeta>,
    v4: HashMap<B256, PoolMeta>,
    pcs_v4_cl: HashMap<B256, PoolMeta>,
}

/// 新策略：从 main_pools 表加载（每 token 一个主池），按 dex 分桶
async fn load_pool_cache() -> Result<PoolCache> {
    let raw = load_main_pool_cache().await?;
    let to_meta = |raw: HashMap<Address, (Address, Address, u32)>| -> HashMap<Address, PoolMeta> {
        raw.into_iter().map(|(addr, (t0, t1, fee))| {
            (addr, PoolMeta { token0: t0, token1: t1, fee_tier: fee })
        }).collect()
    };
    let to_meta_b256 = |raw: HashMap<B256, (Address, Address, u32)>| -> HashMap<B256, PoolMeta> {
        raw.into_iter().map(|(id, (c0, c1, fee))| {
            (id, PoolMeta { token0: c0, token1: c1, fee_tier: fee })
        }).collect()
    };
    let v3 = to_meta(raw.v3);
    let pcs_v3 = to_meta(raw.pcs_v3);
    let v2 = to_meta(raw.v2);
    let v4 = to_meta_b256(raw.v4);
    let pcs_v4_cl = to_meta_b256(raw.pcs_v4_cl);

    info!(
        "[PoolCache] from main_pools: V3={} PCS V3={} V2={} V4={} PCS V4 CL={}",
        v3.len(), pcs_v3.len(), v2.len(), v4.len(), pcs_v4_cl.len()
    );
    Ok(PoolCache { v3, pcs_v3, v2, v4, pcs_v4_cl })
}

async fn detect_bnb_price_token0() -> bool {
    // PCS V3 0x172fcD41E0913e95784454622d1c3724f546f849: token0=USDT, token1=WBNB
    false
}

async fn fetch_one_batch(
    client: Arc<bsc_binance_radar::clients::BscHttpClient>,
    job: &Job,
    pool_cache: &PoolCache,
    bnb_cache: &Arc<BnbPriceCache>,
    bnb_pool_wbnb_is_token0: bool,
    timing: &Timing,
) -> Result<BatchBuffer> {
    let fetch_t0 = std::time::Instant::now();

    let from = job.from;
    let to = job.to;

    // 5 个 getLogs + 2 个 getBlock 并发
    let v3_swap_sig = V3Swap::SIGNATURE_HASH;
    let pcs_v3_swap_sig = PancakeV3Swap::SIGNATURE_HASH;
    let v4_swap_sig = V4Swap::SIGNATURE_HASH;
    let pcs_v4_cl_swap_sig = PcsV4ClSwap::SIGNATURE_HASH;
    // BNB price 池切 V3，监听 PCS V3 Swap event
    let bnb_price_sig = PancakeV3Swap::SIGNATURE_HASH;

    let v3_addrs: Vec<Address> = pool_cache.v3.keys().copied().collect();
    let pcs_v3_addrs: Vec<Address> = pool_cache.pcs_v3.keys().copied().collect();
    let v2_addrs: Vec<Address> = pool_cache.v2.keys().copied().collect();

    let v4_pm = BscContracts::UNISWAP_V4_POOL_MANAGER;
    let pcs_v4_cl_pm = BscContracts::PANCAKESWAP_V4_CL_POOL_MANAGER;
    let bnb_pool = BscContracts::BNB_PRICE_POOL;

    let filter_v3 = Filter::new()
        .address(v3_addrs)
        .from_block(from)
        .to_block(to)
        .event_signature(v3_swap_sig);
    let filter_pcs_v3 = Filter::new()
        .address(pcs_v3_addrs)
        .from_block(from)
        .to_block(to)
        .event_signature(pcs_v3_swap_sig);
    let filter_v4 = Filter::new()
        .address(v4_pm)
        .from_block(from)
        .to_block(to)
        .event_signature(v4_swap_sig);
    let filter_pcs_v4 = Filter::new()
        .address(pcs_v4_cl_pm)
        .from_block(from)
        .to_block(to)
        .event_signature(pcs_v4_cl_swap_sig);
    let filter_v2 = Filter::new()
        .address(v2_addrs)
        .from_block(from)
        .to_block(to)
        .event_signature(V2Swap::SIGNATURE_HASH);
    let filter_bnb = Filter::new()
        .address(bnb_pool)
        .from_block(from)
        .to_block(to)
        .event_signature(bnb_price_sig);

    let f_block_from = client.get_block_by_number(from.into(), alloy::rpc::types::BlockTransactionsKind::Hashes);
    let f_block_to = client.get_block_by_number(to.into(), alloy::rpc::types::BlockTransactionsKind::Hashes);
    let f_v3 = client.get_logs(&filter_v3);
    let f_pcs_v3 = client.get_logs(&filter_pcs_v3);
    let f_v4 = client.get_logs(&filter_v4);
    let f_pcs_v4 = client.get_logs(&filter_pcs_v4);
    let f_v2 = client.get_logs(&filter_v2);
    let f_bnb = client.get_logs(&filter_bnb);

    let (block_from, block_to, v3_logs, pcs_v3_logs, v4_logs, pcs_v4_logs, v2_logs, bnb_logs) =
        tokio::try_join!(f_block_from, f_block_to, f_v3, f_pcs_v3, f_v4, f_pcs_v4, f_v2, f_bnb)?;

    timing.fetch_ms.fetch_add(fetch_t0.elapsed().as_millis() as u64, Ordering::Relaxed);

    let from_ts_ms = block_from
        .map(|b| (b.header.timestamp as i64) * 1000)
        .unwrap_or(0);
    let to_ts_ms = block_to
        .map(|b| (b.header.timestamp as i64) * 1000)
        .unwrap_or(from_ts_ms);

    let process_t0 = std::time::Instant::now();
    let mut buffer = BatchBuffer::default();

    // BNB price logs first（更新 cache 给后续 USD 计算用）
    // V3 池：用 sqrtPriceX96 算 spot price，无成交滑点干扰
    for log in bnb_logs {
        let ts = interpolate_log_ts(&log, from, to, from_ts_ms, to_ts_ms);
        match process_v3_bnb_swap(&log, ts, bnb_pool_wbnb_is_token0) {
            Ok((point, price)) => {
                bnb_cache.set(price);
                buffer.bnb_prices.push(point);
            }
            Err(_) => {}
        }
    }

    let bnb_now = bnb_cache.get();

    // V3
    for log in v3_logs {
        let pool_addr = log.address();
        let meta = match pool_cache.v3.get(&pool_addr) {
            Some(m) => m,
            None => continue,
        };
        let ts = interpolate_log_ts(&log, from, to, from_ts_ms, to_ts_ms);
        match process_v3_swap(&log, bsc_binance_radar::types::CHAIN_BSC, DexType::UniswapV3, meta, pool_addr, ts, bnb_now) {
            Ok(rec) => buffer.swaps.push(rec),
            Err(e) => warn!("V3 process fail: {}", e),
        }
    }

    // PCS V3
    for log in pcs_v3_logs {
        let pool_addr = log.address();
        let meta = match pool_cache.pcs_v3.get(&pool_addr) {
            Some(m) => m,
            None => continue,
        };
        let ts = interpolate_log_ts(&log, from, to, from_ts_ms, to_ts_ms);
        match process_v3_swap(&log, bsc_binance_radar::types::CHAIN_BSC, DexType::PancakeswapV3, meta, pool_addr, ts, bnb_now) {
            Ok(rec) => buffer.swaps.push(rec),
            Err(e) => warn!("PCS V3 process fail: {}", e),
        }
    }

    // V4
    for log in v4_logs {
        let id = match log.topics().get(1) {
            Some(t) => *t,
            None => continue,
        };
        let meta = match pool_cache.v4.get(&id) {
            Some(m) => m,
            None => continue,
        };
        let ts = interpolate_log_ts(&log, from, to, from_ts_ms, to_ts_ms);
        match process_v4_swap(&log, bsc_binance_radar::types::CHAIN_BSC, meta, id, ts, bnb_now) {
            Ok(rec) => buffer.swaps.push(rec),
            Err(e) => warn!("V4 process fail: {}", e),
        }
    }

    // PCS V4 CL
    for log in pcs_v4_logs {
        let id = match log.topics().get(1) {
            Some(t) => *t,
            None => continue,
        };
        let meta = match pool_cache.pcs_v4_cl.get(&id) {
            Some(m) => m,
            None => continue,
        };
        let ts = interpolate_log_ts(&log, from, to, from_ts_ms, to_ts_ms);
        match process_pcs_v4_cl_swap(&log, bsc_binance_radar::types::CHAIN_BSC, meta, id, ts, bnb_now) {
            Ok(rec) => buffer.swaps.push(rec),
            Err(e) => warn!("PCS V4 CL process fail: {}", e),
        }
    }

    // PCS V2（通用主池监控，新增）
    for log in v2_logs {
        let pool_addr = log.address();
        let meta = match pool_cache.v2.get(&pool_addr) {
            Some(m) => m,
            None => continue,
        };
        let ts = interpolate_log_ts(&log, from, to, from_ts_ms, to_ts_ms);
        match process_v2_swap(&log, bsc_binance_radar::types::CHAIN_BSC, meta, pool_addr, ts, bnb_now) {
            Ok(rec) => buffer.swaps.push(rec),
            Err(e) => warn!("V2 process fail: {}", e),
        }
    }

    timing.process_ms.fetch_add(process_t0.elapsed().as_millis() as u64, Ordering::Relaxed);
    Ok(buffer)
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let hours = parse_duration_hours(&cli.duration)?;

    let concurrency: usize = cli.concurrency.unwrap_or_else(|| {
        std::env::var("BF_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(8)
    });
    let blocks_per_batch: u64 = std::env::var("BF_BATCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);
    let flush_workers: usize = std::env::var("BF_FLUSH_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let queue_max: usize = std::env::var("BF_QUEUE_MAX")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);
    let from_block_override: Option<u64> = std::env::var("BF_FROM_BLOCK").ok().and_then(|s| s.parse().ok());
    let to_block_override: Option<u64> = std::env::var("BF_TO_BLOCK").ok().and_then(|s| s.parse().ok());
    let skip_rebuild = std::env::var("BF_SKIP_REBUILD").map(|s| s == "1").unwrap_or(false);
    let skip_migrate = std::env::var("BF_SKIP_MIGRATE").map(|s| s == "1").unwrap_or(false);
    let shard_label = std::env::var("BF_SHARD_LABEL").unwrap_or_else(|_| "main".to_string());

    let http_url = default_http_url();
    let discovery_url = discovery_http_url();

    info!(
        "[Backfill][{}] {}h, fetch workers={}, batch={} blocks via {}",
        shard_label, hours, concurrency, blocks_per_batch, http_url
    );

    init_pool().await?;
    ensure_schema().await?;
    init_watchlist().await?;

    let _target_tokens = watchlist_addresses();
    let _ = select_all_binance_bsc_tokens().await?; // 验证 PG 连接

    let http_client = Arc::new(build_http_client(&http_url)?);
    let _discovery_client = Arc::new(build_http_client(&discovery_url)?);

    let latest = http_client.get_block_number().await?;
    let block_time_secs = 0.45_f64; // BSC 已升级到 ~0.45s/block
    let total_blocks = (hours as f64 * 3600.0 / block_time_secs).ceil() as u64;

    let start = from_block_override.unwrap_or(latest.saturating_sub(total_blocks));
    let end = to_block_override.unwrap_or(latest);

    info!(
        "[Backfill][{}] block time {:.3}s; range {} → {} ({} blocks)",
        shard_label, block_time_secs, start, end, end - start
    );

    create_staging_table().await?;
    info!(
        "[Backfill][{}] staging mode ON: fetcher 写 swaps_staging（无 index），跑完 migrate 到 swaps",
        shard_label
    );

    // 新策略：discovery 已废，直接从 main_pools 表加载主池（每 token 一行，TVL 最大池）
    info!("[Backfill][{}] 加载 main_pools（新策略：每 token 1 主池）", shard_label);
    let pool_cache = Arc::new(load_pool_cache().await?);
    let bnb_cache = Arc::new(BnbPriceCache::new(600.0)); // 启动 placeholder，会被实时 V2 swap 更新
    let bnb_pool_wbnb_is_token0 = detect_bnb_price_token0().await;

    // 切 jobs
    let mut jobs: Vec<Job> = Vec::new();
    let mut idx = 0;
    let mut f = start;
    while f < end {
        let t = (f + blocks_per_batch).min(end);
        jobs.push(Job { from: f, to: t.saturating_sub(1).max(f), index: idx });
        idx += 1;
        f = t;
    }
    info!("[Backfill][{}] {} batches", shard_label, jobs.len());

    let total_jobs = jobs.len();
    let timing = Arc::new(Timing::default());

    // mpsc channel：fetcher → flusher
    let (tx, mut rx) = mpsc::channel::<BatchBuffer>(queue_max);

    // fetch worker（tokio task × concurrency）
    let fetch_sem = Arc::new(Semaphore::new(concurrency));
    let total_logs = Arc::new(AtomicUsize::new(0));
    let total_swaps = Arc::new(AtomicUsize::new(0));
    let total_errors = Arc::new(AtomicUsize::new(0));
    let completed_fetches = Arc::new(AtomicUsize::new(0));

    let t0 = std::time::Instant::now();
    let label = shard_label.clone();
    let progress_t0 = t0;

    let mut fetch_futures: FuturesUnordered<_> = jobs
        .into_iter()
        .map(|job| {
            let client = Arc::clone(&http_client);
            let pool_cache = Arc::clone(&pool_cache);
            let bnb_cache = Arc::clone(&bnb_cache);
            let timing = Arc::clone(&timing);
            let sem = Arc::clone(&fetch_sem);
            let tx = tx.clone();
            let total_logs = Arc::clone(&total_logs);
            let total_swaps = Arc::clone(&total_swaps);
            let total_errors = Arc::clone(&total_errors);
            let completed_fetches = Arc::clone(&completed_fetches);
            let label = label.clone();
            async move {
                let _permit = sem.acquire_owned().await.expect("semaphore");
                match fetch_one_batch(client, &job, &pool_cache, &bnb_cache, bnb_pool_wbnb_is_token0, &timing).await {
                    Ok(buffer) => {
                        total_logs.fetch_add(buffer.swaps.len() + buffer.bnb_prices.len(), Ordering::Relaxed);
                        total_swaps.fetch_add(buffer.swaps.len(), Ordering::Relaxed);
                        if let Err(e) = tx.send(buffer).await {
                            warn!("[Backfill][{}] channel send fail: {}", label, e);
                        }
                    }
                    Err(e) => {
                        warn!("[Backfill][{}] batch {} ({}-{}) fetch fail: {}", label, job.index, job.from, job.to, e);
                        total_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                let done = completed_fetches.fetch_add(1, Ordering::Relaxed) + 1;
                if done % 20 == 0 || done == total_jobs {
                    let pct = (done * 100) / total_jobs.max(1);
                    let elapsed_s = progress_t0.elapsed().as_secs().max(1);
                    let rate = done as f64 / elapsed_s as f64;
                    let eta_h = if rate > 0.0 {
                        ((total_jobs - done) as f64 / rate / 3600.0).max(0.0)
                    } else {
                        0.0
                    };
                    let avg_fetch = timing.fetch_ms.load(Ordering::Relaxed) / done.max(1) as u64;
                    let avg_proc = timing.process_ms.load(Ordering::Relaxed) / done.max(1) as u64;
                    let avg_flush = timing.flush_ms.load(Ordering::Relaxed) / done.max(1) as u64;
                    info!(
                        "[Backfill][{}] {}% {}/{} | logs={} ok={} err={} | {}s {:.2}batch/s ETA={:.1}h | avg ms: fetch={} process={} flush={}",
                        label, pct, done, total_jobs,
                        total_logs.load(Ordering::Relaxed),
                        total_swaps.load(Ordering::Relaxed),
                        total_errors.load(Ordering::Relaxed),
                        elapsed_s, rate, eta_h,
                        avg_fetch, avg_proc, avg_flush
                    );
                }
            }
        })
        .collect();

    // flush worker：单 collector task，收到 buffer 后 spawn 子 task 写 staging（限并发 = flush_workers）
    let flush_sem = Arc::new(Semaphore::new(flush_workers));
    let collector_label = shard_label.clone();
    let collector_timing = Arc::clone(&timing);
    let collector_handle = tokio::spawn(async move {
        let mut child_handles: FuturesUnordered<tokio::task::JoinHandle<()>> = FuturesUnordered::new();
        while let Some(buffer) = rx.recv().await {
            let sem = Arc::clone(&flush_sem);
            let timing = Arc::clone(&collector_timing);
            let label = collector_label.clone();
            child_handles.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await.expect("semaphore");
                let flush_t0 = std::time::Instant::now();
                if !buffer.swaps.is_empty() {
                    if let Err(e) = bulk_insert_swaps_staging(&buffer.swaps).await {
                        warn!("[Backfill][{}] flush swaps fail: {}", label, e);
                    }
                }
                if !buffer.bnb_prices.is_empty() {
                    if let Err(e) = bulk_insert_bnb_prices(&buffer.bnb_prices).await {
                        warn!("[Backfill][{}] flush bnb fail: {}", label, e);
                    }
                }
                timing.flush_ms.fetch_add(flush_t0.elapsed().as_millis() as u64, Ordering::Relaxed);
            }));
            // 限并发：如果 child 太多，await 一些
            while child_handles.len() >= flush_workers * 4 {
                let _ = child_handles.next().await;
            }
        }
        // 等所有子任务
        while let Some(_) = child_handles.next().await {}
    });

    // 等所有 fetch 完成
    while let Some(_) = fetch_futures.next().await {}

    drop(tx); // 关闭 channel，让 collector 退出
    collector_handle.await.context("collector join failed")?;

    let elapsed_s = t0.elapsed().as_secs();
    info!(
        "[Backfill][{}] 抓取+flush 完成 {}s — swaps={}, errors={}",
        shard_label,
        elapsed_s,
        total_swaps.load(Ordering::Relaxed),
        total_errors.load(Ordering::Relaxed)
    );

    if skip_migrate {
        info!("[Backfill][{}] skip migrate (BF_SKIP_MIGRATE=1)", shard_label);
    } else {
        info!("[Backfill][{}] migrating staging → swaps...", shard_label);
        let mig_t0 = std::time::Instant::now();
        let r = migrate_staging_to_swaps().await?;
        info!(
            "[Backfill][{}] migrate done {}s — staged={}, inserted={}",
            shard_label,
            mig_t0.elapsed().as_secs(),
            r.staged,
            r.inserted
        );
    }

    if skip_rebuild {
        info!("[Backfill][{}] skip rebuild (BF_SKIP_REBUILD=1)", shard_label);
    } else {
        info!("[Backfill][{}] 重建 buckets...", shard_label);
        rebuild_buckets_from_swaps(0).await?;
        info!("[Backfill][{}] buckets 重建完成 ✅", shard_label);
    }

    Ok(())
}
