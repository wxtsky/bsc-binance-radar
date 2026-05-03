//! 实时 stream listener + detector + feishu webhook

use anyhow::Result;
use bsc_binance_radar::anomaly::detector::run_detector_loop;
use bsc_binance_radar::anomaly::rules::AnomalyConfig;
use bsc_binance_radar::db::{ensure_schema, init_pool};
use bsc_binance_radar::notifier::feishu::FeishuNotifier;
use bsc_binance_radar::stream_listener::{
    liveness_probe, load_pool_cache_from_db, run_stream_listener,
};
use bsc_binance_radar::swap_processor::BnbPriceCache;
use bsc_binance_radar::token_tracker::init_watchlist;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .with_target(false)
        .init();

    info!("[Radar] starting up...");

    init_pool().await?;
    ensure_schema().await?;
    init_watchlist().await?;

    let cfg = AnomalyConfig::from_env();
    info!("[Radar] anomaly config: {:?}", cfg);

    // pool cache + bnb price cache（全局共享，stream listener 更新）
    let pool_cache = Arc::new(load_pool_cache_from_db().await?);
    let bnb_cache = Arc::new(BnbPriceCache::new(600.0));
    let last_swap_ms = Arc::new(RwLock::new(0_i64));

    let (anomaly_tx, mut anomaly_rx) = mpsc::channel(64);

    // detector loop
    let detector_handle = tokio::spawn(async move {
        if let Err(e) = run_detector_loop(cfg, anomaly_tx).await {
            warn!("[Detector] loop ended: {}", e);
        }
    });

    // feishu notifier
    let _feishu_handle = if let Ok(webhook) = std::env::var("NOTIFY_FEISHU_WEBHOOK") {
        let secret = std::env::var("NOTIFY_FEISHU_SECRET").ok();
        let notifier = FeishuNotifier::new(webhook, secret);
        Some(tokio::spawn(async move {
            while let Some(trigger) = anomaly_rx.recv().await {
                if let Err(e) = notifier.send(&trigger).await {
                    warn!("[Notifier] feishu send fail for {}: {}", trigger.symbol, e);
                } else {
                    info!("[Notifier] feishu sent: {} {} {:?}",
                        trigger.symbol, trigger.token_address, trigger.rule);
                }
            }
        }))
    } else {
        warn!("[Radar] NOTIFY_FEISHU_WEBHOOK not set, anomalies dry-run");
        Some(tokio::spawn(async move {
            while let Some(trigger) = anomaly_rx.recv().await {
                info!("[Notifier:dry] {} {} {:?}",
                    trigger.symbol, trigger.token_address, trigger.rule);
            }
        }))
    };

    // stream listener
    let stream_pool = Arc::clone(&pool_cache);
    let stream_bnb = Arc::clone(&bnb_cache);
    let stream_last = Arc::clone(&last_swap_ms);
    let stream_handle = tokio::spawn(async move {
        loop {
            match run_stream_listener(
                Arc::clone(&stream_pool),
                Arc::clone(&stream_bnb),
                Arc::clone(&stream_last),
            ).await {
                Ok(_) => {
                    warn!("[Stream] loop ended OK, reconnecting in 5s");
                }
                Err(e) => {
                    warn!("[Stream] error: {}, reconnecting in 5s", e);
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    });

    // liveness probe
    let probe_last = Arc::clone(&last_swap_ms);
    let _probe_handle = tokio::spawn(async move {
        liveness_probe(probe_last).await;
    });

    info!("[Radar] running. detector + stream + feishu all up.");

    let _ = tokio::signal::ctrl_c().await;
    info!("[Radar] shutting down");
    detector_handle.abort();
    stream_handle.abort();
    Ok(())
}
