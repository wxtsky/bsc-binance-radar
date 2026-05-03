//! 实时 stream listener + detector + feishu webhook
//!
//! 当前 phase 1：
//!   - init PG / watchlist
//!   - 启动 detector 30s tick（从 token_1min_stats 跑 vol_spike + combo 规则）
//!   - 启动 feishu notifier 消费 anomaly_events
//!
//! phase 2 TODO：
//!   - WSS stream listener 4 dex swap event 实时入库
//!   - BNB price pool 实时反算 BNB/USD 价
//!   - liveness probe（60s 没 swap 自动 remount）

use anyhow::Result;
use bsc_binance_radar::anomaly::detector::run_detector_loop;
use bsc_binance_radar::anomaly::rules::AnomalyConfig;
use bsc_binance_radar::db::{ensure_schema, init_pool};
use bsc_binance_radar::notifier::feishu::FeishuNotifier;
use bsc_binance_radar::token_tracker::init_watchlist;
use tokio::sync::mpsc;
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

    let (anomaly_tx, mut anomaly_rx) = mpsc::channel(64);

    // 启动 detector loop
    let detector_handle = tokio::spawn(async move {
        if let Err(e) = run_detector_loop(cfg, anomaly_tx).await {
            warn!("[Detector] loop ended: {}", e);
        }
    });

    // feishu notifier consumer
    let feishu_handle = if let Ok(webhook) = std::env::var("NOTIFY_FEISHU_WEBHOOK") {
        let secret = std::env::var("NOTIFY_FEISHU_SECRET").ok();
        let notifier = FeishuNotifier::new(webhook, secret);
        Some(tokio::spawn(async move {
            while let Some(trigger) = anomaly_rx.recv().await {
                if let Err(e) = notifier.send(&trigger).await {
                    warn!("[Notifier] feishu send fail for {}: {}", trigger.symbol, e);
                } else {
                    info!("[Notifier] feishu sent: {} {} {:?}", trigger.symbol, trigger.token_address, trigger.rule);
                }
            }
        }))
    } else {
        warn!("[Radar] NOTIFY_FEISHU_WEBHOOK not set, anomaly events will not be pushed");
        // 仍然 drain 防 sender 阻塞
        Some(tokio::spawn(async move {
            while let Some(trigger) = anomaly_rx.recv().await {
                info!("[Notifier:dry] {} {} {:?}", trigger.symbol, trigger.token_address, trigger.rule);
            }
        }))
    };

    // TODO phase 2: stream listener WSS + liveness probe
    info!("[Radar] running. detector ticking. (stream listener phase 2 TODO)");

    let _ = tokio::signal::ctrl_c().await;
    info!("[Radar] shutting down");
    detector_handle.abort();
    if let Some(h) = feishu_handle { h.abort(); }
    Ok(())
}
