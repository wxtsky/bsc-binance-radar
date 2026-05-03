//! Detector：30s tick 跑 anomaly aggregator，触发 rules，发出 events

use crate::anomaly::aggregator::{
    fetch_active_token_baselines, is_in_cooldown, record_anomaly, TokenBaseline,
};
use crate::anomaly::rules::{AnomalyConfig, AnomalyRule, AnomalyTrigger};
use anyhow::Result;
use chrono::Utc;
use serde_json::json;
use tokio::sync::mpsc;
use tracing::{info, warn};

pub async fn run_detector_loop(config: AnomalyConfig, tx: mpsc::Sender<AnomalyTrigger>) -> Result<()> {
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(
        config.detect_interval_ms as u64,
    ));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;
        if let Err(e) = run_one_tick(&config, &tx).await {
            warn!("[Detector] tick fail: {}", e);
        }
    }
}

async fn run_one_tick(config: &AnomalyConfig, tx: &mpsc::Sender<AnomalyTrigger>) -> Result<()> {
    let now_ms = Utc::now().timestamp_millis();
    let baselines = fetch_active_token_baselines(now_ms).await?;

    for b in baselines {
        // baseline 覆盖不足时跳过（避免冷启动误触发）
        if b.baseline_coverage_ms < config.baseline_min_coverage_ms {
            continue;
        }

        // vol_spike: vol_5min / vol_24h_avg_5min ≥ ratio
        let ratio = if b.vol_24h_avg_5min_usd > 0.0 {
            b.vol_5min_usd / b.vol_24h_avg_5min_usd
        } else {
            0.0
        };
        if ratio >= config.vol_spike_ratio {
            try_trigger(
                &b,
                AnomalyRule::VolSpike,
                json!({
                    "vol_5min_usd": b.vol_5min_usd,
                    "vol_24h_avg_5min_usd": b.vol_24h_avg_5min_usd,
                    "ratio": ratio,
                }),
                now_ms,
                config.cooldown_ms,
                tx,
            ).await?;
        }

        // combo: vol_spike + fee/TVL 年化 ≥ N%
        // 简化：用 fee_5min × (288 × 365) 作 vol_24h proxy 算 APR%
        // TVL 暂用 vol_24h_avg × 100（占位，后续补 TVL 真值）
        let est_tvl = b.vol_24h_avg_5min_usd * 100.0;
        let fee_apr_pct = if est_tvl > 0.0 {
            (b.fee_5min_usd * 288.0 * 365.0 / est_tvl) * 100.0
        } else {
            0.0
        };
        if ratio >= config.vol_spike_ratio && fee_apr_pct >= config.fee_tvl_apr {
            try_trigger(
                &b,
                AnomalyRule::Combo,
                json!({
                    "vol_5min_usd": b.vol_5min_usd,
                    "vol_24h_avg_5min_usd": b.vol_24h_avg_5min_usd,
                    "ratio": ratio,
                    "fee_apr_pct": fee_apr_pct,
                    "est_tvl_usd": est_tvl,
                }),
                now_ms,
                config.cooldown_ms,
                tx,
            ).await?;
        }
    }
    Ok(())
}

async fn try_trigger(
    b: &TokenBaseline,
    rule: AnomalyRule,
    metrics: serde_json::Value,
    now_ms: i64,
    cooldown_ms: i64,
    tx: &mpsc::Sender<AnomalyTrigger>,
) -> Result<()> {
    let cooldown_since = now_ms - cooldown_ms;
    if is_in_cooldown(&b.token_address, rule.as_db_str(), cooldown_since).await? {
        return Ok(());
    }
    record_anomaly(
        &b.token_address,
        b.symbol.as_deref(),
        rule.as_db_str(),
        metrics.clone(),
        now_ms,
    ).await?;

    let trigger = AnomalyTrigger {
        token_address: b.token_address.clone(),
        symbol: b.symbol.clone().unwrap_or_default(),
        rule,
        metrics,
        detected_at: now_ms,
    };
    info!("[Detector] triggered {:?} {} {}", rule, b.token_address, trigger.symbol);
    if let Err(e) = tx.send(trigger).await {
        warn!("[Detector] notify channel closed: {}", e);
    }
    Ok(())
}
