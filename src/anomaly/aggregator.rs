//! Detector 用的聚合查询：active tokens（有最近 swap）+ 5min/24h baseline + cooldown

use crate::db::get_pool;
use crate::types::CHAIN_BSC;
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct TokenBaseline {
    pub token_address: Vec<u8>,    // BYTEA(20)
    pub symbol: Option<String>,
    pub vol_5min_usd: f64,
    pub vol_24h_avg_5min_usd: f64, // 24h volume / 288 (288 个 5min 桶)
    pub fee_5min_usd: f64,
    pub baseline_coverage_ms: i64,
}

/// 拿到最近 5min 内有 swap 活动的 token + 24h baseline
/// 新策略：symbol 来自 main_pools.coin（不再 JOIN binance_bsc_tokens）
pub async fn fetch_active_token_baselines(now_ms: i64) -> Result<Vec<TokenBaseline>> {
    let client = get_pool().get().await?;
    let win_5min_start = now_ms - 5 * 60_000;
    let win_24h_start = now_ms - 24 * 60 * 60_000;

    let rows = client.query(
        "WITH active_5min AS (
             SELECT token_address, SUM(total_volume_usd) AS vol5, SUM(total_fees_usd) AS fee5
             FROM token_1min_stats
             WHERE chain = $3 AND bucket_start >= $1
             GROUP BY token_address
             HAVING SUM(total_volume_usd) > 0
         ),
         baseline_24h AS (
             SELECT token_address,
                    SUM(total_volume_usd) AS vol24h,
                    MIN(bucket_start) AS earliest_bucket
             FROM token_1min_stats
             WHERE chain = $3 AND bucket_start >= $2
             GROUP BY token_address
         )
         SELECT a.token_address, b.vol24h, b.earliest_bucket, a.vol5, a.fee5,
                mp.coin
         FROM active_5min a
         LEFT JOIN baseline_24h b ON a.token_address = b.token_address
         LEFT JOIN main_pools mp ON mp.token_addr = a.token_address",
        &[&win_5min_start, &win_24h_start, &CHAIN_BSC],
    ).await?;

    let mut out = Vec::with_capacity(rows.len());
    for r in &rows {
        let token_addr_bytes: &[u8] = r.get(0);
        let vol_24h: Option<f64> = r.get(1);
        let earliest_bucket: Option<i64> = r.get(2);
        let vol_5min: f64 = r.get::<_, Option<f64>>(3).unwrap_or(0.0);
        let fee_5min: f64 = r.get::<_, Option<f64>>(4).unwrap_or(0.0);
        let symbol: Option<String> = r.get(5);

        let coverage_ms = match earliest_bucket {
            Some(eb) => now_ms - eb,
            None => 0,
        };
        let avg_5min = vol_24h.unwrap_or(0.0) / 288.0;

        out.push(TokenBaseline {
            token_address: token_addr_bytes.to_vec(),
            symbol,
            vol_5min_usd: vol_5min,
            vol_24h_avg_5min_usd: avg_5min,
            fee_5min_usd: fee_5min,
            baseline_coverage_ms: coverage_ms,
        });
    }
    Ok(out)
}

/// 检查 token 在 cooldown 内是否触发过（避免短时重复告警）
pub async fn is_in_cooldown(token: &[u8], rule: i16, since_ms: i64) -> Result<bool> {
    let client = get_pool().get().await?;
    let row = client.query_opt(
        "SELECT 1 FROM anomaly_events WHERE token_address = $1 AND rule = $2 AND detected_at >= $3 LIMIT 1",
        &[&token, &rule, &since_ms],
    ).await?;
    Ok(row.is_some())
}

pub async fn record_anomaly(
    token_address: &[u8],
    symbol: Option<&str>,
    rule: i16,
    metrics: serde_json::Value,
    detected_at: i64,
) -> Result<()> {
    let client = get_pool().get().await?;
    client.execute(
        "INSERT INTO anomaly_events (token_address, symbol, rule, metrics, detected_at) VALUES ($1, $2, $3, $4, $5)",
        &[&token_address, &symbol, &rule, &metrics, &detected_at],
    ).await?;
    Ok(())
}
