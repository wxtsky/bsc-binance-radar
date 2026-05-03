use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashSet;
use tokio::fs;

#[derive(Debug, Deserialize)]
struct PerpetualsSeed {
    #[serde(rename = "baseAssets")]
    base_assets: Vec<String>,
    count: u32,
    #[allow(dead_code)]
    source: Option<String>,
    #[allow(dead_code)]
    #[serde(rename = "syncedAt")]
    synced_at: Option<i64>,
}

const GITHUB_RAW: &str =
    "https://raw.githubusercontent.com/wxtsky/bsc-binance-radar/main/seed/binance-perpetuals.json";

/// 从 fapi.binance.com 拉永续 baseAssets。美国 IP 会 451。
async fn fetch_fapi_perpetuals() -> Result<HashSet<String>> {
    let resp = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?
        .get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("fapi exchangeInfo HTTP {}", resp.status());
    }
    let json: serde_json::Value = resp.json().await?;
    let symbols = json
        .get("symbols")
        .and_then(|v| v.as_array())
        .context("symbols not array")?;
    let mut out = HashSet::new();
    for s in symbols {
        let contract_type = s.get("contractType").and_then(|v| v.as_str()).unwrap_or("");
        let status = s.get("status").and_then(|v| v.as_str()).unwrap_or("");
        let quote = s.get("quoteAsset").and_then(|v| v.as_str()).unwrap_or("");
        if contract_type == "PERPETUAL" && status == "TRADING" && quote == "USDT" {
            if let Some(base) = s.get("baseAsset").and_then(|v| v.as_str()) {
                out.insert(base.to_string());
            }
        }
    }
    Ok(out)
}

/// fallback：从 GitHub raw 拉 seed 文件
async fn fetch_github_seed() -> Result<HashSet<String>> {
    let resp = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()?
        .get(GITHUB_RAW)
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("github seed HTTP {}", resp.status());
    }
    let seed: PerpetualsSeed = resp.json().await?;
    Ok(seed.base_assets.into_iter().collect())
}

/// fallback：从本地 seed/binance-perpetuals.json
async fn read_local_seed() -> Result<HashSet<String>> {
    let path = "seed/binance-perpetuals.json";
    let txt = fs::read_to_string(path).await
        .with_context(|| format!("read {}", path))?;
    let seed: PerpetualsSeed = serde_json::from_str(&txt)?;
    Ok(seed.base_assets.into_iter().collect())
}

/// 拉 fapi 永续 baseAssets，地理屏蔽时按 GitHub seed → 本地文件 fallback
pub async fn fetch_active_perpetual_base_assets() -> Result<HashSet<String>> {
    match fetch_fapi_perpetuals().await {
        Ok(s) if !s.is_empty() => return Ok(s),
        Ok(_) => tracing::warn!("[Tracker] fapi returned empty, fallback to seed"),
        Err(e) => tracing::warn!("[Tracker] fapi direct failed ({}); falling back to GitHub seed", e),
    }
    match fetch_github_seed().await {
        Ok(s) if !s.is_empty() => return Ok(s),
        Ok(_) => tracing::warn!("[Tracker] github seed empty, try local file"),
        Err(e) => tracing::warn!("[Tracker] github seed failed: {}", e),
    }
    read_local_seed().await
}
