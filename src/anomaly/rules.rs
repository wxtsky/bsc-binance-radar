//! 异动规则定义 + 阈值
//!
//! ENV 配置：
//!   ANOMALY_VOL_SPIKE_RATIO=5            （5min vol > N × 24h均值 触发 vol_spike）
//!   ANOMALY_FEE_TVL_APR=100              （fee/TVL 年化 ≥ N% 触发 combo）
//!   ANOMALY_DETECT_INTERVAL_MS=30000     （detector tick 间隔）
//!   ANOMALY_COOLDOWN_MS=300000           （同 token 触发后冷静期）
//!   ANOMALY_BASELINE_MIN_COVERAGE_MS=3600000  （baseline 数据不足 1h 不触发）

#[derive(Debug, Clone, Copy)]
pub struct AnomalyConfig {
    pub vol_spike_ratio: f64,
    pub fee_tvl_apr: f64,
    pub detect_interval_ms: i64,
    pub cooldown_ms: i64,
    pub baseline_min_coverage_ms: i64,
}

impl AnomalyConfig {
    pub fn from_env() -> Self {
        Self {
            vol_spike_ratio: std::env::var("ANOMALY_VOL_SPIKE_RATIO")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(5.0),
            fee_tvl_apr: std::env::var("ANOMALY_FEE_TVL_APR")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100.0),
            detect_interval_ms: std::env::var("ANOMALY_DETECT_INTERVAL_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30_000),
            cooldown_ms: std::env::var("ANOMALY_COOLDOWN_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(300_000),
            baseline_min_coverage_ms: std::env::var("ANOMALY_BASELINE_MIN_COVERAGE_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(3_600_000),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AnomalyRule {
    VolSpike,  // 5min vol > N × 24h 均值
    Combo,     // vol_spike ∧ fee/TVL 年化 ≥ N%
}

impl AnomalyRule {
    /// schema: anomaly_events.rule SMALLINT (1=vol_spike, 2=combo)
    pub fn as_db_smallint(&self) -> i16 {
        match self {
            AnomalyRule::VolSpike => 1,
            AnomalyRule::Combo => 2,
        }
    }
    pub fn as_label(&self) -> &'static str {
        match self {
            AnomalyRule::VolSpike => "vol_spike",
            AnomalyRule::Combo => "combo",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AnomalyTrigger {
    pub token_address: Vec<u8>,    // BYTEA(20)
    pub symbol: String,
    pub rule: AnomalyRule,
    pub metrics: serde_json::Value,
    pub detected_at: i64,
}
