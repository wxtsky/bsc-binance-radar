//! 实时 stream listener + detector + feishu webhook
//! TODO: 完整实现（phase 2）

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .with_target(false)
        .init();

    tracing::info!("[Radar] stream + detector binary stub. TODO 实现 phase 2");
    Ok(())
}
