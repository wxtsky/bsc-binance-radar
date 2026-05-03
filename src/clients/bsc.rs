use alloy::providers::{ProviderBuilder, RootProvider};
use alloy::pubsub::PubSubFrontend;
use alloy::rpc::client::ClientBuilder;
use alloy::transports::http::{Client, Http};
use alloy::transports::ws::WsConnect;
use anyhow::{Context, Result};
use url::Url;

/// HTTP RPC client（用于 backfill getLogs / getBlock 等）
pub type BscHttpClient = RootProvider<Http<Client>>;

/// WebSocket subscription client（用于 stream swap event）
pub type BscWsClient = RootProvider<PubSubFrontend>;

pub fn build_http_client(rpc_url: &str) -> Result<BscHttpClient> {
    let url: Url = rpc_url.parse().context("invalid http rpc url")?;
    let client = ProviderBuilder::new().on_http(url);
    Ok(client)
}

pub async fn build_ws_client(wss_url: &str) -> Result<BscWsClient> {
    let ws = WsConnect::new(wss_url);
    let client = ClientBuilder::default()
        .ws(ws)
        .await
        .context("ws connect failed")?;
    let provider = ProviderBuilder::new().on_client(client);
    Ok(provider)
}

/// 默认 RPC URL（HANDOFF：自建节点 :81）
pub fn default_http_url() -> String {
    std::env::var("BF_RPC_URL")
        .or_else(|_| std::env::var("BSC_HTTP_URL"))
        .unwrap_or_else(|_| "http://151.123.172.62:81".to_string())
}

/// Discovery RPC URL（NodeReal archive，扫历史 PoolCreated 用）
pub fn discovery_http_url() -> String {
    std::env::var("BF_DISCOVERY_RPC")
        .unwrap_or_else(|_| {
            "https://bsc-mainnet.nodereal.io/v1/b13fcff9775e4d1bb28a0735292a1819".to_string()
        })
}

pub fn default_ws_url() -> Result<String> {
    std::env::var("BSC_WSS_URL")
        .context("BSC_WSS_URL not set")
}
