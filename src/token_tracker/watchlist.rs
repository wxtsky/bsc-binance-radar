use crate::db::queries::select_all_binance_bsc_tokens;
use anyhow::Result;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// 内存中的白名单：address(lowercase) → (symbol, decimals)
#[derive(Debug, Clone)]
pub struct WatchlistInfo {
    pub symbol: String,
    pub decimals: u8,
}

pub struct Watchlist {
    pub by_address: HashMap<String, WatchlistInfo>,
}

static WATCHLIST: once_cell::sync::OnceCell<Arc<RwLock<Watchlist>>> = once_cell::sync::OnceCell::new();

pub async fn init_watchlist() -> Result<()> {
    let _ = WATCHLIST.set(Arc::new(RwLock::new(Watchlist {
        by_address: HashMap::new(),
    })));
    load_watchlist().await
}

pub async fn load_watchlist() -> Result<()> {
    let tokens = select_all_binance_bsc_tokens().await?;
    let mut map = HashMap::with_capacity(tokens.len());
    for t in &tokens {
        map.insert(
            t.contract_address.to_lowercase(),
            WatchlistInfo {
                symbol: t.symbol.clone(),
                decimals: t.decimals.try_into().unwrap_or(18),
            },
        );
    }
    let watchlist = WATCHLIST
        .get()
        .expect("watchlist not initialized; call init_watchlist first");
    *watchlist.write() = Watchlist { by_address: map };
    tracing::info!("[Watchlist] loaded {} Binance/BSC tokens", tokens.len());
    Ok(())
}

pub fn get_watchlist() -> Arc<RwLock<Watchlist>> {
    WATCHLIST
        .get()
        .expect("watchlist not initialized; call init_watchlist first")
        .clone()
}

pub fn is_watched(address: &str) -> bool {
    let watchlist = get_watchlist();
    let guard = watchlist.read();
    guard.by_address.contains_key(&address.to_lowercase())
}

pub fn watchlist_addresses() -> HashSet<String> {
    let watchlist = get_watchlist();
    let guard = watchlist.read();
    guard.by_address.keys().cloned().collect()
}

pub fn watchlist_size() -> usize {
    let watchlist = get_watchlist();
    let guard = watchlist.read();
    guard.by_address.len()
}
