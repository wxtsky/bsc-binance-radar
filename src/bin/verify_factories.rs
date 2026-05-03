//! 一次性验证 4 个 V3/V4 Factory + Manager 地址 + 事件签名
//! 跑：cargo run --release --bin verify-factories
//!     cargo run --release --bin verify-factories -- --rpc http://...

use alloy::providers::Provider;
use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use anyhow::Result;
use bsc_binance_radar::abis::{PcsV4ClInitialize, PoolCreated, V4Initialize};
use bsc_binance_radar::clients::bsc::{build_http_client, discovery_http_url};
use bsc_binance_radar::contracts::BscContracts;
use clap::Parser;

#[derive(Parser, Debug)]
struct Cli {
    /// RPC URL（覆盖默认 NodeReal）
    #[arg(long)]
    rpc: Option<String>,
}

const SCAN_RANGE: u64 = 49_999;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let rpc_url = cli.rpc.unwrap_or_else(discovery_http_url);
    let client = build_http_client(&rpc_url)?;

    let latest = client.get_block_number().await?;
    let from = latest - SCAN_RANGE;
    let to = latest;

    println!("[verify] RPC = {}", rpc_url);
    println!("[verify] 扫范围 {} → {} ({} blocks)\n", from, to, SCAN_RANGE);

    let mut all_ok = true;

    // V3 Factory（UniV3 + PCS V3 共用 PoolCreated 签名）
    for (name, addr) in [
        ("UniswapV3 Factory", BscContracts::UNISWAP_V3_FACTORY),
        ("PancakeSwap V3 Factory", BscContracts::PANCAKESWAP_V3_FACTORY),
    ] {
        println!("[{}] ({:?})", name, addr);
        let logs = client.get_logs(
            &Filter::new()
                .address(addr)
                .from_block(from)
                .to_block(to)
                .event_signature(PoolCreated::SIGNATURE_HASH),
        ).await;
        match logs {
            Ok(logs) if !logs.is_empty() => {
                println!("  ✅ {} events", logs.len());
                if let Ok(parsed) = PoolCreated::decode_log(&logs.last().unwrap().inner, true) {
                    println!("     sample: token0={:?} token1={:?} fee={} pool={:?}",
                        parsed.token0, parsed.token1, parsed.fee, parsed.pool);
                }
            }
            Ok(_) => {
                println!("  ❌ 0 events");
                all_ok = false;
            }
            Err(e) => {
                println!("  ❌ {}", e);
                all_ok = false;
            }
        }
        println!();
    }

    // UniV4
    {
        let addr = BscContracts::UNISWAP_V4_POOL_MANAGER;
        println!("[UniswapV4 PoolManager] ({:?})", addr);
        let logs = client.get_logs(
            &Filter::new()
                .address(addr)
                .from_block(from)
                .to_block(to)
                .event_signature(V4Initialize::SIGNATURE_HASH),
        ).await;
        match logs {
            Ok(logs) if !logs.is_empty() => {
                println!("  ✅ {} events", logs.len());
                if let Ok(parsed) = V4Initialize::decode_log(&logs.last().unwrap().inner, true) {
                    println!("     sample: id={:?} c0={:?} c1={:?} fee={}",
                        parsed.id, parsed.currency0, parsed.currency1, parsed.fee);
                }
            }
            Ok(_) => { println!("  ❌ 0 events"); all_ok = false; }
            Err(e) => { println!("  ❌ {}", e); all_ok = false; }
        }
        println!();
    }

    // PCS V4 CL
    {
        let addr = BscContracts::PANCAKESWAP_V4_CL_POOL_MANAGER;
        println!("[PancakeSwap V4 CL PoolManager] ({:?})", addr);
        let logs = client.get_logs(
            &Filter::new()
                .address(addr)
                .from_block(from)
                .to_block(to)
                .event_signature(PcsV4ClInitialize::SIGNATURE_HASH),
        ).await;
        match logs {
            Ok(logs) if !logs.is_empty() => {
                println!("  ✅ {} events", logs.len());
                if let Ok(parsed) = PcsV4ClInitialize::decode_log(&logs.last().unwrap().inner, true) {
                    println!("     sample: id={:?} c0={:?} c1={:?} fee={}",
                        parsed.id, parsed.currency0, parsed.currency1, parsed.fee);
                }
            }
            Ok(_) => { println!("  ❌ 0 events"); all_ok = false; }
            Err(e) => { println!("  ❌ {}", e); all_ok = false; }
        }
        println!();
    }

    if all_ok {
        println!("✅ 全部 verify 通过");
        Ok(())
    } else {
        println!("❌ 部分失败");
        std::process::exit(1);
    }
}
