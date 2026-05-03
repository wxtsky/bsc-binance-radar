use crate::types::{BinanceBscToken, BnbPricePoint, ChainId, DexType, PoolInfo, SwapRecord, V4PoolInfo, CHAIN_BSC};
use alloy::primitives::{Address, B256, I256};
use anyhow::{Context, Result};
use futures::pin_mut;
use postgres_types::Type;
use std::collections::HashMap;
use tokio_postgres::binary_copy::BinaryCopyInWriter;

use super::get_pool;

// ============== POOLS ==============

pub async fn bulk_upsert_pools(pools: &[PoolInfo]) -> Result<()> {
    if pools.is_empty() {
        return Ok(());
    }
    let client = get_pool().get().await?;

    for chunk in pools.chunks(1000) {
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::with_capacity(chunk.len() * 6);
        let mut placeholders: Vec<String> = Vec::with_capacity(chunk.len());

        let addr_bytes: Vec<&[u8]> = chunk.iter().map(|p| p.address.as_slice()).collect();
        let t0_bytes: Vec<&[u8]> = chunk.iter().map(|p| p.token0.as_slice()).collect();
        let t1_bytes: Vec<&[u8]> = chunk.iter().map(|p| p.token1.as_slice()).collect();
        let dex_smallints: Vec<i16> = chunk.iter().map(|p| p.dex.as_db_smallint()).collect();

        for i in 0..chunk.len() {
            let base = i * 6;
            placeholders.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${})",
                base + 1, base + 2, base + 3, base + 4, base + 5, base + 6
            ));
            params.push(&addr_bytes[i]);
            params.push(&chunk[i].chain);
            params.push(&dex_smallints[i]);
            params.push(&t0_bytes[i]);
            params.push(&t1_bytes[i]);
            params.push(&chunk[i].fee_tier);
        }

        let sql = format!(
            "INSERT INTO pools (address, chain, dex, token0, token1, fee_tier) VALUES {}
             ON CONFLICT (address, chain) DO UPDATE SET
               dex = EXCLUDED.dex,
               token0 = EXCLUDED.token0,
               token1 = EXCLUDED.token1,
               fee_tier = EXCLUDED.fee_tier",
            placeholders.join(",")
        );
        client.execute(&sql, &params).await.context("bulk_upsert_pools failed")?;
    }
    Ok(())
}

pub async fn bulk_upsert_v4_pools(pools: &[V4PoolInfo]) -> Result<()> {
    if pools.is_empty() {
        return Ok(());
    }
    let client = get_pool().get().await?;

    for chunk in pools.chunks(1000) {
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::with_capacity(chunk.len() * 8);
        let mut placeholders: Vec<String> = Vec::with_capacity(chunk.len());

        let id_bytes: Vec<&[u8]> = chunk.iter().map(|p| p.pool_id.as_slice()).collect();
        let dex_smallints: Vec<i16> = chunk.iter().map(|p| p.dex.as_db_smallint()).collect();
        let c0_bytes: Vec<&[u8]> = chunk.iter().map(|p| p.currency0.as_slice()).collect();
        let c1_bytes: Vec<&[u8]> = chunk.iter().map(|p| p.currency1.as_slice()).collect();
        let hooks_bytes: Vec<&[u8]> = chunk.iter().map(|p| p.hooks.as_slice()).collect();

        for i in 0..chunk.len() {
            let base = i * 8;
            placeholders.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                base + 1, base + 2, base + 3, base + 4, base + 5, base + 6, base + 7, base + 8
            ));
            params.push(&id_bytes[i]);
            params.push(&chunk[i].chain);
            params.push(&dex_smallints[i]);
            params.push(&c0_bytes[i]);
            params.push(&c1_bytes[i]);
            params.push(&chunk[i].fee);
            params.push(&chunk[i].tick_spacing);
            params.push(&hooks_bytes[i]);
        }

        let sql = format!(
            "INSERT INTO v4_pools (pool_id, chain, dex, currency0, currency1, fee, tick_spacing, hooks) VALUES {}
             ON CONFLICT (pool_id, chain, dex) DO UPDATE SET
               currency0 = EXCLUDED.currency0,
               currency1 = EXCLUDED.currency1,
               fee = EXCLUDED.fee,
               tick_spacing = EXCLUDED.tick_spacing,
               hooks = EXCLUDED.hooks",
            placeholders.join(",")
        );
        client.execute(&sql, &params).await.context("bulk_upsert_v4_pools failed")?;
    }
    Ok(())
}

/// 加载所有 V3 / PCS V3 池子到 HashMap<Address, PoolMeta>
pub async fn load_v3_pool_cache(chain: ChainId, dex: DexType) -> Result<HashMap<Address, (Address, Address, u32)>> {
    let client = get_pool().get().await?;
    let dex_smallint = dex.as_db_smallint();
    let rows = client.query(
        "SELECT address, token0, token1, fee_tier FROM pools WHERE chain=$1 AND dex=$2",
        &[&chain, &dex_smallint],
    ).await?;

    let mut map = HashMap::with_capacity(rows.len());
    for r in &rows {
        let addr_bytes: &[u8] = r.get(0);
        let t0_bytes: &[u8] = r.get(1);
        let t1_bytes: &[u8] = r.get(2);
        let fee: i32 = r.get(3);
        if addr_bytes.len() == 20 && t0_bytes.len() == 20 && t1_bytes.len() == 20 {
            let addr = Address::from_slice(addr_bytes);
            let t0 = Address::from_slice(t0_bytes);
            let t1 = Address::from_slice(t1_bytes);
            map.insert(addr, (t0, t1, fee as u32));
        }
    }
    Ok(map)
}

/// 加载所有 V4 / PCS V4 CL 池子到 HashMap<B256, (currency0, currency1, fee)>
pub async fn load_v4_pool_cache(chain: ChainId, dex: DexType) -> Result<HashMap<B256, (Address, Address, u32)>> {
    let client = get_pool().get().await?;
    let dex_smallint = dex.as_db_smallint();
    let rows = client.query(
        "SELECT pool_id, currency0, currency1, fee FROM v4_pools WHERE chain=$1 AND dex=$2",
        &[&chain, &dex_smallint],
    ).await?;

    let mut map = HashMap::with_capacity(rows.len());
    for r in &rows {
        let id_bytes: &[u8] = r.get(0);
        let c0_bytes: &[u8] = r.get(1);
        let c1_bytes: &[u8] = r.get(2);
        let fee: i32 = r.get(3);
        if id_bytes.len() == 32 && c0_bytes.len() == 20 && c1_bytes.len() == 20 {
            let id = B256::from_slice(id_bytes);
            let c0 = Address::from_slice(c0_bytes);
            let c1 = Address::from_slice(c1_bytes);
            map.insert(id, (c0, c1, fee as u32));
        }
    }
    Ok(map)
}

/// 拿所有 V3 / PCS V3 pool addresses (used as getLogs address filter)
pub async fn select_pool_addresses_by_dex(chain: ChainId, dex: DexType) -> Result<Vec<Address>> {
    let client = get_pool().get().await?;
    let dex_smallint = dex.as_db_smallint();
    let rows = client.query(
        "SELECT address FROM pools WHERE chain=$1 AND dex=$2",
        &[&chain, &dex_smallint],
    ).await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in &rows {
        let bytes: &[u8] = r.get(0);
        if bytes.len() == 20 {
            out.push(Address::from_slice(bytes));
        }
    }
    Ok(out)
}

/// 拿所有 V4 / PCS V4 CL pool ids (used as getLogs args.id filter)
pub async fn select_v4_pool_ids(chain: ChainId, dex: DexType) -> Result<Vec<B256>> {
    let client = get_pool().get().await?;
    let dex_smallint = dex.as_db_smallint();
    let rows = client.query(
        "SELECT pool_id FROM v4_pools WHERE chain=$1 AND dex=$2",
        &[&chain, &dex_smallint],
    ).await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in &rows {
        let bytes: &[u8] = r.get(0);
        if bytes.len() == 32 {
            out.push(B256::from_slice(bytes));
        }
    }
    Ok(out)
}

// ============== TOKENS（白名单）==============

pub async fn select_all_binance_bsc_tokens() -> Result<Vec<BinanceBscToken>> {
    let client = get_pool().get().await?;
    let rows = client.query(
        "SELECT contract_address, symbol, base_asset, decimals, updated_at FROM binance_bsc_tokens",
        &[],
    ).await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in &rows {
        let bytes: &[u8] = r.get(0);
        if bytes.len() != 20 {
            continue;
        }
        out.push(BinanceBscToken {
            contract_address: Address::from_slice(bytes),
            symbol: r.get(1),
            base_asset: r.get(2),
            decimals: r.get(3),
            updated_at: r.get(4),
        });
    }
    Ok(out)
}

pub async fn upsert_binance_bsc_token(token: &BinanceBscToken) -> Result<()> {
    let client = get_pool().get().await?;
    let addr_bytes = token.contract_address.as_slice();
    client.execute(
        "INSERT INTO binance_bsc_tokens (contract_address, symbol, base_asset, decimals, updated_at)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (contract_address) DO UPDATE SET
           symbol = EXCLUDED.symbol,
           base_asset = EXCLUDED.base_asset,
           decimals = EXCLUDED.decimals,
           updated_at = EXCLUDED.updated_at",
        &[&addr_bytes, &token.symbol, &token.base_asset, &token.decimals, &token.updated_at],
    ).await?;
    Ok(())
}

// ============== STAGING（binary COPY 高速通道）==============

pub async fn create_staging_table() -> Result<()> {
    let client = get_pool().get().await?;
    client.simple_query(
        "CREATE UNLOGGED TABLE IF NOT EXISTS swaps_staging (
            pool_address BYTEA NOT NULL,
            chain SMALLINT NOT NULL,
            dex SMALLINT NOT NULL,
            tx_hash BYTEA NOT NULL,
            amount0 BYTEA NOT NULL,
            amount1 BYTEA NOT NULL,
            fee_usd DOUBLE PRECISION NOT NULL,
            volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
            timestamp BIGINT NOT NULL,
            block_number BIGINT NOT NULL
        )"
    ).await?;
    Ok(())
}

/// 批量写 swaps_staging（binary COPY 协议，所有列原生 binary 支持）
pub async fn bulk_insert_swaps_staging(swaps: &[SwapRecord]) -> Result<()> {
    if swaps.is_empty() {
        return Ok(());
    }
    let client = get_pool().get().await?;
    let sink = client.copy_in(
        "COPY swaps_staging (pool_address, chain, dex, tx_hash, amount0, amount1, fee_usd, volume_usd, timestamp, block_number) FROM STDIN BINARY"
    ).await?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            Type::BYTEA, Type::INT2, Type::INT2, Type::BYTEA,
            Type::BYTEA, Type::BYTEA,           // amount0/1 改 BYTEA(32) i256 raw
            Type::FLOAT8, Type::FLOAT8,
            Type::INT8, Type::INT8,
        ],
    );
    pin_mut!(writer);

    for s in swaps {
        let dex_smallint = s.dex.as_db_smallint();
        let amount0_bytes = s.amount0.to_be_bytes::<32>();
        let amount1_bytes = s.amount1.to_be_bytes::<32>();
        let pool_addr_slice: &[u8] = s.pool_address.as_slice();
        let tx_hash_slice: &[u8] = s.tx_hash.as_slice();
        let amount0_slice: &[u8] = &amount0_bytes;
        let amount1_slice: &[u8] = &amount1_bytes;

        writer.as_mut().write(&[
            &pool_addr_slice,
            &s.chain,
            &dex_smallint,
            &tx_hash_slice,
            &amount0_slice,
            &amount1_slice,
            &s.fee_usd,
            &s.volume_usd,
            &s.timestamp,
            &s.block_number,
        ]).await?;
    }
    writer.finish().await?;
    Ok(())
}

/// 批量写 bnb_price_history
pub async fn bulk_insert_bnb_prices(rows: &[BnbPricePoint]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let client = get_pool().get().await?;

    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::with_capacity(rows.len() * 5);
    let mut placeholders: Vec<String> = Vec::with_capacity(rows.len());
    let tx_bytes: Vec<&[u8]> = rows.iter().map(|p| p.tx_hash.as_slice()).collect();

    for (i, p) in rows.iter().enumerate() {
        let base = i * 5;
        placeholders.push(format!(
            "(${}, ${}, ${}, ${}, ${})",
            base + 1, base + 2, base + 3, base + 4, base + 5
        ));
        params.push(&p.timestamp);
        params.push(&p.price_usd);
        params.push(&p.block_number);
        params.push(&tx_bytes[i]);
        params.push(&p.log_index);
    }

    let sql = format!(
        "INSERT INTO bnb_price_history (timestamp, price_usd, block_number, tx_hash, log_index) VALUES {}
         ON CONFLICT (tx_hash, log_index) DO NOTHING",
        placeholders.join(",")
    );
    client.execute(&sql, &params).await?;
    Ok(())
}

/// 把 swaps_staging 数据 migrate 到 swaps（hypertable）
pub struct MigrateResult {
    pub staged: i64,
    pub inserted: i64,
}

pub async fn migrate_staging_to_swaps() -> Result<MigrateResult> {
    let client = get_pool().get().await?;

    let staged: i64 = client
        .query_one("SELECT COUNT(*)::BIGINT FROM swaps_staging", &[])
        .await?
        .get(0);

    if staged == 0 {
        return Ok(MigrateResult { staged: 0, inserted: 0 });
    }

    let res = client.execute(
        "INSERT INTO swaps (pool_address, chain, dex, tx_hash, amount0, amount1, fee_usd, volume_usd, timestamp, block_number)
         SELECT pool_address, chain, dex, tx_hash, amount0, amount1, fee_usd, volume_usd, timestamp, block_number
         FROM swaps_staging
         ON CONFLICT (tx_hash, pool_address, amount0, amount1, timestamp) DO NOTHING",
        &[],
    ).await?;

    client.execute("TRUNCATE swaps_staging", &[]).await?;

    Ok(MigrateResult {
        staged,
        inserted: res as i64,
    })
}

// ============== 1min buckets rebuild ==============

pub async fn rebuild_buckets_from_swaps(_range_start_ms: i64) -> Result<()> {
    let mut conn = get_pool().get().await?;
    let txn = conn.transaction().await?;

    txn.simple_query("LOCK TABLE pool_1min_stats IN EXCLUSIVE MODE").await?;
    txn.simple_query("DELETE FROM pool_1min_stats").await?;
    txn.execute(
        "INSERT INTO pool_1min_stats (pool_address, chain, bucket_start, total_fees_usd, total_volume_usd, swap_count)
         SELECT pool_address, chain,
                (timestamp / 60000) * 60000 AS bucket_start,
                SUM(fee_usd),
                SUM(volume_usd),
                COUNT(*)::INT
         FROM swaps
         GROUP BY pool_address, chain, (timestamp / 60000) * 60000",
        &[],
    ).await?;

    txn.simple_query("LOCK TABLE token_1min_stats IN EXCLUSIVE MODE").await?;
    txn.simple_query("DELETE FROM token_1min_stats").await?;
    txn.execute(
        "WITH active_pools AS (
             SELECT address AS pool_id, chain, token0 AS t0, token1 AS t1 FROM pools
             UNION ALL
             SELECT pool_id, chain, currency0, currency1 FROM v4_pools
         ),
         pool_target AS (
             SELECT ap.pool_id, ap.chain, bt.contract_address AS target_token
             FROM active_pools ap
             JOIN binance_bsc_tokens bt
               ON ap.t0 = bt.contract_address
               OR ap.t1 = bt.contract_address
         )
         INSERT INTO token_1min_stats (token_address, chain, bucket_start, total_volume_usd, total_fees_usd, swap_count)
         SELECT pt.target_token, s.chain,
                (s.timestamp / 60000) * 60000,
                SUM(s.volume_usd),
                SUM(s.fee_usd),
                COUNT(*)::INT
         FROM swaps s
         JOIN pool_target pt ON pt.pool_id = s.pool_address AND pt.chain = s.chain
         GROUP BY pt.target_token, s.chain, (s.timestamp / 60000) * 60000",
        &[],
    ).await?;

    txn.commit().await?;
    Ok(())
}
