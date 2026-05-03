use crate::types::{BinanceBscToken, BnbPricePoint, DexType, PoolInfo, SwapRecord, V4PoolInfo};
use anyhow::{Context, Result};
use futures::pin_mut;
use postgres_types::Type;
use tokio_postgres::binary_copy::BinaryCopyInWriter;

use super::get_pool;

// ============== POOLS ==============

pub async fn bulk_upsert_pools(pools: &[PoolInfo]) -> Result<()> {
    if pools.is_empty() {
        return Ok(());
    }
    let client = get_pool().get().await?;

    // 分批：每批 1000，避免单 SQL 参数过多
    for chunk in pools.chunks(1000) {
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            Vec::with_capacity(chunk.len() * 6);
        let mut placeholders: Vec<String> = Vec::with_capacity(chunk.len());

        // pre-collect dex strs
        let dex_strs: Vec<String> = chunk.iter().map(|p| p.dex.as_db_str().to_string()).collect();

        for (i, p) in chunk.iter().enumerate() {
            let base = i * 6;
            placeholders.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${})",
                base + 1, base + 2, base + 3, base + 4, base + 5, base + 6
            ));
            params.push(&p.address);
            params.push(&p.chain);
            params.push(&dex_strs[i]);
            params.push(&p.token0);
            params.push(&p.token1);
            params.push(&p.fee_tier);
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

        client.execute(&sql, &params).await
            .context("bulk_upsert_pools failed")?;
    }
    Ok(())
}

pub async fn bulk_upsert_v4_pools(pools: &[V4PoolInfo]) -> Result<()> {
    if pools.is_empty() {
        return Ok(());
    }
    let client = get_pool().get().await?;

    for chunk in pools.chunks(1000) {
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            Vec::with_capacity(chunk.len() * 7);
        let mut placeholders: Vec<String> = Vec::with_capacity(chunk.len());

        for (i, p) in chunk.iter().enumerate() {
            let base = i * 7;
            placeholders.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${})",
                base + 1, base + 2, base + 3, base + 4, base + 5, base + 6, base + 7
            ));
            params.push(&p.pool_id);
            params.push(&p.chain);
            params.push(&p.currency0);
            params.push(&p.currency1);
            params.push(&p.fee);
            params.push(&p.tick_spacing);
            params.push(&p.hooks);
        }

        let sql = format!(
            "INSERT INTO v4_pools (pool_id, chain, currency0, currency1, fee, tick_spacing, hooks) VALUES {}
             ON CONFLICT (pool_id, chain) DO UPDATE SET
               currency0 = EXCLUDED.currency0,
               currency1 = EXCLUDED.currency1,
               fee = EXCLUDED.fee,
               tick_spacing = EXCLUDED.tick_spacing,
               hooks = EXCLUDED.hooks",
            placeholders.join(",")
        );

        client.execute(&sql, &params).await
            .context("bulk_upsert_v4_pools failed")?;
    }
    Ok(())
}

pub async fn select_pool_addresses_by_dex(chain: &str, dex_str: &str) -> Result<Vec<String>> {
    let client = get_pool().get().await?;
    let rows = client.query(
        "SELECT address FROM pools WHERE chain = $1 AND dex = $2",
        &[&chain, &dex_str],
    ).await?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

pub async fn select_v4_pool_ids_by_namespace(chain: &str, with_pcs_prefix: bool) -> Result<Vec<String>> {
    let client = get_pool().get().await?;
    let sql = if with_pcs_prefix {
        "SELECT pool_id FROM v4_pools WHERE chain = $1 AND pool_id LIKE 'pcsv4cl:%'"
    } else {
        "SELECT pool_id FROM v4_pools WHERE chain = $1 AND pool_id NOT LIKE 'pcsv4cl:%'"
    };
    let rows = client.query(sql, &[&chain]).await?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

pub async fn get_pool_info(address: &str, chain: &str) -> Result<Option<PoolInfo>> {
    let client = get_pool().get().await?;
    let row = client.query_opt(
        "SELECT address, chain, dex, token0, token1, fee_tier FROM pools WHERE address = $1 AND chain = $2",
        &[&address, &chain],
    ).await?;
    Ok(row.map(|r| PoolInfo {
        address: r.get(0),
        chain: r.get(1),
        dex: match r.get::<_, String>(2).as_str() {
            "uniswap-v3" => DexType::UniswapV3,
            "pancakeswap-v3" => DexType::PancakeswapV3,
            "uniswap-v4" => DexType::UniswapV4,
            "pancakeswap-v4-cl" => DexType::PancakeswapV4Cl,
            _ => DexType::PancakeswapV2BnbPrice,
        },
        token0: r.get(3),
        token1: r.get(3),
        fee_tier: r.get(5),
    }))
}

pub async fn get_v4_pool_info(pool_id: &str, chain: &str) -> Result<Option<V4PoolInfo>> {
    let client = get_pool().get().await?;
    let row = client.query_opt(
        "SELECT pool_id, chain, currency0, currency1, fee, tick_spacing, hooks FROM v4_pools WHERE pool_id = $1 AND chain = $2",
        &[&pool_id, &chain],
    ).await?;
    Ok(row.map(|r| V4PoolInfo {
        pool_id: r.get(0),
        chain: r.get(1),
        currency0: r.get(2),
        currency1: r.get(3),
        fee: r.get(4),
        tick_spacing: r.get(5),
        hooks: r.get(6),
    }))
}

// ============== TOKENS（白名单）==============

pub async fn select_all_binance_bsc_tokens() -> Result<Vec<BinanceBscToken>> {
    let client = get_pool().get().await?;
    let rows = client.query(
        "SELECT contract_address, symbol, base_asset, decimals, updated_at FROM binance_bsc_tokens",
        &[],
    ).await?;
    Ok(rows.iter().map(|r| BinanceBscToken {
        contract_address: r.get(0),
        symbol: r.get(1),
        base_asset: r.get(2),
        decimals: r.get(3),
        updated_at: r.get(4),
    }).collect())
}

pub async fn upsert_binance_bsc_token(token: &BinanceBscToken) -> Result<()> {
    let client = get_pool().get().await?;
    client.execute(
        "INSERT INTO binance_bsc_tokens (contract_address, symbol, base_asset, decimals, updated_at)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (contract_address) DO UPDATE SET
           symbol = EXCLUDED.symbol,
           base_asset = EXCLUDED.base_asset,
           decimals = EXCLUDED.decimals,
           updated_at = EXCLUDED.updated_at",
        &[&token.contract_address, &token.symbol, &token.base_asset, &token.decimals, &token.updated_at],
    ).await?;
    Ok(())
}

// ============== STAGING（binary COPY 高速通道）==============

pub async fn create_staging_table() -> Result<()> {
    let client = get_pool().get().await?;
    client.simple_query(
        "CREATE UNLOGGED TABLE IF NOT EXISTS swaps_staging (
            pool_address TEXT NOT NULL,
            chain TEXT NOT NULL,
            dex TEXT NOT NULL,
            tx_hash TEXT NOT NULL,
            amount0 TEXT NOT NULL,
            amount1 TEXT NOT NULL,
            fee_usd DOUBLE PRECISION NOT NULL,
            volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
            timestamp BIGINT NOT NULL,
            block_number BIGINT NOT NULL
        )"
    ).await?;
    Ok(())
}

/// 批量写 swaps_staging（binary COPY 协议，比 unnest INSERT 快 10-50x）
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
            Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT,
            Type::TEXT, Type::TEXT,
            Type::FLOAT8, Type::FLOAT8,
            Type::INT8, Type::INT8,
        ],
    );
    pin_mut!(writer);

    for s in swaps {
        let dex_str = s.dex.as_db_str();
        writer.as_mut().write(&[
            &s.pool_address,
            &s.chain,
            &dex_str,
            &s.tx_hash,
            &s.amount0,
            &s.amount1,
            &s.fee_usd,
            &s.volume_usd,
            &s.timestamp,
            &s.block_number,
        ]).await?;
    }
    writer.finish().await?;
    Ok(())
}

/// 批量写 bnb_price_history（binary COPY）
pub async fn bulk_insert_bnb_prices(rows: &[BnbPricePoint]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let client = get_pool().get().await?;

    // 用 unnest INSERT + ON CONFLICT 因为 bnb_price_history 有 unique (tx_hash, log_index)
    // binary COPY 不支持 ON CONFLICT，重复插会报错。这表数据量小可接受非 COPY。
    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        Vec::with_capacity(rows.len() * 5);
    let mut placeholders: Vec<String> = Vec::with_capacity(rows.len());

    for (i, p) in rows.iter().enumerate() {
        let base = i * 5;
        placeholders.push(format!(
            "(${}, ${}, ${}, ${}, ${})",
            base + 1, base + 2, base + 3, base + 4, base + 5
        ));
        params.push(&p.timestamp);
        params.push(&p.price_usd);
        params.push(&p.block_number);
        params.push(&p.tx_hash);
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
/// - 单大事务 INSERT，PG hash semi-join + ON CONFLICT DO NOTHING 兜底
/// - 完成后 TRUNCATE staging
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

    // DISTINCT ON 处理 staging 内重复，ON CONFLICT 兜底跨 stream/backfill 重复
    let res = client.execute(
        "INSERT INTO swaps (pool_address, chain, dex, tx_hash, amount0, amount1, fee_usd, volume_usd, timestamp, block_number)
         SELECT DISTINCT ON (tx_hash, pool_address, amount0, amount1, timestamp)
                pool_address, chain, dex, tx_hash, amount0, amount1, fee_usd, volume_usd, timestamp, block_number
         FROM swaps_staging
         ORDER BY tx_hash, pool_address, amount0, amount1, timestamp, block_number
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
    // 用单个 client 跑事务
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
             SELECT address AS pool_id, chain, LOWER(token0) AS t0, LOWER(token1) AS t1 FROM pools
             UNION ALL
             SELECT pool_id, chain, LOWER(currency0), LOWER(currency1) FROM v4_pools
         ),
         pool_target AS (
             SELECT ap.pool_id, ap.chain, LOWER(bt.contract_address) AS target_token
             FROM active_pools ap
             JOIN binance_bsc_tokens bt
               ON ap.t0 = LOWER(bt.contract_address)
               OR ap.t1 = LOWER(bt.contract_address)
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
