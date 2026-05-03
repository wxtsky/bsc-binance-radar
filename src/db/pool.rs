use anyhow::{Context, Result};
use deadpool_postgres::{Config, Pool, Runtime};
use once_cell::sync::OnceCell;
use std::str::FromStr;
use tokio_postgres::NoTls;

pub type DbPool = Pool;

static POOL: OnceCell<Pool> = OnceCell::new();

/// 初始化全局 PG 连接池。从 DATABASE_URL 解析。
pub async fn init_pool() -> Result<&'static Pool> {
    if let Some(p) = POOL.get() {
        return Ok(p);
    }
    let url = std::env::var("DATABASE_URL")
        .context("DATABASE_URL not set")?;
    let pg_cfg = tokio_postgres::Config::from_str(&url)
        .context("invalid DATABASE_URL")?;

    let mut cfg = Config::new();
    cfg.host = pg_cfg.get_hosts().first().and_then(|h| match h {
        tokio_postgres::config::Host::Tcp(s) => Some(s.clone()),
        _ => None,
    });
    cfg.port = pg_cfg.get_ports().first().copied();
    cfg.user = pg_cfg.get_user().map(|s| s.to_string());
    cfg.password = pg_cfg
        .get_password()
        .map(|p| String::from_utf8_lossy(p).to_string());
    cfg.dbname = pg_cfg.get_dbname().map(|s| s.to_string());

    // 默认 PG_POOL_MAX=40（HANDOFF：8 worker × 4 INSERT 需 32+ 连接）
    let max_size: usize = std::env::var("PG_POOL_MAX")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(40);
    cfg.pool = Some(deadpool_postgres::PoolConfig::new(max_size));

    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)
        .context("failed to create PG pool")?;

    // 探活
    {
        let client = pool.get().await.context("PG connect probe failed")?;
        client.simple_query("SELECT 1").await
            .context("PG SELECT 1 failed")?;
    }

    let _ = POOL.set(pool);
    Ok(POOL.get().expect("just set"))
}

pub fn get_pool() -> &'static Pool {
    POOL.get().expect("PG pool not initialized; call init_pool() first")
}

/// 一次性 schema 迁移（旧库兼容）：补 timescaledb 扩展、hypertable、索引。
/// 新库由 db/init.sql 处理（docker-entrypoint），此函数仅保护 in-place 升级。
pub async fn ensure_schema() -> Result<()> {
    let client = get_pool().get().await?;

    // 这里不主动创建表（init.sql 已建）；但确保 timescale 扩展存在
    client.simple_query("CREATE EXTENSION IF NOT EXISTS timescaledb").await
        .context("create timescaledb ext failed")?;

    Ok(())
}
