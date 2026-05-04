-- bsc-binance-radar PostgreSQL schema (BYTEA + NUMERIC 优化版)
-- 主要变化（vs 旧 TEXT schema）：
--   - 地址 / hash / pool_id 全 BYTEA（20/32 bytes vs hex TEXT 42/66 chars）
--   - amount0 / amount1 NUMERIC（精确 signed decimal vs TEXT decimal string）
--   - chain / dex 改 SMALLINT 枚举（vs TEXT）
-- 实测每行从 ~700 bytes 降到 ~200 bytes（含索引），3.5x 缩水。

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- chain enum（跟 src/types.rs 对应）
-- 1 = bsc

-- dex enum
-- 1 = uniswap-v3
-- 2 = pancakeswap-v3                    （BNB price 池也走 dex=2，PCS V3 WBNB/USDT 0.01% tier）
-- 3 = uniswap-v4
-- 4 = pancakeswap-v4-cl
-- 5 = pancakeswap-v2                    （通用，36 个主池监控；不再仅 BNB price）

CREATE TABLE IF NOT EXISTS pools (
  address BYTEA NOT NULL,        -- 20 bytes EVM address
  chain SMALLINT NOT NULL,
  dex SMALLINT NOT NULL,
  token0 BYTEA NOT NULL,         -- 20 bytes
  token1 BYTEA NOT NULL,         -- 20 bytes
  fee_tier INTEGER NOT NULL,
  created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
  PRIMARY KEY (address, chain)
);

CREATE TABLE IF NOT EXISTS v4_pools (
  pool_id BYTEA NOT NULL,        -- 32 bytes V4 PoolKey hash
  chain SMALLINT NOT NULL,
  dex SMALLINT NOT NULL,         -- 3=uniswap-v4 / 4=pancakeswap-v4-cl
  currency0 BYTEA NOT NULL,      -- 20 bytes
  currency1 BYTEA NOT NULL,
  fee INTEGER NOT NULL,
  tick_spacing INTEGER NOT NULL,
  hooks BYTEA NOT NULL,          -- 20 bytes
  created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
  PRIMARY KEY (pool_id, chain, dex)
);

CREATE TABLE IF NOT EXISTS swaps (
  id BIGSERIAL,
  pool_address BYTEA NOT NULL,   -- V3: 20 bytes; V4: 32 bytes hash（同列变长存两种）
  chain SMALLINT NOT NULL,
  dex SMALLINT NOT NULL,
  tx_hash BYTEA NOT NULL,        -- 32 bytes
  amount0 BYTEA NOT NULL,        -- 32 bytes (I256 big-endian signed)
  amount1 BYTEA NOT NULL,
  fee_usd DOUBLE PRECISION NOT NULL,
  volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  timestamp BIGINT NOT NULL,
  block_number BIGINT NOT NULL,
  PRIMARY KEY (id, "timestamp")
);
CREATE INDEX IF NOT EXISTS idx_swaps_time ON swaps(timestamp);
CREATE UNIQUE INDEX IF NOT EXISTS uq_swaps_dedup
  ON swaps(tx_hash, pool_address, amount0, amount1, "timestamp");

-- TimescaleDB hypertable，每 7d 一个 chunk
SELECT create_hypertable(
  'swaps', 'timestamp',
  chunk_time_interval => bigint '604800000',
  if_not_exists => true
);

CREATE TABLE IF NOT EXISTS pool_1min_stats (
  pool_address BYTEA NOT NULL,
  chain SMALLINT NOT NULL,
  bucket_start BIGINT NOT NULL,
  total_fees_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  total_volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  swap_count INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (pool_address, chain, bucket_start)
);
CREATE INDEX IF NOT EXISTS idx_pool_1min_stats_bucket ON pool_1min_stats(bucket_start);

CREATE TABLE IF NOT EXISTS token_1min_stats (
  token_address BYTEA NOT NULL,  -- 20 bytes
  chain SMALLINT NOT NULL,
  bucket_start BIGINT NOT NULL,
  total_volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  total_fees_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  swap_count INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (token_address, chain, bucket_start)
);
CREATE INDEX IF NOT EXISTS idx_token_1min_stats_bucket ON token_1min_stats(bucket_start);

CREATE TABLE IF NOT EXISTS binance_bsc_tokens (
  contract_address BYTEA PRIMARY KEY,  -- 20 bytes
  symbol TEXT NOT NULL,
  base_asset TEXT NOT NULL,
  decimals INTEGER NOT NULL DEFAULT 18,
  updated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS anomaly_events (
  id BIGSERIAL PRIMARY KEY,
  token_address BYTEA NOT NULL,
  symbol TEXT,
  rule SMALLINT NOT NULL,        -- 1=vol_spike / 2=combo
  metrics JSONB NOT NULL,
  detected_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_anomaly_events_token_time
  ON anomaly_events(token_address, detected_at);

CREATE TABLE IF NOT EXISTS bnb_price_history (
  timestamp BIGINT NOT NULL,
  price_usd DOUBLE PRECISION NOT NULL,
  block_number BIGINT NOT NULL,
  tx_hash BYTEA NOT NULL,        -- 32 bytes
  log_index INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_bnb_price_time ON bnb_price_history(timestamp);
CREATE UNIQUE INDEX IF NOT EXISTS uq_bnb_price_dedup
  ON bnb_price_history(tx_hash, log_index);

-- main_pools：新策略「每 token 一个主池」索引表
-- 由 scripts/probe-main-pools.ts + gen-main-pools-sql.ts 离线生成数据，定期 refresh
-- backfill / stream 直接 SELECT 这张表加载主池，不再扫 PoolCreated 全集
CREATE TABLE IF NOT EXISTS main_pools (
  coin            VARCHAR(32) PRIMARY KEY,           -- 币安永续 baseAsset (e.g. BTC, ASTER, 1MBABYDOGE)
  token_addr      BYTEA NOT NULL,                    -- 20 bytes 该 token 在 BSC 上的合约
  pool_addr       BYTEA NOT NULL,                    -- 20 bytes (V2/V3) 或 32 bytes (V4 poolId)
  pool_addr_size  SMALLINT NOT NULL,                 -- 20 或 32，方便分辨
  dex             SMALLINT NOT NULL,                 -- 1..5 见上面注释
  base_addr       BYTEA NOT NULL,                    -- 20 bytes WBNB/USDT/USDC/0x0(V4 native BNB)
  base_sym        VARCHAR(10) NOT NULL,              -- WBNB / USDT / USDC / BNB
  fee_bps         INTEGER,                           -- V3/V4 fee tier in basis points (e.g. 100=0.01%)
  tvl_usd         NUMERIC,                           -- OKX top-liquidity 给的 TVL 快照（refresh 时更新）
  is_native_bnb   BOOLEAN NOT NULL DEFAULT FALSE,    -- V4 native BNB 池（base_addr=0x000...000）
  refreshed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (token_addr)
);
CREATE INDEX IF NOT EXISTS idx_main_pools_dex ON main_pools(dex);
CREATE INDEX IF NOT EXISTS idx_main_pools_pool_addr ON main_pools(pool_addr);
COMMENT ON TABLE main_pools IS '币安永续 BSC token 的项目方主池索引（每 token 一行，TVL 最大池）';
