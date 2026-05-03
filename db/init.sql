-- bsc-binance-radar PostgreSQL schema
-- Bootstrapped by docker-entrypoint-initdb.d on first container start.
-- src/db/index.ts also runs CREATE TABLE IF NOT EXISTS at runtime, so this file
-- is not strictly required but keeps fresh containers self-sufficient.

CREATE TABLE IF NOT EXISTS pools (
  address TEXT NOT NULL,
  chain TEXT NOT NULL,
  dex TEXT NOT NULL,
  token0 TEXT NOT NULL,
  token1 TEXT NOT NULL,
  fee_tier INTEGER NOT NULL,
  created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
  PRIMARY KEY (address, chain)
);

CREATE TABLE IF NOT EXISTS v4_pools (
  pool_id TEXT NOT NULL,
  chain TEXT NOT NULL,
  currency0 TEXT NOT NULL,
  currency1 TEXT NOT NULL,
  fee INTEGER NOT NULL,
  tick_spacing INTEGER NOT NULL,
  hooks TEXT NOT NULL,
  created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT,
  PRIMARY KEY (pool_id, chain)
);

CREATE TABLE IF NOT EXISTS swaps (
  id BIGSERIAL PRIMARY KEY,
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
);
CREATE INDEX IF NOT EXISTS idx_swaps_pool_time ON swaps(pool_address, timestamp);
CREATE INDEX IF NOT EXISTS idx_swaps_time ON swaps(timestamp);
-- 已删：idx_swaps_chain_dex_time / idx_swaps_time_chain_valid
-- 这两个索引共 3.3GB，每条 swap INSERT 都要更新它们，但 detector 只查
-- token_1min_stats / pool_1min_stats，rebuild 只用 idx_swaps_time。
-- 删了之后 backfill INSERT 提速 ~30%（详见 HANDOFF § 16.G）。
-- 唯一约束：(tx_hash, pool_address, amount0, amount1) 是 EVM swap 在没存 log_index 情况下
-- 实际可达的最稳定唯一标识。同一 tx 同一 pool 完全相同 amount 出现两次的概率几乎为 0。
-- 配合 INSERT ... ON CONFLICT DO NOTHING 实现 backfill 与 stream 双写下的去重。
CREATE UNIQUE INDEX IF NOT EXISTS uq_swaps_dedup
  ON swaps(tx_hash, pool_address, amount0, amount1);

CREATE TABLE IF NOT EXISTS pool_1min_stats (
  pool_address TEXT NOT NULL,
  chain TEXT NOT NULL,
  bucket_start BIGINT NOT NULL,
  total_fees_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  total_volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  swap_count INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (pool_address, chain, bucket_start)
);
CREATE INDEX IF NOT EXISTS idx_pool_1min_stats_bucket ON pool_1min_stats(bucket_start);

CREATE TABLE IF NOT EXISTS token_1min_stats (
  token_address TEXT NOT NULL,
  chain TEXT NOT NULL,
  bucket_start BIGINT NOT NULL,
  total_volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  total_fees_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  swap_count INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (token_address, chain, bucket_start)
);
CREATE INDEX IF NOT EXISTS idx_token_1min_stats_bucket ON token_1min_stats(bucket_start);

CREATE TABLE IF NOT EXISTS binance_bsc_tokens (
  contract_address TEXT PRIMARY KEY,
  symbol TEXT NOT NULL,
  base_asset TEXT NOT NULL,
  decimals INTEGER NOT NULL DEFAULT 18,
  updated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS anomaly_events (
  id BIGSERIAL PRIMARY KEY,
  token_address TEXT NOT NULL,
  symbol TEXT,
  rule TEXT NOT NULL,
  metrics JSONB NOT NULL,
  detected_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_anomaly_events_token_time
  ON anomaly_events(token_address, detected_at);

-- BNB/USD 历史价：单独表，由 PancakeV2 WBNB/USDT 池（CONTRACTS.bsc.bnbPricePool）的
-- 每笔 swap 反算得出。给非 USDT 池的 swap 重算 volume_usd 时按 timestamp 二分查找。
CREATE TABLE IF NOT EXISTS bnb_price_history (
  timestamp BIGINT NOT NULL,
  price_usd DOUBLE PRECISION NOT NULL,
  block_number BIGINT NOT NULL,
  tx_hash TEXT NOT NULL,
  log_index INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_bnb_price_time ON bnb_price_history(timestamp);
-- 同一 tx 可能有多笔 swap（multi-hop），用 tx_hash + log_index 去重
CREATE UNIQUE INDEX IF NOT EXISTS uq_bnb_price_dedup
  ON bnb_price_history(tx_hash, log_index);
