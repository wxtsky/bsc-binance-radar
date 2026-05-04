-- M2: 新策略「只监控项目方主池」schema 增量
-- 新增 main_pools 表索引 183 个 token 主池（每 token 1 行）
-- 不动现有 pools / v4_pools / swaps；后续 backfill / radar 只读 main_pools

-- dex enum 重新定义（旧 init.sql 第 13-18 行已陈旧）：
--   1 = uniswap-v3
--   2 = pancakeswap-v3
--   3 = uniswap-v4
--   4 = pancakeswap-v4-cl
--   5 = pancakeswap-v2  (升级为通用 V2，不再仅 BNB price 池)

CREATE TABLE IF NOT EXISTS main_pools (
  coin            VARCHAR(32) PRIMARY KEY,           -- 币安永续 baseAsset (e.g. BTC, ASTER, 1MBABYDOGE)
  token_addr      BYTEA NOT NULL,                    -- 20 bytes 该 token 在 BSC 上的合约
  pool_addr       BYTEA NOT NULL,                    -- 20 bytes (V2/V3) 或 32 bytes (V4 poolId)
  pool_addr_size  SMALLINT NOT NULL,                 -- 20 或 32，方便分辨
  dex             SMALLINT NOT NULL,                 -- 1..5 见上面注释
  base_addr       BYTEA NOT NULL,                    -- 20 bytes WBNB/USDT/USDC/0x0(native BNB)
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
