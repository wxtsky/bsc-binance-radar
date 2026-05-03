#!/bin/bash
# mid-flight migrate helper：staging → swaps，每 30 分钟跑一次
# 在 backfill 仍在写 staging 时跑（不冲突，因为 staging 是 UNLOGGED + 老行不会被 backfill 覆盖）
#
# 用法（remote 上 root crontab 或 tmux 跑）：
#   while true; do bash scripts/mid_migrate.sh; sleep 1800; done

set -e
cd "$(dirname "$0")/.."

LOG=backups/mid-migrate-$(date +%Y%m%d).log
echo "=== $(date) ===" >> "$LOG"

# 当前 staging size
docker exec radar-pg psql -U radar -d radar -c "
SELECT
  COUNT(*) AS rows,
  pg_size_pretty(pg_total_relation_size('swaps_staging')) AS staging_size,
  pg_size_pretty(pg_database_size('radar')) AS db_size,
  MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts
FROM swaps_staging;" >> "$LOG" 2>&1

# 取 staging 中前 80% timestamp 作 cutoff（保留最新 20% 给 backfill 写入避开锁）
CUTOFF=$(docker exec radar-pg psql -U radar -d radar -t -c "
SELECT MIN(timestamp) + (MAX(timestamp) - MIN(timestamp)) * 80 / 100 FROM swaps_staging" | tr -d ' ')

if [ -z "$CUTOFF" ] || [ "$CUTOFF" = "" ]; then
  echo "staging 空，跳过" >> "$LOG"
  exit 0
fi

echo "Migrate staging WHERE timestamp < $CUTOFF" >> "$LOG"

# 单事务：INSERT swaps + DELETE staging
docker exec radar-pg psql -U radar -d radar -c "
BEGIN;
INSERT INTO swaps (pool_address, chain, dex, tx_hash, amount0, amount1, fee_usd, volume_usd, timestamp, block_number)
SELECT DISTINCT ON (tx_hash, pool_address, amount0, amount1, timestamp)
       pool_address, chain, dex, tx_hash, amount0, amount1, fee_usd, volume_usd, timestamp, block_number
FROM swaps_staging
WHERE timestamp < $CUTOFF
ORDER BY tx_hash, pool_address, amount0, amount1, timestamp, block_number
ON CONFLICT (tx_hash, pool_address, amount0, amount1, timestamp) DO NOTHING;
DELETE FROM swaps_staging WHERE timestamp < $CUTOFF;
COMMIT;" >> "$LOG" 2>&1

echo "Done. New staging size:" >> "$LOG"
docker exec radar-pg psql -U radar -d radar -c "
SELECT COUNT(*) AS rows, pg_size_pretty(pg_total_relation_size('swaps_staging')) AS size FROM swaps_staging" >> "$LOG" 2>&1
