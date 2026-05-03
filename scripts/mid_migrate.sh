#!/bin/bash
# mid-flight migrate helper：staging → swaps，分小批做
# 每次只 migrate cutoff 之前的 1M rows，避免单事务过大
#
# 用法：tmux 后台 cron-like 跑
#   tmux new -d -s mid-mig 'while true; do bash scripts/mid_migrate.sh; sleep 600; done'

set -e
cd "$(dirname "$0")/.."

LOG=backups/mid-migrate-$(date +%Y%m%d).log
echo "=== $(date) === BATCH MIGRATE START" >> "$LOG"

# staging 状态
docker exec radar-pg psql -U radar -d radar -c "
SELECT
  COUNT(*) AS rows,
  pg_size_pretty(pg_total_relation_size('swaps_staging')) AS staging_size,
  pg_size_pretty(pg_database_size('radar')) AS db_size
FROM swaps_staging;" >> "$LOG" 2>&1

# 取 cutoff：staging 80% 时间点
CUTOFF=$(docker exec radar-pg psql -U radar -d radar -t -c "
SELECT COALESCE(MIN(timestamp) + (MAX(timestamp) - MIN(timestamp)) * 80 / 100, 0)::text FROM swaps_staging" | tr -d ' ')

if [ -z "$CUTOFF" ] || [ "$CUTOFF" = "0" ]; then
  echo "staging 空，跳过" >> "$LOG"
  exit 0
fi

# 分批：每次 INSERT/DELETE 100K rows，循环到 cutoff 之前的全部 migrate 完
BATCHES_DONE=0
while true; do
  RESULT=$(docker exec radar-pg psql -U radar -d radar -t -A -F'|' -c "
WITH batch AS (
  SELECT ctid FROM swaps_staging
  WHERE timestamp < $CUTOFF
  LIMIT 100000
),
ins AS (
  INSERT INTO swaps (pool_address, chain, dex, tx_hash, amount0, amount1, fee_usd, volume_usd, timestamp, block_number)
  SELECT s.pool_address, s.chain, s.dex, s.tx_hash, s.amount0, s.amount1, s.fee_usd, s.volume_usd, s.timestamp, s.block_number
  FROM swaps_staging s WHERE s.ctid IN (SELECT ctid FROM batch)
  ON CONFLICT (tx_hash, pool_address, amount0, amount1, timestamp) DO NOTHING
  RETURNING 1
),
del AS (
  DELETE FROM swaps_staging WHERE ctid IN (SELECT ctid FROM batch)
  RETURNING 1
)
SELECT COALESCE((SELECT COUNT(*) FROM ins), 0)::text || '|' || COALESCE((SELECT COUNT(*) FROM del), 0)::text;
" 2>/dev/null | tr -d '\r' | tr -d ' ')

  BATCHES_DONE=$((BATCHES_DONE + 1))
  echo "  batch $BATCHES_DONE: $RESULT" >> "$LOG"
  DELETED=$(echo "$RESULT" | cut -d'|' -f2)
  if [ -z "$DELETED" ] || [ "$DELETED" = "0" ]; then
    break
  fi
  # 防 runaway
  if [ "$BATCHES_DONE" -gt 1000 ]; then
    echo "  too many batches, break" >> "$LOG"
    break
  fi
done

echo "=== $(date) === DONE batches=$BATCHES_DONE" >> "$LOG"
docker exec radar-pg psql -U radar -d radar -c "
SELECT (SELECT COUNT(*) FROM swaps) AS swaps, (SELECT COUNT(*) FROM swaps_staging) AS staging, pg_size_pretty(pg_database_size('radar')) AS db_size" >> "$LOG" 2>&1
