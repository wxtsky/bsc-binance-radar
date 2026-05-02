import pg from "pg";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const { Pool } = pg;

let pool: pg.Pool | null = null;

function getConnectionString(): string {
  const url = process.env.DATABASE_URL;
  if (!url) {
    throw new Error(
      "DATABASE_URL is required (e.g. postgresql://radar:radar@localhost:5432/radar)"
    );
  }
  return url;
}

export function getPool(): pg.Pool {
  if (!pool) {
    pool = new Pool({
      connectionString: getConnectionString(),
      max: 10,
      idleTimeoutMillis: 30_000,
    });
    pool.on("error", (err) => {
      console.error("[Radar] Unexpected pg pool error:", err);
    });
  }
  return pool;
}

/** 跑 init.sql 把 schema 建好（idempotent，可重复跑） */
export async function initSchema(): Promise<void> {
  const __filename = fileURLToPath(import.meta.url);
  const __dirname = path.dirname(__filename);
  // src/db/index.ts → ../../db/init.sql
  const initPath = path.resolve(__dirname, "..", "..", "db", "init.sql");
  if (!fs.existsSync(initPath)) {
    throw new Error(`init.sql not found at ${initPath}`);
  }
  const sql = fs.readFileSync(initPath, "utf-8");
  await getPool().query(sql);
  await migrateSwapsDedup();
}

/**
 * 一次性迁移：清掉 swaps 表内的重复记录（按 tx_hash+pool+amount0+amount1 分组保留最早 id）
 * 然后再让 init.sql 里的 CREATE UNIQUE INDEX IF NOT EXISTS 生效。
 * 已迁移过则 no-op（依据 uq_swaps_dedup 索引存在性判断）。
 */
async function migrateSwapsDedup(): Promise<void> {
  const idx = await getPool().query<{ count: number }>(
    `SELECT count(*)::int AS count FROM pg_indexes WHERE indexname = 'uq_swaps_dedup'`
  );
  if ((idx.rows[0]?.count ?? 0) > 0) return;

  console.log("[Migrate] swaps 去重 + UNIQUE 索引迁移开始（取 EXCLUSIVE LOCK 防 race）");
  const client = await getPool().connect();
  try {
    await client.query("BEGIN");
    // 禁用并行 maintenance worker：CREATE INDEX 并行 worker 在事务内可能读到 DELETE 之前的快照导致冲突
    await client.query("SET LOCAL max_parallel_maintenance_workers = 0");
    // EXCLUSIVE 锁阻塞 INSERT/DELETE，但允许 SELECT；防止 stream 在 DELETE 与 CREATE INDEX 间插入重复
    await client.query("LOCK TABLE swaps IN EXCLUSIVE MODE");
    const before = await client.query<{ n: string }>(`SELECT count(*)::text AS n FROM swaps`);
    const del = await client.query(
      `DELETE FROM swaps a USING swaps b
       WHERE a.tx_hash = b.tx_hash
         AND a.pool_address = b.pool_address
         AND a.amount0 = b.amount0
         AND a.amount1 = b.amount1
         AND a.id > b.id`
    );
    console.log(`[Migrate] 清掉 ${del.rowCount} 条重复（${before.rows[0]?.n} 总数）`);
    await client.query(
      `CREATE UNIQUE INDEX uq_swaps_dedup ON swaps(tx_hash, pool_address, amount0, amount1)`
    );
    await client.query("COMMIT");
    console.log("[Migrate] uq_swaps_dedup 索引创建完成 ✅");
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    client.release();
  }
}

export async function closeDatabase(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}
