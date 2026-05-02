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
}

export async function closeDatabase(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}
