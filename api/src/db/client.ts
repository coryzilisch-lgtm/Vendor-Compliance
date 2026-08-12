import sql from 'mssql';

/**
 * Connection to the Safety-Dash Fabric SQL Database.
 *
 * This tracker deliberately shares Safety-Dash's SQL DB rather than standing up
 * its own, for two reasons:
 *
 *  1. Capacity. Fabric's `Sql Usage` meter looks allocation-based, not
 *     query-based — an idle database still bills a flat share of the capacity.
 *     The F4 already breaches its interactive-delay threshold several times a
 *     fortnight, so a fourth always-on SQL DB would cost real headroom for a
 *     tracker that gets read a few times a day.
 *  2. `dbo.projects` already lives here, mirrored from gold_safety_projects with
 *     `stage` and `is_active`. Joining against it is what makes "the same active
 *     projects as the other dashboards" true by construction instead of by
 *     a duplicated definition that drifts.
 *
 * NOT the Lakehouse / Warehouse SQL endpoint: mssql/tedious cannot connect to
 * *.datawarehouse.fabric.microsoft.com at all. Only *.database.fabric.microsoft.com.
 */
const config: sql.config = {
  server: process.env.FABRIC_SQL_SERVER!,
  database: process.env.FABRIC_SQL_DATABASE!,
  authentication: {
    type: 'azure-active-directory-service-principal-secret',
    options: {
      clientId: process.env.AZURE_CLIENT_ID!,
      clientSecret: process.env.AZURE_CLIENT_SECRET!,
      tenantId: process.env.AZURE_TENANT_ID!,
    },
  },
  options: {
    encrypt: true,
    trustServerCertificate: false,
    enableArithAbort: true,
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 300_000,
  },
  connectionTimeout: 30_000,
  requestTimeout: 60_000,
};

let _pool: sql.ConnectionPool | null = null;

async function getPool(): Promise<sql.ConnectionPool> {
  if (!_pool) {
    const pool = new sql.ConnectionPool(config);
    pool.on('error', (err) => {
      console.error('[db] Pool error:', err);
      _pool = null;
    });
    await pool.connect();
    _pool = pool;
  }
  return _pool;
}

/**
 * Errors that mean the pooled socket died (idle timeout, Fabric-side close)
 * rather than the query being wrong. This tracker is read a handful of times a
 * day, so its pool sits idle for hours and hits this constantly — a fresh pool
 * plus one retry is the difference between "works" and "fails every morning".
 */
const CONN_ERR = /socket hang up|Connection lost|ECONNCLOSED|ECONNRESET|ETIMEOUT|ESOCKET|Connection is closed/i;

async function resetPool(): Promise<void> {
  const p = _pool;
  _pool = null;
  if (p) await p.close().catch(() => undefined);
}

function bindParam(request: sql.Request, key: string, value: unknown): void {
  if (value === null || value === undefined) {
    request.input(key, sql.NVarChar(4000), null);
  } else if (typeof value === 'boolean') {
    request.input(key, sql.Bit, value);
  } else if (typeof value === 'number' && Number.isInteger(value)) {
    request.input(key, sql.BigInt, value);
  } else if (typeof value === 'number') {
    request.input(key, sql.Decimal(18, 6), value);
  } else if (value instanceof Date) {
    request.input(key, sql.DateTimeOffset, value);
  } else {
    request.input(key, sql.NVarChar(sql.MAX), String(value));
  }
}

export const db = {
  async query<T = Record<string, unknown>>(
    sqlText: string,
    params: Record<string, unknown> = {},
  ): Promise<{ rows: T[] }> {
    const attempt = async (): Promise<{ rows: T[] }> => {
      const pool = await getPool();
      const request = pool.request();
      for (const [key, value] of Object.entries(params)) {
        bindParam(request, key, value);
      }
      const result = await request.query<T>(sqlText);
      const sets = result.recordsets as unknown as T[][];
      const rows = sets[sets.length - 1] ?? [];
      return { rows };
    };
    try {
      return await attempt();
    } catch (err) {
      const msg = String((err as Error)?.message ?? err);
      if (!CONN_ERR.test(msg)) throw err;
      console.warn('[db] connection error — resetting pool and retrying once:', msg);
      await resetPool();
      return attempt();
    }
  },
};
