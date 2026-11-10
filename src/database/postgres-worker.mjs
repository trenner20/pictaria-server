import { readFileSync } from 'node:fs';
import { parentPort, workerData } from 'node:worker_threads';
import pg from 'pg';
import { postgresSql, statements } from './postgres-sql.mjs';

const { port, signal, url, schema, readOnly } = workerData;
const client = new pg.Client({
  connectionString: url,
  connectionTimeoutMillis: 5_000,
  keepAlive: true,
  keepAliveInitialDelayMillis: 5_000,
  statement_timeout: 30_000,
  lock_timeout: 5_000,
  application_name: 'pictaria-server',
});
pg.types.setTypeParser(20, Number);
pg.types.setTypeParser(1700, Number);
client.on('error', () => parentPort.postMessage('disconnected'));

const tableDefinitions = new Map();
const compiledStatements = new Map();
const IDENTITY_TABLES = new Set([
  'processing_runs', 'manual_overrides', 'pending_sync_jobs', 'enrich_queue', 'job_runs',
  'activity_log', 'enrich_timing_runs', 'enrich_photo_executions', 'enrich_provider_attempts', 'embedding_spaces',
]);

async function upsert(sql) {
  const match = /^INSERT OR (IGNORE|REPLACE) INTO\s+(\w+)\s*(?:\(([^)]+)\))?/i.exec(sql);
  if (!match) return sql;
  const [, policy, table, explicitColumns] = match;
  const insert = sql.replace(/^INSERT OR (?:IGNORE|REPLACE)/i, 'INSERT');
  if (policy.toUpperCase() === 'IGNORE') return insert + ' ON CONFLICT DO NOTHING';

  if (!tableDefinitions.has(table)) {
    const { rows } = await client.query(`
      SELECT a.attname AS name, EXISTS (
        SELECT FROM pg_index i
        WHERE i.indrelid = a.attrelid AND i.indisprimary AND a.attnum = ANY(i.indkey)
      ) AS primary_key
      FROM pg_attribute a
      WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped
      ORDER BY a.attnum
    `, [table]);
    tableDefinitions.set(table, rows);
  }
  const definition = tableDefinitions.get(table);
  const keys = definition.filter((row) => row.primary_key).map((row) => row.name);
  if (!keys.length) throw new Error(`Missing primary key for PostgreSQL upsert on ${table}.`);
  const columns = explicitColumns ? explicitColumns.split(',').map((name) => name.trim()) : definition.map((row) => row.name);
  const updates = columns.filter((name) => !keys.includes(name)).map((name) => `${name}=excluded.${name}`);
  return `${insert} ON CONFLICT (${keys.join(', ')}) DO ${updates.length ? `UPDATE SET ${updates.join(', ')}` : 'NOTHING'}`;
}

let outerSavepoint;

async function executeSchema(sql) {
  // Trigger bodies contain semicolons and need a PostgreSQL trigger function.
  // Use one converter for the small, uniform AFTER triggers in the schemas.
  const triggers = new Map();
  sql = sql.replace(/CREATE TRIGGER (?:IF NOT EXISTS )?(\w+)\s+AFTER (INSERT|UPDATE|DELETE) ON (\w+)(?:\s+WHEN ([\s\S]*?))?\s+BEGIN\s+([\s\S]*?)\s+END\s*;/gi,
    (_, name, event, table, condition, body) => {
      triggers.set(name, { event, table, condition, body });
      return `PICTARIA_TRIGGER ${name};`;
    });
  for (const statement of statements(sql)) {
    if (/^DROP TRIGGER IF EXISTS /i.test(statement)) continue;
    if (statement.startsWith('PICTARIA_TRIGGER ')) {
      const name = statement.slice('PICTARIA_TRIGGER '.length);
      const { event, table, condition, body } = triggers.get(name);
      const when = condition && condition !== '1' ? condition.replace(/\|\| '0'/g, 'FALSE') : null;
      const commands = [];
      for (const item of statements(body)) commands.push(postgresSql(await upsert(item)));
      await client.query(`
        CREATE OR REPLACE FUNCTION ${name}() RETURNS trigger LANGUAGE plpgsql
        SET search_path TO ${schema}, pg_catalog AS $trigger$
        BEGIN
          ${when ? `IF ${postgresSql(when)} THEN` : ''}
          ${commands.join(';\n')};
          ${when ? 'END IF;' : ''}
          RETURN NULL;
        END
        $trigger$;
        DROP TRIGGER IF EXISTS ${name} ON ${table};
        CREATE TRIGGER ${name} AFTER ${event} ON ${table}
          FOR EACH ROW EXECUTE FUNCTION ${name}();
      `);
    } else {
      const savepoint = /^SAVEPOINT (\w+)$/i.exec(statement)?.[1];
      if (savepoint && client.getTransactionStatus() === 'I') {
        await client.query('BEGIN');
        outerSavepoint = savepoint;
      }
      await client.query(postgresSql(await upsert(statement)));
      if (outerSavepoint && statement.toLowerCase() === `release ${outerSavepoint}`.toLowerCase()) {
        outerSavepoint = undefined;
        await client.query('COMMIT');
      }
    }
  }
}

async function request({ operation, sql, values }) {
  if (operation === 'open') {
    try {
      await client.connect();
    } catch {
      throw new Error('Cannot connect to PostgreSQL. Check PICTARIA_DATABASE_URL and database permissions.');
    }
    if (!readOnly) await client.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
    await client.query(`SET search_path TO ${schema}, pg_catalog; SET TIME ZONE 'UTC'; SET DateStyle TO 'ISO, YMD'`);
    if (readOnly) await client.query('SET default_transaction_read_only = on; SET default_transaction_isolation = \'repeatable read\'');
    return;
  }
  if (operation === 'close') return client.end();
  if (operation === 'functions') {
    return client.query(readFileSync(new URL('./postgres-functions.sql', import.meta.url), 'utf8')).then(() => undefined);
  }
  if (operation === 'exec') return executeSchema(sql);
  if (operation === 'native') return (await client.query(sql, values)).rows;
  if (operation === 'declare') {
    const [name, ...parameters] = values;
    const query = postgresSql(statements(sql).join(';'));
    await client.query(`DECLARE ${name} NO SCROLL CURSOR FOR ${query}`, parameters);
    return;
  }
  const key = `${operation}: ${sql}`;
  let query = compiledStatements.get(key);
  if (!query) {
    query = postgresSql(await upsert(statements(sql).join(';')));
    const table = /^INSERT INTO\s+(\w+)/i.exec(query)?.[1];
    if (operation === 'run' && IDENTITY_TABLES.has(table) && !/\bRETURNING\b/i.test(query)) {
      query += ' RETURNING id';
    }
    if (compiledStatements.size >= 500) compiledStatements.clear();
    compiledStatements.set(key, query);
  }
  const result = await client.query(query, values.map((value) =>
    value instanceof Uint8Array ? Buffer.from(value) : value));
  if (operation === 'run') return { changes: result.rowCount, lastInsertRowid: result.rows[0]?.id ?? 0 };
  if (operation === 'get') return result.rows[0];
  return result.rows;
}

port.on('message', async (message) => {
  try {
    port.postMessage({ result: await request(message) });
  } catch (error) {
    // Server details may include a bound value. Preserve SQLSTATE for callers.
    const message = error.code === '23505' ? 'PostgreSQL unique constraint failed.'
      : error.code === '23503' ? 'PostgreSQL foreign-key constraint failed.'
      : error.code === '23514' ? 'PostgreSQL check constraint failed.'
      : error.message;
    port.postMessage({ error: { message, code: error.code } });
  } finally {
    Atomics.store(signal, 0, 1);
    Atomics.notify(signal, 0);
  }
});
