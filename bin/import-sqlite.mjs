import { DatabaseSync } from 'node:sqlite';
import pg from 'pg';

import { acquireServerOwner } from '../src/ai/owner.mjs';
import { loadConfig } from '../src/config.mjs';
import { POSTGRES_OWNER_LOCK, POSTGRES_STORES, postgresOptions } from '../src/database/postgres-layout.mjs';
import { Repository } from '../src/enrich/repository.mjs';
import { createFrameLedger } from '../src/frame/ledger.mjs';
import { InsightsRepository } from '../src/insights/repository.mjs';
import { assertPrivateDatabasePath } from '../src/privateDatabase.mjs';
import { createVoiceMetrics } from '../src/voice/metrics.mjs';

const quote = (identifier) => '"' + identifier.replaceAll('"', '""') + '"';

function openSource(path) {
  assertPrivateDatabasePath(path);
  const db = new DatabaseSync(path, { readOnly: true });
  db.exec('BEGIN');
  return db;
}

function initializeDestination(config) {
  const repo = new Repository(null, postgresOptions(config, 'enrich'));
  try {
    repo.initSchema();
  } finally {
    repo.close();
  }
  const frame = postgresOptions(config, 'frame');
  createFrameLedger({ postgres: frame }).close();
  createVoiceMetrics({ postgres: frame }).close();
  new InsightsRepository(null, postgresOptions(config, 'insights')).close();
}

async function tableDefinitions(client, schema) {
  const { rows } = await client.query(`
    SELECT c.relname AS name, array_agg(a.attname::text ORDER BY a.attnum) AS columns,
           max(a.attname::text) FILTER (WHERE a.attidentity <> '') AS identity
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    JOIN pg_attribute a ON a.attrelid=c.oid
    WHERE n.nspname=$1 AND c.relkind='r' AND a.attnum>0 AND NOT a.attisdropped
      AND a.attgenerated='' AND c.relname <> 'pictaria_schema_version'
    GROUP BY c.oid, c.relname
    ORDER BY c.oid
  `, [schema]);
  return rows.map((row) => ({ ...row, target: `${quote(schema)}.${quote(row.name)}` }));
}

async function copyStore(client, schema, source) {
  const tables = await tableDefinitions(client, schema);
  await client.query(`TRUNCATE ${tables.map((table) => table.target).join(', ')}`);
  for (const { name, columns, identity, target } of tables) {
    const available = new Set(source.prepare('SELECT name FROM pragma_table_info(?)').all(name).map((row) => row.name));
    if (columns.some((column) => !available.has(column))) {
      throw new Error(`SQLite table ${name} has an older or incomplete schema. Start this version in SQLite mode once before importing.`);
    }
    await client.query(`ALTER TABLE ${target} DISABLE TRIGGER USER`);
    const names = columns.map(quote).join(', ');
    const query = source.prepare(`SELECT ${names} FROM ${quote(name)}`);
    query.setReadBigInts(true);
    let batch = [];
    let highest = 0n;
    let count = 0;
    async function insertBatch() {
      const tuples = batch.map((_, index) =>
        '(' + columns.map((_, column) => `$${index * columns.length + column + 1}`).join(', ') + ')');
      await client.query(`INSERT INTO ${target} (${names}) VALUES ${tuples.join(', ')}`, batch.flat());
      count += batch.length;
      batch = [];
    }
    for (const row of query.iterate()) {
      batch.push(columns.map((column) => row[column] instanceof Uint8Array ? Buffer.from(row[column]) : row[column]));
      if (identity && row[identity] > highest) highest = row[identity];
      if (batch.length === 100) await insertBatch();
    }
    if (batch.length) await insertBatch();
    if (identity) {
      const sequence = source.prepare('SELECT seq FROM sqlite_sequence WHERE name = ?');
      sequence.setReadBigInts(true);
      const used = sequence.get(name)?.seq ?? 0n;
      const next = (used > highest ? used : highest) + 1n;
      await client.query(`ALTER TABLE ${target} ALTER COLUMN ${quote(identity)} RESTART WITH ${next}`);
    }
    await client.query(`ALTER TABLE ${target} ENABLE TRIGGER USER`);
    if (count > 0) console.log(`Copied ${schema}.${name} (${count} rows)`);
  }
}

async function importSqlite(config) {
  if (!config.postgresUrl) throw new Error('Set PICTARIA_DATABASE_URL to a new, dedicated PostgreSQL database.');
  const paths = [config.databasePath, config.frame.dbPath, config.insights.dbPath];
  // Check paths before taking the SQLite server-owner lock, which may create
  // its own separate lock file.
  for (const path of paths) assertPrivateDatabasePath(path);
  const owner = acquireServerOwner(config.databasePath);
  const client = new pg.Client({ connectionString: config.postgresUrl, connectionTimeoutMillis: 5_000 });
  client.on('error', () => { /* A pending or subsequent query reports the error. */ });
  const sources = [];
  let createdSchemas = false;
  try {
    try {
      await client.connect();
    } catch {
      throw new Error('Cannot connect to the destination PostgreSQL database.');
    }
    const { rows: [{ acquired }] } = await client.query('SELECT pg_try_advisory_lock($1::bigint) AS acquired', [POSTGRES_OWNER_LOCK]);
    if (!acquired) throw new Error('Stop Pictaria before importing into this PostgreSQL database.');
    const schemas = POSTGRES_STORES.map(({ schema }) => schema);
    const { rowCount } = await client.query('SELECT 1 FROM pg_namespace WHERE nspname=ANY($1::text[])', [schemas]);
    if (rowCount) throw new Error('The destination already has Pictaria schemas. Use a fresh database to avoid overwriting data.');
    for (const path of paths) sources.push(openSource(path));
    createdSchemas = true;
    initializeDestination(config);
    await client.query('BEGIN');
    try {
      for (const [index, { schema }] of POSTGRES_STORES.entries()) {
        await copyStore(client, schema, sources[index]);
      }
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
    createdSchemas = false;
    console.log('Import complete. Start Pictaria with the same PICTARIA_DATABASE_URL and local data directory.');
  } catch (error) {
    if (createdSchemas) {
      try {
        await client.query(`DROP SCHEMA IF EXISTS ${POSTGRES_STORES.map(({ schema }) => quote(schema)).join(', ')} CASCADE`);
      } catch {
        console.error('The destination could not be cleaned up. Use a new empty database for another attempt.');
      }
    }
    throw error;
  } finally {
    for (const db of sources) db.close();
    owner.close();
    await client.end();
  }
}

try {
  await importSqlite(loadConfig());
} catch (error) {
  console.error(`SQLite import failed: ${error.message}`);
  process.exitCode = 1;
}
