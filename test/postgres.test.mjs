import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

import { loadConfig } from '../src/config.mjs';
import { PostgresDatabase } from '../src/database/postgres.mjs';
import { Repository } from '../src/enrich/repository.mjs';
import { createFrameLedger } from '../src/frame/ledger.mjs';
import { InsightsRepository } from '../src/insights/repository.mjs';
import { createVoiceMetrics } from '../src/voice/metrics.mjs';

test('selects SQLite by default and accepts a PostgreSQL URL without exposing invalid input', () => {
  assert.equal(loadConfig({}).postgresUrl, null);
  assert.equal(loadConfig({ PICTARIA_DATABASE_URL: 'postgresql://db/pictaria' }).postgresUrl,
    'postgresql://db/pictaria');
  assert.throws(() => loadConfig({ PICTARIA_DATABASE_URL: 'bad-secret' }), {
    message: 'PICTARIA_DATABASE_URL must be a PostgreSQL URL with a host and database name.',
  });
});

const testUrl = process.env.PICTARIA_TEST_POSTGRES_URL;
async function isolatedPostgres(t) {
  const { Client } = (await import('pg')).default;
  const admin = new Client({ connectionString: testUrl });
  await admin.connect();
  const name = `pictaria_test_${randomBytes(8).toString('hex')}`;
  const handles = [];
  t.after(async () => {
    try {
      for (const handle of handles.reverse()) handle.close();
      await admin.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`);
    } finally {
      await admin.end();
    }
  });
  await admin.query(`CREATE DATABASE ${name}`);
  const url = new URL(testUrl);
  url.pathname = `/${name}`;
  const store = (name) => ({ url: url.href, schema: `pictaria_${name}` });
  const own = (handle) => { handles.push(handle); return handle; };
  return { url: url.href, store, own };
}

test('PostgreSQL persists Enrich, Curate, Insights, Frame and voice state', { skip: !testUrl }, async (t) => {
  const { store, own } = await isolatedPostgres(t);
  const repo = own(new Repository(null, store('enrich')));
  repo.initSchema();
  repo.upsertAsset({ id: 'asset' });
  repo.recordProcessingRun({
    assetId: 'asset', provider: 'synthetic', model: 'synthetic',
    promptVersion: 'v2', taxonomyVersion: 'v1', status: 'succeeded',
    normalizedOutput: { caption: 'Alpine lake at dusk', short_caption: 'Alpine lake' },
  });
  assert.equal(repo.searchCaptions('alp lake')[0].assetId, 'asset');
  assert.ok(repo.captionTerms().some((term) => term.term === 'alpine'));
  assert.throws(() => repo.transaction(() => {
    repo.upsertAsset({ id: 'rollback' });
    throw new Error('abort test transaction');
  }), /abort test transaction/);
  assert.equal(repo.db.prepare("SELECT asset_id FROM assets WHERE asset_id='rollback'").get(), undefined);
  repo.db.prepare("INSERT INTO review_list VALUES ('asset','test','2026-01-01')").run();
  const reader = own(new PostgresDatabase(store('enrich'), { readOnly: true }));
  assert.equal(reader.prepare('SELECT asset_id FROM curate_dirty').get().asset_id, 'asset');
  assert.throws(() => reader.prepare('DELETE FROM assets').run(), { code: '25006' });
  reader.exec('BEGIN');
  assert.equal([...reader.prepare('SELECT asset_id FROM assets WHERE asset_id = ?').iterate('asset')][0].asset_id, 'asset');
  reader.exec('COMMIT');

  const insights = own(new InsightsRepository(null, store('insights')));
  insights.beginSweepStaging();
  const asset = { id: 'asset', type: 'IMAGE', day: '2026-01-02', lat: 40, lon: -105 };
  insights.insertAssets([asset, { ...asset, city: 'Boulder' }], { staging: true });
  insights.commitSweepStaging();
  assert.deepEqual(insights.timelinePlaces('2026-01-01', '2026-01-31'), [
    { day: '2026-01-02', label: 'Boulder', isCity: true, count: 1 },
  ]);

  const now = () => new Date('2026-01-03T00:00:00Z');
  const ledger = own(createFrameLedger({ postgres: store('frame'), now, retention: { maxDisplayRows: 1 } }));
  const voice = own(createVoiceMetrics({ postgres: store('frame') }));
  assert.equal(ledger.recordDisplays(['first'], 'frame', now(), 'report-1').recorded, 1);
  assert.equal(ledger.recordDisplays(['first'], 'frame', now(), 'report-1').duplicate, true);
  assert.equal(ledger.recordDisplays(['second'], 'frame', now(), 'report-2').recorded, 1);
  assert.deepEqual(Object.keys(ledger.getDisplayStats(['first', 'second'])), ['second']);
  voice.record('next', { deviceId: 'frame', usedAt: now() });
  voice.record('next', { deviceId: 'frame', usedAt: now() });

  const reopened = own(new Repository(null, store('enrich')));
  reopened.initSchema();
  assert.equal(reopened.latestEnrichment('asset').caption, 'Alpine lake at dusk');
  assert.equal(voice.summary('frame').commands[0].uses, 2);
});

test('SQLite import preserves rows and generated IDs and rejects a second import', { skip: !testUrl }, async (t) => {
  const { url, store, own } = await isolatedPostgres(t);
  const dir = mkdtempSync(join(tmpdir(), 'pictaria-sqlite-import-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const env = {
    ...process.env,
    PICTARIA_DATABASE_URL: url,
    DATABASE_PATH: join(dir, 'enrichment.sqlite'),
    FRAME_DB_PATH: join(dir, 'frame.db'),
    INSIGHTS_DB_PATH: join(dir, 'insights.sqlite'),
  };
  const sqlite = new Repository(env.DATABASE_PATH);
  try {
    sqlite.initSchema();
    sqlite.upsertAsset({ id: 'asset' });
    sqlite.db.exec("INSERT INTO manual_overrides (id, asset_id, tag, action, created_at) VALUES (42, 'asset', 'frame/favorite', 'approve', '2026-01-01')");
    sqlite.db.exec("INSERT INTO manual_overrides SELECT 51, asset_id, tag, action, reason, created_at FROM manual_overrides WHERE id=42");
    sqlite.db.exec('DELETE FROM manual_overrides WHERE id=51');
  } finally {
    sqlite.close();
  }
  createFrameLedger({ dbPath: env.FRAME_DB_PATH }).close();
  createVoiceMetrics({ dbPath: env.FRAME_DB_PATH }).close();
  new InsightsRepository(env.INSIGHTS_DB_PATH).close();

  const runImport = () => spawnSync(process.execPath, [new URL('../bin/import-sqlite.mjs', import.meta.url).pathname], {
    encoding: 'utf8', env, timeout: 20_000,
  });
  const result = runImport();
  assert.equal(result.status, 0, result.stderr);
  const postgres = own(new Repository(null, store('enrich')));
  postgres.initSchema();
  assert.equal(postgres.db.prepare('SELECT id FROM manual_overrides').get().id, 42);
  const next = postgres.db.prepare('INSERT INTO manual_overrides (asset_id, tag, action, created_at) SELECT asset_id, tag, action, created_at FROM manual_overrides').run();
  assert.equal(next.lastInsertRowid, 52);
  const repeat = runImport();
  assert.equal(repeat.status, 1);
  assert.match(repeat.stderr, /already has Pictaria schemas/);
});
