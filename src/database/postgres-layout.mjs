export const POSTGRES_OWNER_LOCK = '78097489725420641';

export const POSTGRES_STORES = [
  { role: 'enrichment.sqlite', schema: 'pictaria_enrich', required: ['assets', 'processing_runs', 'asset_tags', 'pictaria_schema_version'] },
  { role: 'frame.db', schema: 'pictaria_frame', required: ['asset_displays', 'voice_command_stats'] },
  { role: 'insights.sqlite', schema: 'pictaria_insights', required: ['swept_assets', 'pictaria_schema_version'] },
];

export function postgresOptions(config, store) {
  return config.postgresUrl ? { url: config.postgresUrl, schema: `pictaria_${store}` } : undefined;
}

export function postgresPersistentTargets(owner) {
  return POSTGRES_STORES.map(({ role, schema, required }) => ({
    role,
    path: `PostgreSQL schema ${schema}`,
    kind: 'postgres',
    inspect() {
      const [{ present }] = owner.request('native', `
        SELECT count(*) = cardinality($2::text[]) AS present
        FROM information_schema.tables
        WHERE table_schema = $1 AND table_name = ANY($2::text[])
      `, [schema, required]);
      return present ? { valid: true } : {
        valid: false,
        reason: 'required PostgreSQL tables are missing. Import the SQLite databases or restore PostgreSQL before restarting',
      };
    },
  }));
}
