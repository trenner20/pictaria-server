import { DatabaseSync } from 'node:sqlite';
import { PostgresDatabase } from './postgres.mjs';
import { preparePrivateDatabasePath, restrictPrivateDatabaseModes } from '../privateDatabase.mjs';

export function openDatabase(path, postgres, { readOnly = false, logger = console } = {}) {
  if (postgres) return new PostgresDatabase(postgres, { readOnly });
  if (readOnly) return new DatabaseSync(path, { readOnly });
  preparePrivateDatabasePath(path, logger);
  const db = new DatabaseSync(path);
  try {
    db.exec('PRAGMA journal_mode = WAL');
    restrictPrivateDatabaseModes(path, logger);
    return db;
  } catch (error) {
    db.close();
    throw error;
  }
}
