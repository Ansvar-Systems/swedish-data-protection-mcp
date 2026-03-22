/**
 * SQLite database access layer for the CNIL MCP server.
 *
 * Schema:
 *   - decisions    — CNIL deliberations, sanctions, and mises en demeure
 *   - guidelines   — CNIL guidance documents, recommandations, and referentiels
 *   - topics       — controlled vocabulary for data protection topics
 *
 * FTS5 virtual tables back full-text search on decisions and guidelines.
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";

const DB_PATH = process.env["IMY_DB_PATH"] ?? "data/imy.db";

export const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS decisions (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  reference    TEXT    NOT NULL UNIQUE,
  title        TEXT    NOT NULL,
  date         TEXT,
  type         TEXT,
  entity_name  TEXT,
  fine_amount  REAL,
  summary      TEXT,
  full_text    TEXT    NOT NULL,
  topics       TEXT,
  gdpr_articles TEXT,
  status       TEXT    DEFAULT 'final'
);

CREATE INDEX IF NOT EXISTS idx_decisions_date        ON decisions(date);
CREATE INDEX IF NOT EXISTS idx_decisions_type        ON decisions(type);
CREATE INDEX IF NOT EXISTS idx_decisions_entity_name ON decisions(entity_name);
CREATE INDEX IF NOT EXISTS idx_decisions_status      ON decisions(status);

CREATE VIRTUAL TABLE IF NOT EXISTS decisions_fts USING fts5(
  reference, title, entity_name, summary, full_text,
  content='decisions',
  content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS decisions_ai AFTER INSERT ON decisions BEGIN
  INSERT INTO decisions_fts(rowid, reference, title, entity_name, summary, full_text)
  VALUES (new.id, new.reference, new.title, COALESCE(new.entity_name, ''), COALESCE(new.summary, ''), new.full_text);
END;

CREATE TRIGGER IF NOT EXISTS decisions_ad AFTER DELETE ON decisions BEGIN
  INSERT INTO decisions_fts(decisions_fts, rowid, reference, title, entity_name, summary, full_text)
  VALUES ('delete', old.id, old.reference, old.title, COALESCE(old.entity_name, ''), COALESCE(old.summary, ''), old.full_text);
END;

CREATE TRIGGER IF NOT EXISTS decisions_au AFTER UPDATE ON decisions BEGIN
  INSERT INTO decisions_fts(decisions_fts, rowid, reference, title, entity_name, summary, full_text)
  VALUES ('delete', old.id, old.reference, old.title, COALESCE(old.entity_name, ''), COALESCE(old.summary, ''), old.full_text);
  INSERT INTO decisions_fts(rowid, reference, title, entity_name, summary, full_text)
  VALUES (new.id, new.reference, new.title, COALESCE(new.entity_name, ''), COALESCE(new.summary, ''), new.full_text);
END;

CREATE TABLE IF NOT EXISTS guidelines (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  reference TEXT,
  title     TEXT    NOT NULL,
  date      TEXT,
  type      TEXT,
  summary   TEXT,
  full_text TEXT    NOT NULL,
  topics    TEXT,
  language  TEXT    DEFAULT 'sv'
);

CREATE INDEX IF NOT EXISTS idx_guidelines_type ON guidelines(type);
CREATE INDEX IF NOT EXISTS idx_guidelines_date ON guidelines(date);

CREATE VIRTUAL TABLE IF NOT EXISTS guidelines_fts USING fts5(
  reference, title, summary, full_text,
  content='guidelines',
  content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS guidelines_ai AFTER INSERT ON guidelines BEGIN
  INSERT INTO guidelines_fts(rowid, reference, title, summary, full_text)
  VALUES (new.id, COALESCE(new.reference, ''), new.title, COALESCE(new.summary, ''), new.full_text);
END;

CREATE TRIGGER IF NOT EXISTS guidelines_ad AFTER DELETE ON guidelines BEGIN
  INSERT INTO guidelines_fts(guidelines_fts, rowid, reference, title, summary, full_text)
  VALUES ('delete', old.id, COALESCE(old.reference, ''), old.title, COALESCE(old.summary, ''), old.full_text);
END;

CREATE TRIGGER IF NOT EXISTS guidelines_au AFTER UPDATE ON guidelines BEGIN
  INSERT INTO guidelines_fts(guidelines_fts, rowid, reference, title, summary, full_text)
  VALUES ('delete', old.id, COALESCE(old.reference, ''), old.title, COALESCE(old.summary, ''), old.full_text);
  INSERT INTO guidelines_fts(rowid, reference, title, summary, full_text)
  VALUES (new.id, COALESCE(new.reference, ''), new.title, COALESCE(new.summary, ''), new.full_text);
END;

CREATE TABLE IF NOT EXISTS topics (
  id          TEXT PRIMARY KEY,
  name_local  TEXT NOT NULL,
  name_en     TEXT NOT NULL,
  description TEXT
);
`;

// --- Interfaces ---------------------------------------------------------------

export interface Decision {
  id: number;
  reference: string;
  title: string;
  date: string | null;
  type: string | null;
  entity_name: string | null;
  fine_amount: number | null;
  summary: string | null;
  full_text: string;
  topics: string | null;
  gdpr_articles: string | null;
  status: string;
}

export interface Guideline {
  id: number;
  reference: string | null;
  title: string;
  date: string | null;
  type: string | null;
  summary: string | null;
  full_text: string;
  topics: string | null;
  language: string;
}

export interface Topic {
  id: string;
  name_local: string;
  name_en: string;
  description: string | null;
}

// --- DB singleton -------------------------------------------------------------

let _db: Database.Database | null = null;

export function getDb(): Database.Database {
  if (_db) return _db;

  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }

  _db = new Database(DB_PATH);
  _db.pragma("journal_mode = WAL");
  _db.pragma("foreign_keys = ON");
  _db.exec(SCHEMA_SQL);

  return _db;
}

// --- Decision queries ---------------------------------------------------------

export interface SearchDecisionsOptions {
  query: string;
  type?: string | undefined;
  topic?: string | undefined;
  limit?: number | undefined;
}

export function searchDecisions(opts: SearchDecisionsOptions): Decision[] {
  const db = getDb();
  const limit = opts.limit ?? 20;

  const conditions: string[] = ["decisions_fts MATCH :query"];
  const params: Record<string, unknown> = { query: opts.query, limit };

  if (opts.type) {
    conditions.push("d.type = :type");
    params["type"] = opts.type;
  }
  if (opts.topic) {
    conditions.push("d.topics LIKE :topic");
    params["topic"] = `%"${opts.topic}"%`;
  }

  const where = conditions.join(" AND ");
  return db
    .prepare(
      `SELECT d.* FROM decisions_fts f
       JOIN decisions d ON d.id = f.rowid
       WHERE ${where}
       ORDER BY rank
       LIMIT :limit`,
    )
    .all(params) as Decision[];
}

export function getDecision(reference: string): Decision | null {
  const db = getDb();
  return (
    (db
      .prepare("SELECT * FROM decisions WHERE reference = ? LIMIT 1")
      .get(reference) as Decision | undefined) ?? null
  );
}

// --- Guideline queries --------------------------------------------------------

export interface SearchGuidelinesOptions {
  query: string;
  type?: string | undefined;
  topic?: string | undefined;
  limit?: number | undefined;
}

export function searchGuidelines(opts: SearchGuidelinesOptions): Guideline[] {
  const db = getDb();
  const limit = opts.limit ?? 20;

  const conditions: string[] = ["guidelines_fts MATCH :query"];
  const params: Record<string, unknown> = { query: opts.query, limit };

  if (opts.type) {
    conditions.push("g.type = :type");
    params["type"] = opts.type;
  }
  if (opts.topic) {
    conditions.push("g.topics LIKE :topic");
    params["topic"] = `%"${opts.topic}"%`;
  }

  const where = conditions.join(" AND ");
  return db
    .prepare(
      `SELECT g.* FROM guidelines_fts f
       JOIN guidelines g ON g.id = f.rowid
       WHERE ${where}
       ORDER BY rank
       LIMIT :limit`,
    )
    .all(params) as Guideline[];
}

export function getGuideline(id: number): Guideline | null {
  const db = getDb();
  return (
    (db
      .prepare("SELECT * FROM guidelines WHERE id = ? LIMIT 1")
      .get(id) as Guideline | undefined) ?? null
  );
}

// --- Topic queries ------------------------------------------------------------

export function listTopics(): Topic[] {
  const db = getDb();
  return db
    .prepare("SELECT * FROM topics ORDER BY id")
    .all() as Topic[];
}
