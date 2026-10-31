-- Independent private D1 queue. Does not alter the legacy Supabase archive.
CREATE TABLE IF NOT EXISTS stories (
  id TEXT PRIMARY KEY,
  event_key TEXT NOT NULL UNIQUE,
  title TEXT NOT NULL,
  source TEXT NOT NULL,
  source_url TEXT NOT NULL,
  published_at INTEGER NOT NULL,
  priority REAL NOT NULL,
  snippet TEXT NOT NULL,
  assets TEXT NOT NULL DEFAULT '[]',
  state TEXT NOT NULL DEFAULT 'new'
    CHECK(state IN ('new','processing','ready','pending','approved','rejected','ignored','failed')),
  attempts INTEGER NOT NULL DEFAULT 0,
  retry_at INTEGER NOT NULL DEFAULT 0,
  lease_until INTEGER NOT NULL DEFAULT 0,
  editorial TEXT,
  message_id TEXT,
  notification TEXT NOT NULL DEFAULT 'new'
    CHECK(notification IN ('new','sending','sent','uncertain')),
  pack_delivery TEXT NOT NULL DEFAULT 'new'
    CHECK(pack_delivery IN ('new','sending','sent','uncertain')),
  approved_at INTEGER,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS stories_queue ON stories(state,retry_at,priority DESC,published_at DESC);
CREATE INDEX IF NOT EXISTS stories_review ON stories(state,notification,published_at);
CREATE TABLE IF NOT EXISTS daily_budget (day TEXT PRIMARY KEY, used INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS runtime (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS job_lock (name TEXT PRIMARY KEY, owner TEXT NOT NULL, until_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS inbox (
  update_id INTEGER PRIMARY KEY,
  payload TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'new' CHECK(state IN ('new','processing','done','failed')),
  lease_until INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS inbox_queue ON inbox(state,lease_until,update_id);
