-- Additive migration: original stories and legacy Supabase tables are untouched.
CREATE TABLE IF NOT EXISTS visual_jobs (
  story_id TEXT PRIMARY KEY REFERENCES stories(id),
  state TEXT NOT NULL DEFAULT 'queued' CHECK(state IN ('queued','rendering','review','approved','rejected','blocked')),
  manifest TEXT,
  image TEXT,
  lease_until INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  review_message TEXT,
  created_at INTEGER NOT NULL,
  approved_at INTEGER
);
CREATE TABLE IF NOT EXISTS visual_assets (
  story_id TEXT NOT NULL REFERENCES visual_jobs(story_id),
  position INTEGER NOT NULL CHECK(position BETWEEN 0 AND 5),
  filename TEXT NOT NULL,
  png BLOB,
  delivery TEXT NOT NULL DEFAULT 'new' CHECK(delivery IN ('new','sending','sent','uncertain')),
  file_id TEXT,
  message_id TEXT,
  PRIMARY KEY(story_id,position)
);
CREATE TABLE IF NOT EXISTS visual_budget (day TEXT PRIMARY KEY, used INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS visual_queue ON visual_jobs(state,lease_until,created_at);
