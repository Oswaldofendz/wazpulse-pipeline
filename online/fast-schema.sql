-- Additive fast-lane records. No old story, approval or delivery is changed.
CREATE INDEX IF NOT EXISTS stories_source_url ON stories(source_url);
CREATE TABLE IF NOT EXISTS fast_budget (
  day TEXT PRIMARY KEY,
  alerts_used INTEGER NOT NULL DEFAULT 0,
  requests_used INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS fast_events (
  story_id TEXT PRIMARY KEY REFERENCES stories(id),
  kind TEXT NOT NULL CHECK(kind IN ('market','news')),
  topic TEXT NOT NULL,
  detected_at INTEGER NOT NULL,
  published_at INTEGER NOT NULL,
  delivered_at INTEGER,
  detail TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS fast_events_time ON fast_events(detected_at);
CREATE TABLE IF NOT EXISTS fast_seen (
  url TEXT PRIMARY KEY,
  seen_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS fast_seen_time ON fast_seen(seen_at);
