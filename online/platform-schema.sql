-- Additive: keeps previous packages, approvals and ambiguous deliveries intact.
CREATE TABLE IF NOT EXISTS channel_images (
  story_id TEXT PRIMARY KEY REFERENCES stories(id),
  state TEXT NOT NULL DEFAULT 'new',
  manifest TEXT NOT NULL,
  image TEXT,
  provider TEXT,
  request_id TEXT,
  error TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS channel_jobs (
  story_id TEXT NOT NULL REFERENCES stories(id),
  platform TEXT NOT NULL CHECK(platform IN ('x','instagram','tiktok')),
  state TEXT NOT NULL DEFAULT 'draft',
  draft_message TEXT,
  delivery TEXT NOT NULL DEFAULT 'new',
  lease_until INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  created_at INTEGER NOT NULL,
  approved_at INTEGER,
  PRIMARY KEY(story_id,platform)
);
CREATE TABLE IF NOT EXISTS channel_assets (
  story_id TEXT NOT NULL,
  platform TEXT NOT NULL,
  position INTEGER NOT NULL,
  filename TEXT NOT NULL,
  png BLOB,
  file_id TEXT,
  message_id TEXT,
  PRIMARY KEY(story_id,platform,position),
  FOREIGN KEY(story_id,platform) REFERENCES channel_jobs(story_id,platform)
);
CREATE TABLE IF NOT EXISTS image_provider_budget (
  day TEXT NOT NULL,
  provider TEXT NOT NULL,
  used INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(day,provider)
);
CREATE INDEX IF NOT EXISTS channel_queue ON channel_jobs(state,created_at);
