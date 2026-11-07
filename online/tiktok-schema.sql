-- Additive. Draft uploads never change editorial approval or publish a post.
CREATE TABLE IF NOT EXISTS tiktok_uploads (
  story_id TEXT PRIMARY KEY REFERENCES stories(id),
  account_id TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  state TEXT NOT NULL,
  publish_id TEXT,
  error TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  media_expires INTEGER NOT NULL,
  checked_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS tiktok_upload_assets (
  story_id TEXT NOT NULL REFERENCES tiktok_uploads(story_id),
  position INTEGER NOT NULL CHECK(position BETWEEN 0 AND 4),
  jpeg BLOB NOT NULL,
  PRIMARY KEY(story_id,position)
);
