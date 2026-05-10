-- Clear carousel_urls that contain .png files so the next pipeline cycle
-- regenerates them as JPEG (required by TikTok Content Posting API).
-- Run this in the Supabase SQL editor once, then Railway auto-picks it up.

UPDATE pulse_posts
SET compliance_flags = compliance_flags - 'carousel_urls'
WHERE
  status = 'approved'
  AND compliance_flags -> 'carousel_urls' IS NOT NULL
  AND compliance_flags -> 'published_platforms' -> 'tiktok' IS NULL
  AND (compliance_flags -> 'carousel_urls')::text ILIKE '%.png%';

-- Verify: should return 0 rows after update
SELECT id, compliance_flags -> 'carousel_urls' AS carousel_urls
FROM pulse_posts
WHERE
  compliance_flags -> 'carousel_urls' IS NOT NULL
  AND (compliance_flags -> 'carousel_urls')::text ILIKE '%.png%';
