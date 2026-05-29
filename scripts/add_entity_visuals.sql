-- ============================================================================
-- pulse_entity_visuals — persistent cache for LLM-described unknown entities
--
-- When the catalog _SUBJECTS in ai_image_generator.py doesn't match anything
-- in a headline, we ask the LLM how to visualize the entity (e.g. "Concentrix",
-- "Dawn Labs") and cache the result here. Survives backend restarts (unlike
-- the in-memory news-angle cache).
--
-- INSTRUCCIONES:
--   1. Supabase Dashboard del proyecto wacapital → SQL Editor → New query
--   2. Pega todo este bloque
--   3. Run
-- ============================================================================

CREATE TABLE IF NOT EXISTS pulse_entity_visuals (
    name        text PRIMARY KEY,             -- lowercase normalized key
    display     text NOT NULL,                -- original capitalization (e.g. "Concentrix")
    description jsonb NOT NULL,               -- {industry, category, visual_description, color_palette}
    hits        int  NOT NULL DEFAULT 1,      -- usage counter; helps spot popular entities
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS pulse_entity_visuals_updated_idx
    ON pulse_entity_visuals (updated_at DESC);

-- Same grant strategy as the other pulse_* tables.
GRANT ALL PRIVILEGES ON TABLE pulse_entity_visuals TO service_role, anon, authenticated;

-- Sanity check:
--   SELECT count(*) FROM pulse_entity_visuals;   -- expected: 0
