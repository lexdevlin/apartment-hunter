-- ============================================================
-- Apartment Hunter — Supabase schema
-- Run this once in the Supabase SQL editor to create the table.
-- ============================================================

CREATE TABLE IF NOT EXISTS listings (
    -- Primary key: the canonical listing URL (stable across scrape runs)
    url                TEXT        PRIMARY KEY,

    -- Scraper-populated fields
    listing_id         TEXT        NOT NULL,           -- MD5(url)[:12], used for display
    source             TEXT        NOT NULL,           -- "streeteasy" | "craigslist" | "zillow" | "apartments_com"
    title              TEXT,
    price              INTEGER,                        -- monthly rent in USD (raw integer, e.g. 2800)
    neighborhood       TEXT,
    address            TEXT,
    floor              TEXT,                           -- e.g. "3" or "Garden"
    bedrooms           INTEGER,
    bathrooms          REAL,
    rent_stabilized    BOOLEAN,
    dishwasher         BOOLEAN,
    washer_dryer       BOOLEAN,
    date_listed        DATE,                           -- when listing was posted on the source site
    nearest_subway     TEXT,                           -- full proximity string, e.g. "DeKalb Av (L) ~4 min"
    subway_lines       TEXT,                           -- compact version, e.g. "(L) ~4 min | (M) ~9 min"
    date_found         TIMESTAMPTZ,                    -- when our scraper first saw this listing
    last_seen          TIMESTAMPTZ,                    -- most recent scrape run that found this listing
    delisted           BOOLEAN     DEFAULT FALSE,      -- TRUE = confirmed gone (404/410 or gone-pattern match)
    priority_score     REAL,                           -- 0–100 computed each run
    is_priority        BOOLEAN     DEFAULT FALSE,
    reviewed           BOOLEAN     DEFAULT FALSE,      -- TRUE = user has reviewed this listing

    -- Listing photo (og:image from the detail page, populated during enrichment)
    image_url          TEXT,

    -- UI-owned field: never written by the scraper
    user_status        TEXT        DEFAULT NULL        -- NULL | 'saved' | 'skipped'
        CHECK (user_status IN ('saved', 'skipped') OR user_status IS NULL),

    -- Housekeeping
    created_at         TIMESTAMPTZ DEFAULT now(),
    updated_at         TIMESTAMPTZ DEFAULT now()
);

-- ============================================================
-- If you already ran the schema without image_url, add the column:
--   ALTER TABLE listings ADD COLUMN IF NOT EXISTS image_url TEXT;
-- ============================================================

-- ============================================================
-- Auto-update updated_at on every write
-- ============================================================
CREATE OR REPLACE FUNCTION _set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_listings_updated_at ON listings;
CREATE TRIGGER trg_listings_updated_at
    BEFORE UPDATE ON listings
    FOR EACH ROW EXECUTE FUNCTION _set_updated_at();

-- ============================================================
-- Indexes for common UI query patterns
-- ============================================================

-- Priority listings first, then score descending
CREATE INDEX IF NOT EXISTS idx_listings_priority
    ON listings (is_priority DESC, priority_score DESC NULLS LAST);

-- Filter by source
CREATE INDEX IF NOT EXISTS idx_listings_source
    ON listings (source);

-- Filter out delisted / user-skipped
CREATE INDEX IF NOT EXISTS idx_listings_status
    ON listings (delisted, user_status);

-- ============================================================
-- Row-Level Security (optional but recommended)
-- ============================================================
-- If you are using the anon key from a public Streamlit app,
-- enable RLS and restrict write access to the service role:
--
--   ALTER TABLE listings ENABLE ROW LEVEL SECURITY;
--
--   CREATE POLICY "allow_read" ON listings
--       FOR SELECT USING (true);
--
--   CREATE POLICY "allow_write_service" ON listings
--       FOR ALL USING (auth.role() = 'service_role');
--
-- The scraper should always use the SERVICE ROLE key (SUPABASE_KEY env var).
-- The Streamlit UI can use the anon key for reads and the service key for
-- user_status updates, or you can expose both operations via the service key
-- if the app is not public.
