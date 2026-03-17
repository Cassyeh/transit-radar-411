-- Enables PostGIS geospatial extension.
CREATE EXTENSION IF NOT EXISTS postgis;

-- ============================================================
-- CONFIRMATION
-- ============================================================
DO $$
BEGIN
    RAISE NOTICE '00_extensions — PostGIS enabled successfully.';
END $$;