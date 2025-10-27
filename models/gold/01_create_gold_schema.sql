-- ====================================================================
-- GOLD LAYER: Analytics and Reporting Layer
-- ====================================================================
-- Purpose: Aggregated and curated data for business intelligence
-- Layer: Gold (Analytics-ready data)
-- Schema: analytics (from settings.env: GOLD_DATABASE_SCHEMA_NAME)
-- ====================================================================

-- Create Gold schema
CREATE SCHEMA IF NOT EXISTS analytics;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA analytics TO postgres;

-- Log schema creation
DO $$
BEGIN
  RAISE NOTICE 'Gold schema created successfully: analytics';
END $$;

