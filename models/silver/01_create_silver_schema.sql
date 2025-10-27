-- ====================================================================
-- SILVER LAYER: Dimensional Model (Star Schema)
-- ====================================================================
-- Purpose: Fact and dimension tables for analytical queries
-- Layer: Silver (Cleaned and modeled data)
-- Schema: dim_facts (from settings.env: SILVER_DATABASE_SCHEMA_NAME)
-- ====================================================================

-- Create Silver schema
CREATE SCHEMA IF NOT EXISTS dim_facts;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA dim_facts TO postgres;

-- Log schema creation
DO $$
BEGIN
  RAISE NOTICE 'Silver schema created successfully: dim_facts';
END $$;

