-- ====================================================================
-- BRONZE LAYER: Raw Transformed Data Storage
-- ====================================================================
-- Purpose: Store transformed data with audit trail and lineage tracking
-- Layer: Bronze (Raw but cleaned data)
-- ====================================================================

-- Create Bronze schema
CREATE SCHEMA IF NOT EXISTS bronze;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA bronze TO postgres;

-- Log schema creation
DO $$
BEGIN
  RAISE NOTICE 'Bronze schema created successfully';
END $$;

