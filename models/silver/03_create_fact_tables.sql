-- ====================================================================
-- SILVER LAYER: Fact Table (Star Schema)
-- ====================================================================
-- Fact Table: fact_toot_engagement
-- Purpose: Core metrics table with foreign keys to dimensions
-- ====================================================================

CREATE TABLE IF NOT EXISTS dim_facts.fact_toot_engagement (
    -- Surrogate Primary Key
    toot_engagement_key SERIAL PRIMARY KEY,
    
    -- Natural Key (Business Key)
    toot_id TEXT NOT NULL UNIQUE,
    
    -- Foreign Keys to Dimensions
    date_key INTEGER,
    account_key INTEGER,
    content_key INTEGER,
    sentiment_key INTEGER,
    
    -- Additive Facts (Metrics)
    replies_count BIGINT DEFAULT 0,
    reblogs_count BIGINT DEFAULT 0,
    favourites_count BIGINT DEFAULT 0,
    quotes_count BIGINT DEFAULT 0,
    total_engagement BIGINT DEFAULT 0,  -- Sum of all engagement metrics
    
    -- Degenerate Dimensions (Low cardinality attributes in fact table)
    visibility VARCHAR(20),
    language VARCHAR(10),
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    edited_at TIMESTAMP WITH TIME ZONE,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT fk_date FOREIGN KEY (date_key) REFERENCES dim_facts.dim_date (date_key),
    CONSTRAINT fk_account FOREIGN KEY (account_key) REFERENCES dim_facts.dim_account (account_key),
    CONSTRAINT fk_content FOREIGN KEY (content_key) REFERENCES dim_facts.dim_content (content_key),
    CONSTRAINT fk_sentiment FOREIGN KEY (sentiment_key) REFERENCES dim_facts.dim_sentiment (sentiment_key)
);

-- Create indexes for query performance
CREATE INDEX IF NOT EXISTS idx_fact_toot_id ON dim_facts.fact_toot_engagement (toot_id);
CREATE INDEX IF NOT EXISTS idx_fact_date_key ON dim_facts.fact_toot_engagement (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_account_key ON dim_facts.fact_toot_engagement (account_key);
CREATE INDEX IF NOT EXISTS idx_fact_content_key ON dim_facts.fact_toot_engagement (content_key);
CREATE INDEX IF NOT EXISTS idx_fact_sentiment_key ON dim_facts.fact_toot_engagement (sentiment_key);
CREATE INDEX IF NOT EXISTS idx_fact_created_at ON dim_facts.fact_toot_engagement (created_at);
CREATE INDEX IF NOT EXISTS idx_fact_total_engagement ON dim_facts.fact_toot_engagement (total_engagement DESC);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fact_date_account ON dim_facts.fact_toot_engagement (date_key, account_key);
CREATE INDEX IF NOT EXISTS idx_fact_date_sentiment ON dim_facts.fact_toot_engagement (date_key, sentiment_key);

-- Log fact table creation
DO $$
BEGIN
  RAISE NOTICE 'Silver fact table created: fact_toot_engagement';
END $$;

