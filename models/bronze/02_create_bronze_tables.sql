-- ====================================================================
-- BRONZE LAYER: Main Data Table
-- ====================================================================
-- Table: transformed_toots_with_sentiment_data
-- Purpose: Store complete transformed toot data with metadata
-- ====================================================================

CREATE TABLE IF NOT EXISTS bronze.transformed_toots_with_sentiment_data (
    -- Primary Key
    id TEXT PRIMARY KEY,
    
    -- Temporal Information
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    edited_at TIMESTAMP WITH TIME ZONE,
    
    -- Reply/Thread Context
    in_reply_to_id TEXT,
    in_reply_to_account_id TEXT,
    
    -- Content Flags
    sensitive BOOLEAN DEFAULT FALSE,
    spoiler_text TEXT,
    visibility TEXT,
    language TEXT,
    
    -- URIs
    uri TEXT,
    url TEXT,
    
    -- Engagement Metrics (Additive Facts)
    replies_count BIGINT DEFAULT 0,
    reblogs_count BIGINT DEFAULT 0,
    favourites_count BIGINT DEFAULT 0,
    quotes_count BIGINT DEFAULT 0,
    
    -- Content
    content TEXT,
    content_clean TEXT,
    spoiler_text_clean TEXT,
    
    -- Complex JSON Fields (Preserved for audit)
    reblog TEXT,
    account TEXT,
    media_attachments TEXT,
    mentions TEXT,
    tags TEXT,
    emojis TEXT,
    quote TEXT,
    card TEXT,
    poll TEXT,
    quote_approval TEXT,
    application TEXT,
    
    -- Extracted Account Fields
    account_id TEXT,
    account_username TEXT,
    account_display_name TEXT,
    account_followers_count BIGINT,
    account_following_count BIGINT,
    account_statuses_count BIGINT,
    account_is_bot BOOLEAN,
    account_created_at TIMESTAMP WITH TIME ZONE,
    
    -- Extracted Content Metadata
    tag_names TEXT,
    mention_usernames TEXT,
    media_count BIGINT,
    media_types TEXT,
    
    -- Content Flags (Derived)
    is_reblog BOOLEAN,
    has_poll BOOLEAN,
    has_card BOOLEAN,
    
    -- Sentiment Analysis Results
    sentiment_score DOUBLE PRECISION,
    sentiment_value TEXT,
    sentiment_model_name TEXT,
    
    -- Metadata for Lineage and Audit
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    pipeline_run_id TEXT,
    data_version TEXT DEFAULT '1.0',
    
    -- Constraints
    CONSTRAINT unique_bronze_toot_id UNIQUE (id)
);

-- Create indexes for common query patterns and joins
CREATE INDEX IF NOT EXISTS idx_bronze_created_at ON bronze.transformed_toots_with_sentiment_data (created_at);
CREATE INDEX IF NOT EXISTS idx_bronze_account_id ON bronze.transformed_toots_with_sentiment_data (account_id);
CREATE INDEX IF NOT EXISTS idx_bronze_language ON bronze.transformed_toots_with_sentiment_data (language);
CREATE INDEX IF NOT EXISTS idx_bronze_sentiment_value ON bronze.transformed_toots_with_sentiment_data (sentiment_value);
CREATE INDEX IF NOT EXISTS idx_bronze_ingestion_timestamp ON bronze.transformed_toots_with_sentiment_data (ingestion_timestamp);

-- Log table creation
DO $$
BEGIN
  RAISE NOTICE 'Bronze table created: transformed_toots_with_sentiment_data';
END $$;

