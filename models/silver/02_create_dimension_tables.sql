-- ====================================================================
-- SILVER LAYER: Dimension Tables (Star Schema)
-- ====================================================================
-- Dimensions: Date, Account, Content, Sentiment
-- Note: Account uses SCD Type 2 for historical tracking
-- ====================================================================

-- ====================================================================
-- DIM_DATE: Time Dimension
-- ====================================================================
CREATE TABLE IF NOT EXISTS dim_facts.dim_date (
    date_key INTEGER PRIMARY KEY,  -- Format: YYYYMMDD
    date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week_of_year INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    hour INTEGER  -- For hourly analysis (0-23)
);

CREATE INDEX IF NOT EXISTS idx_dim_date_date ON dim_facts.dim_date (date);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dim_facts.dim_date (year, month);


-- ====================================================================
-- DIM_ACCOUNT: Account/User Dimension (SCD Type 2)
-- ====================================================================
CREATE TABLE IF NOT EXISTS dim_facts.dim_account (
    account_key SERIAL PRIMARY KEY,  -- Surrogate key
    account_id TEXT NOT NULL,  -- Natural key (can have multiple rows for history)
    
    -- Account Attributes
    username TEXT NOT NULL,
    display_name TEXT,
    followers_count BIGINT DEFAULT 0,
    following_count BIGINT DEFAULT 0,
    statuses_count BIGINT DEFAULT 0,
    is_bot BOOLEAN DEFAULT FALSE,
    account_created_at TIMESTAMP WITH TIME ZONE,
    
    -- Derived Attributes
    account_age_days INTEGER,  -- Calculated: days since account_created_at
    influence_tier TEXT,  -- Micro (<10k), Mid (10k-100k), Macro (100k-1M), Mega (>1M)
    engagement_ratio DOUBLE PRECISION,  -- followers_count / following_count
    
    -- SCD Type 2 Tracking
    valid_from TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP WITH TIME ZONE DEFAULT '9999-12-31 23:59:59+00'::TIMESTAMP WITH TIME ZONE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_account_id ON dim_facts.dim_account (account_id);
CREATE INDEX IF NOT EXISTS idx_dim_account_current ON dim_facts.dim_account (account_id, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_account_username ON dim_facts.dim_account (username);
CREATE INDEX IF NOT EXISTS idx_dim_account_influence_tier ON dim_facts.dim_account (influence_tier);


-- ====================================================================
-- DIM_CONTENT: Content Metadata Dimension
-- ====================================================================
CREATE TABLE IF NOT EXISTS dim_facts.dim_content (
    content_key SERIAL PRIMARY KEY,  -- Surrogate key
    toot_id TEXT NOT NULL UNIQUE,  -- Natural key (one row per toot)
    
    -- Content Attributes
    language VARCHAR(10),
    visibility VARCHAR(20),
    content_length INTEGER,
    content_clean_length INTEGER,
    
    -- Media Attributes
    has_media BOOLEAN DEFAULT FALSE,
    media_count INTEGER DEFAULT 0,
    media_types TEXT,
    
    -- Interaction Attributes
    has_poll BOOLEAN DEFAULT FALSE,
    has_card BOOLEAN DEFAULT FALSE,
    is_reblog BOOLEAN DEFAULT FALSE,
    is_reply BOOLEAN DEFAULT FALSE,
    is_sensitive BOOLEAN DEFAULT FALSE,
    has_spoiler BOOLEAN DEFAULT FALSE,
    
    -- Hashtag and Mention Attributes
    hashtag_count INTEGER DEFAULT 0,
    mention_count INTEGER DEFAULT 0,
    tag_names TEXT,  -- Comma-separated for analysis
    mention_usernames TEXT,  -- Comma-separated
    
    -- Content Classification
    content_type VARCHAR(50),  -- Original, Reply, Reblog, Quote
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint on toot_id (one content record per toot)
    CONSTRAINT unique_dim_content_toot_id UNIQUE (toot_id)
);

CREATE INDEX IF NOT EXISTS idx_dim_content_toot_id ON dim_facts.dim_content (toot_id);
CREATE INDEX IF NOT EXISTS idx_dim_content_language ON dim_facts.dim_content (language);
CREATE INDEX IF NOT EXISTS idx_dim_content_visibility ON dim_facts.dim_content (visibility);
CREATE INDEX IF NOT EXISTS idx_dim_content_has_media ON dim_facts.dim_content (has_media);
CREATE INDEX IF NOT EXISTS idx_dim_content_content_type ON dim_facts.dim_content (content_type);


-- ====================================================================
-- DIM_SENTIMENT: Sentiment Analysis Dimension
-- ====================================================================
CREATE TABLE IF NOT EXISTS dim_facts.dim_sentiment (
    sentiment_key SERIAL PRIMARY KEY,  -- Surrogate key
    
    -- Sentiment Attributes
    sentiment_value VARCHAR(20),  -- positive, negative, neutral
    sentiment_score_min DOUBLE PRECISION,
    sentiment_score_max DOUBLE PRECISION,
    sentiment_confidence VARCHAR(20),  -- high, medium, low
    sentiment_model_name TEXT,
    
    -- Unique constraint on combination
    CONSTRAINT unique_sentiment_combo UNIQUE (sentiment_value, sentiment_score_min, sentiment_score_max, sentiment_model_name)
);

CREATE INDEX IF NOT EXISTS idx_dim_sentiment_value ON dim_facts.dim_sentiment (sentiment_value);
CREATE INDEX IF NOT EXISTS idx_dim_sentiment_confidence ON dim_facts.dim_sentiment (sentiment_confidence);

-- Pre-populate sentiment dimension with common values
INSERT INTO dim_facts.dim_sentiment (sentiment_value, sentiment_score_min, sentiment_score_max, sentiment_confidence, sentiment_model_name)
VALUES 
    ('positive', 0.75, 1.0, 'high', 'cardiffnlp/twitter-roberta-base-sentiment-latest'),
    ('positive', 0.50, 0.75, 'medium', 'cardiffnlp/twitter-roberta-base-sentiment-latest'),
    ('negative', 0.75, 1.0, 'high', 'cardiffnlp/twitter-roberta-base-sentiment-latest'),
    ('negative', 0.50, 0.75, 'medium', 'cardiffnlp/twitter-roberta-base-sentiment-latest'),
    ('neutral', 0.0, 1.0, 'low', 'cardiffnlp/twitter-roberta-base-sentiment-latest')
ON CONFLICT (sentiment_value, sentiment_score_min, sentiment_score_max, sentiment_model_name) DO NOTHING;


-- Log dimension creation
DO $$
BEGIN
  RAISE NOTICE 'Silver dimension tables created: dim_date, dim_account, dim_content, dim_sentiment';
END $$;

