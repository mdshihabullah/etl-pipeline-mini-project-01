-- ====================================================================
-- GOLD LAYER: Materialized Views for Analytics
-- ====================================================================
-- Purpose: Pre-aggregated views optimized for analyst queries
-- Refresh: Triggered by Bronze data updates
-- ====================================================================

-- ====================================================================
-- MV_DAILY_ENGAGEMENT_SUMMARY: Daily aggregated engagement metrics
-- ====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_daily_engagement_summary AS
SELECT 
    d.date,
    d.year,
    d.month,
    d.month_name,
    d.day_name,
    d.is_weekend,
    COUNT(DISTINCT f.toot_id) AS total_toots,
    COUNT(DISTINCT f.account_key) AS unique_accounts,
    SUM(f.total_engagement) AS total_engagement,
    ROUND(AVG(f.total_engagement), 2) AS avg_engagement_per_toot,
    SUM(f.replies_count) AS total_replies,
    SUM(f.reblogs_count) AS total_reblogs,
    SUM(f.favourites_count) AS total_favourites,
    SUM(f.quotes_count) AS total_quotes,
    
    -- Sentiment distribution
    COUNT(CASE WHEN s.sentiment_value = 'positive' THEN 1 END) AS positive_toots,
    COUNT(CASE WHEN s.sentiment_value = 'negative' THEN 1 END) AS negative_toots,
    COUNT(CASE WHEN s.sentiment_value = 'neutral' THEN 1 END) AS neutral_toots,
    ROUND(100.0 * COUNT(CASE WHEN s.sentiment_value = 'positive' THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS positive_pct,
    
    -- Language distribution (top language)
    MODE() WITHIN GROUP (ORDER BY f.language) FILTER (WHERE f.language IS NOT NULL) AS top_language,
    
    -- Content metrics
    COUNT(CASE WHEN c.has_media THEN 1 END) AS toots_with_media,
    COUNT(CASE WHEN c.is_reply THEN 1 END) AS reply_toots,
    COUNT(CASE WHEN c.is_reblog THEN 1 END) AS reblogs,
    
    CURRENT_TIMESTAMP AS refreshed_at
FROM dim_facts.fact_toot_engagement f
JOIN dim_facts.dim_date d ON f.date_key = d.date_key
LEFT JOIN dim_facts.dim_sentiment s ON f.sentiment_key = s.sentiment_key
LEFT JOIN dim_facts.dim_content c ON f.content_key = c.content_key
GROUP BY d.date, d.year, d.month, d.month_name, d.day_name, d.is_weekend
ORDER BY d.date DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_engagement_date ON analytics.mv_daily_engagement_summary (date);


-- ====================================================================
-- MV_TOP_PERFORMING_CONTENT: Top content by engagement
-- ====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_top_performing_content AS
SELECT 
    f.toot_id,
    LEFT(COALESCE(b.content_clean, b.content), 200) AS content_preview,
    a.username AS account_username,
    a.display_name AS account_display_name,
    f.created_at,
    f.total_engagement,
    f.replies_count,
    f.reblogs_count,
    f.favourites_count,
    f.quotes_count,
    s.sentiment_value,
    f.language,
    c.tag_names AS hashtags,
    c.has_media,
    c.media_types,
    a.followers_count AS account_followers,
    a.influence_tier,
    
    -- Calculate engagement rate
    ROUND(100.0 * f.total_engagement / NULLIF(a.followers_count, 0), 4) AS engagement_rate,
    
    -- Ranking
    ROW_NUMBER() OVER (ORDER BY f.total_engagement DESC) AS engagement_rank,
    
    CURRENT_TIMESTAMP AS refreshed_at
FROM dim_facts.fact_toot_engagement f
LEFT JOIN bronze.transformed_toots_with_sentiment_data b ON f.toot_id = b.id
LEFT JOIN dim_facts.dim_account a ON f.account_key = a.account_key AND a.is_current = TRUE
LEFT JOIN dim_facts.dim_sentiment s ON f.sentiment_key = s.sentiment_key
LEFT JOIN dim_facts.dim_content c ON f.content_key = c.content_key
ORDER BY f.total_engagement DESC
LIMIT 1000;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_top_content_toot_id ON analytics.mv_top_performing_content (toot_id);


-- ====================================================================
-- MV_ACCOUNT_INFLUENCE_ANALYSIS: Account performance metrics
-- ====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_account_influence_analysis AS
SELECT 
    a.account_id,
    a.username,
    a.display_name,
    a.influence_tier,
    a.followers_count,
    a.following_count,
    a.statuses_count,
    a.is_bot,
    
    -- Engagement metrics
    COUNT(f.toot_id) AS total_toots_in_dataset,
    SUM(f.total_engagement) AS total_engagement,
    ROUND(AVG(f.total_engagement), 2) AS avg_engagement_per_toot,
    MAX(f.total_engagement) AS max_engagement,
    
    -- Sentiment distribution
    ROUND(100.0 * SUM(CASE WHEN s.sentiment_value = 'positive' THEN 1 ELSE 0 END) / NULLIF(COUNT(f.toot_id), 0), 2) AS positive_pct,
    ROUND(100.0 * SUM(CASE WHEN s.sentiment_value = 'negative' THEN 1 ELSE 0 END) / NULLIF(COUNT(f.toot_id), 0), 2) AS negative_pct,
    
    -- Content patterns
    SUM(CASE WHEN c.has_media THEN 1 ELSE 0 END) AS toots_with_media,
    SUM(CASE WHEN c.is_reply THEN 1 ELSE 0 END) AS reply_count,
    ROUND(AVG(c.hashtag_count), 2) AS avg_hashtags_per_toot,
    
    -- Temporal patterns
    MIN(f.created_at) AS first_toot,
    MAX(f.created_at) AS last_toot,
    
    CURRENT_TIMESTAMP AS refreshed_at
FROM dim_facts.dim_account a
JOIN dim_facts.fact_toot_engagement f ON a.account_key = f.account_key
LEFT JOIN dim_facts.dim_sentiment s ON f.sentiment_key = s.sentiment_key
LEFT JOIN dim_facts.dim_content c ON f.content_key = c.content_key
WHERE a.is_current = TRUE
GROUP BY a.account_id, a.username, a.display_name, a.influence_tier, 
         a.followers_count, a.following_count, a.statuses_count, a.is_bot
ORDER BY total_engagement DESC;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_account_analysis_id ON analytics.mv_account_influence_analysis (account_id);


-- ====================================================================
-- MV_HASHTAG_PERFORMANCE: Hashtag analytics
-- ====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_hashtag_performance AS
WITH hashtag_split AS (
    SELECT 
        f.toot_id,
        f.total_engagement,
        f.date_key,
        s.sentiment_value,
        UNNEST(STRING_TO_ARRAY(LOWER(c.tag_names), ',')) AS hashtag
    FROM dim_facts.fact_toot_engagement f
    LEFT JOIN dim_facts.dim_content c ON f.content_key = c.content_key
    LEFT JOIN dim_facts.dim_sentiment s ON f.sentiment_key = s.sentiment_key
    WHERE c.tag_names IS NOT NULL AND c.tag_names != ''
)
SELECT 
    TRIM(hashtag) AS hashtag,
    COUNT(DISTINCT toot_id) AS total_mentions,
    SUM(total_engagement) AS total_engagement,
    ROUND(AVG(total_engagement), 2) AS avg_engagement,
    MAX(total_engagement) AS max_engagement,
    
    -- Sentiment distribution
    COUNT(CASE WHEN sentiment_value = 'positive' THEN 1 END) AS positive_mentions,
    COUNT(CASE WHEN sentiment_value = 'negative' THEN 1 END) AS negative_mentions,
    COUNT(CASE WHEN sentiment_value = 'neutral' THEN 1 END) AS neutral_mentions,
    
    -- Trending score (engagement per mention)
    ROUND(SUM(total_engagement)::NUMERIC / NULLIF(COUNT(DISTINCT toot_id), 0), 2) AS trending_score,
    
    -- Temporal
    COUNT(DISTINCT date_key) AS days_active,
    
    CURRENT_TIMESTAMP AS refreshed_at
FROM hashtag_split
WHERE hashtag IS NOT NULL AND TRIM(hashtag) != ''
GROUP BY TRIM(hashtag)
HAVING COUNT(DISTINCT toot_id) >= 2  -- Filter out single mentions
ORDER BY total_engagement DESC
LIMIT 500;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_hashtag_name ON analytics.mv_hashtag_performance (hashtag);


-- ====================================================================
-- MV_HOURLY_POSTING_PATTERNS: Temporal posting patterns
-- ====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_hourly_posting_patterns AS
SELECT 
    EXTRACT(HOUR FROM f.created_at) AS hour_of_day,
    d.day_name,
    d.is_weekend,
    COUNT(f.toot_id) AS toot_volume,
    SUM(f.total_engagement) AS total_engagement,
    ROUND(AVG(f.total_engagement), 2) AS avg_engagement,
    
    -- Rank hours for optimal posting times
    ROW_NUMBER() OVER (ORDER BY AVG(f.total_engagement) DESC) AS engagement_rank,
    
    CURRENT_TIMESTAMP AS refreshed_at
FROM dim_facts.fact_toot_engagement f
JOIN dim_facts.dim_date d ON f.date_key = d.date_key
GROUP BY EXTRACT(HOUR FROM f.created_at), d.day_name, d.is_weekend
ORDER BY hour_of_day, d.day_name;

CREATE INDEX IF NOT EXISTS idx_mv_hourly_patterns_hour ON analytics.mv_hourly_posting_patterns (hour_of_day);


-- ====================================================================
-- MV_SENTIMENT_TRENDS: Sentiment analysis over time
-- ====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_sentiment_trends AS
SELECT 
    d.date,
    s.sentiment_value,
    COUNT(f.toot_id) AS toot_count,
    ROUND(AVG(f.total_engagement), 2) AS avg_engagement,
    SUM(f.total_engagement) AS total_engagement,
    
    -- Calculate percentage change from previous day (window function)
    ROUND(100.0 * (COUNT(f.toot_id) - LAG(COUNT(f.toot_id)) OVER (PARTITION BY s.sentiment_value ORDER BY d.date)) 
          / NULLIF(LAG(COUNT(f.toot_id)) OVER (PARTITION BY s.sentiment_value ORDER BY d.date), 0), 2) AS sentiment_change_pct,
    
    CURRENT_TIMESTAMP AS refreshed_at
FROM dim_facts.fact_toot_engagement f
JOIN dim_facts.dim_date d ON f.date_key = d.date_key
LEFT JOIN dim_facts.dim_sentiment s ON f.sentiment_key = s.sentiment_key
WHERE s.sentiment_value IS NOT NULL
GROUP BY d.date, s.sentiment_value
ORDER BY d.date DESC, s.sentiment_value;

CREATE INDEX IF NOT EXISTS idx_mv_sentiment_trends_date ON analytics.mv_sentiment_trends (date, sentiment_value);


-- ====================================================================
-- MV_VIRAL_CONTENT_INDICATORS: Identify viral content patterns
-- ====================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_viral_content_indicators AS
SELECT 
    f.toot_id,
    LEFT(COALESCE(b.content_clean, b.content), 200) AS content_preview,
    a.username,
    f.created_at,
    f.total_engagement,
    
    -- Calculate virality metrics
    ROUND(f.total_engagement::NUMERIC / NULLIF(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - f.created_at)) / 3600, 0), 2) AS engagement_per_hour,
    ROUND(f.total_engagement::NUMERIC / NULLIF(a.followers_count, 0) * 100, 2) AS engagement_rate,
    
    -- Virality score (composite metric)
    ROUND((f.total_engagement * 0.4 + f.reblogs_count * 0.4 + f.quotes_count * 0.2)::NUMERIC 
          / NULLIF(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - f.created_at)) / 3600, 0), 2) AS virality_score,
    
    -- Content features
    c.has_media,
    c.media_count,
    c.hashtag_count,
    c.mention_count,
    c.content_type,
    s.sentiment_value,
    a.influence_tier,
    
    CURRENT_TIMESTAMP AS refreshed_at
FROM dim_facts.fact_toot_engagement f
LEFT JOIN bronze.transformed_toots_with_sentiment_data b ON f.toot_id = b.id
LEFT JOIN dim_facts.dim_account a ON f.account_key = a.account_key AND a.is_current = TRUE
LEFT JOIN dim_facts.dim_content c ON f.content_key = c.content_key
LEFT JOIN dim_facts.dim_sentiment s ON f.sentiment_key = s.sentiment_key
WHERE f.total_engagement > 10  -- Filter for meaningful engagement
ORDER BY f.total_engagement DESC
LIMIT 500;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_viral_content_toot_id ON analytics.mv_viral_content_indicators (toot_id);


-- Log materialized view creation
DO $$
BEGIN
  RAISE NOTICE 'Gold materialized views created successfully';
  RAISE NOTICE '  - mv_daily_engagement_summary';
  RAISE NOTICE '  - mv_top_performing_content';
  RAISE NOTICE '  - mv_account_influence_analysis';
  RAISE NOTICE '  - mv_hashtag_performance';
  RAISE NOTICE '  - mv_hourly_posting_patterns';
  RAISE NOTICE '  - mv_sentiment_trends';
  RAISE NOTICE '  - mv_viral_content_indicators';
END $$;

