"""Silver Layer ETL - Transform Bronze data into dimensional model."""

import psycopg
import logging
from typing import Optional
from datetime import datetime

from extractor.config import config

logger = logging.getLogger(__name__)


class SilverLayerETL:
    """ETL process to populate Silver layer dimensional model from Bronze layer."""
    
    def __init__(self):
        """Initialize the Silver ETL with database configuration."""
        self.db_config = {
            'dbname': config.database_name,
            'user': config.database_user,
            'password': config.database_password,
            'host': config.database_host,
            'port': config.database_port
        }
        self.bronze_schema = config.bronze_database_schema_name
        self.silver_schema = config.silver_database_schema_name
        
        logger.info(f"SilverLayerETL initialized: {self.bronze_schema} → {self.silver_schema}")
    
    def execute_etl(self) -> bool:
        """
        Execute complete ETL process: Bronze → Silver.
        
        Order:
        1. Populate dim_date
        2. Populate dim_account (SCD Type 2)
        3. Populate dim_content
        4. Populate dim_sentiment
        5. Populate fact_toot_engagement
        
        Returns:
            True if successful, False otherwise
        """
        logger.info("\n" + "="*60)
        logger.info("SILVER LAYER ETL: Bronze → Silver")
        logger.info("="*60)
        
        try:
            with psycopg.connect(**self.db_config) as conn:
                # Execute ETL steps in order
                if not self._populate_dim_date(conn):
                    return False
                
                if not self._populate_dim_account_scd2(conn):
                    return False
                
                if not self._populate_dim_content(conn):
                    return False
                
                if not self._populate_dim_sentiment(conn):
                    return False
                
                if not self._populate_fact_toot_engagement(conn):
                    return False
                
                conn.commit()
                
                logger.info("\n✅ Silver layer ETL completed successfully")
                return True
                
        except psycopg.OperationalError as e:
            logger.error(f"Database connection failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Silver ETL failed: {e}", exc_info=True)
            return False
    
    def _populate_dim_date(self, conn: psycopg.Connection) -> bool:
        """Populate dim_date dimension."""
        logger.info("Populating dim_date...")
        
        sql = f"""
        INSERT INTO {self.silver_schema}.dim_date (
            date_key, date, year, quarter, month, month_name, 
            week_of_year, day_of_month, day_of_week, day_name, 
            is_weekend, hour
        )
        SELECT DISTINCT
            TO_CHAR(created_at, 'YYYYMMDD')::INTEGER AS date_key,
            DATE(created_at) AS date,
            EXTRACT(YEAR FROM created_at)::INTEGER AS year,
            EXTRACT(QUARTER FROM created_at)::INTEGER AS quarter,
            EXTRACT(MONTH FROM created_at)::INTEGER AS month,
            TO_CHAR(created_at, 'Month') AS month_name,
            EXTRACT(WEEK FROM created_at)::INTEGER AS week_of_year,
            EXTRACT(DAY FROM created_at)::INTEGER AS day_of_month,
            EXTRACT(DOW FROM created_at)::INTEGER AS day_of_week,
            TO_CHAR(created_at, 'Day') AS day_name,
            CASE WHEN EXTRACT(DOW FROM created_at) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
            EXTRACT(HOUR FROM created_at)::INTEGER AS hour
        FROM {self.bronze_schema}.transformed_toots_with_sentiment_data
        ON CONFLICT (date_key) DO NOTHING
        """
        
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows_inserted = cur.rowcount
                logger.info(f"  ✅ dim_date: {rows_inserted} rows inserted")
                return True
        except Exception as e:
            logger.error(f"  ❌ Failed to populate dim_date: {e}")
            return False
    
    def _populate_dim_account_scd2(self, conn: psycopg.Connection) -> bool:
        """
        Populate dim_account with SCD Type 2 (Slowly Changing Dimension).
        
        Logic:
        - If account_id doesn't exist, insert new row
        - If account exists but attributes changed, expire old row and insert new
        - If account unchanged, skip
        """
        logger.info("Populating dim_account (SCD Type 2)...")
        
        sql = f"""
        WITH new_accounts AS (
            SELECT DISTINCT
                account_id,
                account_username AS username,
                account_display_name AS display_name,
                account_followers_count AS followers_count,
                account_following_count AS following_count,
                account_statuses_count AS statuses_count,
                account_is_bot AS is_bot,
                account_created_at,
                
                -- Calculate derived attributes
                CASE 
                    WHEN account_created_at IS NOT NULL 
                    THEN (EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - account_created_at)) / 86400)::INTEGER
                    ELSE NULL
                END AS account_age_days,
                
                CASE 
                    WHEN account_followers_count >= 1000000 THEN 'Mega'
                    WHEN account_followers_count >= 100000 THEN 'Macro'
                    WHEN account_followers_count >= 10000 THEN 'Mid'
                    ELSE 'Micro'
                END AS influence_tier,
                
                CASE 
                    WHEN account_following_count > 0 
                    THEN account_followers_count::FLOAT / account_following_count::FLOAT
                    ELSE 0
                END AS engagement_ratio,
                
                MAX(ingestion_timestamp) AS ingestion_timestamp
            FROM {self.bronze_schema}.transformed_toots_with_sentiment_data
            WHERE account_id IS NOT NULL
            GROUP BY 
                account_id, account_username, account_display_name,
                account_followers_count, account_following_count, 
                account_statuses_count, account_is_bot, account_created_at
        ),
        current_accounts AS (
            SELECT * FROM {self.silver_schema}.dim_account WHERE is_current = TRUE
        ),
        changed_accounts AS (
            -- Find accounts that have changed
            SELECT n.*
            FROM new_accounts n
            LEFT JOIN current_accounts c ON n.account_id = c.account_id
            WHERE c.account_id IS NULL  -- New account
               OR c.followers_count != n.followers_count  -- Follower count changed
               OR c.following_count != n.following_count  -- Following count changed
               OR c.statuses_count != n.statuses_count   -- Status count changed
               OR c.username != n.username  -- Username changed
        ),
        expire_old_records AS (
            -- Expire old records for changed accounts
            UPDATE {self.silver_schema}.dim_account
            SET 
                is_current = FALSE,
                valid_to = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE account_id IN (SELECT account_id FROM changed_accounts)
              AND is_current = TRUE
            RETURNING account_id
        )
        -- Insert new/changed records
        INSERT INTO {self.silver_schema}.dim_account (
            account_id, username, display_name, followers_count, following_count,
            statuses_count, is_bot, account_created_at, account_age_days,
            influence_tier, engagement_ratio, valid_from, valid_to, is_current
        )
        SELECT 
            account_id, username, display_name, followers_count, following_count,
            statuses_count, is_bot, account_created_at, account_age_days,
            influence_tier, engagement_ratio,
            ingestion_timestamp AS valid_from,
            '9999-12-31 23:59:59+00'::TIMESTAMP WITH TIME ZONE AS valid_to,
            TRUE AS is_current
        FROM changed_accounts
        ON CONFLICT DO NOTHING
        """
        
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows_inserted = cur.rowcount
                logger.info(f"  ✅ dim_account: {rows_inserted} rows inserted/updated (SCD Type 2)")
                return True
        except Exception as e:
            logger.error(f"  ❌ Failed to populate dim_account: {e}")
            conn.rollback()
            return False
    
    def _populate_dim_content(self, conn: psycopg.Connection) -> bool:
        """Populate dim_content dimension."""
        logger.info("Populating dim_content...")
        
        sql = f"""
        INSERT INTO {self.silver_schema}.dim_content (
            toot_id, language, visibility, content_length, content_clean_length,
            has_media, media_count, media_types, has_poll, has_card,
            is_reblog, is_reply, is_sensitive, has_spoiler,
            hashtag_count, mention_count, tag_names, mention_usernames, content_type
        )
        SELECT DISTINCT
            id AS toot_id,
            language,
            visibility,
            LENGTH(content) AS content_length,
            LENGTH(content_clean) AS content_clean_length,
            (media_count > 0) AS has_media,
            media_count,
            media_types,
            has_poll,
            has_card,
            is_reblog,
            (in_reply_to_id IS NOT NULL) AS is_reply,
            sensitive AS is_sensitive,
            (spoiler_text IS NOT NULL AND spoiler_text != '') AS has_spoiler,
            
            -- Count hashtags and mentions
            CASE 
                WHEN tag_names IS NOT NULL AND tag_names != '' 
                THEN array_length(string_to_array(tag_names, ','), 1)
                ELSE 0
            END AS hashtag_count,
            
            CASE 
                WHEN mention_usernames IS NOT NULL AND mention_usernames != ''
                THEN array_length(string_to_array(mention_usernames, ','), 1)
                ELSE 0
            END AS mention_count,
            
            tag_names,
            mention_usernames,
            
            -- Classify content type
            CASE 
                WHEN is_reblog THEN 'Reblog'
                WHEN in_reply_to_id IS NOT NULL THEN 'Reply'
                WHEN quote IS NOT NULL THEN 'Quote'
                ELSE 'Original'
            END AS content_type
            
        FROM {self.bronze_schema}.transformed_toots_with_sentiment_data
        ON CONFLICT (toot_id) DO NOTHING
        """
        
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows_inserted = cur.rowcount
                logger.info(f"  ✅ dim_content: {rows_inserted} rows inserted")
                return True
        except Exception as e:
            logger.error(f"  ❌ Failed to populate dim_content: {e}")
            return False
    
    def _populate_dim_sentiment(self, conn: psycopg.Connection) -> bool:
        """Populate dim_sentiment dimension."""
        logger.info("Populating dim_sentiment...")
        
        sql = f"""
        INSERT INTO {self.silver_schema}.dim_sentiment (
            sentiment_value, sentiment_score_min, sentiment_score_max, 
            sentiment_confidence, sentiment_model_name
        )
        SELECT DISTINCT
            sentiment_value,
            CASE 
                WHEN sentiment_score >= 0.75 THEN 0.75
                WHEN sentiment_score >= 0.50 THEN 0.50
                ELSE 0.0
            END AS sentiment_score_min,
            CASE 
                WHEN sentiment_score >= 0.75 THEN 1.0
                WHEN sentiment_score >= 0.50 THEN 0.75
                ELSE 0.50
            END AS sentiment_score_max,
            CASE 
                WHEN sentiment_score >= 0.75 THEN 'high'
                WHEN sentiment_score >= 0.50 THEN 'medium'
                ELSE 'low'
            END AS sentiment_confidence,
            sentiment_model_name
        FROM {self.bronze_schema}.transformed_toots_with_sentiment_data
        WHERE sentiment_value IS NOT NULL
        ON CONFLICT (sentiment_value, sentiment_score_min, sentiment_score_max, sentiment_model_name) 
        DO NOTHING
        """
        
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows_inserted = cur.rowcount
                logger.info(f"  ✅ dim_sentiment: {rows_inserted} rows inserted")
                return True
        except Exception as e:
            logger.error(f"  ❌ Failed to populate dim_sentiment: {e}")
            return False
    
    def _populate_fact_toot_engagement(self, conn: psycopg.Connection) -> bool:
        """Populate fact_toot_engagement fact table."""
        logger.info("Populating fact_toot_engagement...")
        
        sql = f"""
        INSERT INTO {self.silver_schema}.fact_toot_engagement (
            toot_id, date_key, account_key, content_key, sentiment_key,
            replies_count, reblogs_count, favourites_count, quotes_count,
            total_engagement, visibility, language, created_at, edited_at
        )
        WITH deduplicated_bronze AS (
            -- Keep only the most recent version of each toot (handles duplicates from multiple pipeline runs)
            SELECT DISTINCT ON (id)
                id,
                created_at,
                account_id,
                sentiment_value,
                sentiment_model_name,
                sentiment_score,
                replies_count,
                reblogs_count,
                favourites_count,
                quotes_count,
                visibility,
                language,
                edited_at
            FROM {self.bronze_schema}.transformed_toots_with_sentiment_data
            ORDER BY id, ingestion_timestamp DESC
        )
        SELECT 
            b.id AS toot_id,
            TO_CHAR(b.created_at, 'YYYYMMDD')::INTEGER AS date_key,
            a.account_key,
            c.content_key,
            s.sentiment_key,
            b.replies_count,
            b.reblogs_count,
            b.favourites_count,
            b.quotes_count,
            (COALESCE(b.replies_count, 0) + COALESCE(b.reblogs_count, 0) + 
             COALESCE(b.favourites_count, 0) + COALESCE(b.quotes_count, 0)) AS total_engagement,
            b.visibility,
            b.language,
            b.created_at,
            b.edited_at
        FROM deduplicated_bronze b
        LEFT JOIN {self.silver_schema}.dim_account a 
            ON b.account_id = a.account_id AND a.is_current = TRUE
        LEFT JOIN {self.silver_schema}.dim_content c 
            ON b.id = c.toot_id
        LEFT JOIN {self.silver_schema}.dim_sentiment s 
            ON LOWER(b.sentiment_value) = LOWER(s.sentiment_value)
            AND b.sentiment_model_name = s.sentiment_model_name
            AND b.sentiment_score IS NOT NULL
            AND b.sentiment_score >= s.sentiment_score_min
            AND b.sentiment_score < s.sentiment_score_max
        ON CONFLICT (toot_id) DO UPDATE SET
            replies_count = EXCLUDED.replies_count,
            reblogs_count = EXCLUDED.reblogs_count,
            favourites_count = EXCLUDED.favourites_count,
            quotes_count = EXCLUDED.quotes_count,
            total_engagement = EXCLUDED.total_engagement,
            edited_at = EXCLUDED.edited_at,
            loaded_at = CURRENT_TIMESTAMP
        """
        
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows_inserted = cur.rowcount
                logger.info(f"  ✅ fact_toot_engagement: {rows_inserted} rows inserted/updated")
                return True
        except Exception as e:
            logger.error(f"  ❌ Failed to populate fact_toot_engagement: {e}")
            return False
    
    def get_silver_stats(self) -> Optional[dict]:
        """Get statistics about Silver layer."""
        try:
            with psycopg.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    stats = {}
                    
                    # Count records in each table
                    tables = ['dim_date', 'dim_account', 'dim_content', 'dim_sentiment', 'fact_toot_engagement']
                    for table in tables:
                        cur.execute(f"SELECT COUNT(*) FROM {self.silver_schema}.{table}")
                        stats[table] = cur.fetchone()[0]
                    
                    # Current accounts
                    cur.execute(f"SELECT COUNT(*) FROM {self.silver_schema}.dim_account WHERE is_current = TRUE")
                    stats['current_accounts'] = cur.fetchone()[0]
                    
                    return stats
                    
        except Exception as e:
            logger.error(f"Failed to get Silver stats: {e}")
            return None

