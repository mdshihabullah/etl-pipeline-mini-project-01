import polars as pl
from mastodon import Mastodon, MastodonAPIError, MastodonNetworkError, MastodonRatelimitError
from datetime import datetime, timezone, timedelta
from tenacity import (
    retry, 
    stop_after_attempt, 
    wait_exponential, 
    retry_if_exception_type,
    before_sleep_log
)
import requests
import json
from typing import List, Dict, Optional
import logging
import sys
from .config import config

# Get logger (logging configuration handled in main.py)
logger = logging.getLogger(__name__)

class MastodonHashtagCrawler:
    """Robust Mastodon hashtag data crawler using fetch_next pagination."""
    
    def __init__(self):
        """Initialize the crawler with configuration from environment."""
        self.base_url = config.mastodon_base_url
        self.hashtag = config.hashtag
        self.limit_per_page = config.toots_limit_per_page
        
        try:
            self.api = Mastodon(
                api_base_url=self.base_url,
                request_timeout=config.api_timeout,
                version_check_mode='none'  # Avoid version check failures
            )
            logger.info(f"Successfully connected to {self.base_url}")
        except Exception as e:
            logger.error(f"Failed to initialize Mastodon API client: {e}")
            raise
        
    def _calculate_cutoff_time(self) -> datetime:
        """Calculate the cutoff time based on configuration."""
        now = datetime.now(timezone.utc)
        if config.time_period_unit == "hours":
            return now - timedelta(hours=config.time_period_value)
        else:  # days
            return now - timedelta(days=config.time_period_value)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((
            requests.exceptions.RequestException,
            requests.exceptions.Timeout,
            MastodonNetworkError,
            MastodonAPIError
        )),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    def _fetch_initial_page(self, hashtag: str) -> Optional[List[Dict]]:
        """
        Fetch the initial page of toots for a hashtag with retry logic.
        
        Returns:
            List of toots or None if no toots found
        """
        try:
            page = self.api.timeline_hashtag(
                hashtag=hashtag,
                limit=self.limit_per_page
            )
            
            # Handle empty response
            if page is None:
                return []
            
            # Validate response is a list
            if not isinstance(page, list):
                logger.error(f"Unexpected response type: {type(page)}")
                return []
                
            return page
            
        except MastodonRatelimitError as e:
            logger.error(f"Rate limit exceeded: {e}")
            # Don't retry rate limit errors
            raise
        except MastodonAPIError as e:
            if '404' in str(e):
                logger.warning(f"Hashtag '{hashtag}' not found or no toots available")
                return []
            logger.error(f"API error fetching initial page: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching initial page: {e}")
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((
            requests.exceptions.RequestException,
            requests.exceptions.Timeout,
            MastodonNetworkError
        )),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    def _fetch_next_page_safe(self, previous_page: List[Dict]) -> Optional[List[Dict]]:
        """
        Safely fetch the next page using Mastodon.py's fetch_next utility.
        
        Returns:
            Next page of toots or None if no more pages
        """
        if not previous_page:
            return None
            
        try:
            next_page = self.api.fetch_next(previous_page)
            
            # Validate response
            if next_page is not None and not isinstance(next_page, list):
                logger.error(f"Unexpected next page type: {type(next_page)}")
                return None
                
            return next_page
            
        except MastodonRatelimitError as e:
            logger.error(f"Rate limit exceeded while fetching next page: {e}")
            # Don't retry rate limit errors
            return None
        except (MastodonAPIError, AttributeError) as e:
            # AttributeError can occur if pagination info is missing
            if '404' in str(e) or 'NoneType' in str(e):
                logger.debug("No more pages available")
                return None
            logger.warning(f"Error fetching next page: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching next page: {e}")
            return None
    
    def fetch_hashtag_toots(self, hashtag: Optional[str] = None) -> pl.DataFrame:
        """
        Fetch all toots for a hashtag within configured time window.
        
        Args:
            hashtag: Optional hashtag override (uses config if not provided)
        
        Returns:
            Polars DataFrame with toot data
        """
        # Use provided hashtag or fall back to config
        hashtag = (hashtag or self.hashtag).lstrip('#').strip()
        
        if not hashtag:
            logger.error("No hashtag provided")
            return self._create_dataframe([])
        
        cutoff_time = self._calculate_cutoff_time()
        
        results = []
        page_count = 0
        total_toots = 0
        reached_cutoff = False
        
        logger.info(
            f"Starting to fetch toots for #{hashtag} from last "
            f"{config.time_period_value} {config.time_period_unit}"
        )
        logger.info(f"Cutoff time: {cutoff_time.isoformat()}")
        
        try:
            # Fetch initial page
            current_page = self._fetch_initial_page(hashtag)
            
            if not current_page:
                logger.info(f"No toots found for hashtag #{hashtag}")
                return self._create_dataframe(results)
            
            while current_page and not reached_cutoff:
                page_count += 1
                page_size = len(current_page)
                logger.info(f"Processing page {page_count} with {page_size} toots")
                
                # Process toots in current page
                for toot in current_page:
                    try:
                        # Validate toot structure
                        if not isinstance(toot, dict):
                            logger.warning(f"Skipping invalid toot (not a dict): {type(toot)}")
                            continue
                        
                        if 'created_at' not in toot:
                            logger.warning("Skipping toot without created_at field")
                            continue
                        
                        # Get timestamp (Mastodon.py returns datetime objects directly)
                        created_at = toot.get('created_at')
                        if not created_at or not isinstance(created_at, datetime):
                            logger.warning("Skipping toot with invalid created_at field")
                            continue
                        
                        # Ensure timezone-aware datetime
                        if created_at.tzinfo is None:
                            created_at = created_at.replace(tzinfo=timezone.utc)
                        
                        # Check if we've gone beyond our time window
                        if created_at < cutoff_time:
                            logger.info(f"Reached cutoff time at {created_at.isoformat()}")
                            reached_cutoff = True
                            break
                        
                        # Extract and add toot data
                        extracted_data = self._extract_toot_data(toot, created_at)
                        if extracted_data:
                            results.append(extracted_data)
                            total_toots += 1
                            
                    except Exception as e:
                        logger.warning(f"Error processing individual toot: {e}")
                        continue
                
                if reached_cutoff:
                    break
                
                # Fetch next page
                next_page = self._fetch_next_page_safe(current_page)
                
                if next_page is None or len(next_page) == 0:
                    logger.info("No more pages available")
                    break
                    
                current_page = next_page
                
                # Safety checks
                if page_count > 100:
                    logger.warning("Reached maximum page limit (100)")
                    break
                
                # Check for duplicate IDs (indicates pagination loop)
                if results and next_page:
                    first_id = next_page[0].get('id') if next_page else None
                    if first_id and any(r['id'] == first_id for r in results[-10:]):
                        logger.warning("Detected pagination loop, stopping")
                        break
                    
        except KeyboardInterrupt:
            logger.info("Crawling interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error during crawling: {e}", exc_info=True)
        finally:
            logger.info(f"Crawling complete: {total_toots} toots across {page_count} pages")
        
        return self._create_dataframe(results)
    
    def _extract_toot_data(self, toot: Dict, created_at: datetime) -> Optional[Dict]:
        """
        Extract all available data from a toot with error handling.
        
        Returns:
            Extracted data dict or None if extraction fails
        """
        try:
            # Safely extract numeric fields with validation
            def safe_int(value, default=None):
                try:
                    return int(value) if value is not None else default
                except (ValueError, TypeError):
                    return default
            
            # Safely extract datetime fields
            def safe_datetime(value):
                if value is None:
                    return None
                if isinstance(value, datetime):
                    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
                try:
                    return datetime.fromisoformat(str(value).replace('Z', '+00:00'))
                except (ValueError, AttributeError, TypeError):
                    return None
            
            # Safely serialize complex objects to JSON
            def safe_json(value):
                if value is None or (isinstance(value, (list, dict)) and not value):
                    return None
                try:
                    return json.dumps(value, default=str)
                except (TypeError, ValueError):
                    return None
            
            # Extract account info
            account = toot.get('account', {}) or {}
            
            return {
                # Basic fields
                'id': str(toot.get('id', '')),
                'created_at': created_at,
                'in_reply_to_id': str(toot.get('in_reply_to_id')) if toot.get('in_reply_to_id') else None,
                'in_reply_to_account_id': str(toot.get('in_reply_to_account_id')) if toot.get('in_reply_to_account_id') else None,
                'sensitive': bool(toot.get('sensitive', False)),
                'spoiler_text': toot.get('spoiler_text') or None,
                'visibility': toot.get('visibility') or None,
                'language': toot.get('language') or None,
                'uri': toot.get('uri') or None,
                'url': toot.get('url') or None,
                
                # Engagement metrics
                'replies_count': safe_int(toot.get('replies_count')),
                'reblogs_count': safe_int(toot.get('reblogs_count')),
                'favourites_count': safe_int(toot.get('favourites_count')),
                'quotes_count': safe_int(toot.get('quotes_count')),
                
                # Edit info
                'edited_at': safe_datetime(toot.get('edited_at')),
                
                # Content
                'content': toot.get('content') or None,
                
                # Complex nested objects (stored as JSON strings)
                'reblog': safe_json(toot.get('reblog')),
                'account': safe_json(account),
                'media_attachments': safe_json(toot.get('media_attachments')),
                'mentions': safe_json(toot.get('mentions')),
                'tags': safe_json(toot.get('tags')),
                'emojis': safe_json(toot.get('emojis')),
                'quote': safe_json(toot.get('quote')),
                'card': safe_json(toot.get('card')),
                'poll': safe_json(toot.get('poll')),
                'quote_approval': safe_json(toot.get('quote_approval')),
                'application': safe_json(toot.get('application')),
                
                # Additional account fields for easy access
                'account_id': str(account.get('id')) if account.get('id') else None,
                'account_username': account.get('username') or None,
                'account_display_name': account.get('display_name') or None,
            }
        except Exception as e:
            logger.warning(f"Failed to extract toot data: {e}")
            return None
    
    def _create_dataframe(self, results: List[Dict]) -> pl.DataFrame:
        """Create a Polars DataFrame from the results with proper error handling."""
        schema = {
            # Basic fields
            'id': pl.Utf8,
            'created_at': pl.Datetime('us', 'UTC'),
            'in_reply_to_id': pl.Utf8,
            'in_reply_to_account_id': pl.Utf8,
            'sensitive': pl.Boolean,
            'spoiler_text': pl.Utf8,
            'visibility': pl.Utf8,
            'language': pl.Utf8,
            'uri': pl.Utf8,
            'url': pl.Utf8,
            
            # Engagement metrics
            'replies_count': pl.Int64,
            'reblogs_count': pl.Int64,
            'favourites_count': pl.Int64,
            'quotes_count': pl.Int64,
            
            # Edit info
            'edited_at': pl.Datetime('us', 'UTC'),
            
            # Content
            'content': pl.Utf8,
            
            # Complex nested objects (JSON strings)
            'reblog': pl.Utf8,
            'account': pl.Utf8,
            'media_attachments': pl.Utf8,
            'mentions': pl.Utf8,
            'tags': pl.Utf8,
            'emojis': pl.Utf8,
            'quote': pl.Utf8,
            'card': pl.Utf8,
            'poll': pl.Utf8,
            'quote_approval': pl.Utf8,
            'application': pl.Utf8,
            
            # Additional account fields for easy access
            'account_id': pl.Utf8,
            'account_username': pl.Utf8,
            'account_display_name': pl.Utf8,
        }
        
        if not results:
            return pl.DataFrame(schema=schema)
        
        try:
            # Create DataFrame with explicit schema to avoid inference issues
            # Set infer_schema_length to None to scan all rows for schema inference
            df = pl.DataFrame(results, schema_overrides=schema, infer_schema_length=None)
            
            # Sort by created_at descending
            if 'created_at' in df.columns:
                df = df.sort('created_at', descending=True)
            
            logger.info(f"DataFrame created successfully with {len(df)} rows")
            return df
            
        except Exception as e:
            logger.error(f"Failed to create DataFrame with schema_overrides: {e}")
            logger.info("Attempting fallback method...")
            
            # Fallback: Create empty DataFrame with schema, then extend with data
            try:
                df = pl.DataFrame(schema=schema)
                
                # Process results in smaller batches to avoid schema inference issues
                batch_size = 500
                dfs = []
                
                for i in range(0, len(results), batch_size):
                    batch = results[i:i + batch_size]
                    batch_df = pl.DataFrame(batch, schema_overrides=schema, infer_schema_length=None)
                    dfs.append(batch_df)
                
                if dfs:
                    df = pl.concat(dfs)
                    logger.info(f"DataFrame created successfully using batch method with {len(df)} rows")
                
                # Sort by created_at descending
                if 'created_at' in df.columns and not df.is_empty():
                    df = df.sort('created_at', descending=True)
                
                return df
                
            except Exception as e2:
                logger.error(f"Fallback method also failed: {e2}")
                logger.warning("Returning empty DataFrame")
                return pl.DataFrame(schema=schema)


def main():
    """Main execution with proper error handling."""
    try:
        # Initialize crawler
        crawler = MastodonHashtagCrawler()
        
        # Fetch toots (uses hashtag from config)
        df = crawler.fetch_hashtag_toots()
        
        # Display results
        print(f"\n{'='*60}")
        print("Mastodon Hashtag Crawler Results")
        print(f"{'='*60}")
        print(f"Hashtag: #{config.hashtag}")
        print(f"Time Period: {config.time_period_value} {config.time_period_unit}")
        print(f"Instance: {config.mastodon_base_url}")
        print(f"{'='*60}\n")
        
        print(f"#### DataFrame Shape: {df.shape}")
        print("\n#### Columns and Data Types:")
        for col, dtype in zip(df.columns, df.dtypes):
            print(f"  {col}: {dtype}")
        
        # Display first 5 rows
        print("\n#### First 5 Toots:")
        if not df.is_empty():
            with pl.Config(
                tbl_rows=5, 
                tbl_width_chars=150, 
                fmt_str_lengths=50,
                set_tbl_hide_dataframe_shape=True
            ):
                print(df.head())
        else:
            print("No data retrieved")
        
        # Display summary statistics
        if not df.is_empty():
            print("\n#### Engagement Statistics:")
            stats = df.select([
                pl.col('reblogs_count').mean().round(2).alias('avg_reblogs'),
                pl.col('favourites_count').mean().round(2).alias('avg_favourites'),
                pl.col('replies_count').mean().round(2).alias('avg_replies'),
                pl.col('reblogs_count').max().alias('max_reblogs'),
                pl.col('favourites_count').max().alias('max_favourites'),
                pl.len().alias('total_toots')
            ])
            print(stats)
            
            # Language distribution
            print("\n#### Language Distribution:")
            lang_dist = (
                df.group_by('language')
                .agg(pl.len().alias('count'))
                .sort('count', descending=True)
                .head(5)
            )
            print(lang_dist)
            
        return df
        
    except KeyboardInterrupt:
        print("\n\nCrawling interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error in main: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    df = main()
