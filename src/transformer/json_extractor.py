"""JSON field extraction utilities for Mastodon data."""

import polars as pl
import json
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class JSONExtractor:
    """Extract and parse JSON fields from Mastodon toots."""
    
    @staticmethod
    def extract_tag_names(json_str: Optional[str]) -> Optional[str]:
        """Extract tag names from tags JSON."""
        if not json_str:
            return None
        try:
            tags = json.loads(json_str)
            if not tags or not isinstance(tags, list):
                return None
            names = [tag.get('name', '') for tag in tags if isinstance(tag, dict)]
            return ','.join(names) if names else None
        except (json.JSONDecodeError, AttributeError):
            return None
    
    @staticmethod
    def extract_mention_usernames(json_str: Optional[str]) -> Optional[str]:
        """Extract usernames from mentions JSON."""
        if not json_str:
            return None
        try:
            mentions = json.loads(json_str)
            if not mentions or not isinstance(mentions, list):
                return None
            usernames = [m.get('username', '') for m in mentions if isinstance(m, dict)]
            return ','.join(usernames) if usernames else None
        except (json.JSONDecodeError, AttributeError):
            return None
    
    @staticmethod
    def count_media(json_str: Optional[str]) -> Optional[int]:
        """Count number of media attachments."""
        if not json_str:
            return None
        try:
            media = json.loads(json_str)
            if not media or not isinstance(media, list):
                return None
            return len(media)
        except (json.JSONDecodeError, AttributeError):
            return None
    
    @staticmethod
    def extract_media_types(json_str: Optional[str]) -> Optional[str]:
        """Extract media types from media attachments."""
        if not json_str:
            return None
        try:
            media = json.loads(json_str)
            if not media or not isinstance(media, list):
                return None
            types = [m.get('type', '') for m in media if isinstance(m, dict)]
            return ','.join(types) if types else None
        except (json.JSONDecodeError, AttributeError):
            return None
    
    @staticmethod
    def extract_account_field(json_str: Optional[str], field: str):
        """Extract a specific field from account JSON."""
        if not json_str:
            return None
        try:
            account = json.loads(json_str)
            if not account or not isinstance(account, dict):
                return None
            return account.get(field)
        except (json.JSONDecodeError, AttributeError):
            return None
    
    @staticmethod
    def extract_json_fields(df: pl.DataFrame) -> pl.DataFrame:
        """
        Extract useful fields from JSON columns.
        
        Args:
            df: DataFrame with JSON columns
            
        Returns:
            DataFrame with extracted fields added
        """
        extractor = JSONExtractor()
        
        # Extract from tags
        if 'tags' in df.columns:
            df = df.with_columns(
                pl.col('tags').map_elements(
                    extractor.extract_tag_names,
                    return_dtype=pl.Utf8
                ).alias('tag_names')
            )
        
        # Extract from mentions
        if 'mentions' in df.columns:
            df = df.with_columns(
                pl.col('mentions').map_elements(
                    extractor.extract_mention_usernames,
                    return_dtype=pl.Utf8
                ).alias('mention_usernames')
            )
        
        # Extract media info
        if 'media_attachments' in df.columns:
            df = df.with_columns([
                pl.col('media_attachments').map_elements(
                    extractor.count_media,
                    return_dtype=pl.Int64
                ).alias('media_count'),
                pl.col('media_attachments').map_elements(
                    extractor.extract_media_types,
                    return_dtype=pl.Utf8
                ).alias('media_types')
            ])
        
        # Extract account fields
        if 'account' in df.columns:
            df = df.with_columns([
                pl.col('account').map_elements(
                    lambda x: extractor.extract_account_field(x, 'followers_count'),
                    return_dtype=pl.Int64
                ).alias('account_followers_count'),
                pl.col('account').map_elements(
                    lambda x: extractor.extract_account_field(x, 'following_count'),
                    return_dtype=pl.Int64
                ).alias('account_following_count'),
                pl.col('account').map_elements(
                    lambda x: extractor.extract_account_field(x, 'statuses_count'),
                    return_dtype=pl.Int64
                ).alias('account_statuses_count'),
                pl.col('account').map_elements(
                    lambda x: extractor.extract_account_field(x, 'bot'),
                    return_dtype=pl.Boolean
                ).alias('account_is_bot'),
                pl.col('account').map_elements(
                    lambda x: extractor.extract_account_field(x, 'created_at'),
                    return_dtype=pl.Utf8
                ).alias('account_created_at')
            ])
        
        # Add boolean flags
        if 'reblog' in df.columns:
            df = df.with_columns(
                pl.col('reblog').is_not_null().alias('is_reblog')
            )
        
        if 'poll' in df.columns:
            df = df.with_columns(
                pl.col('poll').is_not_null().alias('has_poll')
            )
        
        if 'card' in df.columns:
            df = df.with_columns(
                pl.col('card').is_not_null().alias('has_card')
            )
        
        return df

