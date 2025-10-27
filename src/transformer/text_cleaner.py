"""Text cleaning utilities for Mastodon content."""

import polars as pl
import re
from typing import Optional
from html import unescape
import logging

logger = logging.getLogger(__name__)


class TextCleaner:
    """Clean and normalize text content from Mastodon toots."""
    
    @staticmethod
    def clean_html(text: Optional[str]) -> Optional[str]:
        """
        Remove HTML tags and clean text content while preserving emojis.
        
        Args:
            text: HTML text to clean
            
        Returns:
            Cleaned plain text or None
        """
        if not text or text == '':
            return None
        
        try:
            # Remove HTML tags but preserve content
            text = re.sub(r'<br\s*/?>', ' ', text)  # Replace <br> with space
            text = re.sub(r'<[^>]+>', '', text)  # Remove all other tags
            # Unescape HTML entities (preserves emojis)
            text = unescape(text)
            # Remove excessive whitespace but keep single spaces
            text = re.sub(r'\s+', ' ', text)
            # Strip leading/trailing whitespace
            text = text.strip()
            
            return text if text else None
        except Exception as e:
            logger.warning(f"Error cleaning HTML: {e}")
            return None
    
    @staticmethod
    def clean_text_columns(df: pl.DataFrame) -> pl.DataFrame:
        """
        Clean HTML from text columns in DataFrame.
        
        Args:
            df: DataFrame with content columns
            
        Returns:
            DataFrame with cleaned text columns added
        """
        cleaner = TextCleaner()
        
        # Clean content column
        if 'content' in df.columns:
            df = df.with_columns(
                pl.col('content').map_elements(
                    cleaner.clean_html,
                    return_dtype=pl.Utf8
                ).alias('content_clean')
            )
        
        # Clean spoiler_text column
        if 'spoiler_text' in df.columns:
            df = df.with_columns(
                pl.col('spoiler_text').map_elements(
                    cleaner.clean_html,
                    return_dtype=pl.Utf8
                ).alias('spoiler_text_clean')
            )
        
        return df
    
    @staticmethod
    def normalize_nulls(df: pl.DataFrame) -> pl.DataFrame:
        """
        Convert empty strings to null across string columns.
        
        Args:
            df: DataFrame to normalize
            
        Returns:
            DataFrame with empty strings converted to NULL
        """
        string_columns = [col for col, dtype in zip(df.columns, df.dtypes) if dtype == pl.Utf8]
        
        for col in string_columns:
            df = df.with_columns(
                pl.when(pl.col(col).str.strip_chars() == '')
                .then(None)
                .otherwise(pl.col(col))
                .alias(col)
            )
        
        return df

