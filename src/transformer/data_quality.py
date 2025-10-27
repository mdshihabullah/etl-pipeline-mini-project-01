"""Data quality checks and validation for Mastodon data."""

import polars as pl
import logging

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """Perform data quality checks and fixes on Mastodon data."""
    
    @staticmethod
    def perform_quality_checks(df: pl.DataFrame) -> pl.DataFrame:
        """
        Perform comprehensive data quality checks and fixes.
        
        Args:
            df: DataFrame to check
            
        Returns:
            DataFrame with quality issues fixed
        """
        # Ensure numeric fields are not negative
        numeric_cols = ['replies_count', 'reblogs_count', 'favourites_count', 'quotes_count']
        for col in numeric_cols:
            if col in df.columns:
                df = df.with_columns(
                    pl.when(pl.col(col) < 0)
                    .then(None)
                    .otherwise(pl.col(col))
                    .alias(col)
                )
        
        # Validate language codes
        if 'language' in df.columns:
            df = df.with_columns(
                pl.when(
                    (pl.col('language').is_not_null()) & 
                    (pl.col('language').str.len_chars() > 10)
                )
                .then(None)
                .otherwise(pl.col('language'))
                .alias('language')
            )
        
        # Ensure visibility is in expected values
        if 'visibility' in df.columns:
            valid_visibility = ['public', 'unlisted', 'private', 'direct']
            df = df.with_columns(
                pl.when(~pl.col('visibility').is_in(valid_visibility))
                .then(None)
                .otherwise(pl.col('visibility'))
                .alias('visibility')
            )
        
        # Remove duplicate records by id
        if 'id' in df.columns:
            original_count = len(df)
            df = df.unique(subset=['id'], keep='first')
            duplicates_removed = original_count - len(df)
            if duplicates_removed > 0:
                logger.warning(f"Removed {duplicates_removed} duplicate records")
        
        return df
    
    @staticmethod
    def get_summary_stats(df: pl.DataFrame) -> dict:
        """
        Get summary statistics of the data.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with summary statistics
        """
        if df.is_empty():
            return {}
        
        stats = {
            'total_records': len(df),
            'null_counts': {},
            'unique_counts': {}
        }
        
        # Count nulls for each column
        for col in df.columns:
            null_count = df[col].null_count()
            if null_count > 0:
                stats['null_counts'][col] = null_count
        
        # Count unique values for key columns
        key_cols = ['account_id', 'language', 'visibility']
        for col in key_cols:
            if col in df.columns:
                stats['unique_counts'][col] = df[col].n_unique()
        
        return stats

