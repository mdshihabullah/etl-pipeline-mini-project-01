"""Main transformer orchestrator for Mastodon data pipeline."""

import polars as pl
import logging

from .text_cleaner import TextCleaner
from .json_extractor import JSONExtractor
from .sentiment_analyzer import SentimentAnalyzer
from .data_quality import DataQualityChecker

logger = logging.getLogger(__name__)


class MastodonDataTransformer:
    """
    Transform and clean Mastodon toot data for database loading.
    
    This class orchestrates all transformation steps:
    1. Text cleaning (HTML removal)
    2. Null normalization
    3. JSON field extraction
    4. Data quality checks
    5. Sentiment analysis
    """
    
    def __init__(self, enable_sentiment: bool = True):
        """
        Initialize the transformer.
        
        Args:
            enable_sentiment: Whether to enable sentiment analysis
        """
        self.text_cleaner = TextCleaner()
        self.json_extractor = JSONExtractor()
        self.sentiment_analyzer = SentimentAnalyzer(enable_sentiment=enable_sentiment)
        self.quality_checker = DataQualityChecker()
        
        logger.info("MastodonDataTransformer initialized")
    
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Apply all transformations to the DataFrame.
        
        Args:
            df: Raw DataFrame from extractor
            
        Returns:
            Transformed DataFrame ready for database loading
        """
        if df.is_empty():
            logger.warning("Empty DataFrame provided, returning as-is")
            return df
        
        logger.info(f"Starting transformation of {len(df)} records")
        
        # 1. Clean HTML from content and spoiler_text
        logger.info("Cleaning HTML content...")
        df = self.text_cleaner.clean_text_columns(df)
        
        # 2. Normalize empty strings to nulls
        logger.info("Normalizing empty values to nulls...")
        df = self.text_cleaner.normalize_nulls(df)
        
        # 3. Extract useful fields from JSON columns
        logger.info("Extracting fields from JSON columns...")
        df = self.json_extractor.extract_json_fields(df)
        
        # 4. Data quality checks and fixes
        logger.info("Performing data quality checks...")
        df = self.quality_checker.perform_quality_checks(df)
        
        # 5. Add sentiment analysis
        logger.info("Performing sentiment analysis...")
        df = self.sentiment_analyzer.add_sentiment_analysis(df)
        
        logger.info(f"Transformation complete. Final shape: {df.shape}")
        
        return df
    
    def get_summary_stats(self, df: pl.DataFrame) -> dict:
        """
        Get summary statistics of the transformed data.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Dictionary with summary statistics
        """
        return self.quality_checker.get_summary_stats(df)

