"""Sentiment analysis for Mastodon content."""

import polars as pl
from typing import List, Dict, Optional
import logging

from extractor.config import config

logger = logging.getLogger(__name__)


class SentimentAnalyzer:
    """Analyze sentiment of Mastodon toots using transformer models."""
    
    def __init__(self, enable_sentiment: bool = True):
        """
        Initialize sentiment analyzer.
        
        Args:
            enable_sentiment: Whether to enable sentiment analysis
        """
        self.enable_sentiment = enable_sentiment
        self.sentiment_pipeline = None
        self.sentiment_model_name = config.sentiment_model_name
        self.sentiment_threshold = config.sentiment_model_threshold
        
        if self.enable_sentiment:
            self._load_sentiment_model()
    
    def _load_sentiment_model(self):
        """Load the sentiment analysis model."""
        try:
            from transformers import pipeline
            import torch
            
            logger.info(f"Loading sentiment model: {self.sentiment_model_name}")
            
            # Determine device
            if torch.cuda.is_available():
                device = 0
                logger.info("Using CUDA for sentiment analysis")
            elif torch.backends.mps.is_available():
                device = "mps"
                logger.info("Using MPS (Apple Silicon) for sentiment analysis")
            else:
                device = -1
                logger.info("Using CPU for sentiment analysis")
            
            self.sentiment_pipeline = pipeline(
                "sentiment-analysis",
                model=self.sentiment_model_name,
                device=device,
                truncation=True,
                max_length=512
            )
            
            logger.info("Sentiment model loaded successfully")
            
        except ImportError as e:
            logger.error(f"Failed to import transformers/torch: {e}")
            logger.warning("Sentiment analysis will be disabled")
            self.enable_sentiment = False
            self.sentiment_pipeline = None
        except Exception as e:
            logger.error(f"Failed to load sentiment model: {e}")
            logger.warning("Sentiment analysis will be disabled")
            self.enable_sentiment = False
            self.sentiment_pipeline = None
    
    def add_sentiment_analysis(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Add sentiment analysis columns to DataFrame.
        
        Args:
            df: DataFrame with content to analyze
            
        Returns:
            DataFrame with sentiment columns added
        """
        if not self.enable_sentiment or self.sentiment_pipeline is None:
            logger.warning("Sentiment analysis disabled, skipping")
            return df
        
        # Use cleaned content if available, otherwise original content
        text_col = 'content_clean' if 'content_clean' in df.columns else 'content'
        
        if text_col not in df.columns:
            logger.warning(f"No text column ({text_col}) found")
            return df
        
        # Extract texts
        texts = df[text_col].to_list()
        total_texts = len(texts)
        
        logger.info(f"Processing {total_texts} texts for sentiment analysis...")
        
        # Process in batches
        batch_size = 32
        sentiment_results = []
        
        for i in range(0, total_texts, batch_size):
            batch = texts[i:i + batch_size]
            batch_results = self._process_sentiment_batch(batch)
            sentiment_results.extend(batch_results)
            
            if (i + batch_size) % 100 == 0:
                logger.info(f"Processed {min(i + batch_size, total_texts)}/{total_texts} texts")
        
        logger.info(f"Sentiment analysis complete for {total_texts} texts")
        
        # Extract scores and labels
        scores = [r['score'] for r in sentiment_results]
        labels = [r['label'] for r in sentiment_results]
        
        # Add columns
        df = df.with_columns([
            pl.Series('sentiment_score', scores).cast(pl.Float64),
            pl.Series('sentiment_value', labels).cast(pl.Utf8),
            pl.lit(self.sentiment_model_name).alias('sentiment_model_name')
        ])
        
        return df
    
    def _process_sentiment_batch(self, texts: List[Optional[str]]) -> List[Dict]:
        """Process a batch of texts for sentiment analysis."""
        # Initialize results list with None for all texts
        results = [{'score': None, 'label': None} for _ in texts]
        processed_texts = []
        valid_indices = []
        
        for idx, text in enumerate(texts):
            if text and isinstance(text, str) and text.strip():
                processed_texts.append(text[:512])
                valid_indices.append(idx)
        
        if processed_texts:
            try:
                predictions = self.sentiment_pipeline(processed_texts)
                
                # Map predictions back to original indices
                for pred_idx, text_idx in enumerate(valid_indices):
                    pred = predictions[pred_idx]
                    label = self._map_sentiment_label(pred['label'])
                    score = pred['score']
                    
                    # Apply threshold
                    if score < self.sentiment_threshold:
                        label = 'Neutral'
                    
                    results[text_idx] = {'score': round(score, 4), 'label': label}
                    
            except Exception as e:
                logger.error(f"Error in sentiment prediction: {e}")
        
        return results
    
    def _map_sentiment_label(self, label: str) -> str:
        """Map model-specific labels to standard sentiment labels."""
        label_lower = label.lower()
        
        if 'pos' in label_lower:
            return 'Positive'
        elif 'neg' in label_lower:
            return 'Negative'
        elif 'neu' in label_lower:
            return 'Neutral'
        else:
            return label
