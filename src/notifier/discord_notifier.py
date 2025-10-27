"""Discord notification service for pipeline alerts."""

import requests
import polars as pl
import logging
from typing import Dict, Optional
from datetime import datetime, timezone

from extractor.config import config

logger = logging.getLogger(__name__)


class DiscordNotifier:
    """Send notifications to Discord webhook with pipeline statistics."""
    
    def __init__(self, webhook_url: Optional[str] = None):
        """
        Initialize Discord notifier.
        
        Args:
            webhook_url: Discord webhook URL (uses config if not provided)
        """
        self.webhook_url = webhook_url or config.discord_webhook_url
        
        if not self.webhook_url:
            logger.warning("No Discord webhook URL configured")
            self.enabled = False
        else:
            self.enabled = True
            logger.info("Discord notifier initialized")
    
    def send_pipeline_summary(self, df: pl.DataFrame, pipeline_stage: str = "Transformation", load_failed: bool = False) -> bool:
        """
        Send pipeline summary statistics to Discord.
        
        Args:
            df: Polars DataFrame with processed data
            pipeline_stage: Name of the pipeline stage
            load_failed: Whether database loading failed
            
        Returns:
            True if notification sent successfully, False otherwise
        """
        if not self.enabled:
            logger.warning("Discord notifier disabled, skipping notification")
            return False
        
        if df.is_empty():
            logger.warning("Empty DataFrame, skipping notification")
            return False
        
        try:
            # Generate summary statistics
            stats = self._generate_summary_stats(df)
            
            # Check for high negative sentiment alert
            sentiment_alert = self._check_sentiment_alert(df)
            
            # Build Discord embed message
            embed = self._build_embed(stats, sentiment_alert, pipeline_stage, load_failed)
            
            # Send to Discord
            return self._send_webhook(embed)
            
        except Exception as e:
            logger.error(f"Failed to send Discord notification: {e}")
            return False
    
    def _generate_summary_stats(self, df: pl.DataFrame) -> Dict:
        """Generate comprehensive summary statistics from DataFrame."""
        stats = {
            'total_records': len(df),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
            'columns': len(df.columns)
        }
        
        # Hashtag info
        if 'created_at' in df.columns:
            stats['date_range'] = {
                'start': df['created_at'].min(),
                'end': df['created_at'].max()
            }
        
        # Engagement metrics
        if 'reblogs_count' in df.columns and 'favourites_count' in df.columns:
            stats['engagement'] = {
                'total_reblogs': int(df['reblogs_count'].sum() or 0),
                'total_favourites': int(df['favourites_count'].sum() or 0),
                'avg_reblogs': round(float(df['reblogs_count'].mean() or 0), 2),
                'avg_favourites': round(float(df['favourites_count'].mean() or 0), 2)
            }
        
        # Sentiment stats
        if 'sentiment_value' in df.columns:
            sentiment_counts = (
                df.group_by('sentiment_value')
                .agg(pl.len().alias('count'))
                .sort('count', descending=True)
            )
            
            stats['sentiment'] = {}
            for row in sentiment_counts.iter_rows(named=True):
                if row['sentiment_value']:
                    stats['sentiment'][row['sentiment_value']] = row['count']
            
            # Average scores
            if 'sentiment_score' in df.columns:
                avg_score = df['sentiment_score'].mean()
                if avg_score is not None:
                    stats['avg_sentiment_score'] = round(float(avg_score), 4)
        
        # Language distribution
        if 'language' in df.columns:
            lang_counts = (
                df.filter(pl.col('language').is_not_null())
                .group_by('language')
                .agg(pl.len().alias('count'))
                .sort('count', descending=True)
                .head(5)
            )
            stats['top_languages'] = [
                f"{row['language']}: {row['count']}" 
                for row in lang_counts.iter_rows(named=True)
            ]
        
        # Account stats
        if 'account_username' in df.columns:
            unique_accounts = df['account_username'].n_unique()
            stats['unique_accounts'] = unique_accounts
        
        return stats
    
    def _check_sentiment_alert(self, df: pl.DataFrame) -> Optional[Dict]:
        """
        Check if negative sentiment exceeds positive + neutral combined.
        
        Returns:
            Alert dict if condition met, None otherwise
        """
        if 'sentiment_value' not in df.columns:
            return None
        
        sentiment_counts = (
            df.group_by('sentiment_value')
            .agg(pl.len().alias('count'))
        )
        
        # Extract counts
        counts = {row['sentiment_value']: row['count'] 
                  for row in sentiment_counts.iter_rows(named=True)
                  if row['sentiment_value']}
        
        negative_count = counts.get('Negative', 0)
        positive_count = counts.get('Positive', 0)
        neutral_count = counts.get('Neutral', 0)
        
        # Check alert condition
        if negative_count > (positive_count + neutral_count):
            return {
                'triggered': True,
                'negative': negative_count,
                'positive': positive_count,
                'neutral': neutral_count,
                'percentage': round((negative_count / len(df)) * 100, 2)
            }
        
        return None
    
    def _build_embed(self, stats: Dict, sentiment_alert: Optional[Dict], stage: str, load_failed: bool = False) -> Dict:
        """Build Discord embed message with statistics."""
        
        # Determine color and title based on alerts
        if sentiment_alert and load_failed:
            color = 0xFF0000  # Red for multiple alerts
            title = "🚨 Pipeline Status Report - SENTIMENT ALERT + DB LOAD FAILED"
        elif sentiment_alert:
            color = 0xFF0000  # Red for sentiment alert
            title = "⚠️ Pipeline Status Report - HIGH NEGATIVE SENTIMENT ALERT"
        elif load_failed:
            color = 0xFFA500  # Orange for warning
            title = "⚠️ Pipeline Status Report - DB LOAD FAILED"
        else:
            color = 0x00FF00  # Green for success
            title = "✅ Pipeline Status Report"
        
        embed = {
            "title": title,
            "color": color,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "fields": []
        }
        
        # Add basic stats with load status
        summary_value = (
            f"**Total Records:** {stats['total_records']:,}\n"
            f"**Columns:** {stats['columns']}\n"
            f"**Timestamp:** {stats['timestamp']}"
        )
        if load_failed:
            summary_value += "\n⚠️ **DB Load:** Failed (data saved to CSV)"
        else:
            summary_value += "\n✅ **DB Load:** Success"
        
        embed["fields"].append({
            "name": "📊 Processing Summary",
            "value": summary_value,
            "inline": False
        })
        
        # Add date range if available
        if 'date_range' in stats:
            dr = stats['date_range']
            embed["fields"].append({
                "name": "📅 Date Range",
                "value": f"**From:** {dr['start']}\n**To:** {dr['end']}",
                "inline": True
            })
        
        # Add engagement stats
        if 'engagement' in stats:
            eng = stats['engagement']
            embed["fields"].append({
                "name": "💬 Engagement Metrics",
                "value": (
                    f"**Total Reblogs:** {eng['total_reblogs']:,}\n"
                    f"**Total Favourites:** {eng['total_favourites']:,}\n"
                    f"**Avg Reblogs:** {eng['avg_reblogs']}\n"
                    f"**Avg Favourites:** {eng['avg_favourites']}"
                ),
                "inline": True
            })
        
        # Add sentiment distribution
        if 'sentiment' in stats:
            sent = stats['sentiment']
            sentiment_text = "\n".join([
                f"**{key}:** {value:,}" for key, value in sent.items()
            ])
            
            if 'avg_sentiment_score' in stats:
                sentiment_text += f"\n**Avg Score:** {stats['avg_sentiment_score']}"
            
            embed["fields"].append({
                "name": "😊 Sentiment Distribution",
                "value": sentiment_text,
                "inline": False
            })
        
        # Add sentiment alert if triggered
        if sentiment_alert:
            embed["fields"].append({
                "name": "🚨 SENTIMENT ALERT",
                "value": (
                    f"⚠️ **Negative sentiment exceeds Positive + Neutral!**\n\n"
                    f"🔴 Negative: {sentiment_alert['negative']:,} "
                    f"({sentiment_alert['percentage']}%)\n"
                    f"🟢 Positive: {sentiment_alert['positive']:,}\n"
                    f"⚪ Neutral: {sentiment_alert['neutral']:,}\n\n"
                    f"**Action Required:** Review negative toots for issues."
                ),
                "inline": False
            })
        
        # Add language stats
        if 'top_languages' in stats and stats['top_languages']:
            embed["fields"].append({
                "name": "🌍 Top Languages",
                "value": "\n".join(stats['top_languages']),
                "inline": True
            })
        
        # Add account stats
        if 'unique_accounts' in stats:
            embed["fields"].append({
                "name": "👥 Unique Accounts",
                "value": f"**{stats['unique_accounts']:,}** different users",
                "inline": True
            })
        
        # Add hashtag info
        embed["fields"].append({
            "name": "🏷️ Hashtag",
            "value": f"#{config.hashtag}",
            "inline": True
        })
        
        # Add footer
        embed["footer"] = {
            "text": f"Mastodon Data Pipeline | {config.mastodon_base_url}"
        }
        
        return {"embeds": [embed]}
    
    def _send_webhook(self, payload: Dict) -> bool:
        """
        Send payload to Discord webhook.
        
        Args:
            payload: Discord webhook payload
            
        Returns:
            True if successful, False otherwise
        """
        try:
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 204:
                logger.info("Discord notification sent successfully")
                return True
            else:
                logger.error(f"Discord webhook failed: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.Timeout:
            logger.error("Discord webhook request timed out")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Discord webhook request failed: {e}")
            return False
    
    def send_error_alert(self, error_message: str, stage: str = "Pipeline") -> bool:
        """
        Send error alert to Discord.
        
        Args:
            error_message: Error message to send
            stage: Pipeline stage where error occurred
            
        Returns:
            True if sent successfully, False otherwise
        """
        if not self.enabled:
            return False
        
        embed = {
            "embeds": [{
                "title": f"❌ {stage} Error",
                "description": f"```\n{error_message}\n```",
                "color": 0xFF0000,  # Red
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "footer": {
                    "text": "Mastodon Data Pipeline"
                }
            }]
        }
        
        return self._send_webhook(embed)
    
    def send_most_positive_toots(self, df: pl.DataFrame, top_n: int = 5) -> bool:
        """
        Send most positive toots to Discord.
        
        Args:
            df: Polars DataFrame with sentiment data
            top_n: Number of top positive toots to display
            
        Returns:
            True if notification sent successfully, False otherwise
        """
        if not self.enabled:
            logger.warning("Discord notifier disabled, skipping notification")
            return False
        
        if df.is_empty():
            logger.warning("Empty DataFrame, skipping notification")
            return False
        
        # Check if sentiment columns exist
        if 'sentiment_value' not in df.columns or 'sentiment_score' not in df.columns:
            logger.warning("Sentiment columns not found, skipping positive toots notification")
            return False
        
        try:
            # Filter for positive sentiment and sort by sentiment score (case-insensitive)
            positive_toots = (
                df.filter(pl.col('sentiment_value').str.to_lowercase() == 'positive')
                .sort('sentiment_score', descending=True)
                .head(top_n)
            )
            
            if positive_toots.is_empty():
                logger.info("No positive toots found, skipping notification")
                return False
            
            # Build embed message
            embed = {
                "title": "🌟 Most Positive Toots",
                "description": f"Top {len(positive_toots)} most positive toots from #{config.hashtag}",
                "color": 0x00FF00,  # Green
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "fields": []
            }
            
            # Add each positive toot as a field
            for idx, row in enumerate(positive_toots.iter_rows(named=True), 1):
                content = row.get('content_clean', row.get('content', ''))
                if content:
                    # Truncate content to 200 characters
                    content_preview = content[:200] + '...' if len(content) > 200 else content
                else:
                    content_preview = "_No content available_"
                
                username = row.get('account_username', 'Unknown')
                sentiment_score = row.get('sentiment_score', 0)
                
                # Get engagement metrics
                engagement_metrics = []
                if 'favourites_count' in row and row['favourites_count']:
                    engagement_metrics.append(f"❤️ {row['favourites_count']}")
                if 'reblogs_count' in row and row['reblogs_count']:
                    engagement_metrics.append(f"🔄 {row['reblogs_count']}")
                if 'replies_count' in row and row['replies_count']:
                    engagement_metrics.append(f"💬 {row['replies_count']}")
                
                engagement_str = " | ".join(engagement_metrics) if engagement_metrics else "No engagement yet"
                
                embed["fields"].append({
                    "name": f"#{idx} - @{username} (Score: {sentiment_score:.3f})",
                    "value": f"{content_preview}\n\n{engagement_str}",
                    "inline": False
                })
            
            embed["footer"] = {
                "text": f"Mastodon Data Pipeline | {config.mastodon_base_url}"
            }
            
            return self._send_webhook({"embeds": [embed]})
            
        except Exception as e:
            logger.error(f"Failed to send positive toots notification: {e}")
            return False
    
    def send_most_negative_toots(self, df: pl.DataFrame, top_n: int = 5) -> bool:
        """
        Send most negative toots to Discord.
        
        Args:
            df: Polars DataFrame with sentiment data
            top_n: Number of top negative toots to display
            
        Returns:
            True if notification sent successfully, False otherwise
        """
        if not self.enabled:
            logger.warning("Discord notifier disabled, skipping notification")
            return False
        
        if df.is_empty():
            logger.warning("Empty DataFrame, skipping notification")
            return False
        
        # Check if sentiment columns exist
        if 'sentiment_value' not in df.columns or 'sentiment_score' not in df.columns:
            logger.warning("Sentiment columns not found, skipping negative toots notification")
            return False
        
        try:
            # Filter for negative sentiment and sort by sentiment score (highest score = most negative, case-insensitive)
            negative_toots = (
                df.filter(pl.col('sentiment_value').str.to_lowercase() == 'negative')
                .sort('sentiment_score', descending=True)
                .head(top_n)
            )
            
            if negative_toots.is_empty():
                logger.info("No negative toots found, skipping notification")
                return False
            
            # Build embed message
            embed = {
                "title": "⚠️ Most Negative Toots",
                "description": f"Top {len(negative_toots)} most negative toots from #{config.hashtag}",
                "color": 0xFF0000,  # Red
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "fields": []
            }
            
            # Add each negative toot as a field
            for idx, row in enumerate(negative_toots.iter_rows(named=True), 1):
                content = row.get('content_clean', row.get('content', ''))
                if content:
                    # Truncate content to 200 characters
                    content_preview = content[:200] + '...' if len(content) > 200 else content
                else:
                    content_preview = "_No content available_"
                
                username = row.get('account_username', 'Unknown')
                sentiment_score = row.get('sentiment_score', 0)
                
                # Get engagement metrics
                engagement_metrics = []
                if 'favourites_count' in row and row['favourites_count']:
                    engagement_metrics.append(f"❤️ {row['favourites_count']}")
                if 'reblogs_count' in row and row['reblogs_count']:
                    engagement_metrics.append(f"🔄 {row['reblogs_count']}")
                if 'replies_count' in row and row['replies_count']:
                    engagement_metrics.append(f"💬 {row['replies_count']}")
                
                engagement_str = " | ".join(engagement_metrics) if engagement_metrics else "No engagement yet"
                
                embed["fields"].append({
                    "name": f"#{idx} - @{username} (Score: {sentiment_score:.3f})",
                    "value": f"{content_preview}\n\n{engagement_str}",
                    "inline": False
                })
            
            embed["footer"] = {
                "text": f"Mastodon Data Pipeline | {config.mastodon_base_url}"
            }
            
            return self._send_webhook({"embeds": [embed]})
            
        except Exception as e:
            logger.error(f"Failed to send negative toots notification: {e}")
            return False

