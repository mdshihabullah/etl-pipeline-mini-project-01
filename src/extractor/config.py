from pydantic_settings import BaseSettings
from pydantic import Field, field_validator
from typing import Literal

class MastodonConfig(BaseSettings):
    """Configuration for Mastodon data pipeline application."""
    
    # Mastodon API Configuration
    mastodon_base_url: str = Field(
        default="https://mastodon.social",
        description="Base URL of Mastodon instance"
    )
    api_timeout: int = Field(
        default=30,
        ge=5,
        le=120,
        description="API request timeout in seconds"
    )
    hashtag: str = Field(
        default="ai",
        description="Hashtag to crawl (without #)"
    )
    time_period_unit: Literal["hours", "days"] = Field(
        default="hours",
        description="Unit for time period"
    )
    time_period_value: int = Field(
        default=1,
        ge=1,
        le=30,
        description="Number of time units to look back"
    )
    toots_limit_per_page: int = Field(
        default=40,
        ge=1,
        le=40,
        description="Number of toots per API request (max 40)"
    )
    
    # Sentiment Analysis Configuration
    sentiment_model_name: str = Field(
        default="cardiffnlp/twitter-roberta-base-sentiment-latest",
        description="HuggingFace model name for sentiment analysis"
    )
    sentiment_model_threshold: float = Field(
        default=0.75,
        ge=0.0,
        le=1.0,
        description="Confidence threshold for sentiment classification"
    )
    
    # Discord Notification Configuration
    notify_via_discord: bool = Field(
        default=False,
        description="Enable/disable Discord notifications"
    )
    discord_webhook_url: str = Field(
        default="",
        description="Discord webhook URL for notifications"
    )
    
    # Database Configuration
    database_name: str = Field(
        default="mastodon_toots",
        description="PostgreSQL database name"
    )
    database_user: str = Field(
        default="postgres",
        description="PostgreSQL username"
    )
    database_password: str = Field(
        default="postgres",
        description="PostgreSQL password"
    )
    database_host: str = Field(
        default="localhost",
        description="PostgreSQL host"
    )
    database_port: int = Field(
        default=5432,
        ge=1,
        le=65535,
        description="PostgreSQL port"
    )
    database_schema_name: str = Field(
        default="hashtag_based",
        description="PostgreSQL schema name (legacy)"
    )
    database_table_name: str = Field(
        default="toots_with_sentiment_data",
        description="PostgreSQL table name (legacy)"
    )
    
    # Medallion Architecture - Layer Configuration
    bronze_database_schema_name: str = Field(
        default="bronze",
        description="Bronze layer schema name (raw transformed data)"
    )
    bronze_database_table_name: str = Field(
        default="transformed_toots_with_sentiment_data",
        description="Bronze layer table name"
    )
    silver_database_schema_name: str = Field(
        default="dim_facts",
        description="Silver layer schema name (dimensional model)"
    )
    gold_database_schema_name: str = Field(
        default="analytics",
        description="Gold layer schema name (analytics views)"
    )
    
    @field_validator('mastodon_base_url')
    @classmethod
    def validate_url(cls, v: str) -> str:
        """
        Validate Mastodon base URL format.
        
        Args:
            v: URL string to validate
            
        Returns:
            Validated URL without trailing slash
            
        Raises:
            ValueError: If URL is empty or missing http(s) scheme
        """
        if not v:
            raise ValueError('Base URL cannot be empty')
        if not v.startswith(('http://', 'https://')):
            raise ValueError('URL must start with http:// or https://')
        return v.rstrip('/')
    
    @field_validator('hashtag')
    @classmethod
    def clean_hashtag(cls, v: str) -> str:
        """
        Clean and validate hashtag input.
        
        Args:
            v: Hashtag string to clean
            
        Returns:
            Cleaned hashtag without # prefix and whitespace
            
        Raises:
            ValueError: If hashtag is empty
        """
        if not v:
            raise ValueError('Hashtag cannot be empty')
        # Remove # if present at the beginning
        return v.lstrip('#').strip()
    
    @field_validator('discord_webhook_url')
    @classmethod
    def validate_webhook_url(cls, v: str) -> str:
        """
        Validate Discord webhook URL format.
        
        Args:
            v: Webhook URL to validate
            
        Returns:
            Validated webhook URL
            
        Raises:
            ValueError: If URL format is invalid
        """
        if v and not v.startswith('https://discord.com/api/webhooks/'):
            raise ValueError('Invalid Discord webhook URL format')
        return v
    
    class Config:
        """Pydantic configuration for settings management."""
        # Try settings.env first, then fall back to .env
        env_file = 'settings.env'
        env_file_encoding = 'utf-8'
        # Use exact env variable names (no prefix transformation)
        env_prefix = ''
        # Allow reading from multiple possible locations
        extra = 'ignore'

# Create singleton instance
config = MastodonConfig()
