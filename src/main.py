"""
Main ETL Pipeline for Mastodon Data Processing

This script orchestrates the complete data pipeline:
1. Extract: Fetch toots from Mastodon API
2. Transform: Clean, enrich, and analyze data
3. Load: Store data in PostgreSQL database
4. Notify: Send summary to Discord
"""

import logging
from datetime import datetime

from extractor.hashtag_data_extractor import MastodonHashtagCrawler
from transformer import MastodonDataTransformer
from loader import (
    BronzeLayerLoader,
    SilverLayerETL,
    GoldLayerRefresh,
    ModelExecutor
)
from notifier import DiscordNotifier
from extractor.config import config

# Configure logging
from pathlib import Path
import sys
log_dir = Path(__file__).parent.parent / 'logs'
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f'pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def run_etl_pipeline():
    """Execute the complete ETL pipeline with Medallion Architecture."""
    
    pipeline_start = datetime.now()
    logger.info("=" * 80)
    logger.info("STARTING MASTODON DATA PIPELINE (MEDALLION ARCHITECTURE)")
    logger.info("=" * 80)
    logger.info(f"Pipeline started at: {pipeline_start}")
    logger.info("Configuration:")
    logger.info(f"  - Hashtag: #{config.hashtag}")
    logger.info(f"  - Time Period: {config.time_period_value} {config.time_period_unit}")
    logger.info(f"  - Instance: {config.mastodon_base_url}")
    logger.info(f"  - Bronze: {config.database_name}.{config.bronze_database_schema_name}")
    logger.info(f"  - Silver: {config.database_name}.{config.silver_database_schema_name}")
    logger.info(f"  - Gold: {config.database_name}.{config.gold_database_schema_name}")
    logger.info("="*80)
    
    # ═══════════════════════════════════════════════════════════
    # STEP 0: APPLY DATABASE MODELS (First-time setup)
    # ═══════════════════════════════════════════════════════════
    logger.info("\n" + "="*80)
    logger.info("STEP 0/6: APPLYING MEDALLION ARCHITECTURE MODELS")
    logger.info("="*80)
    
    model_executor = ModelExecutor()
    models_applied = model_executor.apply_all_models()
    
    if not models_applied:
        logger.warning("⚠️  Failed to apply some models, but continuing...")
        # Continue anyway - tables might already exist
    
    try:
        # ═══════════════════════════════════════════════════════════
        # STEP 1: EXTRACT
        # ═══════════════════════════════════════════════════════════
        logger.info("\n" + "="*80)
        logger.info("STEP 1/6: EXTRACTING DATA FROM MASTODON")
        logger.info("="*80)
        
        crawler = MastodonHashtagCrawler()
        raw_df = crawler.fetch_hashtag_toots()
        
        if raw_df.is_empty():
            logger.error("❌ No data extracted. Pipeline terminated.")
            return False
        
        logger.info(f"✅ Successfully extracted {len(raw_df)} toots")
        logger.info(f"   Columns: {len(raw_df.columns)}")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 2: TRANSFORM
        # ═══════════════════════════════════════════════════════════
        logger.info("\n" + "="*80)
        logger.info("STEP 2/6: TRANSFORMING DATA")
        logger.info("="*80)
        
        transformer = MastodonDataTransformer(enable_sentiment=True)
        clean_df = transformer.transform(raw_df)
        
        logger.info(f"✅ Successfully transformed {len(clean_df)} records")
        logger.info(f"   Final columns: {len(clean_df.columns)}")
        
        # Display sample
        logger.info("\n📊 Sample of transformed data:")
        sample_cols = ['id', 'content_clean', 'sentiment_value', 'sentiment_score']
        sample_cols = [col for col in sample_cols if col in clean_df.columns]
        logger.info(f"\n{clean_df.select(sample_cols).head(3)}")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 3: LOAD TO BRONZE LAYER
        # ═══════════════════════════════════════════════════════════
        logger.info("\n" + "="*80)
        logger.info("STEP 3/6: LOADING DATA TO BRONZE LAYER")
        logger.info("="*80)
        
        bronze_loader = BronzeLayerLoader()
        bronze_success = bronze_loader.load_to_bronze(clean_df, mode='append')
        
        if not bronze_success:
            logger.error("❌ Failed to load data to Bronze layer")
            
            # Fallback: Save to CSV
            output_file = f"mastodon_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            clean_df.write_csv(output_file)
            logger.warning(f"   Fallback: Data saved to CSV: {output_file}")
            return False
        
        # Get Bronze statistics
        bronze_stats = bronze_loader.get_bronze_stats()
        if bronze_stats:
            logger.info("✅ Bronze Layer Stats:")
            logger.info(f"   - Total rows: {bronze_stats['row_count']}")
            logger.info(f"   - Pipeline runs: {bronze_stats['total_pipeline_runs']}")
            logger.info(f"   - Latest ingestion: {bronze_stats['latest_ingestion']}")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 4: POPULATE SILVER LAYER (Dimensional Model)
        # ═══════════════════════════════════════════════════════════
        logger.info("\n" + "="*80)
        logger.info("STEP 4/6: POPULATING SILVER LAYER (Dimensional Model)")
        logger.info("="*80)
        
        silver_etl = SilverLayerETL()
        silver_success = silver_etl.execute_etl()
        
        if not silver_success:
            logger.error("❌ Failed to populate Silver layer")
            return False
        
        # Get Silver statistics
        silver_stats = silver_etl.get_silver_stats()
        if silver_stats:
            logger.info("✅ Silver Layer Stats:")
            for table, count in silver_stats.items():
                logger.info(f"   - {table}: {count} rows")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 5: REFRESH GOLD LAYER (Analytics Views)
        # ═══════════════════════════════════════════════════════════
        logger.info("\n" + "="*80)
        logger.info("STEP 5/6: REFRESHING GOLD LAYER (Materialized Views)")
        logger.info("="*80)
        
        gold_refresher = GoldLayerRefresh()
        gold_success = gold_refresher.refresh_all_views()
        
        if not gold_success:
            logger.warning("⚠️  Some Gold layer views failed to refresh")
        
        # Get Gold statistics
        gold_stats = gold_refresher.get_gold_stats()
        if gold_stats:
            logger.info("✅ Gold Layer Stats:")
            for view_name, info in gold_stats.items():
                if isinstance(info, dict):
                    logger.info(f"   - {view_name}: {info['row_count']} rows ({info['size']})")
                else:
                    logger.info(f"   - {view_name}: {info} rows")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 6: NOTIFY
        # ═══════════════════════════════════════════════════════════
        logger.info("\n" + "="*80)
        logger.info("STEP 6/6: SENDING NOTIFICATIONS")
        logger.info("="*80)
        
        if config.notify_via_discord:
            notifier = DiscordNotifier()
            if notifier.enabled:
                # Send main pipeline summary
                pipeline_status = "Success" if (bronze_success and silver_success and gold_success) else "Partial"
                notification_sent = notifier.send_pipeline_summary(
                    df=clean_df,
                    pipeline_stage=pipeline_status,
                    load_failed=not bronze_success
                )
                
                if notification_sent:
                    logger.info("✅ Main pipeline notification sent")
                else:
                    logger.warning("⚠️  Failed to send main notification")
                
                # Send most positive toots alert
                positive_sent = notifier.send_most_positive_toots(clean_df)
                if positive_sent:
                    logger.info("✅ Most positive toots notification sent")
                else:
                    logger.warning("⚠️  Failed to send positive toots notification")
                
                # Send most negative toots alert
                negative_sent = notifier.send_most_negative_toots(clean_df)
                if negative_sent:
                    logger.info("✅ Most negative toots notification sent")
                else:
                    logger.warning("⚠️  Failed to send negative toots notification")
            else:
                logger.warning("⚠️  Discord notifications enabled but webhook URL not configured")
        else:
            logger.info("ℹ️  Discord notifications disabled (NOTIFY_VIA_DISCORD=false)")
        
        # ═══════════════════════════════════════════════════════════
        # PIPELINE SUMMARY
        # ═══════════════════════════════════════════════════════════
        pipeline_end = datetime.now()
        duration = (pipeline_end - pipeline_start).total_seconds()
        
        logger.info("\n" + "="*80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        logger.info(f"Start Time: {pipeline_start}")
        logger.info(f"End Time: {pipeline_end}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Records Processed: {len(clean_df)}")
        logger.info("="*80)
        
        return True
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Pipeline interrupted by user")
        return False
        
    except Exception as e:
        logger.error(f"\n❌ Pipeline failed with error: {e}", exc_info=True)
        
        # Send error notification
        try:
            notifier = DiscordNotifier()
            if notifier.enabled:
                notifier.send_error_alert(
                    error_message=str(e),
                    stage="ETL Pipeline"
                )
        except Exception:
            pass  # Don't fail if notification fails
        
        return False


def main():
    """Entry point for the ETL pipeline."""
    try:
        success = run_etl_pipeline()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.critical(f"Critical error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

