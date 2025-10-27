"""Gold Layer Refresh - Refresh materialized views for analytics."""

import psycopg
import logging
from typing import Optional, List
from datetime import datetime

from extractor.config import config

logger = logging.getLogger(__name__)


class GoldLayerRefresh:
    """Refresh Gold layer materialized views."""
    
    def __init__(self):
        """Initialize the Gold refresh with database configuration."""
        self.db_config = {
            'dbname': config.database_name,
            'user': config.database_user,
            'password': config.database_password,
            'host': config.database_host,
            'port': config.database_port
        }
        self.gold_schema = config.gold_database_schema_name
        
        # List of materialized views to refresh
        self.materialized_views = [
            'mv_daily_engagement_summary',
            'mv_top_performing_content',
            'mv_account_influence_analysis',
            'mv_hashtag_performance',
            'mv_hourly_posting_patterns',
            'mv_sentiment_trends',
            'mv_viral_content_indicators'
        ]
        
        logger.info(f"GoldLayerRefresh initialized for schema: {self.gold_schema}")
    
    def refresh_all_views(self, concurrent: bool = False) -> bool:
        """
        Refresh all materialized views.
        
        Args:
            concurrent: If True, use CONCURRENTLY (slower but non-blocking)
            
        Returns:
            True if all views refreshed successfully
        """
        logger.info("\n" + "="*60)
        logger.info("GOLD LAYER REFRESH: Updating Materialized Views")
        logger.info("="*60)
        
        try:
            with psycopg.connect(**self.db_config) as conn:
                success_count = 0
                
                for mv_name in self.materialized_views:
                    if self._refresh_view(conn, mv_name, concurrent):
                        success_count += 1
                    else:
                        logger.warning(f"Failed to refresh {mv_name}, continuing...")
                
                conn.commit()
                
                logger.info(f"\n✅ Refreshed {success_count}/{len(self.materialized_views)} materialized views")
                return success_count == len(self.materialized_views)
                
        except psycopg.OperationalError as e:
            logger.error(f"Database connection failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Gold layer refresh failed: {e}", exc_info=True)
            return False
    
    def _refresh_view(self, conn: psycopg.Connection, view_name: str, concurrent: bool = False) -> bool:
        """
        Refresh a single materialized view.
        
        Args:
            conn: Database connection
            view_name: Name of the materialized view
            concurrent: If True, use CONCURRENTLY
            
        Returns:
            True if successful
        """
        start_time = datetime.now()
        concurrent_keyword = "CONCURRENTLY" if concurrent else ""
        
        try:
            with conn.cursor() as cur:
                sql = f"REFRESH MATERIALIZED VIEW {concurrent_keyword} {self.gold_schema}.{view_name}"
                logger.info(f"  Refreshing {view_name}...")
                
                cur.execute(sql)
                
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"  ✅ {view_name} refreshed in {duration:.2f}s")
                return True
                
        except Exception as e:
            logger.error(f"  ❌ Failed to refresh {view_name}: {e}")
            return False
    
    def refresh_specific_views(self, view_names: List[str], concurrent: bool = False) -> bool:
        """
        Refresh specific materialized views.
        
        Args:
            view_names: List of view names to refresh
            concurrent: If True, use CONCURRENTLY
            
        Returns:
            True if all specified views refreshed successfully
        """
        logger.info(f"Refreshing {len(view_names)} specific views...")
        
        try:
            with psycopg.connect(**self.db_config) as conn:
                success_count = 0
                
                for view_name in view_names:
                    if view_name in self.materialized_views:
                        if self._refresh_view(conn, view_name, concurrent):
                            success_count += 1
                    else:
                        logger.warning(f"View {view_name} not found in known views list")
                
                conn.commit()
                
                return success_count == len(view_names)
                
        except Exception as e:
            logger.error(f"Failed to refresh specific views: {e}")
            return False
    
    def get_gold_stats(self) -> Optional[dict]:
        """Get statistics about Gold layer materialized views."""
        try:
            with psycopg.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    stats = {}
                    
                    # Get row counts for each materialized view
                    for mv_name in self.materialized_views:
                        try:
                            cur.execute(f"SELECT COUNT(*) FROM {self.gold_schema}.{mv_name}")
                            stats[mv_name] = cur.fetchone()[0]
                        except Exception as e:
                            logger.warning(f"Could not get stats for {mv_name}: {e}")
                            stats[mv_name] = 0
                    
                    # Get last refresh times
                    cur.execute(f"""
                        SELECT 
                            matviewname,
                            pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) AS size
                        FROM pg_matviews
                        WHERE schemaname = '{self.gold_schema}'
                    """)
                    
                    view_info = {row[0]: {'size': row[1]} for row in cur.fetchall()}
                    
                    for mv_name, info in view_info.items():
                        if mv_name in stats:
                            stats[mv_name] = {
                                'row_count': stats[mv_name],
                                'size': info['size']
                            }
                    
                    return stats
                    
        except Exception as e:
            logger.error(f"Failed to get Gold stats: {e}")
            return None
    
    def drop_and_recreate_view(self, view_name: str) -> bool:
        """
        Drop and recreate a materialized view (useful for schema changes).
        
        Args:
            view_name: Name of the materialized view
            
        Returns:
            True if successful
        """
        logger.info(f"Dropping and recreating {view_name}...")
        
        try:
            with psycopg.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    # This would require the SQL definition, which should come from the model files
                    # For now, just document this for manual operation
                    logger.info("Note: For schema changes, re-run model executor with force flag")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to drop/recreate {view_name}: {e}")
            return False


def main():
    """Main function for testing Gold layer refresh."""
    refresher = GoldLayerRefresh()
    
    # Refresh all views
    success = refresher.refresh_all_views()
    
    if success:
        # Get and display stats
        stats = refresher.get_gold_stats()
        if stats:
            print("\n" + "="*60)
            print("GOLD LAYER STATISTICS")
            print("="*60)
            for view_name, info in stats.items():
                if isinstance(info, dict):
                    print(f"  {view_name}: {info['row_count']} rows, {info['size']}")
                else:
                    print(f"  {view_name}: {info} rows")
    
    return success


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)

