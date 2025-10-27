"""SQL Model Executor for Medallion Architecture.

This module handles the execution of SQL model files to create and maintain
the Bronze, Silver, and Gold layer schemas, tables, and views.
"""

import psycopg
import logging
from pathlib import Path
from typing import Optional

from extractor.config import config

logger = logging.getLogger(__name__)


class ModelExecutor:
    """Execute SQL model files to set up database schema."""
    
    def __init__(self):
        """Initialize the model executor with database configuration."""
        self.db_config = {
            'dbname': config.database_name,
            'user': config.database_user,
            'password': config.database_password,
            'host': config.database_host,
            'port': config.database_port
        }
        self.models_dir = Path(__file__).parent.parent.parent / 'models'
        logger.info(f"ModelExecutor initialized with models directory: {self.models_dir}")
    
    def execute_sql_file(self, conn: psycopg.Connection, sql_file: Path) -> bool:
        """
        Execute a single SQL file.
        
        Args:
            conn: Database connection
            sql_file: Path to SQL file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Executing SQL file: {sql_file.name}")
            
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Execute the SQL
            with conn.cursor() as cur:
                cur.execute(sql_content)
            
            conn.commit()
            logger.info(f"✅ Successfully executed: {sql_file.name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to execute {sql_file.name}: {e}")
            conn.rollback()
            return False
    
    def apply_layer_models(self, layer: str, force: bool = False) -> bool:
        """
        Apply all SQL models for a specific layer.
        
        Args:
            layer: Layer name ('bronze', 'silver', 'gold')
            force: If True, apply even if already applied
            
        Returns:
            True if all models applied successfully
        """
        layer_dir = self.models_dir / layer
        
        if not layer_dir.exists():
            logger.warning(f"Layer directory not found: {layer_dir}")
            return False
        
        # Get all SQL files sorted by name
        sql_files = sorted(layer_dir.glob('*.sql'))
        
        if not sql_files:
            logger.warning(f"No SQL files found in {layer_dir}")
            return False
        
        logger.info(f"\n{'='*60}")
        logger.info(f"APPLYING {layer.upper()} LAYER MODELS")
        logger.info(f"{'='*60}")
        logger.info(f"Found {len(sql_files)} SQL files")
        
        try:
            with psycopg.connect(**self.db_config) as conn:
                success_count = 0
                
                for sql_file in sql_files:
                    if self.execute_sql_file(conn, sql_file):
                        success_count += 1
                    else:
                        logger.error(f"Failed to apply {sql_file.name}, stopping execution")
                        return False
                
                logger.info(f"\n✅ Successfully applied {success_count}/{len(sql_files)} models for {layer} layer")
                return True
                
        except psycopg.OperationalError as e:
            logger.error(f"Database connection failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error applying {layer} models: {e}", exc_info=True)
            return False
    
    def apply_all_models(self, force: bool = False) -> bool:
        """
        Apply all models for all layers in order: Bronze → Silver → Gold.
        
        Args:
            force: If True, apply even if already applied
            
        Returns:
            True if all models applied successfully
        """
        logger.info("\n" + "="*60)
        logger.info("APPLYING MEDALLION ARCHITECTURE MODELS")
        logger.info("="*60)
        
        layers = ['bronze', 'silver', 'gold']
        
        for layer in layers:
            success = self.apply_layer_models(layer, force=force)
            if not success:
                logger.error(f"Failed to apply {layer} layer models")
                return False
        
        logger.info("\n" + "="*60)
        logger.info("✅ ALL MODELS APPLIED SUCCESSFULLY")
        logger.info("="*60)
        return True
    
    def verify_schemas(self) -> dict:
        """
        Verify that all schemas exist and return schema information.
        
        Returns:
            Dictionary with schema verification results
        """
        try:
            with psycopg.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    # Check schemas
                    cur.execute("""
                        SELECT schema_name 
                        FROM information_schema.schemata 
                        WHERE schema_name IN ('bronze', 'dim_facts', 'analytics')
                        ORDER BY schema_name
                    """)
                    schemas = [row[0] for row in cur.fetchall()]
                    
                    # Count tables in each schema
                    schema_info = {}
                    for schema in ['bronze', 'dim_facts', 'analytics']:
                        if schema in schemas:
                            cur.execute(f"""
                                SELECT COUNT(*) 
                                FROM information_schema.tables 
                                WHERE table_schema = '{schema}'
                            """)
                            table_count = cur.fetchone()[0]
                            
                            # Count materialized views for analytics
                            if schema == 'analytics':
                                cur.execute(f"""
                                    SELECT COUNT(*) 
                                    FROM pg_matviews 
                                    WHERE schemaname = '{schema}'
                                """)
                                mv_count = cur.fetchone()[0]
                                schema_info[schema] = {
                                    'exists': True,
                                    'tables': table_count,
                                    'materialized_views': mv_count
                                }
                            else:
                                schema_info[schema] = {
                                    'exists': True,
                                    'tables': table_count
                                }
                        else:
                            schema_info[schema] = {'exists': False}
                    
                    return schema_info
                    
        except Exception as e:
            logger.error(f"Failed to verify schemas: {e}")
            return {}


def main():
    """Main function for testing model executor."""
    executor = ModelExecutor()
    
    # Apply all models
    success = executor.apply_all_models()
    
    if success:
        # Verify schemas
        schema_info = executor.verify_schemas()
        print("\n" + "="*60)
        print("SCHEMA VERIFICATION")
        print("="*60)
        for schema, info in schema_info.items():
            if info.get('exists'):
                print(f"✅ {schema}: {info.get('tables', 0)} tables", end='')
                if 'materialized_views' in info:
                    print(f", {info['materialized_views']} materialized views")
                else:
                    print()
            else:
                print(f"❌ {schema}: Not found")
    
    return success


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)

