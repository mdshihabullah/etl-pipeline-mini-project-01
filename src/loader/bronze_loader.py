"""Bronze Layer Loader - Store transformed data with metadata."""

import psycopg
import polars as pl
import logging
from typing import Optional
from datetime import datetime
import uuid

from extractor.config import config

logger = logging.getLogger(__name__)


class BronzeLayerLoader:
    """Load transformed data to Bronze layer with metadata tracking."""
    
    def __init__(self):
        """Initialize the Bronze loader with database configuration."""
        self.db_config = {
            'dbname': config.database_name,
            'user': config.database_user,
            'password': config.database_password,
            'host': config.database_host,
            'port': config.database_port
        }
        self.schema_name = config.bronze_database_schema_name
        self.table_name = config.bronze_database_table_name
        self.full_table_name = f"{self.schema_name}.{self.table_name}"
        
        # Generate unique pipeline run ID
        self.pipeline_run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        
        logger.info(f"BronzeLayerLoader initialized for {self.full_table_name}")
        logger.info(f"Pipeline Run ID: {self.pipeline_run_id}")
    
    def load_to_bronze(self, df: pl.DataFrame, mode: str = 'append') -> bool:
        """
        Load DataFrame to Bronze layer with metadata.
        
        Args:
            df: Polars DataFrame to load
            mode: 'append' to add data, 'replace' to truncate and reload
            
        Returns:
            True if successful, False otherwise
        """
        if df.is_empty():
            logger.warning("Empty DataFrame provided, nothing to load")
            return False
        
        try:
            logger.info(f"Loading {len(df)} records to Bronze layer")
            logger.info(f"Mode: {mode}")
            
            # Add metadata columns
            df_with_metadata = self._add_metadata(df)
            
            with psycopg.connect(**self.db_config) as conn:
                # Handle replace mode
                if mode == 'replace':
                    self._truncate_table(conn)
                
                # Insert data
                records_loaded = self._insert_data(conn, df_with_metadata)
                
                conn.commit()
                
                logger.info(f"✅ Successfully loaded {records_loaded} records to Bronze layer")
                return True
                
        except psycopg.OperationalError as e:
            logger.error(f"Database connection failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to load data to Bronze: {e}", exc_info=True)
            return False
    
    def _add_metadata(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Add metadata columns to DataFrame.
        
        Args:
            df: Original DataFrame
            
        Returns:
            DataFrame with metadata columns added
        """
        # Add metadata columns
        df = df.with_columns([
            pl.lit(datetime.now()).alias('ingestion_timestamp'),
            pl.lit(self.pipeline_run_id).alias('pipeline_run_id'),
            pl.lit('1.0').alias('data_version')
        ])
        
        logger.info("Added metadata columns: ingestion_timestamp, pipeline_run_id, data_version")
        return df
    
    def _truncate_table(self, conn: psycopg.Connection):
        """Truncate Bronze table."""
        try:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {self.full_table_name} CASCADE")
            logger.info(f"Truncated table {self.full_table_name}")
        except Exception:
            logger.error("Failed to truncate table")
            raise
    
    def _insert_data(self, conn: psycopg.Connection, df: pl.DataFrame) -> int:
        """
        Insert data using efficient COPY method with upsert fallback.
        
        Args:
            conn: Database connection
            df: DataFrame to insert
            
        Returns:
            Number of records inserted
        """
        columns = df.columns
        column_names = ', '.join(columns)
        
        try:
            with conn.cursor() as cur:
                # Use COPY for efficient bulk insert
                with cur.copy(f"COPY {self.full_table_name} ({column_names}) FROM STDIN WITH (FORMAT CSV, NULL '')") as copy:
                    csv_data = df.write_csv(include_header=False)
                    copy.write(csv_data.encode('utf-8'))
                
                records_inserted = len(df)
                logger.info(f"Inserted {records_inserted} records using COPY")
                return records_inserted
                
        except psycopg.errors.UniqueViolation as e:
            logger.warning(f"Duplicate records encountered, using upsert strategy")
            conn.rollback()
            return self._upsert_data(conn, df)
        except Exception as e:
            logger.error(f"COPY insert failed: {e}")
            conn.rollback()
            return self._upsert_data(conn, df)
    
    def _upsert_data(self, conn: psycopg.Connection, df: pl.DataFrame) -> int:
        """
        Insert data with ON CONFLICT DO UPDATE (upsert).
        
        Args:
            conn: Database connection
            df: DataFrame to upsert
            
        Returns:
            Number of records upserted
        """
        columns = df.columns
        column_names = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        # Build update clause for ON CONFLICT (update metadata only)
        update_cols = ['ingestion_timestamp', 'pipeline_run_id', 'data_version']
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols if col in columns])
        
        upsert_sql = f"""
        INSERT INTO {self.full_table_name} ({column_names})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {update_clause}
        """
        
        records_upserted = 0
        batch_size = 1000
        
        try:
            with conn.cursor() as cur:
                data = [tuple(row) for row in df.iter_rows()]
                
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    cur.executemany(upsert_sql, batch)
                    records_upserted += len(batch)
                    
                    if (i + batch_size) % 5000 == 0:
                        logger.info(f"Upserted {records_upserted}/{len(data)} records")
                
                logger.info(f"Upserted {records_upserted} records")
                return records_upserted
                
        except Exception as e:
            logger.error(f"Upsert failed: {e}")
            raise
    
    def get_bronze_stats(self) -> Optional[dict]:
        """
        Get statistics about Bronze layer data.
        
        Returns:
            Dictionary with Bronze layer statistics
        """
        try:
            with psycopg.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    # Get row count
                    cur.execute(f"SELECT COUNT(*) FROM {self.full_table_name}")
                    row_count = cur.fetchone()[0]
                    
                    # Get latest ingestion
                    cur.execute(f"""
                        SELECT 
                            MAX(ingestion_timestamp) as latest_ingestion,
                            COUNT(DISTINCT pipeline_run_id) as total_runs
                        FROM {self.full_table_name}
                    """)
                    result = cur.fetchone()
                    
                    stats = {
                        'row_count': row_count,
                        'latest_ingestion': result[0],
                        'total_pipeline_runs': result[1],
                        'current_run_id': self.pipeline_run_id
                    }
                    
                    return stats
                    
        except Exception as e:
            logger.error(f"Failed to get Bronze stats: {e}")
            return None

