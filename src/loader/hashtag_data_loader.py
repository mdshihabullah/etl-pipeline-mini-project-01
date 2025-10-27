"""PostgreSQL data loader for Mastodon toots."""

import psycopg
import polars as pl
import logging
from typing import Optional

from extractor.config import config

logger = logging.getLogger(__name__)


class MastodonDataLoader:
    """Load Mastodon toot data to PostgreSQL database."""
    
    def __init__(self):
        """Initialize the loader with database configuration."""
        self.db_config = {
            'dbname': config.database_name,
            'user': config.database_user,
            'password': config.database_password,
            'host': config.database_host,
            'port': config.database_port
        }
        self.schema_name = config.database_schema_name
        self.table_name = config.database_table_name
        self.full_table_name = f"{self.schema_name}.{self.table_name}"
        
        logger.info(f"Loader initialized for {self.full_table_name}")
    
    def load_to_postgres(self, df: pl.DataFrame, mode: str = 'append') -> bool:
        """
        Load DataFrame to PostgreSQL.
        
        Args:
            df: Polars DataFrame to load
            mode: 'append' to add data, 'replace' to drop and recreate table
            
        Returns:
            True if successful, False otherwise
        """
        if df.is_empty():
            logger.warning("Empty DataFrame provided, nothing to load")
            return False
        
        try:
            logger.info(f"Connecting to database: {self.db_config['dbname']}")
            
            with psycopg.connect(**self.db_config) as conn:
                # Create schema if not exists
                self._create_schema(conn)
                
                # Create or replace table
                if mode == 'replace':
                    self._drop_table(conn)
                
                self._create_table(conn, df)
                
                # Insert data
                records_loaded = self._insert_data(conn, df)
                
                conn.commit()
                
                logger.info(f"✅ Successfully loaded {records_loaded} records to {self.full_table_name}")
                return True
                
        except psycopg.OperationalError as e:
            logger.error(f"Database connection failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to load data: {e}", exc_info=True)
            return False
    
    def _create_schema(self, conn: psycopg.Connection):
        """Create schema if it doesn't exist."""
        try:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
            logger.info(f"Schema '{self.schema_name}' ready")
        except Exception as e:
            logger.error(f"Failed to create schema: {e}")
            raise
    
    def _drop_table(self, conn: psycopg.Connection):
        """Drop table if exists."""
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {self.full_table_name} CASCADE")
            logger.info(f"Dropped existing table {self.full_table_name}")
        except Exception as e:
            logger.error(f"Failed to drop table: {e}")
            raise
    
    def _create_table(self, conn: psycopg.Connection, df: pl.DataFrame):
        """Create table with appropriate schema based on DataFrame structure."""
        
        # Map Polars dtypes to PostgreSQL types
        type_mapping = {
            pl.Utf8: 'TEXT',
            pl.Int64: 'BIGINT',
            pl.Float64: 'DOUBLE PRECISION',
            pl.Boolean: 'BOOLEAN',
            pl.Datetime: 'TIMESTAMP WITH TIME ZONE',
        }
        
        # Build column definitions
        columns = []
        for col, dtype in zip(df.columns, df.dtypes):
            # Get base type
            if isinstance(dtype, pl.Datetime):
                pg_type = 'TIMESTAMP WITH TIME ZONE'
            else:
                pg_type = type_mapping.get(dtype, 'TEXT')
            
            columns.append(f"{col} {pg_type}")
        
        # Add primary key and indexes
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.full_table_name} (
            {', '.join(columns)},
            PRIMARY KEY (id),
            CONSTRAINT unique_toot_id UNIQUE (id)
        )
        """
        
        try:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                
                # Create indexes for common query patterns
                self._create_indexes(cur)
                
            logger.info(f"Table {self.full_table_name} ready with {len(columns)} columns")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
    
    def _create_indexes(self, cur):
        """Create indexes for better query performance."""
        indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_created_at ON {self.full_table_name} (created_at)",
            f"CREATE INDEX IF NOT EXISTS idx_sentiment_value ON {self.full_table_name} (sentiment_value)",
            f"CREATE INDEX IF NOT EXISTS idx_account_username ON {self.full_table_name} (account_username)",
            f"CREATE INDEX IF NOT EXISTS idx_language ON {self.full_table_name} (language)",
        ]
        
        for index_sql in indexes:
            try:
                cur.execute(index_sql)
            except Exception:
                logger.warning("Index creation failed (may already exist)")
    
    def _insert_data(self, conn: psycopg.Connection, df: pl.DataFrame) -> int:
        """
        Insert data using efficient COPY method.
        
        Args:
            conn: Database connection
            df: DataFrame to insert
            
        Returns:
            Number of records inserted
        """
        # Convert DataFrame to list of tuples for insertion
        columns = df.columns
        column_names = ', '.join(columns)
        
        # Use COPY for efficient bulk insert
        try:
            with conn.cursor() as cur:
                # Use COPY with CSV format for efficiency (no header!)
                with cur.copy(f"COPY {self.full_table_name} ({column_names}) FROM STDIN WITH (FORMAT CSV, NULL '')") as copy:
                    # Write CSV data WITHOUT header
                    csv_data = df.write_csv(include_header=False)
                    copy.write(csv_data.encode('utf-8'))
                
                records_inserted = len(df)
                logger.info(f"Inserted {records_inserted} records using COPY")
                return records_inserted
                
        except psycopg.errors.UniqueViolation as e:
            logger.warning(f"Duplicate records encountered, using upsert strategy")
            # CRITICAL: Rollback the aborted transaction before proceeding
            conn.rollback()
            logger.info("Transaction rolled back, starting fresh for upsert")
            # Fall back to upsert for handling duplicates
            return self._upsert_data(conn, df)
        except Exception as e:
            logger.error(f"COPY insert failed: {e}")
            # CRITICAL: Rollback the aborted transaction before proceeding
            conn.rollback()
            logger.info("Transaction rolled back, starting fresh for batch insert")
            # Fall back to regular insert
            return self._insert_data_batch(conn, df)
    
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
        
        # Build update clause for ON CONFLICT
        update_cols = [col for col in columns if col != 'id']
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
        
        upsert_sql = f"""
        INSERT INTO {self.full_table_name} ({column_names})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {update_clause}
        """
        
        records_upserted = 0
        batch_size = 1000
        
        try:
            with conn.cursor() as cur:
                # Convert to list of tuples
                data = [tuple(row) for row in df.iter_rows()]
                
                # Insert in batches
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
    
    def _insert_data_batch(self, conn: psycopg.Connection, df: pl.DataFrame) -> int:
        """
        Fallback: Insert data in batches using regular INSERT.
        
        Args:
            conn: Database connection
            df: DataFrame to insert
            
        Returns:
            Number of records inserted
        """
        columns = df.columns
        column_names = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        
        insert_sql = f"INSERT INTO {self.full_table_name} ({column_names}) VALUES ({placeholders})"
        
        records_inserted = 0
        batch_size = 1000
        
        try:
            with conn.cursor() as cur:
                data = [tuple(row) for row in df.iter_rows()]
                
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    cur.executemany(insert_sql, batch)
                    records_inserted += len(batch)
                    
                    if (i + batch_size) % 5000 == 0:
                        logger.info(f"Inserted {records_inserted}/{len(data)} records")
                
                logger.info(f"Batch inserted {records_inserted} records")
                return records_inserted
                
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            raise
    
    def verify_data(self) -> Optional[dict]:
        """
        Verify data in database and return statistics.
        
        Returns:
            Dictionary with table statistics or None if error
        """
        try:
            with psycopg.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    # Get row count
                    cur.execute(f"SELECT COUNT(*) FROM {self.full_table_name}")
                    row_count = cur.fetchone()[0]
                    
                    # Get date range
                    cur.execute(f"""
                        SELECT 
                            MIN(created_at) as earliest,
                            MAX(created_at) as latest
                        FROM {self.full_table_name}
                    """)
                    date_range = cur.fetchone()
                    
                    # Get sentiment distribution
                    cur.execute(f"""
                        SELECT sentiment_value, COUNT(*) 
                        FROM {self.full_table_name}
                        WHERE sentiment_value IS NOT NULL
                        GROUP BY sentiment_value
                    """)
                    sentiment_dist = dict(cur.fetchall())
                    
                    stats = {
                        'row_count': row_count,
                        'earliest_toot': date_range[0],
                        'latest_toot': date_range[1],
                        'sentiment_distribution': sentiment_dist
                    }
                    
                    logger.info(f"Database verification: {row_count} rows found")
                    return stats
                    
        except Exception as e:
            logger.error(f"Verification failed: {e}")
            return None

