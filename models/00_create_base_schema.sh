#!/bin/bash
set -e

# This script runs automatically when PostgreSQL container starts for the first time
# It uses environment variables from settings.env to create the schema dynamically

echo "================================================"
echo "Initializing PostgreSQL database schema"
echo "================================================"

# Get configuration from environment variables
# Use POSTGRES_* variables set by PostgreSQL Docker image
SCHEMA_NAME="${DATABASE_SCHEMA_NAME:-hashtag_based}"
DB_USER="${POSTGRES_USER:-postgres}"
DB_NAME="${POSTGRES_DB:-mastodon_toots}"

echo "Creating schema: $SCHEMA_NAME"
echo "Database: $DB_NAME"
echo "User: $DB_USER"

# Execute SQL commands using environment variables
psql -v ON_ERROR_STOP=1 --username "$DB_USER" --dbname "$DB_NAME" <<-EOSQL
    -- Create schema for hashtag data
    CREATE SCHEMA IF NOT EXISTS ${SCHEMA_NAME};
    
    -- Grant permissions
    GRANT ALL PRIVILEGES ON SCHEMA ${SCHEMA_NAME} TO ${DB_USER};
    
    -- Enable useful extensions in current database
    CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
    CREATE EXTENSION IF NOT EXISTS pg_trgm;
    
    -- Log initialization
    DO \$\$
    BEGIN
      RAISE NOTICE 'Schema ${SCHEMA_NAME} created successfully';
      RAISE NOTICE 'Extensions enabled: pg_stat_statements, pg_trgm';
    END \$\$;
EOSQL

# Create metabase database separately (must be done from postgres database)
psql -v ON_ERROR_STOP=1 --username "$DB_USER" --dbname "postgres" <<-EOSQL
    -- Create metabase database if it doesn't exist
    SELECT 'CREATE DATABASE metabase'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec
    
    GRANT ALL PRIVILEGES ON DATABASE metabase TO ${DB_USER};
EOSQL

echo "================================================"
echo "✅ Database initialization completed"
echo "   Schema: $SCHEMA_NAME"
echo "   Database: $DB_NAME"
echo "   Metabase: created"
echo "================================================"

