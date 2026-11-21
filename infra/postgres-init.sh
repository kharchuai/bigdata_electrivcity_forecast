#!/usr/bin/env bash
set -euo pipefail

# Mounted into docker-entrypoint-initdb.d: runs only for a new PostgreSQL volume.
# The application database is created by POSTGRES_DB in the official image.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  --set=catalog_db="$ICEBERG_DATABASE" --set=catalog_user="$ICEBERG_USER" \
  --set=catalog_password="$ICEBERG_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'catalog_user', :'catalog_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = :'catalog_user')
\gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'catalog_db', :'catalog_user')
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = :'catalog_db')
\gexec
SQL
