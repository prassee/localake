#!/bin/bash
set -euo pipefail

DB_NAME="${DB_NAME:-nessie}"
DB_USER="${DB_USER:-nessie}"
DB_PASSWORD="${DB_PASSWORD:-nessiepass}"
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"

export PGPASSWORD="$DB_PASSWORD"

usage() {
  cat <<'EOF'
Usage: manage_replication.sh <action>

Actions:
  drop-publication       Drop the olake_pub publication
  drop-slot              Drop the olake_slot replication slot
  truncate               Truncate all tables in the public schema
  reset-ids              Reset identity columns in the public schema
  vacuum                 Run VACUUM ANALYZE on all tables in the public schema
EOF
}

run_sql() {
  psql -h "$PGHOST" -p "$PGPORT" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 -c "$1"
}

case "${1:-}" in
  drop-publication)
    run_sql "DROP PUBLICATION IF EXISTS olake_pub;"
    ;;
  drop-slot)
    run_sql "SELECT pg_drop_replication_slot('olake_slot');" || true
    ;;
  truncate)
    run_sql "DO $$ BEGIN FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP EXECUTE format('TRUNCATE TABLE public.%I RESTART IDENTITY CASCADE', r.tablename); END LOOP; END $$;"
    ;;
  reset-ids)
    run_sql "DO $$ BEGIN FOR r IN (SELECT tablename, attname FROM pg_attribute JOIN pg_class ON pg_class.oid = pg_attribute.attrelid JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace WHERE pg_namespace.nspname = 'public' AND pg_attribute.attidentity <> '' AND pg_attribute.attnum > 0) LOOP EXECUTE format('ALTER TABLE public.%I ALTER COLUMN %I RESTART WITH 1', r.tablename, r.attname); END LOOP; END $$;"
    ;;
  vacuum)
    run_sql "DO $$ BEGIN FOR r IN (SELECT schemaname, tablename FROM pg_tables WHERE schemaname = 'public') LOOP EXECUTE format('VACUUM ANALYZE public.%I', r.tablename); END LOOP; END $$;"
    ;;
  help|-h|--help)
    usage
    ;;
  *)
    usage >&2
    exit 1
    ;;
 esac
