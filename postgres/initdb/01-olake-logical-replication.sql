-- Enable logical replication support for the existing Nessie database
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'olake_replica') THEN
    CREATE ROLE olake_replica WITH LOGIN REPLICATION PASSWORD 'olakepass';
  ELSE
    ALTER ROLE olake_replica WITH LOGIN REPLICATION PASSWORD 'olakepass';
  END IF;
END $$;

GRANT CONNECT ON DATABASE nessie TO olake_replica;

DO $$
DECLARE
  r record;
BEGIN
  FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = 'public'
  LOOP
    EXECUTE format('GRANT SELECT ON TABLE public.%I TO olake_replica', r.tablename);
  END LOOP;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'olake_pub') THEN
    CREATE PUBLICATION olake_pub FOR ALL TABLES;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'olake_slot') THEN
    PERFORM pg_create_logical_replication_slot('olake_slot', 'pgoutput');
  END IF;
END $$;
