-- Read-only role for the Autoangel Console (and any other reader).
--
-- Run as a superuser or the database owner, once, after the Engine has migrated:
--   psql "$ADMIN_URL" -f docs/readonly-role.sql
-- Then create a login that belongs to the group role:
--   CREATE ROLE console_ro LOGIN PASSWORD '...' IN ROLE autoangel_readonly;
--
-- The group role can read every table, view and continuous aggregate in the
-- public schema, now and in the future. It cannot write or change schema.
-- Statements are separated by blank lines (the integration test relies on it).

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'autoangel_readonly') THEN
    CREATE ROLE autoangel_readonly NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
  END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO autoangel_readonly;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO autoangel_readonly;

GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO autoangel_readonly;

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO autoangel_readonly;

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO autoangel_readonly;

-- Nothing may be created in public by this role (PostgreSQL 15+ already
-- denies it; this makes it explicit on older servers).
REVOKE CREATE ON SCHEMA public FROM autoangel_readonly;
