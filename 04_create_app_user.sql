-- ============================================================
-- Post-RDS Setup: Create Read-Only Application User
-- Run as dbadmin (the RDS master user) after RDS is up.
-- ============================================================
-- This separates the admin account (dbadmin, used only for setup)
-- from the application account (emr_reader, used by Spark jobs).
-- Principle of least privilege: emr_reader can only SELECT,
-- and only on the ecommerce schema.
-- ============================================================

-- Create the read-only role used by EMR Spark jobs
-- Password here is a placeholder; the real password is generated
-- by Terraform and stored in Secrets Manager.
CREATE ROLE emr_reader
    WITH LOGIN
    PASSWORD 'REPLACE_WITH_VALUE_FROM_SECRETS_MANAGER'
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    CONNECTION LIMIT 20;   -- cap connections from Spark partitions

-- Grant schema visibility
GRANT CONNECT ON DATABASE ordersdb TO emr_reader;
GRANT USAGE   ON SCHEMA ecommerce  TO emr_reader;

-- Grant read-only on the target table (and future tables in schema)
GRANT SELECT ON ecommerce.orders TO emr_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA ecommerce
    GRANT SELECT ON TABLES TO emr_reader;

-- Verify
SELECT
    rolname,
    rolcanlogin,
    rolcreatedb,
    rolsuper,
    rolconnlimit
FROM pg_roles
WHERE rolname IN ('dbadmin', 'emr_reader');
