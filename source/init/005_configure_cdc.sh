#!/bin/sh
set -eu

: "${CDC_DB_USER:?CDC_DB_USER must be set}"
: "${CDC_DB_PASSWORD:?CDC_DB_PASSWORD must be set}"

psql \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set=ON_ERROR_STOP=1 \
    --set=cdc_user="$CDC_DB_USER" \
    --set=cdc_password="$CDC_DB_PASSWORD" <<'SQL'
SELECT format(
    'CREATE ROLE %I WITH LOGIN REPLICATION PASSWORD %L',
    :'cdc_user',
    :'cdc_password'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = :'cdc_user'
)
\gexec

SELECT format(
    'ALTER ROLE %I WITH LOGIN REPLICATION PASSWORD %L',
    :'cdc_user',
    :'cdc_password'
)
\gexec

SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'cdc_user')
\gexec
SELECT format('GRANT USAGE ON SCHEMA mms, krd, prm TO %I', :'cdc_user')
\gexec
SELECT format('GRANT SELECT ON ALL TABLES IN SCHEMA mms, krd, prm TO %I', :'cdc_user')
\gexec
SELECT format(
    'ALTER DEFAULT PRIVILEGES IN SCHEMA mms, krd, prm GRANT SELECT ON TABLES TO %I',
    :'cdc_user'
)
\gexec

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_publication
        WHERE pubname = 'lakehouse_cdc_publication'
    ) THEN
        CREATE PUBLICATION lakehouse_cdc_publication FOR TABLE
            mms.customers,
            mms.customer_addresses,
            mms.customer_contacts,
            mms.customer_relations,
            krd.loan_applications,
            krd.loans,
            krd.installments,
            krd.collaterals,
            prm.currencies,
            prm.branches,
            prm.products,
            prm.status_codes,
            prm.rate_parameters;
    END IF;
END
$$;

ALTER PUBLICATION lakehouse_cdc_publication SET TABLE
    mms.customers,
    mms.customer_addresses,
    mms.customer_contacts,
    mms.customer_relations,
    krd.loan_applications,
    krd.loans,
    krd.installments,
    krd.collaterals,
    prm.currencies,
    prm.branches,
    prm.products,
    prm.status_codes,
    prm.rate_parameters;

ALTER TABLE mms.customers REPLICA IDENTITY FULL;
ALTER TABLE mms.customer_addresses REPLICA IDENTITY FULL;
ALTER TABLE mms.customer_contacts REPLICA IDENTITY FULL;
ALTER TABLE mms.customer_relations REPLICA IDENTITY FULL;
ALTER TABLE krd.loan_applications REPLICA IDENTITY FULL;
ALTER TABLE krd.loans REPLICA IDENTITY FULL;
ALTER TABLE krd.installments REPLICA IDENTITY FULL;
ALTER TABLE krd.collaterals REPLICA IDENTITY FULL;
ALTER TABLE prm.currencies REPLICA IDENTITY FULL;
ALTER TABLE prm.branches REPLICA IDENTITY FULL;
ALTER TABLE prm.products REPLICA IDENTITY FULL;
ALTER TABLE prm.status_codes REPLICA IDENTITY FULL;
ALTER TABLE prm.rate_parameters REPLICA IDENTITY FULL;

COMMENT ON PUBLICATION lakehouse_cdc_publication IS
    'Explicit local CDC allowlist for synthetic mms, krd and prm source tables.';
SQL
