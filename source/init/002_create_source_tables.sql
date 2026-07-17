-- Reference and parameter tables are created first because customer and loan
-- tables reference their stable business codes.

CREATE TABLE prm.currencies (
    currency_code VARCHAR(3) PRIMARY KEY,
    currency_name VARCHAR(100) NOT NULL,
    minor_unit SMALLINT NOT NULL DEFAULT 2 CHECK (minor_unit BETWEEN 0 AND 4),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE prm.branches (
    branch_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    branch_code VARCHAR(20) NOT NULL UNIQUE,
    branch_name VARCHAR(200) NOT NULL,
    city VARCHAR(100) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE prm.products (
    product_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_code VARCHAR(30) NOT NULL UNIQUE,
    product_name VARCHAR(200) NOT NULL,
    product_type VARCHAR(30) NOT NULL,
    default_currency_code VARCHAR(3) NOT NULL REFERENCES prm.currencies (currency_code),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE prm.status_codes (
    status_domain VARCHAR(50) NOT NULL,
    status_code VARCHAR(30) NOT NULL,
    status_name VARCHAR(200) NOT NULL,
    is_terminal BOOLEAN NOT NULL DEFAULT FALSE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (status_domain, status_code)
);

CREATE TABLE prm.rate_parameters (
    rate_parameter_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_code VARCHAR(30) NOT NULL REFERENCES prm.products (product_code),
    rate_type VARCHAR(30) NOT NULL,
    annual_rate NUMERIC(9, 6) NOT NULL CHECK (annual_rate >= 0),
    effective_from DATE NOT NULL,
    effective_to DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (effective_to IS NULL OR effective_to >= effective_from),
    UNIQUE (product_code, rate_type, effective_from)
);

CREATE TABLE mms.customers (
    customer_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_no VARCHAR(30) NOT NULL UNIQUE,
    customer_type VARCHAR(20) NOT NULL CHECK (customer_type IN ('INDIVIDUAL', 'CORPORATE')),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    legal_name VARCHAR(200),
    national_id VARCHAR(20),
    tax_id VARCHAR(20),
    date_of_birth DATE,
    segment_code VARCHAR(30) NOT NULL,
    status_code VARCHAR(30) NOT NULL,
    home_branch_code VARCHAR(20) REFERENCES prm.branches (branch_code),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (
        (customer_type = 'INDIVIDUAL' AND first_name IS NOT NULL AND last_name IS NOT NULL)
        OR (customer_type = 'CORPORATE' AND legal_name IS NOT NULL)
    )
);

CREATE UNIQUE INDEX ux_mms_customers_national_id
    ON mms.customers (national_id)
    WHERE national_id IS NOT NULL;

CREATE UNIQUE INDEX ux_mms_customers_tax_id
    ON mms.customers (tax_id)
    WHERE tax_id IS NOT NULL;

CREATE TABLE mms.customer_addresses (
    address_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES mms.customers (customer_id),
    address_type VARCHAR(20) NOT NULL,
    address_line VARCHAR(500) NOT NULL,
    district VARCHAR(100),
    city VARCHAR(100) NOT NULL,
    postal_code VARCHAR(20),
    country_code VARCHAR(2) NOT NULL DEFAULT 'TR',
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    valid_from DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE TABLE mms.customer_contacts (
    contact_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES mms.customers (customer_id),
    contact_type VARCHAR(20) NOT NULL CHECK (contact_type IN ('EMAIL', 'PHONE')),
    contact_value VARCHAR(320) NOT NULL,
    is_primary BOOLEAN NOT NULL DEFAULT FALSE,
    is_verified BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (customer_id, contact_type, contact_value)
);

CREATE TABLE mms.customer_relations (
    relation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_customer_id BIGINT NOT NULL REFERENCES mms.customers (customer_id),
    target_customer_id BIGINT NOT NULL REFERENCES mms.customers (customer_id),
    relation_type VARCHAR(30) NOT NULL,
    valid_from DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (source_customer_id <> target_customer_id),
    CHECK (valid_to IS NULL OR valid_to >= valid_from),
    UNIQUE (source_customer_id, target_customer_id, relation_type, valid_from)
);

CREATE TABLE krd.loan_applications (
    application_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    application_no VARCHAR(40) NOT NULL UNIQUE,
    customer_no VARCHAR(30) NOT NULL REFERENCES mms.customers (customer_no),
    product_code VARCHAR(30) NOT NULL REFERENCES prm.products (product_code),
    branch_code VARCHAR(20) NOT NULL REFERENCES prm.branches (branch_code),
    requested_amount NUMERIC(18, 2) NOT NULL CHECK (requested_amount > 0),
    currency_code VARCHAR(3) NOT NULL REFERENCES prm.currencies (currency_code),
    term_months SMALLINT NOT NULL CHECK (term_months > 0),
    status_code VARCHAR(30) NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    decision_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE krd.loans (
    loan_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    loan_no VARCHAR(40) NOT NULL UNIQUE,
    application_id BIGINT NOT NULL UNIQUE REFERENCES krd.loan_applications (application_id),
    customer_no VARCHAR(30) NOT NULL REFERENCES mms.customers (customer_no),
    product_code VARCHAR(30) NOT NULL REFERENCES prm.products (product_code),
    branch_code VARCHAR(20) NOT NULL REFERENCES prm.branches (branch_code),
    principal_amount NUMERIC(18, 2) NOT NULL CHECK (principal_amount > 0),
    currency_code VARCHAR(3) NOT NULL REFERENCES prm.currencies (currency_code),
    annual_interest_rate NUMERIC(9, 6) NOT NULL CHECK (annual_interest_rate >= 0),
    term_months SMALLINT NOT NULL CHECK (term_months > 0),
    status_code VARCHAR(30) NOT NULL,
    disbursed_at TIMESTAMPTZ,
    maturity_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE krd.installments (
    installment_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    loan_id BIGINT NOT NULL REFERENCES krd.loans (loan_id),
    installment_no SMALLINT NOT NULL CHECK (installment_no > 0),
    due_date DATE NOT NULL,
    principal_amount NUMERIC(18, 2) NOT NULL CHECK (principal_amount >= 0),
    interest_amount NUMERIC(18, 2) NOT NULL CHECK (interest_amount >= 0),
    paid_amount NUMERIC(18, 2) NOT NULL DEFAULT 0 CHECK (paid_amount >= 0),
    status_code VARCHAR(30) NOT NULL,
    paid_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (loan_id, installment_no)
);

CREATE TABLE krd.collaterals (
    collateral_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    loan_id BIGINT NOT NULL REFERENCES krd.loans (loan_id),
    collateral_type VARCHAR(30) NOT NULL,
    collateral_reference VARCHAR(100) NOT NULL,
    appraised_value NUMERIC(18, 2) NOT NULL CHECK (appraised_value >= 0),
    currency_code VARCHAR(3) NOT NULL REFERENCES prm.currencies (currency_code),
    status_code VARCHAR(30) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (collateral_type, collateral_reference)
);

COMMENT ON TABLE mms.customers IS
    'Synthetic customer master used only by the local source and CDC pilot.';
COMMENT ON COLUMN mms.customers.national_id IS
    'Synthetic sensitive PII; never populate with a real national identifier.';
COMMENT ON COLUMN mms.customers.tax_id IS
    'Synthetic sensitive PII; never populate with a real tax identifier.';
COMMENT ON TABLE mms.customer_contacts IS
    'Synthetic customer contact data classified as PII.';
COMMENT ON TABLE krd.loans IS
    'Synthetic loan contracts used only by the local source and CDC pilot.';
COMMENT ON TABLE prm.rate_parameters IS
    'Synthetic effective-dated lending rate parameters.';

-- The workload simulator updates mutable records explicitly. These indexes support
-- common source lookups without pretending to be a production tuning baseline.
CREATE INDEX ix_mms_addresses_customer ON mms.customer_addresses (customer_id);
CREATE INDEX ix_mms_contacts_customer ON mms.customer_contacts (customer_id);
CREATE INDEX ix_krd_applications_customer ON krd.loan_applications (customer_no);
CREATE INDEX ix_krd_loans_customer ON krd.loans (customer_no);
CREATE INDEX ix_krd_installments_due_date ON krd.installments (due_date);
