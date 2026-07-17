create schema ci.cdc_raw_vault;
create schema ci.gold_dbt;

create table ci.cdc_raw_vault.hub_customer (
    customer_hk varchar,
    customer_bk varchar
);
insert into ci.cdc_raw_vault.hub_customer values ('CUSTOMER_HK_1', 'C000001');

create table ci.cdc_raw_vault.hub_loan_application (
    loan_application_hk varchar,
    loan_application_bk varchar
);
insert into ci.cdc_raw_vault.hub_loan_application values ('APPLICATION_HK_1', 'A000001');

create table ci.cdc_raw_vault.hub_loan (
    loan_hk varchar,
    loan_bk varchar
);
insert into ci.cdc_raw_vault.hub_loan values ('LOAN_HK_1', 'L000001');

create table ci.cdc_raw_vault.hub_product (
    product_hk varchar,
    product_bk varchar
);
insert into ci.cdc_raw_vault.hub_product values ('PRODUCT_HK_1', 'CONSUMER_TRY');

create table ci.cdc_raw_vault.hub_branch (
    branch_hk varchar,
    branch_bk varchar
);
insert into ci.cdc_raw_vault.hub_branch values ('BRANCH_HK_1', 'IST001');

create table ci.cdc_raw_vault.hub_currency (
    currency_hk varchar,
    currency_bk varchar
);
insert into ci.cdc_raw_vault.hub_currency values ('CURRENCY_HK_1', 'TRY');

create table ci.cdc_raw_vault.link_application_context (
    application_context_hk varchar,
    loan_application_hk varchar,
    customer_hk varchar,
    product_hk varchar,
    branch_hk varchar,
    currency_hk varchar
);
insert into ci.cdc_raw_vault.link_application_context values (
    'APPLICATION_CONTEXT_HK_1',
    'APPLICATION_HK_1',
    'CUSTOMER_HK_1',
    'PRODUCT_HK_1',
    'BRANCH_HK_1',
    'CURRENCY_HK_1'
);

create table ci.cdc_raw_vault.link_loan_context (
    loan_context_hk varchar,
    loan_hk varchar,
    loan_application_hk varchar,
    customer_hk varchar,
    product_hk varchar,
    branch_hk varchar,
    currency_hk varchar
);
insert into ci.cdc_raw_vault.link_loan_context values (
    'LOAN_CONTEXT_HK_1',
    'LOAN_HK_1',
    'APPLICATION_HK_1',
    'CUSTOMER_HK_1',
    'PRODUCT_HK_1',
    'BRANCH_HK_1',
    'CURRENCY_HK_1'
);

create table ci.cdc_raw_vault.sat_customer_details (
    customer_hk varchar,
    customer_type varchar,
    date_of_birth varchar,
    segment_code varchar,
    status_code varchar,
    home_branch_code varchar,
    updated_at varchar,
    effective_from timestamp(3) with time zone,
    source_lsn bigint,
    kafka_partition integer,
    kafka_offset bigint
);
insert into ci.cdc_raw_vault.sat_customer_details values (
    'CUSTOMER_HK_1',
    'INDIVIDUAL',
    '1990-01-01Z',
    'RETAIL',
    'ACTIVE',
    'IST001',
    '2026-01-02T09:00:00.000000Z',
    timestamp '2026-01-02 09:00:00 UTC',
    100,
    0,
    1
);

create table ci.cdc_raw_vault.sat_product_details (
    product_hk varchar,
    product_name varchar,
    product_type varchar,
    default_currency_code varchar,
    is_active varchar,
    updated_at varchar,
    effective_from timestamp(3) with time zone,
    source_lsn bigint,
    kafka_partition integer,
    kafka_offset bigint
);
insert into ci.cdc_raw_vault.sat_product_details values (
    'PRODUCT_HK_1',
    'Consumer Loan TRY',
    'CONSUMER_LOAN',
    'TRY',
    'true',
    '2026-01-01T09:00:00.000000Z',
    timestamp '2026-01-01 09:00:00 UTC',
    101,
    0,
    2
);

create table ci.cdc_raw_vault.sat_branch_details (
    branch_hk varchar,
    branch_name varchar,
    city varchar,
    is_active varchar,
    updated_at varchar,
    effective_from timestamp(3) with time zone,
    source_lsn bigint,
    kafka_partition integer,
    kafka_offset bigint
);
insert into ci.cdc_raw_vault.sat_branch_details values (
    'BRANCH_HK_1',
    'Istanbul Central Branch',
    'Istanbul',
    'true',
    '2026-01-01T09:00:00.000000Z',
    timestamp '2026-01-01 09:00:00 UTC',
    102,
    0,
    3
);

create table ci.cdc_raw_vault.sat_currency_details (
    currency_hk varchar,
    currency_name varchar,
    minor_unit varchar,
    is_active varchar,
    updated_at varchar,
    effective_from timestamp(3) with time zone,
    source_lsn bigint,
    kafka_partition integer,
    kafka_offset bigint
);
insert into ci.cdc_raw_vault.sat_currency_details values (
    'CURRENCY_HK_1',
    'Turkish Lira',
    '2',
    'true',
    '2026-01-01T09:00:00.000000Z',
    timestamp '2026-01-01 09:00:00 UTC',
    103,
    0,
    4
);

create table ci.cdc_raw_vault.sat_loan_application_details (
    loan_application_hk varchar,
    requested_amount varchar,
    term_months varchar,
    status_code varchar,
    applied_at varchar,
    decision_at varchar,
    updated_at varchar,
    effective_from timestamp(3) with time zone,
    source_lsn bigint,
    kafka_partition integer,
    kafka_offset bigint
);
insert into ci.cdc_raw_vault.sat_loan_application_details values (
    'APPLICATION_HK_1',
    '100000.00',
    '12',
    'APPROVED',
    '2026-01-03T09:00:00.000000Z',
    '2026-01-04T09:00:00.000000Z',
    '2026-01-04T09:00:00.000000Z',
    timestamp '2026-01-04 09:00:00 UTC',
    104,
    0,
    5
);

create table ci.cdc_raw_vault.sat_loan_details (
    loan_hk varchar,
    principal_amount varchar,
    annual_interest_rate varchar,
    term_months varchar,
    status_code varchar,
    disbursed_at varchar,
    maturity_date varchar,
    updated_at varchar,
    effective_from timestamp(3) with time zone,
    source_lsn bigint,
    kafka_partition integer,
    kafka_offset bigint
);
insert into ci.cdc_raw_vault.sat_loan_details values (
    'LOAN_HK_1',
    '100000.00',
    '0.250000',
    '12',
    'ACTIVE',
    '2026-01-05T09:00:00.000000Z',
    '2027-01-05Z',
    '2026-01-05T09:00:00.000000Z',
    timestamp '2026-01-05 09:00:00 UTC',
    105,
    0,
    6
);
