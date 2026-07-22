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
    kafka_offset bigint,
    load_datetime timestamp(3) with time zone,
    source_event_id varchar,
    source_position varchar,
    record_source varchar,
    bronze_object_key varchar,
    kafka_topic varchar,
    load_batch_id varchar
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
    1,
    timestamp '2026-01-02 09:00:01 UTC',
    'EVENT_CUSTOMER_1', 'customers:0:1', 'core_banking.mms.customers',
    'bronze/customers/1.json', 'customers', 'ci-batch'
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
    kafka_offset bigint,
    load_datetime timestamp(3) with time zone,
    source_event_id varchar,
    source_position varchar,
    record_source varchar,
    bronze_object_key varchar,
    kafka_topic varchar,
    load_batch_id varchar
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
    2,
    timestamp '2026-01-01 09:00:01 UTC',
    'EVENT_PRODUCT_1', 'products:0:2', 'core_banking.prm.products',
    'bronze/products/2.json', 'products', 'ci-batch'
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
    kafka_offset bigint,
    load_datetime timestamp(3) with time zone,
    source_event_id varchar,
    source_position varchar,
    record_source varchar,
    bronze_object_key varchar,
    kafka_topic varchar,
    load_batch_id varchar
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
    3,
    timestamp '2026-01-01 09:00:01 UTC',
    'EVENT_BRANCH_1', 'branches:0:3', 'core_banking.prm.branches',
    'bronze/branches/3.json', 'branches', 'ci-batch'
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
    kafka_offset bigint,
    load_datetime timestamp(3) with time zone,
    source_event_id varchar,
    source_position varchar,
    record_source varchar,
    bronze_object_key varchar,
    kafka_topic varchar,
    load_batch_id varchar
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
    4,
    timestamp '2026-01-01 09:00:01 UTC',
    'EVENT_CURRENCY_1', 'currencies:0:4', 'core_banking.prm.currencies',
    'bronze/currencies/4.json', 'currencies', 'ci-batch'
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
    kafka_offset bigint,
    load_datetime timestamp(3) with time zone,
    source_event_id varchar,
    source_position varchar,
    record_source varchar,
    bronze_object_key varchar,
    kafka_topic varchar,
    load_batch_id varchar
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
    5,
    timestamp '2026-01-04 09:00:01 UTC',
    'EVENT_APPLICATION_1', 'applications:0:5',
    'core_banking.krd.loan_applications', 'bronze/applications/5.json',
    'applications', 'ci-batch'
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
    kafka_offset bigint,
    load_datetime timestamp(3) with time zone,
    source_event_id varchar,
    source_position varchar,
    record_source varchar,
    bronze_object_key varchar,
    kafka_topic varchar,
    load_batch_id varchar
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
    6,
    timestamp '2026-01-05 09:00:01 UTC',
    'EVENT_LOAN_1', 'loans:0:6', 'core_banking.krd.loans',
    'bronze/loans/6.json', 'loans', 'ci-batch'
);

create table ci.cdc_raw_vault.sat_entity_record_status (
    entity_type varchar,
    entity_hk varchar,
    entity_business_key varchar,
    record_status varchar,
    is_deleted boolean,
    source_event_id varchar,
    effective_from timestamp(3) with time zone,
    load_datetime timestamp(3) with time zone,
    source_lsn bigint,
    kafka_partition integer,
    kafka_offset bigint
);
insert into ci.cdc_raw_vault.sat_entity_record_status values
    ('CUSTOMER', 'CUSTOMER_HK_1', 'C000001', 'ACTIVE', false, 'STATUS_CUSTOMER_1', timestamp '2026-01-02 09:00:00 UTC', timestamp '2026-01-02 09:00:01 UTC', 100, 0, 1),
    ('LOAN_APPLICATION', 'APPLICATION_HK_1', 'A000001', 'ACTIVE', false, 'STATUS_APPLICATION_1', timestamp '2026-01-03 09:00:00 UTC', timestamp '2026-01-03 09:00:01 UTC', 104, 0, 5),
    ('LOAN', 'LOAN_HK_1', 'L000001', 'ACTIVE', false, 'STATUS_LOAN_1', timestamp '2026-01-05 09:00:00 UTC', timestamp '2026-01-05 09:00:01 UTC', 105, 0, 6),
    ('PRODUCT', 'PRODUCT_HK_1', 'CONSUMER_TRY', 'ACTIVE', false, 'STATUS_PRODUCT_1', timestamp '2026-01-01 09:00:00 UTC', timestamp '2026-01-01 09:00:01 UTC', 101, 0, 2),
    ('BRANCH', 'BRANCH_HK_1', 'IST001', 'ACTIVE', false, 'STATUS_BRANCH_1', timestamp '2026-01-01 09:00:00 UTC', timestamp '2026-01-01 09:00:01 UTC', 102, 0, 3),
    ('CURRENCY', 'CURRENCY_HK_1', 'TRY', 'ACTIVE', false, 'STATUS_CURRENCY_1', timestamp '2026-01-01 09:00:00 UTC', timestamp '2026-01-01 09:00:01 UTC', 103, 0, 4);

create table ci.cdc_raw_vault.sat_application_context_effectivity (
    loan_application_hk varchar,
    application_context_hk varchar,
    customer_hk varchar,
    product_hk varchar,
    branch_hk varchar,
    currency_hk varchar,
    record_status varchar,
    is_deleted boolean,
    source_event_id varchar,
    source_position varchar,
    record_source varchar,
    bronze_object_key varchar,
    kafka_topic varchar,
    kafka_partition integer,
    kafka_offset bigint,
    source_lsn bigint,
    load_batch_id varchar,
    effective_from timestamp(3) with time zone,
    load_datetime timestamp(3) with time zone
);
insert into ci.cdc_raw_vault.sat_application_context_effectivity values (
    'APPLICATION_HK_1', 'APPLICATION_CONTEXT_HK_1', 'CUSTOMER_HK_1',
    'PRODUCT_HK_1', 'BRANCH_HK_1', 'CURRENCY_HK_1', 'ACTIVE', false,
    'APPLICATION_CONTEXT_EVENT_1', 'applications:0:5',
    'core_banking.krd.loan_applications', 'bronze/applications/5.json',
    'applications', 0, 5, 104, 'ci-batch',
    timestamp '2026-01-04 09:00:00 UTC', timestamp '2026-01-04 09:00:01 UTC'
);

create table ci.cdc_raw_vault.sat_loan_context_effectivity (
    loan_hk varchar,
    loan_context_hk varchar,
    loan_application_hk varchar,
    customer_hk varchar,
    product_hk varchar,
    branch_hk varchar,
    currency_hk varchar,
    record_status varchar,
    is_deleted boolean,
    source_event_id varchar,
    source_position varchar,
    record_source varchar,
    bronze_object_key varchar,
    kafka_topic varchar,
    kafka_partition integer,
    kafka_offset bigint,
    source_lsn bigint,
    load_batch_id varchar,
    effective_from timestamp(3) with time zone,
    load_datetime timestamp(3) with time zone
);
insert into ci.cdc_raw_vault.sat_loan_context_effectivity values (
    'LOAN_HK_1', 'LOAN_CONTEXT_HK_1', 'APPLICATION_HK_1', 'CUSTOMER_HK_1',
    'PRODUCT_HK_1', 'BRANCH_HK_1', 'CURRENCY_HK_1', 'ACTIVE', false,
    'LOAN_CONTEXT_EVENT_1', 'loans:0:6', 'core_banking.krd.loans',
    'bronze/loans/6.json', 'loans', 0, 6, 105, 'ci-batch',
    timestamp '2026-01-05 09:00:00 UTC', timestamp '2026-01-05 09:00:01 UTC'
);
