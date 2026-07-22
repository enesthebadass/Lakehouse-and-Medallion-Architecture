with current_entity_status as (
    select * from {{ ref('int_current_entity_status') }}
),

latest_customer_satellite as (
    select
        *,
        row_number() over (
            partition by customer_hk
            order by
                effective_from desc nulls last,
                source_lsn desc nulls last,
                kafka_partition desc,
                kafka_offset desc
        ) as detail_rank
    from {{ source('cdc_raw_vault', 'sat_customer_details') }}
),

latest_product_satellite as (
    select
        *,
        row_number() over (
            partition by product_hk
            order by
                effective_from desc nulls last,
                source_lsn desc nulls last,
                kafka_partition desc,
                kafka_offset desc
        ) as detail_rank
    from {{ source('cdc_raw_vault', 'sat_product_details') }}
),

latest_branch_satellite as (
    select
        *,
        row_number() over (
            partition by branch_hk
            order by
                effective_from desc nulls last,
                source_lsn desc nulls last,
                kafka_partition desc,
                kafka_offset desc
        ) as detail_rank
    from {{ source('cdc_raw_vault', 'sat_branch_details') }}
),

latest_currency_satellite as (
    select
        *,
        row_number() over (
            partition by currency_hk
            order by
                effective_from desc nulls last,
                source_lsn desc nulls last,
                kafka_partition desc,
                kafka_offset desc
        ) as detail_rank
    from {{ source('cdc_raw_vault', 'sat_currency_details') }}
),

latest_application_satellite as (
    select
        *,
        row_number() over (
            partition by loan_application_hk
            order by
                effective_from desc nulls last,
                source_lsn desc nulls last,
                kafka_partition desc,
                kafka_offset desc
        ) as detail_rank
    from {{ source('cdc_raw_vault', 'sat_loan_application_details') }}
),

latest_loan_satellite as (
    select
        *,
        row_number() over (
            partition by loan_hk
            order by
                effective_from desc nulls last,
                source_lsn desc nulls last,
                kafka_partition desc,
                kafka_offset desc
        ) as detail_rank
    from {{ source('cdc_raw_vault', 'sat_loan_details') }}
),

lineage as (
    select
        'dim_customer_current' as gold_model,
        hub.customer_bk as gold_business_key,
        'current_attributes' as lineage_role,
        'sat_customer_details' as raw_vault_object,
        sat.source_event_id,
        sat.source_position,
        sat.record_source,
        sat.bronze_object_key,
        sat.kafka_topic,
        sat.kafka_partition,
        sat.kafka_offset,
        sat.source_lsn,
        sat.load_batch_id,
        sat.effective_from as source_effective_from
    from {{ source('cdc_raw_vault', 'hub_customer') }} as hub
    inner join latest_customer_satellite as sat
        on hub.customer_hk = sat.customer_hk
        and sat.detail_rank = 1
    inner join current_entity_status as entity_status
        on hub.customer_hk = entity_status.entity_hk
        and entity_status.entity_type = 'CUSTOMER'

    union all

    select
        'dim_product_current',
        hub.product_bk,
        'current_attributes',
        'sat_product_details',
        sat.source_event_id,
        sat.source_position,
        sat.record_source,
        sat.bronze_object_key,
        sat.kafka_topic,
        sat.kafka_partition,
        sat.kafka_offset,
        sat.source_lsn,
        sat.load_batch_id,
        sat.effective_from
    from {{ source('cdc_raw_vault', 'hub_product') }} as hub
    inner join latest_product_satellite as sat
        on hub.product_hk = sat.product_hk
        and sat.detail_rank = 1
    inner join current_entity_status as entity_status
        on hub.product_hk = entity_status.entity_hk
        and entity_status.entity_type = 'PRODUCT'

    union all

    select
        'dim_branch_current',
        hub.branch_bk,
        'current_attributes',
        'sat_branch_details',
        sat.source_event_id,
        sat.source_position,
        sat.record_source,
        sat.bronze_object_key,
        sat.kafka_topic,
        sat.kafka_partition,
        sat.kafka_offset,
        sat.source_lsn,
        sat.load_batch_id,
        sat.effective_from
    from {{ source('cdc_raw_vault', 'hub_branch') }} as hub
    inner join latest_branch_satellite as sat
        on hub.branch_hk = sat.branch_hk
        and sat.detail_rank = 1
    inner join current_entity_status as entity_status
        on hub.branch_hk = entity_status.entity_hk
        and entity_status.entity_type = 'BRANCH'

    union all

    select
        'dim_currency_current',
        hub.currency_bk,
        'current_attributes',
        'sat_currency_details',
        sat.source_event_id,
        sat.source_position,
        sat.record_source,
        sat.bronze_object_key,
        sat.kafka_topic,
        sat.kafka_partition,
        sat.kafka_offset,
        sat.source_lsn,
        sat.load_batch_id,
        sat.effective_from
    from {{ source('cdc_raw_vault', 'hub_currency') }} as hub
    inner join latest_currency_satellite as sat
        on hub.currency_hk = sat.currency_hk
        and sat.detail_rank = 1
    inner join current_entity_status as entity_status
        on hub.currency_hk = entity_status.entity_hk
        and entity_status.entity_type = 'CURRENCY'

    union all

    select
        'fct_loan_applications_current',
        hub.loan_application_bk,
        'current_attributes',
        'sat_loan_application_details',
        sat.source_event_id,
        sat.source_position,
        sat.record_source,
        sat.bronze_object_key,
        sat.kafka_topic,
        sat.kafka_partition,
        sat.kafka_offset,
        sat.source_lsn,
        sat.load_batch_id,
        sat.effective_from
    from {{ source('cdc_raw_vault', 'hub_loan_application') }} as hub
    inner join latest_application_satellite as sat
        on hub.loan_application_hk = sat.loan_application_hk
        and sat.detail_rank = 1
    inner join current_entity_status as entity_status
        on hub.loan_application_hk = entity_status.entity_hk
        and entity_status.entity_type = 'LOAN_APPLICATION'

    union all

    select
        'fct_loan_applications_current',
        hub.loan_application_bk,
        'business_context',
        'sat_application_context_effectivity',
        context.source_event_id,
        context.source_position,
        context.record_source,
        context.bronze_object_key,
        context.kafka_topic,
        context.kafka_partition,
        context.kafka_offset,
        context.source_lsn,
        context.load_batch_id,
        context.effective_from
    from {{ source('cdc_raw_vault', 'hub_loan_application') }} as hub
    inner join {{ ref('int_current_application_context') }} as context
        on hub.loan_application_hk = context.loan_application_hk

    union all

    select
        'fct_loans_current',
        hub.loan_bk,
        'current_attributes',
        'sat_loan_details',
        sat.source_event_id,
        sat.source_position,
        sat.record_source,
        sat.bronze_object_key,
        sat.kafka_topic,
        sat.kafka_partition,
        sat.kafka_offset,
        sat.source_lsn,
        sat.load_batch_id,
        sat.effective_from
    from {{ source('cdc_raw_vault', 'hub_loan') }} as hub
    inner join latest_loan_satellite as sat
        on hub.loan_hk = sat.loan_hk
        and sat.detail_rank = 1
    inner join current_entity_status as entity_status
        on hub.loan_hk = entity_status.entity_hk
        and entity_status.entity_type = 'LOAN'

    union all

    select
        'fct_loans_current',
        hub.loan_bk,
        'business_context',
        'sat_loan_context_effectivity',
        context.source_event_id,
        context.source_position,
        context.record_source,
        context.bronze_object_key,
        context.kafka_topic,
        context.kafka_partition,
        context.kafka_offset,
        context.source_lsn,
        context.load_batch_id,
        context.effective_from
    from {{ source('cdc_raw_vault', 'hub_loan') }} as hub
    inner join {{ ref('int_current_loan_context') }} as context
        on hub.loan_hk = context.loan_hk

    union all

    select
        'agg_customer_loan_portfolio',
        portfolio.customer_id || '|' || portfolio.currency_code,
        'contributing_loan',
        'sat_loan_details',
        sat.source_event_id,
        sat.source_position,
        sat.record_source,
        sat.bronze_object_key,
        sat.kafka_topic,
        sat.kafka_partition,
        sat.kafka_offset,
        sat.source_lsn,
        sat.load_batch_id,
        sat.effective_from
    from {{ ref('agg_customer_loan_portfolio') }} as portfolio
    inner join {{ ref('fct_loans_current') }} as loan
        on portfolio.customer_hk = loan.customer_hk
        and portfolio.currency_hk = loan.currency_hk
    inner join latest_loan_satellite as sat
        on loan.loan_hk = sat.loan_hk
        and sat.detail_rank = 1

    union all

    select
        'agg_customer_loan_portfolio',
        portfolio.customer_id || '|' || portfolio.currency_code,
        'customer_classification',
        'sat_customer_details',
        sat.source_event_id,
        sat.source_position,
        sat.record_source,
        sat.bronze_object_key,
        sat.kafka_topic,
        sat.kafka_partition,
        sat.kafka_offset,
        sat.source_lsn,
        sat.load_batch_id,
        sat.effective_from
    from {{ ref('agg_customer_loan_portfolio') }} as portfolio
    inner join latest_customer_satellite as sat
        on portfolio.customer_hk = sat.customer_hk
        and sat.detail_rank = 1
)

select *
from lineage
