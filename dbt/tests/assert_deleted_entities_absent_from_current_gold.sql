with ranked_status as (
    select
        entity_type,
        entity_hk,
        record_status,
        row_number() over (
            partition by entity_type, entity_hk
            order by
                source_lsn desc nulls last,
                effective_from desc nulls last,
                kafka_partition desc,
                kafka_offset desc
        ) as status_rank
    from {{ source('cdc_raw_vault', 'sat_entity_record_status') }}
),

deleted_entities as (
    select entity_type, entity_hk
    from ranked_status
    where status_rank = 1 and record_status = 'DELETED'
),

current_entities as (
    select 'CUSTOMER' as entity_type, customer_hk as entity_hk
    from {{ ref('dim_customer_current') }}
    union all
    select 'PRODUCT', product_hk from {{ ref('dim_product_current') }}
    union all
    select 'BRANCH', branch_hk from {{ ref('dim_branch_current') }}
    union all
    select 'CURRENCY', currency_hk from {{ ref('dim_currency_current') }}
    union all
    select 'LOAN_APPLICATION', loan_application_hk
    from {{ ref('fct_loan_applications_current') }}
    union all
    select 'LOAN', loan_hk from {{ ref('fct_loans_current') }}
)

select current_entities.*
from current_entities
inner join deleted_entities
    on current_entities.entity_type = deleted_entities.entity_type
    and current_entities.entity_hk = deleted_entities.entity_hk
