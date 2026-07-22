with ranked_application_details as (
    select
        loan_application_hk,
        requested_amount,
        term_months,
        status_code,
        applied_at,
        decision_at,
        updated_at,
        effective_from,
        load_datetime,
        source_lsn,
        kafka_partition,
        kafka_offset,
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

current_application_details as (
    select
        loan_application_hk,
        try_cast(nullif(trim(requested_amount), '') as decimal(18, 2)) as requested_amount,
        try_cast(nullif(trim(term_months), '') as integer) as term_months,
        status_code,
        {{ cdc_timestamp('applied_at') }} as applied_at,
        {{ cdc_timestamp('decision_at') }} as decision_at,
        {{ cdc_timestamp('updated_at') }} as source_updated_at,
        effective_from as source_effective_from,
        load_datetime as source_load_datetime
    from ranked_application_details
    where detail_rank = 1
)

select
    hub.loan_application_hk,
    hub.loan_application_bk as application_id,
    context.application_context_hk,
    customer.customer_hk,
    customer.customer_id,
    product.product_hk,
    product.product_code,
    branch.branch_hk,
    branch.branch_code,
    currency.currency_hk,
    currency.currency_code,
    details.requested_amount,
    details.term_months,
    details.status_code as application_status_code,
    details.applied_at,
    details.decision_at,
    case
        when details.decision_at is not null
            then date_diff('hour', details.applied_at, details.decision_at)
    end as decision_duration_hours,
    details.status_code in ('APPROVED', 'REJECTED') as is_decided,
    details.status_code = 'APPROVED' as is_approved,
    details.source_updated_at,
    details.source_effective_from,
    details.source_load_datetime,
    context.effective_from as context_effective_from,
    context.load_datetime as context_load_datetime,
    current_timestamp as dbt_loaded_at
from {{ source('cdc_raw_vault', 'hub_loan_application') }} as hub
inner join {{ ref('int_current_entity_status') }} as entity_status
    on hub.loan_application_hk = entity_status.entity_hk
    and entity_status.entity_type = 'LOAN_APPLICATION'
inner join current_application_details as details
    on hub.loan_application_hk = details.loan_application_hk
inner join {{ ref('int_current_application_context') }} as context
    on hub.loan_application_hk = context.loan_application_hk
inner join {{ ref('dim_customer_current') }} as customer
    on context.customer_hk = customer.customer_hk
inner join {{ ref('dim_product_current') }} as product
    on context.product_hk = product.product_hk
inner join {{ ref('dim_branch_current') }} as branch
    on context.branch_hk = branch.branch_hk
inner join {{ ref('dim_currency_current') }} as currency
    on context.currency_hk = currency.currency_hk
