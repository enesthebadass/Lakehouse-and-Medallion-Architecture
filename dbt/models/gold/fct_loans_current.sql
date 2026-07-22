with ranked_loan_details as (
    select
        loan_hk,
        principal_amount,
        annual_interest_rate,
        term_months,
        status_code,
        disbursed_at,
        maturity_date,
        updated_at,
        effective_from,
        load_datetime,
        source_lsn,
        kafka_partition,
        kafka_offset,
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

current_loan_details as (
    select
        loan_hk,
        try_cast(nullif(trim(principal_amount), '') as decimal(18, 2)) as principal_amount,
        try_cast(nullif(trim(annual_interest_rate), '') as decimal(9, 6)) as annual_interest_rate,
        try_cast(nullif(trim(term_months), '') as integer) as term_months,
        status_code,
        {{ cdc_timestamp('disbursed_at') }} as disbursed_at,
        {{ cdc_date('maturity_date') }} as maturity_date,
        {{ cdc_timestamp('updated_at') }} as source_updated_at,
        effective_from as source_effective_from,
        load_datetime as source_load_datetime
    from ranked_loan_details
    where detail_rank = 1
)

select
    hub.loan_hk,
    hub.loan_bk as loan_id,
    context.loan_context_hk,
    application.loan_application_hk,
    application.loan_application_bk as application_id,
    customer.customer_hk,
    customer.customer_id,
    product.product_hk,
    product.product_code,
    branch.branch_hk,
    branch.branch_code,
    currency.currency_hk,
    currency.currency_code,
    details.principal_amount,
    details.annual_interest_rate,
    details.term_months,
    details.status_code as loan_status_code,
    details.disbursed_at,
    details.maturity_date,
    details.status_code = 'ACTIVE' as is_active,
    details.status_code = 'DELINQUENT' as is_delinquent,
    details.source_updated_at,
    details.source_effective_from,
    details.source_load_datetime,
    context.effective_from as context_effective_from,
    context.load_datetime as context_load_datetime,
    current_timestamp as dbt_loaded_at
from {{ source('cdc_raw_vault', 'hub_loan') }} as hub
inner join {{ ref('int_current_entity_status') }} as entity_status
    on hub.loan_hk = entity_status.entity_hk
    and entity_status.entity_type = 'LOAN'
inner join current_loan_details as details
    on hub.loan_hk = details.loan_hk
inner join {{ ref('int_current_loan_context') }} as context
    on hub.loan_hk = context.loan_hk
inner join {{ source('cdc_raw_vault', 'hub_loan_application') }} as application
    on context.loan_application_hk = application.loan_application_hk
inner join {{ ref('dim_customer_current') }} as customer
    on context.customer_hk = customer.customer_hk
inner join {{ ref('dim_product_current') }} as product
    on context.product_hk = product.product_hk
inner join {{ ref('dim_branch_current') }} as branch
    on context.branch_hk = branch.branch_hk
inner join {{ ref('dim_currency_current') }} as currency
    on context.currency_hk = currency.currency_hk
