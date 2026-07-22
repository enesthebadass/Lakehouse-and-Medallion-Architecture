with expected_gold_rows as (
    select 'dim_customer_current' as gold_model, customer_id as gold_business_key
    from {{ ref('dim_customer_current') }}

    union all

    select 'dim_product_current', product_code
    from {{ ref('dim_product_current') }}

    union all

    select 'dim_branch_current', branch_code
    from {{ ref('dim_branch_current') }}

    union all

    select 'dim_currency_current', currency_code
    from {{ ref('dim_currency_current') }}

    union all

    select 'fct_loan_applications_current', application_id
    from {{ ref('fct_loan_applications_current') }}

    union all

    select 'fct_loans_current', loan_id
    from {{ ref('fct_loans_current') }}

    union all

    select
        'agg_customer_loan_portfolio',
        customer_id || '|' || currency_code
    from {{ ref('agg_customer_loan_portfolio') }}
),

lineage_keys as (
    select distinct gold_model, gold_business_key
    from {{ ref('gold_row_lineage') }}
)

select expected.gold_model, expected.gold_business_key
from expected_gold_rows as expected
left join lineage_keys as lineage
    on expected.gold_model = lineage.gold_model
    and expected.gold_business_key = lineage.gold_business_key
where lineage.gold_business_key is null
