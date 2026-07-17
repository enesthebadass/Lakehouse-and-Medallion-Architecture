select
    customer_hk,
    currency_hk,
    count(*) as row_count
from {{ ref('agg_customer_loan_portfolio') }}
group by customer_hk, currency_hk
having count(*) > 1
