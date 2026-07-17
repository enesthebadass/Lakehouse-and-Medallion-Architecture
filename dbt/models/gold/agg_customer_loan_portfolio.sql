select
    customer.customer_hk,
    customer.customer_id,
    customer.customer_type,
    customer.segment_code,
    loans.currency_hk,
    loans.currency_code,
    count(*) as loan_count,
    count_if(loans.loan_status_code = 'ACTIVE') as active_loan_count,
    count_if(loans.loan_status_code = 'DELINQUENT') as delinquent_loan_count,
    count_if(loans.loan_status_code = 'CLOSED') as closed_loan_count,
    sum(loans.principal_amount) as total_principal_amount,
    avg(loans.annual_interest_rate) as average_annual_interest_rate,
    min(loans.disbursed_at) as first_disbursed_at,
    max(loans.disbursed_at) as latest_disbursed_at,
    current_timestamp as dbt_loaded_at
from {{ ref('fct_loans_current') }} as loans
inner join {{ ref('dim_customer_current') }} as customer
    on loans.customer_hk = customer.customer_hk
group by
    customer.customer_hk,
    customer.customer_id,
    customer.customer_type,
    customer.segment_code,
    loans.currency_hk,
    loans.currency_code
