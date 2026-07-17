select *
from {{ ref('fct_loans_current') }}
where
    principal_amount <= 0
    or annual_interest_rate < 0
    or term_months <= 0
    or disbursed_at is null
    or maturity_date < cast(disbursed_at as date)
    or is_active <> (loan_status_code = 'ACTIVE')
    or is_delinquent <> (loan_status_code = 'DELINQUENT')
