with fact_totals as (
    select
        currency_hk,
        count(*) as loan_count,
        sum(principal_amount) as principal_amount
    from {{ ref('fct_loans_current') }}
    group by currency_hk
),

portfolio_totals as (
    select
        currency_hk,
        sum(loan_count) as loan_count,
        sum(total_principal_amount) as principal_amount
    from {{ ref('agg_customer_loan_portfolio') }}
    group by currency_hk
)

select
    coalesce(fact.currency_hk, portfolio.currency_hk) as currency_hk,
    fact.loan_count as fact_loan_count,
    portfolio.loan_count as portfolio_loan_count,
    fact.principal_amount as fact_principal_amount,
    portfolio.principal_amount as portfolio_principal_amount
from fact_totals as fact
full outer join portfolio_totals as portfolio
    on fact.currency_hk = portfolio.currency_hk
where
    fact.currency_hk is null
    or portfolio.currency_hk is null
    or fact.loan_count <> portfolio.loan_count
    or fact.principal_amount <> portfolio.principal_amount
