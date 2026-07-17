-- 1. Application funnel. Never combine requested amounts across currencies.
select
    currency_code,
    application_status_code,
    count(*) as application_count,
    sum(requested_amount) as requested_amount,
    round(100.0 * count(*) / sum(count(*)) over (partition by currency_code), 2) as status_share_pct
from lakehouse.gold_dbt.fct_loan_applications_current
group by currency_code, application_status_code
order by currency_code, application_status_code;

-- 2. Product approval performance.
select
    applications.product_code,
    products.product_name,
    applications.currency_code,
    count(*) as application_count,
    count_if(applications.is_approved) as approved_application_count,
    round(100.0 * count_if(applications.is_approved) / nullif(count(*), 0), 2) as approval_rate_pct,
    sum(applications.requested_amount) as requested_amount
from lakehouse.gold_dbt.fct_loan_applications_current as applications
inner join lakehouse.gold_dbt.dim_product_current as products
    on applications.product_hk = products.product_hk
group by applications.product_code, products.product_name, applications.currency_code
order by applications.currency_code, applications.product_code;

-- 3. Current principal exposure and delinquency by product.
select
    loans.product_code,
    products.product_name,
    loans.currency_code,
    count(*) as loan_count,
    count_if(loans.is_delinquent) as delinquent_loan_count,
    round(100.0 * count_if(loans.is_delinquent) / nullif(count(*), 0), 2) as delinquency_rate_pct,
    sum(loans.principal_amount) as principal_amount
from lakehouse.gold_dbt.fct_loans_current as loans
inner join lakehouse.gold_dbt.dim_product_current as products
    on loans.product_hk = products.product_hk
group by loans.product_code, products.product_name, loans.currency_code
order by loans.currency_code, loans.product_code;

-- 4. Customer-segment portfolio. Currency remains part of the reporting grain.
select
    customers.customer_type,
    customers.segment_code,
    loans.currency_code,
    count(distinct loans.customer_hk) as customer_count,
    count(*) as loan_count,
    count_if(loans.is_delinquent) as delinquent_loan_count,
    sum(loans.principal_amount) as principal_amount
from lakehouse.gold_dbt.fct_loans_current as loans
inner join lakehouse.gold_dbt.dim_customer_current as customers
    on loans.customer_hk = customers.customer_hk
group by customers.customer_type, customers.segment_code, loans.currency_code
order by loans.currency_code, customers.customer_type, customers.segment_code;

-- 5. Branch lending performance.
select
    branches.branch_code,
    branches.branch_name,
    branches.city,
    loans.currency_code,
    count(*) as loan_count,
    count_if(loans.is_delinquent) as delinquent_loan_count,
    sum(loans.principal_amount) as principal_amount
from lakehouse.gold_dbt.fct_loans_current as loans
inner join lakehouse.gold_dbt.dim_branch_current as branches
    on loans.branch_hk = branches.branch_hk
group by branches.branch_code, branches.branch_name, branches.city, loans.currency_code
order by loans.currency_code, branches.branch_code;

-- 6. Decision turnaround. Pending applications are excluded from duration metrics.
select
    product_code,
    branch_code,
    count_if(is_decided) as decided_application_count,
    avg(decision_duration_hours) filter (where is_decided) as average_decision_hours,
    approx_percentile(decision_duration_hours, 0.50) filter (where is_decided) as median_decision_hours,
    approx_percentile(decision_duration_hours, 0.95) filter (where is_decided) as p95_decision_hours
from lakehouse.gold_dbt.fct_loan_applications_current
group by product_code, branch_code
order by product_code, branch_code;
