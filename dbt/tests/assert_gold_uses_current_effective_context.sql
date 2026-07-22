select
    'LOAN_APPLICATION' as entity_type,
    fact.loan_application_hk as entity_hk,
    fact.application_context_hk as gold_context_hk,
    context.application_context_hk as expected_context_hk
from {{ ref('fct_loan_applications_current') }} as fact
inner join {{ ref('int_current_application_context') }} as context
    on fact.loan_application_hk = context.loan_application_hk
where fact.application_context_hk <> context.application_context_hk

union all

select
    'LOAN',
    fact.loan_hk,
    fact.loan_context_hk,
    context.loan_context_hk
from {{ ref('fct_loans_current') }} as fact
inner join {{ ref('int_current_loan_context') }} as context
    on fact.loan_hk = context.loan_hk
where fact.loan_context_hk <> context.loan_context_hk
