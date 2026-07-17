select *
from {{ ref('fct_loan_applications_current') }}
where
    requested_amount <= 0
    or term_months <= 0
    or decision_at < applied_at
    or is_decided <> (application_status_code in ('APPROVED', 'REJECTED'))
    or is_approved <> (application_status_code = 'APPROVED')
    or (application_status_code in ('APPROVED', 'REJECTED') and decision_at is null)
    or (application_status_code = 'PENDING' and decision_at is not null)
