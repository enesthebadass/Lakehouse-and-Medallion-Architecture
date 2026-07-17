INSERT INTO prm.currencies (currency_code, currency_name, minor_unit)
VALUES
    ('TRY', 'Turkish Lira', 2),
    ('USD', 'US Dollar', 2),
    ('EUR', 'Euro', 2)
ON CONFLICT (currency_code) DO NOTHING;

INSERT INTO prm.branches (branch_code, branch_name, city)
VALUES
    ('IST001', 'Istanbul Corporate Branch', 'Istanbul'),
    ('ANK001', 'Ankara Central Branch', 'Ankara'),
    ('IZM001', 'Izmir Commercial Branch', 'Izmir')
ON CONFLICT (branch_code) DO NOTHING;

INSERT INTO prm.products (
    product_code,
    product_name,
    product_type,
    default_currency_code
)
VALUES
    ('CONSUMER_TRY', 'Consumer Loan TRY', 'CONSUMER_LOAN', 'TRY'),
    ('MORTGAGE_TRY', 'Mortgage Loan TRY', 'MORTGAGE', 'TRY'),
    ('SME_WORKING_TRY', 'SME Working Capital TRY', 'SME_LOAN', 'TRY')
ON CONFLICT (product_code) DO NOTHING;

INSERT INTO prm.status_codes (
    status_domain,
    status_code,
    status_name,
    is_terminal
)
VALUES
    ('CUSTOMER', 'ACTIVE', 'Active', FALSE),
    ('CUSTOMER', 'PASSIVE', 'Passive', TRUE),
    ('LOAN_APPLICATION', 'PENDING', 'Pending', FALSE),
    ('LOAN_APPLICATION', 'APPROVED', 'Approved', TRUE),
    ('LOAN_APPLICATION', 'REJECTED', 'Rejected', TRUE),
    ('LOAN', 'ACTIVE', 'Active', FALSE),
    ('LOAN', 'DELINQUENT', 'Delinquent', FALSE),
    ('LOAN', 'CLOSED', 'Closed', TRUE),
    ('INSTALLMENT', 'PENDING', 'Pending', FALSE),
    ('INSTALLMENT', 'PAID', 'Paid', TRUE),
    ('INSTALLMENT', 'OVERDUE', 'Overdue', FALSE),
    ('COLLATERAL', 'ACTIVE', 'Active', FALSE),
    ('COLLATERAL', 'RELEASED', 'Released', TRUE)
ON CONFLICT (status_domain, status_code) DO NOTHING;

INSERT INTO prm.rate_parameters (
    product_code,
    rate_type,
    annual_rate,
    effective_from
)
VALUES
    ('CONSUMER_TRY', 'BASE', 0.420000, DATE '2026-01-01'),
    ('MORTGAGE_TRY', 'BASE', 0.360000, DATE '2026-01-01'),
    ('SME_WORKING_TRY', 'BASE', 0.390000, DATE '2026-01-01')
ON CONFLICT (product_code, rate_type, effective_from) DO NOTHING;
