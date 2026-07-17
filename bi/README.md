# Power BI Report Server Consumption

This directory contains the tested SQL and controlled CSV fallback for the dbt Gold
lending mart. It does not contain a `.pbix`; the final report must be authored with
the same Power BI Desktop version that is compatible with the company's Power BI
Report Server.

## Storage Mode Decision

Use **Import mode through an organization-approved Trino-compatible ODBC driver**.
Power BI Report Server supports cached ODBC data and scheduled refresh, but does not
support generic ODBC as a DirectQuery source. Microsoft documents the current support
matrix here:

- <https://learn.microsoft.com/power-bi/report-server/data-sources>
- <https://learn.microsoft.com/power-bi/report-server/scheduled-refresh>

The open-source Trino project maintains JDBC and several language clients, but does
not maintain an ODBC driver. Driver selection, licensing, x64 compatibility, support,
and Power BI Report Server certification therefore require an institutional decision:

- <https://trino.io/docs/current/client.html>

Do not select DirectQuery in ordinary Power BI Desktop and assume that it will remain
supported after publishing to Power BI Report Server. For this target, Import is the
explicit decision.

## Gold Semantic Surface

Run and test dbt first:

```bash
docker compose run --rm dbt run
docker compose run --rm dbt test
```

Import these tables from catalog `lakehouse`, schema `gold_dbt`:

| Table | Grain | Purpose |
|---|---|---|
| `dim_customer_current` | one customer | PII-minimized customer segmentation |
| `dim_product_current` | one lending product | product labels and type |
| `dim_branch_current` | one branch | branch and city analysis |
| `dim_currency_current` | one currency | mandatory financial-measure context |
| `fct_loan_applications_current` | one application | funnel, approval, amount, decision duration |
| `fct_loans_current` | one disbursed loan | principal exposure and delinquency status |
| `agg_customer_loan_portfolio` | customer and currency | dashboard-ready portfolio summary |

`principal_amount` is original principal exposure, not current outstanding balance.
Outstanding balance requires the installment Satellite, which is not implemented in
the current Raw Vault. Never sum financial amounts across currencies without a
currency filter or grouping.

## Local ODBC Connection

1. Install an approved **64-bit** Trino-compatible ODBC driver on the Windows machine.
2. Open **ODBC Data Sources (64-bit)** and create a System DSN named
   `TrinoLakehouse`.
3. Use host `localhost`, port `8082`, catalog `lakehouse`, schema `gold_dbt`, and HTTP
   for this local demo only.
4. Confirm that <http://localhost:8082/v1/info> responds from Windows.
5. Open Power BI Desktop optimized for Power BI Report Server.
6. Select **Get Data > ODBC**, choose `TrinoLakehouse`, and select **Import**.
7. Load the seven Gold tables listed above.

If the driver's navigator does not expose catalogs correctly, use a static Power Query:

```powerquery
let
    Source = Odbc.Query(
        "dsn=TrinoLakehouse",
        "SELECT * FROM lakehouse.gold_dbt.fct_loans_current"
    )
in
    Source
```

The local Trino coordinator has no authentication or TLS. That setting is only for
development. Production requires TLS certificate validation, identity integration,
least-privilege service credentials, and policy enforcement.

## Relationships

Create single-direction, one-to-many relationships from dimensions to facts:

| One side | Many side | Cardinality |
|---|---|---|
| `dim_customer_current.customer_hk` | `fct_loan_applications_current.customer_hk` | 1:* |
| `dim_customer_current.customer_hk` | `fct_loans_current.customer_hk` | 1:* |
| `dim_product_current.product_hk` | both facts' `product_hk` | 1:* |
| `dim_branch_current.branch_hk` | both facts' `branch_hk` | 1:* |
| `dim_currency_current.currency_hk` | both facts' `currency_hk` | 1:* |

Use the same customer and currency dimensions for `agg_customer_loan_portfolio` only
when that aggregate is included in the semantic model. Do not create a bidirectional
fact-to-fact relationship; it introduces ambiguous filter paths.

## Measures

Create these base measures and format rates as percentages:

```dax
Application Count = COUNTROWS(fct_loan_applications_current)

Approved Application Count =
CALCULATE([Application Count], fct_loan_applications_current[is_approved] = TRUE())

Approval Rate = DIVIDE([Approved Application Count], [Application Count])

Requested Amount = SUM(fct_loan_applications_current[requested_amount])

Average Decision Hours =
AVERAGE(fct_loan_applications_current[decision_duration_hours])

Loan Count = COUNTROWS(fct_loans_current)

Delinquent Loan Count =
CALCULATE([Loan Count], fct_loans_current[is_delinquent] = TRUE())

Delinquency Rate = DIVIDE([Delinquent Loan Count], [Loan Count])

Principal Amount = SUM(fct_loans_current[principal_amount])
```

## Report Pages

### Lending Overview

- Mandatory slicer: `dim_currency_current.currency_code`
- KPI cards: Application Count, Approval Rate, Loan Count, Delinquency Rate
- Clustered column chart: Requested Amount by product name
- Stacked bar chart: Application Count by application status
- Column chart: Principal Amount by branch name
- Matrix: customer segment, Loan Count, Principal Amount, Delinquency Rate

### Credit Risk

- KPI cards: Principal Amount, Delinquent Loan Count, Delinquency Rate
- Bar chart: Delinquency Rate by product name
- Bar chart: Delinquency Rate by branch name
- Matrix: customer segment and currency with principal and delinquent loan count
- Detail table: loan ID, customer ID, product, branch, status, principal, maturity date

### Application Operations

- KPI cards: Application Count, Approval Rate, Average Decision Hours
- Funnel chart: application status and Application Count
- Bar chart: Approval Rate by product name
- Bar chart: Average Decision Hours by branch name
- Detail table: application ID, applied date, decision date, status, requested amount

The executable examples in `sql/kpi_queries.sql` provide the expected backend
aggregations for validating the report measures.

## Publish and Refresh

After publishing the `.pbix` to Power BI Report Server:

1. Install the same x64 ODBC driver on the report-server host.
2. Create the same System DSN on that host; a user DSN is insufficient for the
   report-server service account.
3. Open the report's **Manage > Data sources** page and set credentials.
4. Create a scheduled refresh plan and run it once manually.
5. Compare report totals with `sql/kpi_queries.sql`.

This server-side refresh is not yet certified by the repository alone. It becomes
verified only after the driver, DSN, credentials, refresh history, and report totals
have all been checked on the actual Power BI Report Server host.

## Controlled CSV Fallback

When no approved ODBC driver is available, export all Gold tables with row counts and
SHA-256 checksums:

```bash
./bi/export_powerbi_csv.sh
```

The output is written under ignored path `exports/powerbi/`. Load the files with
**Get Data > Text/CSV** for a local demo. For Power BI Report Server scheduled refresh,
place the export on a network share reachable by the report-server service account;
Microsoft does not support scheduled refresh from a local desktop file path.

For a production fallback, replicate the governed Gold tables to SQL Server and use
the native SQL Server connector. Power BI Report Server supports both Import and
DirectQuery for SQL Server, unlike generic ODBC DirectQuery.
