# dbt Transformation Project

This project transforms the registered CDC Raw Vault tables through Trino. It builds
current customer, product, branch, and currency dimensions; loan application and loan
facts; and a currency-safe customer loan portfolio aggregate. The customer dimension
excludes raw direct identifiers and derives an analytics-safe age band.

Run the technical catalog bootstrap before dbt so that `gold_dbt` has an S3A location:

```bash
docker compose run --rm trino-init
```

Then validate and execute the project:

```bash
docker compose run --rm dbt debug
docker compose run --rm dbt compile
docker compose run --rm dbt run
docker compose run --rm dbt test
```

Validated locally with dbt Core `1.10.22`, `dbt-trino` `1.10.2`, and Trino `482`:

- Seven Gold models build as Delta tables: four dimensions, two facts, and one aggregate.
- The current dataset produces 100 customers, 40 applications, and 28 loans.
- 145 source, model, relationship, and business-rule tests pass without warnings.
- Consecutive `dbt run` executions recreate the model successfully.

The `cdc_timestamp` and `cdc_date` macros normalize Debezium ISO-8601 values before
business calculations. Power BI relationships, measures, and fallbacks are documented
in `../bi/README.md`.

Gold table models set `on_table_exists: drop`. The dbt-trino default uses relation
renames during replacement, which can leave a renamed Delta relation pointing at an
invalid table location in this local catalog. Drop-and-create is deterministic here;
incremental production marts should use an explicitly designed merge strategy.

The local profile uses unauthenticated HTTP because the local Trino coordinator is
not exposed as a production service. Production must use TLS, an identity provider,
least-privilege service credentials, and governed catalog permissions.
