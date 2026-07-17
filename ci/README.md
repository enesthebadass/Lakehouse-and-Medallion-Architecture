# Continuous Integration

The GitHub Actions workflow in `.github/workflows/ci.yml` separates correctness
gates from security baselines.

## Blocking gates

- Ruff lint and import-order checks
- Python byte-code compilation
- Bronze CDC writer unit tests
- Shell and YAML syntax checks
- Docker Compose configuration validation
- Airflow DAG import and task-graph tests
- dbt debug, compile, run, and test against an isolated Trino catalog

The dbt job starts a temporary Trino container with the Memory connector and loads
`ci/trino/bootstrap.sql`. This deterministic fixture represents the Raw Vault table
contract and makes SQL model tests fast and independent of MinIO. It does not replace
the full local integration test through PostgreSQL, Debezium, Kafka, MinIO, Delta
Lake, Hive Metastore, and Trino.

## Security baselines

`pip-audit` checks the three Python dependency sets. Trivy scans the repository for
high and critical vulnerabilities, secrets, and configuration findings, then scans
the Airflow, Trino, and Spark runtime images.

These jobs are intentionally non-blocking during baseline collection. Findings must
be triaged, documented, and assigned an exception or remediation before the relevant
scan can become a release gate. A non-blocking result is not security approval.

Dependabot checks GitHub Actions and the root, Bronze writer, and dbt Python
dependency files weekly.

## Local checks

Create a Python 3.11 virtual environment, then run:

```bash
python -m pip install -r requirements-dev.txt -r cdc/bronze/requirements.txt
ruff check .
python -m compileall -q cdc dags scripts source trino tests
PYTHONPATH=cdc/bronze python -m unittest discover -s cdc/bronze -p 'test_*.py'
docker compose config --quiet
bash -n source/init/005_configure_cdc.sh bi/export_powerbi_csv.sh
```

Airflow and dbt integration checks use containers in CI so that developers do not
need local Airflow or Trino installations. See the workflow for the exact commands.
