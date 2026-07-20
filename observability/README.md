# Local Observability Baseline

The optional observability profile adds a file-provisioned Prometheus, Grafana, and
Alertmanager baseline without changing the default demo startup path. A small
read-only exporter converts Kafka Connect status, Kafka consumer offsets, Airflow task
state, reconciliation results, and Gold freshness into Prometheus metrics. Trino is
scraped directly through its native OpenMetrics endpoint.

Start the profile:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.observability.yml \
  --profile observability \
  up -d --build
```

Open Grafana at <http://localhost:3000> and select the provisioned **Lakehouse Pipeline
Operations** dashboard. The local password is configured through
`GRAFANA_ADMIN_PASSWORD`; never reuse the local default outside the demo.

Configuration layout:

```text
observability/
|-- exporter/
|-- prometheus/
|-- alertmanager/
|-- grafana/
`-- metric-contract.yaml
```

See `OPERATIONS_RUNBOOK.md` for alert triage, incident response, backup/restore,
provisional RPO/RTO, and end-to-end validation. Local Alertmanager intentionally has
a null receiver and proves rule evaluation only; production notification routing is
an external SRE/security integration.
