import importlib.util
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
OBSERVABILITY = ROOT / "observability"


def load_yaml(path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_compose_overlay_is_optional_pinned_and_non_privileged():
    compose = load_yaml(ROOT / "docker-compose.observability.yml")
    services = compose["services"]
    assert set(services) == {
        "pipeline-metrics-exporter",
        "alertmanager",
        "prometheus",
        "grafana",
    }
    for service in services.values():
        assert "observability" in service["profiles"]
        assert not service.get("privileged", False)
        assert "/var/run/docker.sock" not in json.dumps(service)
        if "image" in service:
            assert not service["image"].endswith(":latest")


def test_metric_contract_covers_day19_requirements_without_overclaiming():
    contract = load_yaml(OBSERVABILITY / "metric-contract.yaml")
    requirements = {metric["requirement"] for metric in contract["metrics"]}
    expected = {
        "kafka_lag",
        "connector_status",
        "airflow_failure",
        "spark_duration",
        "trino_query_metrics",
        "data_quality_reconciliation",
        "dashboard_data_quality",
        "dashboard_freshness",
    }
    assert expected <= requirements
    assert contract["status"]["local_observability_baseline"] == "implemented_poc"
    assert contract["status"]["production_monitoring"] == "designed_external"
    assert contract["service_objectives"]["status"].startswith("provisional")
    assert len(contract["non_claims"]) >= 5


def test_prometheus_scrapes_pipeline_and_trino_and_loads_alerts():
    config = load_yaml(OBSERVABILITY / "prometheus/prometheus.yml")
    jobs = {job["job_name"]: job for job in config["scrape_configs"]}
    assert {"prometheus", "lakehouse-pipeline", "trino"} <= set(jobs)
    assert jobs["trino"]["metrics_path"] == "/metrics"
    assert jobs["trino"]["basic_auth"]["username"] == "monitoring"
    assert config["rule_files"]
    assert config["alerting"]["alertmanagers"]


def test_alerts_cover_availability_pipeline_and_data_quality():
    rules = load_yaml(OBSERVABILITY / "prometheus/rules.yml")
    alerts = {
        rule["alert"]: rule
        for group in rules["groups"]
        for rule in group["rules"]
        if "alert" in rule
    }
    expected = {
        "CdcConnectorDown",
        "KafkaConsumerLagHigh",
        "AirflowLatestDagRunFailed",
        "SparkTaskDurationHigh",
        "TrinoQueryFailuresDetected",
        "ReconciliationFailed",
        "GoldGrainViolation",
        "GoldLoadStale",
    }
    assert expected <= set(alerts)
    for alert in alerts.values():
        assert alert["labels"]["severity"] in {"warning", "critical"}
        assert alert["annotations"]["runbook"].startswith("OPERATIONS_RUNBOOK.md#")


def test_grafana_dashboard_is_provisioned_with_required_queries():
    dashboard = json.loads(
        (OBSERVABILITY / "grafana/dashboards/lakehouse-pipeline.json").read_text()
    )
    expressions = {
        target["expr"]
        for panel in dashboard["panels"]
        for target in panel.get("targets", [])
    }
    rendered = "\n".join(expressions)
    for metric in (
        "lakehouse_cdc_connector_up",
        "lakehouse_kafka_consumer_lag_total",
        "lakehouse_reconciliation_failures",
        "lakehouse_spark_task_latest_duration_seconds",
        "trino_execution_name_QueryManager_RunningQueries",
    ):
        assert metric in rendered
    assert dashboard["uid"] == "lakehouse-pipeline-ops"
    assert dashboard["refresh"] == "15s"


def test_exporter_keeps_other_collectors_visible_when_one_fails():
    module_path = OBSERVABILITY / "exporter/exporter.py"
    spec = importlib.util.spec_from_file_location("lakehouse_exporter", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    application = module.MetricsApplication(module.Settings())

    def healthy(_settings):
        return [module.sample("example_metric", 1, label='a"b')]

    def broken(_settings):
        raise RuntimeError("expected test failure")

    application.collectors = (("healthy", healthy), ("broken", broken))
    output = application.render().decode("utf-8")

    assert 'example_metric{label="a\\"b"} 1' in output
    assert 'collector="healthy"} 1' in output
    assert 'collector="broken"} 0' in output


def test_e2e_script_covers_full_path_and_can_resume_by_run_id():
    script = (ROOT / "operations/end_to_end_smoke_test.sh").read_text()

    for expected in (
        "core-banking-workload",
        "kafka-consumer-groups.sh",
        "airflow dags trigger",
        "trino-init",
        "dbt run",
        "dbt test",
        "export_powerbi_csv.sh",
    ):
        assert expected in script
    assert "E2E_RUN_ID" in script
    assert "FROM dag_run" in script


if __name__ == "__main__":
    tests = [
        value
        for name, value in sorted(globals().items())
        if name.startswith("test_") and callable(value)
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"PASS {len(tests)} observability blueprint tests")
