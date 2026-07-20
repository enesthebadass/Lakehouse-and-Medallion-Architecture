"""Expose local pipeline control-plane metrics in Prometheus text format."""

from __future__ import annotations

import json
import math
import os
import re
import sys
import time
import urllib.request
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

METRIC_NAME = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")


@dataclass(frozen=True)
class Settings:
    listen_port: int = int(os.getenv("METRICS_PORT", "9108"))
    connect_url: str = os.getenv("KAFKA_CONNECT_URL", "http://debezium-connect:8083")
    connector_name: str = os.getenv(
        "CDC_CONNECTOR_NAME", "core-banking-postgres-cdc"
    )
    kafka_bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
    )
    kafka_consumer_group: str = os.getenv(
        "KAFKA_CONSUMER_GROUP", "bronze-cdc-writer-v1"
    )
    airflow_dsn: str = os.getenv(
        "AIRFLOW_DATABASE_DSN",
        "dbname=airflow user=airflow password=airflow host=postgres port=5432",
    )
    monitored_dags: tuple[str, ...] = tuple(
        item.strip()
        for item in os.getenv(
            "MONITORED_DAGS",
            "cdc_raw_vault_incremental,lakehouse_medallion_pipeline",
        ).split(",")
        if item.strip()
    )
    spark_task_ids: tuple[str, ...] = tuple(
        item.strip()
        for item in os.getenv(
            "SPARK_TASK_IDS",
            "validate_bronze_and_quarantine,load_incremental_hubs_links,"
            "load_satellite_and_delete_history,reconcile_source_bronze_silver,"
            "generate_bronze_dirty_data,process_silver_data_vault,"
            "process_gold_star_schema",
        ).split(",")
        if item.strip()
    )
    trino_url: str = os.getenv("TRINO_URL", "http://trino:8080")
    trino_user: str = os.getenv("TRINO_USER", "monitoring")


def escape_label(value: Any) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def sample(name: str, value: float | int, **labels: Any) -> str:
    if not METRIC_NAME.fullmatch(name):
        raise ValueError(f"Invalid metric name: {name}")
    numeric_value = float(value)
    if not math.isfinite(numeric_value):
        numeric_value = 0.0
    label_text = ""
    if labels:
        values = ",".join(
            f'{key}="{escape_label(label_value)}"'
            for key, label_value in sorted(labels.items())
        )
        label_text = f"{{{values}}}"
    return f"{name}{label_text} {numeric_value:g}"


def fetch_json(url: str, *, method: str = "GET", body: str | None = None, headers=None):
    request = urllib.request.Request(
        url,
        data=body.encode("utf-8") if body is not None else None,
        method=method,
        headers=headers or {"Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read())


def trino_query(settings: Settings, sql: str) -> list[list[Any]]:
    headers = {
        "Accept": "application/json",
        "Content-Type": "text/plain; charset=utf-8",
        "X-Trino-User": settings.trino_user,
    }
    page = fetch_json(
        f"{settings.trino_url.rstrip('/')}/v1/statement",
        method="POST",
        body=sql,
        headers=headers,
    )
    rows: list[list[Any]] = []
    while True:
        if page.get("error"):
            raise RuntimeError(page["error"].get("message", "Trino query failed"))
        rows.extend(page.get("data", []))
        next_uri = page.get("nextUri")
        if not next_uri:
            return rows
        page = fetch_json(next_uri, headers=headers)


def connector_metrics(settings: Settings) -> list[str]:
    status = fetch_json(
        f"{settings.connect_url.rstrip('/')}/connectors/{settings.connector_name}/status"
    )
    connector_state = status.get("connector", {}).get("state", "UNKNOWN").lower()
    tasks = status.get("tasks", [])
    lines = [
        sample(
            "lakehouse_cdc_connector_up",
            connector_state == "running",
            connector=settings.connector_name,
        ),
        sample(
            "lakehouse_cdc_connector_state",
            1,
            connector=settings.connector_name,
            state=connector_state,
        ),
    ]
    for state in ("running", "failed", "paused", "unassigned"):
        task_count = sum(
            task.get("state", "UNKNOWN").lower() == state for task in tasks
        )
        lines.append(
            sample(
                "lakehouse_cdc_connector_tasks",
                task_count,
                connector=settings.connector_name,
                state=state,
            )
        )
    return lines


def kafka_lag_metrics(settings: Settings) -> list[str]:
    from confluent_kafka import (  # Imported lazily for unit-test portability.
        Consumer,
        ConsumerGroupTopicPartitions,
        TopicPartition,
    )
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    request = ConsumerGroupTopicPartitions(settings.kafka_consumer_group)
    result = admin.list_consumer_group_offsets([request], request_timeout=10)[
        settings.kafka_consumer_group
    ].result(timeout=10)
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "lakehouse-observability-watermark-reader",
            "enable.auto.commit": False,
        }
    )
    lines: list[str] = []
    total_lag = 0
    try:
        for partition in result.topic_partitions:
            if partition.offset < 0:
                continue
            topic_partition = TopicPartition(partition.topic, partition.partition)
            _, high_watermark = consumer.get_watermark_offsets(
                topic_partition, timeout=10, cached=False
            )
            lag = max(high_watermark - partition.offset, 0)
            total_lag += lag
            lines.append(
                sample(
                    "lakehouse_kafka_consumer_lag",
                    lag,
                    group=settings.kafka_consumer_group,
                    topic=partition.topic,
                    partition=partition.partition,
                )
            )
    finally:
        consumer.close()
    lines.append(
        sample(
            "lakehouse_kafka_consumer_lag_total",
            total_lag,
            group=settings.kafka_consumer_group,
        )
    )
    return lines


def airflow_metrics(settings: Settings) -> list[str]:
    import psycopg2  # Imported lazily for unit-test portability.

    lines: list[str] = []
    with psycopg2.connect(settings.airflow_dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH ranked AS (
                    SELECT dag_id, state, start_date, end_date,
                           row_number() OVER (
                               PARTITION BY dag_id ORDER BY execution_date DESC
                           ) AS row_rank
                    FROM dag_run
                    WHERE dag_id = ANY(%s)
                )
                SELECT dag_id, COALESCE(state, 'unknown'),
                       COALESCE(EXTRACT(EPOCH FROM (end_date - start_date)), 0)
                FROM ranked
                WHERE row_rank = 1
                """,
                (list(settings.monitored_dags),),
            )
            for dag_id, state, duration in cursor.fetchall():
                lines.append(
                    sample(
                        "lakehouse_airflow_dag_latest_state",
                        1,
                        dag_id=dag_id,
                        state=state,
                    )
                )
                lines.append(
                    sample(
                        "lakehouse_airflow_dag_latest_duration_seconds",
                        duration or 0,
                        dag_id=dag_id,
                    )
                )

            cursor.execute(
                """
                SELECT count(*)
                FROM dag_run
                WHERE dag_id = ANY(%s)
                  AND state = 'failed'
                  AND execution_date >= now() - interval '24 hours'
                """,
                (list(settings.monitored_dags),),
            )
            lines.append(
                sample(
                    "lakehouse_airflow_failed_dag_runs_24h",
                    cursor.fetchone()[0],
                )
            )

            cursor.execute(
                """
                WITH ranked AS (
                    SELECT dag_id, task_id, state, start_date, end_date,
                           row_number() OVER (
                               PARTITION BY dag_id, task_id
                               ORDER BY start_date DESC NULLS LAST
                           ) AS row_rank
                    FROM task_instance
                    WHERE task_id = ANY(%s)
                )
                SELECT dag_id, task_id, COALESCE(state, 'unknown'),
                       COALESCE(EXTRACT(EPOCH FROM (end_date - start_date)), 0)
                FROM ranked
                WHERE row_rank = 1
                """,
                (list(settings.spark_task_ids),),
            )
            for dag_id, task_id, state, duration in cursor.fetchall():
                lines.append(
                    sample(
                        "lakehouse_spark_task_latest_duration_seconds",
                        duration or 0,
                        dag_id=dag_id,
                        task_id=task_id,
                    )
                )
                lines.append(
                    sample(
                        "lakehouse_spark_task_latest_state",
                        1,
                        dag_id=dag_id,
                        task_id=task_id,
                        state=state,
                    )
                )
    return lines


def reconciliation_metrics(settings: Settings) -> list[str]:
    rows = trino_query(
        settings,
        """
        WITH latest_batch AS (
            SELECT load_batch_id
            FROM lakehouse.audit.cdc_raw_vault_reconciliation
            ORDER BY checked_at DESC
            LIMIT 1
        )
        SELECT count(*) AS check_count,
               count_if(NOT passed) AS failed_count,
               COALESCE(to_unixtime(max(checked_at)), 0) AS checked_at_epoch
        FROM lakehouse.audit.cdc_raw_vault_reconciliation
        WHERE load_batch_id = (SELECT load_batch_id FROM latest_batch)
        """,
    )
    check_count, failed_count, checked_at = rows[-1]
    return [
        sample("lakehouse_reconciliation_checks", check_count),
        sample("lakehouse_reconciliation_failures", failed_count),
        sample("lakehouse_reconciliation_last_check_timestamp_seconds", checked_at),
    ]


def gold_metrics(settings: Settings) -> list[str]:
    rows = trino_query(
        settings,
        """
        SELECT count(*) AS row_count,
               count(DISTINCT loan_id) AS distinct_grain,
               COALESCE(to_unixtime(max(dbt_loaded_at)), 0) AS loaded_at_epoch
        FROM lakehouse.gold_dbt.fct_loans_current
        """,
    )
    row_count, distinct_grain, loaded_at = rows[-1]
    return [
        sample(
            "lakehouse_gold_table_rows",
            row_count,
            table="fct_loans_current",
        ),
        sample(
            "lakehouse_gold_table_grain_valid",
            row_count == distinct_grain,
            table="fct_loans_current",
        ),
        sample(
            "lakehouse_gold_last_load_timestamp_seconds",
            loaded_at,
            table="fct_loans_current",
        ),
    ]


class MetricsApplication:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.collectors: tuple[tuple[str, Callable[[Settings], list[str]]], ...] = (
            ("connector", connector_metrics),
            ("kafka_lag", kafka_lag_metrics),
            ("airflow", airflow_metrics),
            ("reconciliation", reconciliation_metrics),
            ("gold", gold_metrics),
        )

    def render(self) -> bytes:
        lines = [sample("lakehouse_metrics_exporter_up", 1)]
        for collector_name, collector in self.collectors:
            started = time.monotonic()
            try:
                lines.extend(collector(self.settings))
                lines.append(
                    sample(
                        "lakehouse_observability_collector_up",
                        1,
                        collector=collector_name,
                    )
                )
            except Exception as error:  # A failed source must not hide other metrics.
                lines.append(
                    sample(
                        "lakehouse_observability_collector_up",
                        0,
                        collector=collector_name,
                    )
                )
                print(
                    json.dumps(
                        {
                            "event": "collector_failed",
                            "collector": collector_name,
                            "error_type": type(error).__name__,
                            "error": str(error),
                        },
                        ensure_ascii=True,
                    ),
                    file=sys.stderr,
                    flush=True,
                )
            lines.append(
                sample(
                    "lakehouse_observability_collector_duration_seconds",
                    time.monotonic() - started,
                    collector=collector_name,
                )
            )
        lines.append(sample("lakehouse_metrics_generated_timestamp_seconds", time.time()))
        return ("\n".join(lines) + "\n").encode("utf-8")


def handler(application: MetricsApplication):
    class MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/healthz":
                body = b"ok\n"
                status = 200
                content_type = "text/plain; charset=utf-8"
            elif self.path == "/metrics":
                body = application.render()
                status = 200
                content_type = "text/plain; version=0.0.4; charset=utf-8"
            else:
                body = b"not found\n"
                status = 404
                content_type = "text/plain; charset=utf-8"
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format_string: str, *args: Any) -> None:
            print(format_string % args, flush=True)

    return MetricsHandler


def main() -> None:
    settings = Settings()
    application = MetricsApplication(settings)
    server = ThreadingHTTPServer(("0.0.0.0", settings.listen_port), handler(application))
    print(
        json.dumps(
            {"event": "metrics_exporter_started", "port": settings.listen_port}
        ),
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
