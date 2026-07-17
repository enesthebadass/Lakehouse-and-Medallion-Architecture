"""Idempotently register the local PostgreSQL Debezium connector."""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://debezium-connect:8083").rstrip("/")
CONNECTOR_NAME = os.getenv("CDC_CONNECTOR_NAME", "core-banking-postgres-cdc")
CONFIG_PATH = Path(
    os.getenv("CDC_CONNECTOR_CONFIG", "/cdc/connectors/core-banking-postgres.json")
)


def request(method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    http_request = urllib.request.Request(
        f"{CONNECT_URL}{path}",
        data=body,
        method=method,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(http_request, timeout=10) as response:
        response_body = response.read()
        return json.loads(response_body) if response_body else None


def wait_for_connect() -> None:
    last_error: Exception | None = None
    for attempt in range(1, 31):
        try:
            request("GET", "/connector-plugins")
            print(json.dumps({"message": "kafka_connect_ready", "attempt": attempt}))
            return
        except (urllib.error.URLError, TimeoutError) as error:
            last_error = error
            time.sleep(2)
    raise RuntimeError("Kafka Connect REST API did not become ready") from last_error


def connector_config() -> dict[str, Any]:
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    required_environment = {
        "database.user": "CDC_DB_USER",
        "database.password": "CDC_DB_PASSWORD",
        "database.dbname": "CORE_BANKING_DB_NAME",
        "topic.prefix": "CDC_TOPIC_PREFIX",
    }
    for property_name, environment_name in required_environment.items():
        value = os.getenv(environment_name)
        if not value:
            raise RuntimeError(f"Required environment variable {environment_name} is not set")
        config[property_name] = value
    return config


def wait_for_connector() -> dict[str, Any]:
    last_status: dict[str, Any] | None = None
    for _ in range(60):
        try:
            last_status = request("GET", f"/connectors/{CONNECTOR_NAME}/status")
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
            time.sleep(2)
            continue

        connector_running = last_status["connector"]["state"] == "RUNNING"
        tasks = last_status.get("tasks", [])
        tasks_running = bool(tasks) and all(task["state"] == "RUNNING" for task in tasks)
        if connector_running and tasks_running:
            return last_status
        if any(task["state"] == "FAILED" for task in tasks):
            break
        time.sleep(2)
    raise RuntimeError(f"Connector did not reach RUNNING state: {last_status}")


def main() -> int:
    try:
        wait_for_connect()
        config = connector_config()
        request("PUT", f"/connectors/{CONNECTOR_NAME}/config", config)
        status = wait_for_connector()
        print(
            json.dumps(
                {
                    "message": "connector_running",
                    "connector": CONNECTOR_NAME,
                    "connector_state": status["connector"]["state"],
                    "task_states": [task["state"] for task in status["tasks"]],
                    "topic_prefix": config["topic.prefix"],
                },
                sort_keys=True,
            )
        )
        return 0
    except Exception as error:
        if isinstance(error, urllib.error.HTTPError):
            error_body = error.read().decode("utf-8", errors="replace")
            print(json.dumps({"message": "connector_registration_failed", "error": error_body}))
        else:
            print(json.dumps({"message": "connector_registration_failed", "error": str(error)}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
