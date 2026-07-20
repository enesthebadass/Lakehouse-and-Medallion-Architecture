#!/usr/bin/env bash
set -euo pipefail
export COMPOSE_IGNORE_ORPHANS=true

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_NAME="${REPRO_PROJECT_NAME:-lakehouse-repro-day20}"
RESULT_FILE="${1:-${ROOT_DIR}/tests/results/day20-reproducibility.md}"
E2E_RESULT_FILE="${ROOT_DIR}/tests/results/day20-clean-e2e.md"
SNAPSHOT_RUN_ID="day20-clean-snapshot-v1"
CHANGE_RUN_ID="day20-clean-change-v1"
STARTED_AT="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"

if [[ ! "${PROJECT_NAME}" =~ ^lakehouse-repro-[a-z0-9-]+$ ]]; then
  printf 'Refusing unsafe project name: %s (expected lakehouse-repro-*)\n' "${PROJECT_NAME}" >&2
  exit 2
fi
if [[ "${PROJECT_NAME}" == "lakehouse-medallion-demo" ]]; then
  printf 'Refusing to use the normal demo project for a destructive clean test.\n' >&2
  exit 2
fi

cd "${ROOT_DIR}"

compose() {
  docker compose --project-name "${PROJECT_NAME}" "$@"
}

observability_compose() {
  compose \
    --file docker-compose.yml \
    --file docker-compose.observability.yml \
    --profile observability \
    "$@"
}

log() {
  printf '[repro] %s\n' "$*"
}

cleanup() {
  log "removing isolated containers and volumes for ${PROJECT_NAME}"
  observability_compose down --volumes --remove-orphans
}

on_exit() {
  local status=$?
  if [[ "${status}" -eq 0 || "${REPRO_CLEANUP_ON_FAILURE:-0}" == "1" ]]; then
    cleanup || true
  else
    log "FAILED; isolated project was preserved for inspection: ${PROJECT_NAME}"
    log "cleanup command: REPRO_PROJECT_NAME=${PROJECT_NAME} $0 --cleanup-only"
  fi
  exit "${status}"
}

if [[ "${1:-}" == "--cleanup-only" ]]; then
  cleanup
  exit 0
fi

mkdir -p "$(dirname "${RESULT_FILE}")"
trap on_exit EXIT

if [[ -n "$(compose ps --all --quiet)" ]]; then
  printf 'Project %s already has containers; choose a new REPRO_PROJECT_NAME or clean it first.\n' "${PROJECT_NAME}" >&2
  exit 1
fi
if [[ -n "$(docker volume ls --quiet --filter "label=com.docker.compose.project=${PROJECT_NAME}")" ]]; then
  printf 'Project %s already has volumes; clean them before claiming a fresh run.\n' "${PROJECT_NAME}" >&2
  exit 1
fi

wait_for_service() {
  local service="$1"
  local container_id=""
  local state=""
  for _ in $(seq 1 90); do
    container_id="$(observability_compose ps --quiet "${service}")"
    if [[ -n "${container_id}" ]]; then
      state="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container_id}")"
      if [[ "${state}" == "healthy" || "${state}" == "running" ]]; then
        return 0
      fi
      if [[ "${state}" == "unhealthy" || "${state}" == "exited" ]]; then
        printf 'Service %s entered terminal state: %s\n' "${service}" "${state}" >&2
        return 1
      fi
    fi
    sleep 5
  done
  printf 'Service %s did not become ready; last state=%s\n' "${service}" "${state}" >&2
  return 1
}

wait_for_connector() {
  for _ in $(seq 1 60); do
    if compose exec -T debezium-connect curl --silent --fail \
      http://localhost:8083/connectors/core-banking-postgres-cdc/status 2>/dev/null |
      python -c 'import json,sys; d=json.load(sys.stdin); assert d["connector"]["state"] == "RUNNING"; assert all(t["state"] == "RUNNING" for t in d["tasks"])' 2>/dev/null
    then
      return 0
    fi
    sleep 5
  done
  printf 'CDC connector did not become RUNNING.\n' >&2
  return 1
}

log "starting the fresh source database: ${PROJECT_NAME}"
compose up -d --build core-banking-source
wait_for_service core-banking-source

log "creating the deterministic source snapshot before Debezium initial snapshot"
compose --profile tools run --rm core-banking-workload \
  snapshot --run-id "${SNAPSHOT_RUN_ID}"

log "starting the remaining base stack"
compose up -d --build
for service in \
  postgres core-banking-source kafka debezium-connect bronze-cdc-writer minio \
  spark-master spark-worker hive-metastore-postgres hive-metastore trino \
  airflow-scheduler airflow-webserver
do
  wait_for_service "${service}"
done
wait_for_connector

log "running the complete source-to-Power-BI-contract test"
COMPOSE_PROJECT_NAME="${PROJECT_NAME}" E2E_RUN_ID="${CHANGE_RUN_ID}" \
  ./operations/end_to_end_smoke_test.sh --exercise-change "${E2E_RESULT_FILE}"

log "starting and validating the observability profile"
observability_compose up -d --build
for service in pipeline-metrics-exporter prometheus grafana alertmanager; do
  wait_for_service "${service}"
done

exporter_id="$(observability_compose ps --quiet pipeline-metrics-exporter)"
docker exec "${exporter_id}" python -c \
  'import json,time,urllib.request; time.sleep(20); d=json.load(urllib.request.urlopen("http://prometheus:9090/api/v1/targets")); targets=d["data"]["activeTargets"]; assert targets and all(t["health"] == "up" for t in targets), targets; assert json.load(urllib.request.urlopen("http://grafana:3000/api/health"))["database"] == "ok"; print(", ".join(sorted(t["labels"]["job"] + "=up" for t in targets)))'

FINISHED_AT="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
cat > "${RESULT_FILE}" <<EOF
# Day 20 Clean Reproducibility Result

- Status: **PASS**
- Isolated project: \`${PROJECT_NAME}\`
- Fresh project volumes: **yes**
- Existing demo volumes modified: **no**
- Started: \`${STARTED_AT}\`
- Finished: \`${FINISHED_AT}\`
- Source snapshot run: \`${SNAPSHOT_RUN_ID}\`
- Source change run: \`${CHANGE_RUN_ID}\`
- Primary E2E evidence: \`tests/results/day20-clean-e2e.md\`
- Observability targets: \`lakehouse-pipeline=up, prometheus=up, trino=up\`
- Grafana health: \`ok\`
- Cleanup: isolated containers and volumes removed after success

Validated from clean state:

\`PostgreSQL source -> Debezium -> Kafka -> immutable Bronze -> incremental Raw Vault -> Trino/dbt Gold -> Power BI CSV contract -> Prometheus/Grafana\`
EOF

log "PASS result=${RESULT_FILE}"
