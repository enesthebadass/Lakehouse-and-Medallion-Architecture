#!/usr/bin/env bash
set -euo pipefail
export COMPOSE_IGNORE_ORPHANS=true

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:---read-only}"
RESULT_FILE="${2:-${ROOT_DIR}/tests/results/day19-e2e.md}"
CDC_GROUP="${BRONZE_CDC_CONSUMER_GROUP:-bronze-cdc-writer-v1}"
EXPECTED_CDC_TOPIC_COUNT="${EXPECTED_CDC_TOPIC_COUNT:-13}"
CDC_LAG_STABLE_SAMPLES="${CDC_LAG_STABLE_SAMPLES:-6}"
RUN_ID="${E2E_RUN_ID:-day19-$(date -u +'%Y%m%dT%H%M%SZ')}"
STARTED_AT="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"

if [[ "${MODE}" != "--read-only" && "${MODE}" != "--exercise-change" ]]; then
  printf 'Usage: %s [--read-only|--exercise-change] [result-file]\n' "$0" >&2
  exit 2
fi

cd "${ROOT_DIR}"
mkdir -p "$(dirname "${RESULT_FILE}")"

log() {
  printf '[e2e] %s\n' "$*"
}

trino_scalar() {
  docker compose exec -T trino trino \
    --output-format TSV \
    --execute "$1" | tr -d '\r\n'
}

kafka_group_snapshot() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:9092 \
    --group "${CDC_GROUP}" \
    --describe 2>/dev/null |
    awk '$6 ~ /^[0-9]+$/ {partitions += 1; lag += $6} END {print partitions + 0, lag + 0}'
}

kafka_lag() {
  kafka_group_snapshot | awk '{print $2}'
}

wait_for_zero_lag() {
  local snapshot partition_count lag
  local stable_samples=0
  for _ in $(seq 1 120); do
    snapshot="$(kafka_group_snapshot)"
    read -r partition_count lag <<< "${snapshot}"
    if [[ "${partition_count}" -ge "${EXPECTED_CDC_TOPIC_COUNT}" && "${lag}" == "0" ]]; then
      stable_samples=$((stable_samples + 1))
      if [[ "${stable_samples}" -ge "${CDC_LAG_STABLE_SAMPLES}" ]]; then
        return 0
      fi
    else
      stable_samples=0
    fi
    sleep 5
  done
  printf 'Kafka group did not stabilize; topics=%s/%s lag=%s stable_samples=%s/%s\n' \
    "${partition_count}" "${EXPECTED_CDC_TOPIC_COUNT}" "${lag}" \
    "${stable_samples}" "${CDC_LAG_STABLE_SAMPLES}" >&2
  return 1
}

airflow_run_state() {
  docker compose exec -T postgres psql \
    -U airflow \
    -d airflow \
    -Atc "SELECT state FROM dag_run WHERE dag_id = 'cdc_raw_vault_incremental' AND run_id = '${RUN_ID}' ORDER BY id DESC LIMIT 1" |
    tr -d '\r[:space:]'
}

wait_for_airflow_run() {
  local state=""
  for _ in $(seq 1 120); do
    state="$(airflow_run_state)"
    case "${state}" in
      success)
        return 0
        ;;
      failed)
        printf 'Airflow run failed: %s\n' "${RUN_ID}" >&2
        return 1
        ;;
    esac
    sleep 10
  done
  printf 'Airflow run timed out: %s state=%s\n' "${RUN_ID}" "${state}" >&2
  return 1
}

log "checking required services"
for service in core-banking-source kafka debezium-connect bronze-cdc-writer minio trino airflow-webserver; do
  container_id="$(docker compose ps -q "${service}")"
  if [[ -z "${container_id}" ]]; then
    printf 'Required service is not running: %s\n' "${service}" >&2
    exit 1
  fi
done

connector_status="$(
  docker compose exec -T debezium-connect \
    curl --silent --fail \
    http://localhost:8083/connectors/core-banking-postgres-cdc/status
)"
python -c \
  'import json,sys; value=json.load(sys.stdin); assert value["connector"]["state"] == "RUNNING"; assert all(task["state"] == "RUNNING" for task in value["tasks"])' \
  <<< "${connector_status}"

if [[ "${MODE}" == "--exercise-change" ]]; then
  log "writing a deterministic operational change batch: ${RUN_ID}"
  docker compose --profile tools run --rm core-banking-workload \
    changes --run-id "${RUN_ID}"
fi

log "waiting for Bronze consumer lag to reach zero"
wait_for_zero_lag

if [[ "${MODE}" == "--exercise-change" ]]; then
  log "triggering incremental CDC Raw Vault DAG"
  airflow_state="$(airflow_run_state)"
  if [[ -z "${airflow_state}" ]]; then
    docker compose exec -T airflow-webserver airflow dags trigger \
      --run-id "${RUN_ID}" cdc_raw_vault_incremental >/dev/null
  elif [[ "${airflow_state}" == "failed" ]]; then
    printf 'Existing Airflow run already failed: %s\n' "${RUN_ID}" >&2
    exit 1
  else
    log "reusing existing Airflow run: ${RUN_ID} state=${airflow_state}"
  fi
  wait_for_airflow_run

  log "registering Delta tables and rebuilding tested dbt Gold models"
  docker compose --profile tools run --rm trino-init
  docker compose --profile tools run --rm dbt run
  docker compose --profile tools run --rm dbt test
fi

source_rows="$(
  docker compose exec -T core-banking-source psql \
    -U "${CORE_BANKING_DB_USER:-core_banking}" \
    -d "${CORE_BANKING_DB_NAME:-core_banking}" \
    -Atc "SELECT count(*) FROM mms.customers"
)"
bronze_objects="$(
  docker compose exec -T minio sh -c \
    "mc alias set e2e http://localhost:9000 minioadmin minioadmin >/dev/null && mc find e2e/lakehouse/bronze/cdc --name '*.json' | wc -l" |
    tr -d '[:space:]'
)"
reconciliation_failures="$(trino_scalar "
  WITH latest_batch AS (
    SELECT load_batch_id
    FROM lakehouse.audit.cdc_raw_vault_reconciliation
    ORDER BY checked_at DESC
    LIMIT 1
  )
  SELECT count_if(NOT passed)
  FROM lakehouse.audit.cdc_raw_vault_reconciliation
  WHERE load_batch_id = (SELECT load_batch_id FROM latest_batch)
")"
gold_loan_rows="$(trino_scalar "SELECT count(*) FROM lakehouse.gold_dbt.fct_loans_current")"
gold_loan_grain="$(trino_scalar "
  SELECT count(*) = count(DISTINCT loan_id)
  FROM lakehouse.gold_dbt.fct_loans_current
")"

if [[ "${source_rows}" -le 0 || "${bronze_objects}" -le 0 || "${gold_loan_rows}" -le 0 ]]; then
  printf 'Empty stage detected: source=%s bronze=%s gold=%s\n' \
    "${source_rows}" "${bronze_objects}" "${gold_loan_rows}" >&2
  exit 1
fi
if [[ "${reconciliation_failures}" != "0" || "${gold_loan_grain}" != "true" ]]; then
  printf 'Quality check failed: reconciliation=%s gold_grain=%s\n' \
    "${reconciliation_failures}" "${gold_loan_grain}" >&2
  exit 1
fi

log "exporting the governed Gold surface for Power BI"
./bi/export_powerbi_csv.sh
exported_tables="$(awk 'END {print NR - 1}' exports/powerbi/manifest.csv)"
if [[ "${exported_tables}" != "7" ]]; then
  printf 'Expected 7 Power BI exports, found %s\n' "${exported_tables}" >&2
  exit 1
fi

FINISHED_AT="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
cat > "${RESULT_FILE}" <<EOF
# End-to-End Test Result

- Status: **PASS**
- Mode: \`${MODE}\`
- Run ID: \`${RUN_ID}\`
- Started: \`${STARTED_AT}\`
- Finished: \`${FINISHED_AT}\`
- CDC connector: \`RUNNING\`
- Kafka Bronze consumer lag: \`$(kafka_lag)\`
- Source \`mms.customers\` rows: \`${source_rows}\`
- Bronze CDC objects: \`${bronze_objects}\`
- Latest reconciliation failures: \`${reconciliation_failures}\`
- Gold loan rows: \`${gold_loan_rows}\`
- Gold loan grain valid: \`${gold_loan_grain}\`
- Power BI exported tables: \`${exported_tables}\`

Validated path:

\`source -> Debezium -> Kafka -> immutable Bronze -> CDC Raw Vault -> dbt Gold -> Power BI CSV contract\`
EOF

log "PASS result=${RESULT_FILE}"
