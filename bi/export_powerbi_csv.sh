#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${1:-${ROOT_DIR}/exports/powerbi}"
SCHEMA="lakehouse.gold_dbt"

TABLES=(
  dim_customer_current
  dim_product_current
  dim_branch_current
  dim_currency_current
  fct_loan_applications_current
  fct_loans_current
  agg_customer_loan_portfolio
)

mkdir -p "${OUTPUT_DIR}"
cd "${ROOT_DIR}"

manifest_tmp="${OUTPUT_DIR}/.manifest.csv.tmp"
printf 'exported_at_utc,schema_name,table_name,file_name,row_count,sha256\n' > "${manifest_tmp}"

for table_name in "${TABLES[@]}"; do
  output_file="${OUTPUT_DIR}/${table_name}.csv"
  temporary_file="${OUTPUT_DIR}/.${table_name}.csv.tmp"

  docker compose exec -T trino trino \
    --output-format CSV_HEADER \
    --execute "SELECT * FROM ${SCHEMA}.${table_name}" > "${temporary_file}"

  row_count="$(
    docker compose exec -T trino trino \
      --output-format TSV \
      --execute "SELECT count(*) FROM ${SCHEMA}.${table_name}" | tr -d '\r\n'
  )"
  checksum="$(sha256sum "${temporary_file}" | awk '{print $1}')"
  exported_at="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"

  mv "${temporary_file}" "${output_file}"
  printf '%s,%s,%s,%s,%s,%s\n' \
    "${exported_at}" "${SCHEMA}" "${table_name}" "$(basename "${output_file}")" \
    "${row_count}" "${checksum}" >> "${manifest_tmp}"
done

mv "${manifest_tmp}" "${OUTPUT_DIR}/manifest.csv"
printf 'Power BI export completed: %s\n' "${OUTPUT_DIR}"
