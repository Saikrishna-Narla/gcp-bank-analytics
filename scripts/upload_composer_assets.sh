#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-mygcpproject-515308}"
REGION="${REGION:-us-east1}"
COMPOSER_ENV="${COMPOSER_ENV:-bank-analytics-composer}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

gcloud config set project "$PROJECT_ID"
DAG_PREFIX="$(gcloud composer environments describe "$COMPOSER_ENV" \
  --project "$PROJECT_ID" --location "$REGION" \
  --format='value(config.dagGcsPrefix)')"
if [[ -z "$DAG_PREFIX" ]]; then
  echo "Could not find the DAG bucket for Composer environment ${COMPOSER_ENV}." >&2
  exit 2
fi

gcloud storage cp "$ROOT/dags/bank_customer_pipeline.py" "$DAG_PREFIX/"
gcloud storage cp "$ROOT/sql/gold_views.sql" "$DAG_PREFIX/gold_views.sql"
echo "Uploaded DAG and KPI SQL to ${DAG_PREFIX}. Wait for Airflow to parse the DAG."
