#!/usr/bin/env bash
# Synthetic demo: upload Day 0/1/2 pipe-delimited extracts, then process in sequence.
# Prerequisites: gcloud installed/authenticated, project billing enabled, and the IAM setup from deploy.sh.
# Usage: PROJECT_ID=mygcpproject-515308 REGION=us-east1 BUCKET=id-bank-analytics bash scripts/run_demo.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROJECT_ID="${PROJECT_ID:-mygcpproject-515308}"
REGION="${REGION:-us-east1}"
BUCKET="${BUCKET:-id-bank-analytics}"
DATASET="${DATASET:-bank_analytics}"
PIPELINE_SA="${PIPELINE_SA:-bank-pipeline-sa@${PROJECT_ID}.iam.gserviceaccount.com}"
# Step 1: Select the GCP project.
gcloud config set project "$PROJECT_ID"
# Step 2: Upload updated metadata and the three synthetic interleaved extracts.
gcloud storage cp "$ROOT/config/entities.json" "gs://${BUCKET}/config/entities.json"
gcloud storage cp "$ROOT/spark/pipeline.py" "gs://${BUCKET}/code/spark/pipeline.py"
gcloud storage cp "$ROOT/data/day0/day0.txt" "gs://${BUCKET}/inbound/day0/day0.txt"
gcloud storage cp "$ROOT/data/day1/day1.txt" "gs://${BUCKET}/inbound/day1/day1.txt"
gcloud storage cp "$ROOT/data/day2/day2.txt" "gs://${BUCKET}/inbound/day2/day2.txt"
# Step 3: Submit one ephemeral Dataproc Serverless batch per day in order.
submit_day() {
  local day="$1" full_refresh="$2"
  gcloud dataproc batches submit pyspark "gs://${BUCKET}/code/spark/pipeline.py" \
    --project="$PROJECT_ID" --region="$REGION" --batch="bank-${day}" \
    --service-account="$PIPELINE_SA" --staging-bucket="$BUCKET" --version=2.2 \
    --labels="pipeline=bank-customer-analytics,day=${day}" -- \
    --input-uri="gs://${BUCKET}/inbound/${day}/*.txt" \
    --config-uri="gs://${BUCKET}/config/entities.json" \
    --project="$PROJECT_ID" --dataset="$DATASET" --batch-id="$day" \
    --full-refresh="$full_refresh" --temporary-gcs-bucket="$BUCKET"
}
submit_day day0 true
submit_day day1 false
submit_day day2 false
# Step 4: Create/update reusable KPI views after the three batches complete.
SQL_FILE="$(mktemp)"
trap 'rm -f "$SQL_FILE"' EXIT
sed "s/__PROJECT__/${PROJECT_ID}/g" "$ROOT/sql/gold_views.sql" > "$SQL_FILE"
bq query --project_id="$PROJECT_ID" --use_legacy_sql=false < "$SQL_FILE"
# Step 5: Print the tables to inspect in BigQuery.
echo "Done. Inspect ${PROJECT_ID}.${DATASET}: metadata_entity_config, bronze_*, silver_*, quarantine_rows, pipeline_audit, dim_customer, dim_account, fact_transaction, kpi_* views."




