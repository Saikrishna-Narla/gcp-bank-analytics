#!/usr/bin/env bash
set -euo pipefail

# Production bootstrap. Set PROJECT_ID explicitly; this script never creates keys
# and scopes data access to the dataset/bucket instead of granting project Editor.
: "${PROJECT_ID:?Set PROJECT_ID to the target Google Cloud project}"
REGION="${REGION:-us-east1}"
BUCKET="${BUCKET:-${PROJECT_ID}-bank-analytics}"
TEMP_BUCKET="${TEMP_BUCKET:-${PROJECT_ID}-bank-analytics-tmp}"
DATASET="${DATASET:-bank_analytics}"
COMPOSER_ENV="${COMPOSER_ENV:-bank-analytics-composer}"
: "${COMPOSER_ENV_SIZE:?Set COMPOSER_ENV_SIZE after sizing for the workload}"
: "${COMPOSER_IMAGE_VERSION:?Set an immutable, tested COMPOSER_IMAGE_VERSION}"
PIPELINE_SA="${PIPELINE_SA:-bank-pipeline-sa@${PROJECT_ID}.iam.gserviceaccount.com}"
COMPOSER_SA="${COMPOSER_SA:-bank-composer-sa@${PROJECT_ID}.iam.gserviceaccount.com}"
ROOT="$(cd "$(dirname "$0")" && pwd)"

gcloud config set project "$PROJECT_ID" >/dev/null
gcloud services enable storage.googleapis.com bigquery.googleapis.com dataproc.googleapis.com \
  composer.googleapis.com iam.googleapis.com --project "$PROJECT_ID"

ensure_service_account() {
  local name="$1"
  local display_name="$2"
  if ! gcloud iam service-accounts describe "$name" --project "$PROJECT_ID" >/dev/null 2>&1; then
    gcloud iam service-accounts create "${name%@*}" --display-name="$display_name" --project "$PROJECT_ID"
  fi
}
ensure_service_account "$PIPELINE_SA" "Bank analytics Dataproc runtime"
ensure_service_account "$COMPOSER_SA" "Bank analytics Composer orchestrator"

# Project-wide roles are limited to job submission/worker control. Table and
# object permissions are granted at the dataset and bucket resource boundaries.
for role in roles/dataproc.worker roles/bigquery.jobUser; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${PIPELINE_SA}" --role="$role" --quiet >/dev/null
done
for role in roles/composer.worker roles/dataproc.editor roles/bigquery.jobUser; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${COMPOSER_SA}" --role="$role" --quiet >/dev/null
done

remove_project_role_if_bound() {
  local identity="$1"
  local role="$2"
  local member="serviceAccount:${identity}"
  local binding
  binding="$(gcloud projects get-iam-policy "$PROJECT_ID" \
    --flatten='bindings[].members' --filter="bindings.role:${role} AND bindings.members:${member}" \
    --format='value(bindings.role)')"
  if [[ -n "$binding" ]]; then
    gcloud projects remove-iam-policy-binding "$PROJECT_ID" \
      --member="$member" --role="$role" --quiet >/dev/null
  fi
}
# Remove broad grants from the previous demo bootstrap; resource-level grants
# below replace them. This keeps rerunning the bootstrap least-privilege.
remove_project_role_if_bound "$PIPELINE_SA" roles/bigquery.dataEditor
remove_project_role_if_bound "$PIPELINE_SA" roles/storage.objectAdmin
remove_project_role_if_bound "$COMPOSER_SA" roles/bigquery.dataEditor

if ! gcloud storage buckets describe "gs://${BUCKET}" --project "$PROJECT_ID" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${BUCKET}" --project="$PROJECT_ID" --location="$REGION" \
    --uniform-bucket-level-access --public-access-prevention=enforced
fi
if ! gcloud storage buckets describe "gs://${TEMP_BUCKET}" --project "$PROJECT_ID" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${TEMP_BUCKET}" --project="$PROJECT_ID" --location="$REGION" \
    --uniform-bucket-level-access --public-access-prevention=enforced
fi
gcloud storage buckets update "gs://${BUCKET}" --versioning --public-access-prevention=enforced
gcloud storage buckets update "gs://${TEMP_BUCKET}" \
  --lifecycle-file="$ROOT/config/temp_bucket_lifecycle.json" --public-access-prevention=enforced
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
  --member="serviceAccount:${PIPELINE_SA}" --role=roles/storage.objectViewer --quiet >/dev/null
gcloud storage buckets add-iam-policy-binding "gs://${TEMP_BUCKET}" \
  --member="serviceAccount:${PIPELINE_SA}" --role=roles/storage.objectAdmin --quiet >/dev/null

if ! bq --project_id="$PROJECT_ID" --location="$REGION" show "${PROJECT_ID}:${DATASET}" >/dev/null 2>&1; then
  bq --project_id="$PROJECT_ID" --location="$REGION" mk --dataset "${PROJECT_ID}:${DATASET}"
fi
for identity in "$PIPELINE_SA" "$COMPOSER_SA"; do
  bq --project_id="$PROJECT_ID" add-iam-policy-binding \
    --member="serviceAccount:${identity}" --role=roles/bigquery.dataEditor "${PROJECT_ID}:${DATASET}"
done
# Upgrade the existing demo audit table before Spark begins writing the new
# correlation field. IF NOT EXISTS keeps this safe across repeated deployments.
if bq --project_id="$PROJECT_ID" --location="$REGION" show "${PROJECT_ID}:${DATASET}.pipeline_audit" >/dev/null 2>&1; then
  bq --project_id="$PROJECT_ID" --location="$REGION" query --use_legacy_sql=false \
    "ALTER TABLE \`${PROJECT_ID}.${DATASET}.pipeline_audit\` ADD COLUMN IF NOT EXISTS run_id STRING"
fi
gcloud iam service-accounts add-iam-policy-binding "$PIPELINE_SA" \
  --member="serviceAccount:${COMPOSER_SA}" --role=roles/iam.serviceAccountUser --quiet >/dev/null

for prefix in inbound/day0 inbound/day1 inbound/day2 code/spark config archive; do
  printf 'managed prefix\n' | gcloud storage cp - "gs://${BUCKET}/${prefix}/.keep" >/dev/null
done
gcloud storage cp "$ROOT/spark/pipeline.py" "gs://${BUCKET}/code/spark/pipeline.py"
gcloud storage cp "$ROOT/config/entities.json" "gs://${BUCKET}/config/entities.json"
printf 'temporary artifacts\n' | gcloud storage cp - "gs://${TEMP_BUCKET}/dataproc/.keep" >/dev/null

if ! gcloud composer environments describe "$COMPOSER_ENV" --location "$REGION" \
  --project "$PROJECT_ID" >/dev/null 2>&1; then
  gcloud composer environments create "$COMPOSER_ENV" --location "$REGION" \
    --image-version "$COMPOSER_IMAGE_VERSION" --service-account "$COMPOSER_SA" \
    --project "$PROJECT_ID" --environment-size "$COMPOSER_ENV_SIZE"
fi
DAG_PREFIX="$(gcloud composer environments describe "$COMPOSER_ENV" --location "$REGION" \
  --project "$PROJECT_ID" --format='value(config.dagGcsPrefix)')"
if [[ -z "$DAG_PREFIX" ]]; then
  echo "Could not resolve Composer DAG bucket for ${COMPOSER_ENV}." >&2
  exit 2
fi
gcloud storage cp "$ROOT/dags/bank_customer_pipeline.py" "${DAG_PREFIX}/"
gcloud storage cp "$ROOT/sql/gold_views.sql" "${DAG_PREFIX}/gold_views.sql"

cat <<EOF
Deployment assets uploaded.
Project: ${PROJECT_ID}
Region: ${REGION}
Bucket: gs://${BUCKET}
Dataset: ${PROJECT_ID}.${DATASET}
Composer environment: ${COMPOSER_ENV}

Before scheduling production runs:
- Validate the source contract and rules in config/entities.json against approved source data.
- Set Composer Airflow Variables BANK_PROJECT_ID, BANK_REGION, BANK_BUCKET,
  BANK_TEMP_BUCKET, BANK_DATASET, BANK_PIPELINE_SA, BANK_SCHEDULE, and BANK_MAX_ACTIVE_RUNS.
- Configure alerting, retention, CMEK/VPC controls, and an approved Composer size.
- Restrict/rotate input data access under your organization's data classification policy.
EOF

