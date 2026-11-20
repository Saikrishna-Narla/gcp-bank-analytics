#!/usr/bin/env bash
set -euo pipefail
# STEP 0: Edit PROJECT_ID. Use a billing-enabled project and event-approved region.
PROJECT_ID="YOUR_GCP_PROJECT_ID"
REGION="us-central1"
BUCKET="${PROJECT_ID}-bank-analytics"
DATASET="bank_analytics"
COMPOSER_ENV="bank-analytics-composer"
PIPELINE_SA="bank-pipeline-sa@${PROJECT_ID}.iam.gserviceaccount.com"
COMPOSER_SA="bank-composer-sa@${PROJECT_ID}.iam.gserviceaccount.com"
ROOT="$(cd "$(dirname "$0")" && pwd)"
if [[ "$PROJECT_ID" == "YOUR_GCP_PROJECT_ID" ]]; then echo 'Edit PROJECT_ID first.' >&2; exit 2; fi
gcloud config set project "$PROJECT_ID"
# STEP 1: Enable the managed services used by the pipeline.
gcloud services enable storage.googleapis.com bigquery.googleapis.com dataproc.googleapis.com composer.googleapis.com iam.googleapis.com --project "$PROJECT_ID"
# STEP 2: Create dedicated runtime identity. No service-account key is created/downloaded.
gcloud iam service-accounts create bank-pipeline-sa --display-name='Bank analytics pipeline runtime' --project "$PROJECT_ID" 2>/dev/null || true
gcloud iam service-accounts create bank-composer-sa --display-name='Bank analytics Composer environment' --project "$PROJECT_ID" 2>/dev/null || true
# Hackathon-simple project grants; production should scope these to specific bucket/dataset resources.
for role in roles/dataproc.worker roles/bigquery.jobUser roles/bigquery.dataEditor roles/storage.objectAdmin; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:${PIPELINE_SA}" --role="$role" --quiet >/dev/null
done
# Composer's identity only orchestrates. It submits batches and can act as the runtime identity.
for role in roles/composer.worker roles/dataproc.editor; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:${COMPOSER_SA}" --role="$role" --quiet >/dev/null
done
gcloud iam service-accounts add-iam-policy-binding "$PIPELINE_SA" --member="serviceAccount:${COMPOSER_SA}" --role=roles/iam.serviceAccountUser --quiet
# STEP 3: Create same-region bucket and BigQuery dataset.
gcloud storage buckets create "gs://${BUCKET}" --project="$PROJECT_ID" --location="$REGION" --uniform-bucket-level-access 2>/dev/null || true
gcloud storage buckets update "gs://${BUCKET}" --versioning
bq --location="$REGION" mk --dataset "${PROJECT_ID}:${DATASET}" 2>/dev/null || true
# STEP 4: Create requested GCS prefixes and upload Spark app/config.
for prefix in inbound/day0 inbound/day1 inbound/day2 code/spark config tmp/dataproc archive; do
  printf 'folder marker\n' | gcloud storage cp - "gs://${BUCKET}/${prefix}/.keep" >/dev/null
done
gcloud storage cp "$ROOT/spark/pipeline.py" "gs://${BUCKET}/code/spark/pipeline.py"
gcloud storage cp "$ROOT/config/entities.json" "gs://${BUCKET}/config/entities.json"
# STEP 5: Create Composer environment. This can take 20–40 minutes.
gcloud composer environments create "$COMPOSER_ENV" --location "$REGION" --image-version composer-3-airflow-2 --service-account "$COMPOSER_SA" --project "$PROJECT_ID" --environment-size small || true
# STEP 6: Allow Composer to submit batches and impersonate runtime SA; upload DAG.
COMPOSER_BUCKET=$(gcloud composer environments describe "$COMPOSER_ENV" --location "$REGION" --format='value(config.dagGcsPrefix)' | sed 's#/dags$##')
gcloud storage cp "$ROOT/dags/bank_customer_pipeline.py" "${COMPOSER_BUCKET}/dags/bank_customer_pipeline.py"
# STEP 7: Add these Airflow Variables in Composer UI > Admin > Variables.
cat <<EOF
Composer DAG bucket: ${COMPOSER_BUCKET}
BANK_PROJECT_ID=${PROJECT_ID}
BANK_REGION=${REGION}
BANK_BUCKET=${BUCKET}
BANK_DATASET=${DATASET}
BANK_PIPELINE_SA=${PIPELINE_SA}
Upload data to gs://${BUCKET}/inbound/day0/, day1/, day2/.
EOF
