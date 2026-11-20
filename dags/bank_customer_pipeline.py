"""Composer orchestration for BankEvalDB baseline and incremental processing."""
from datetime import datetime, timedelta, timezone
import hashlib
import uuid
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule


PROJECT_ID = Variable.get("BANK_PROJECT_ID", default_var="mygcpproject-515308")
REGION = Variable.get("BANK_REGION", default_var="us-east1")
BUCKET = Variable.get("BANK_BUCKET", default_var="id-bank-analytics")
TEMP_BUCKET = Variable.get("BANK_TEMP_BUCKET", default_var=f"{BUCKET}-tmp")
DATASET = Variable.get("BANK_DATASET", default_var="bank_analytics")
SCHEDULE = Variable.get("BANK_SCHEDULE", default_var=None)
MAX_ACTIVE_RUNS = int(Variable.get("BANK_MAX_ACTIVE_RUNS", default_var="1"))
ENABLE_REPLAY_CHECKS = Variable.get("BANK_ENABLE_REPLAY_CHECKS", default_var="false").lower() == "true"
PIPELINE_SA = Variable.get(
    "BANK_PIPELINE_SA",
    default_var=f"bank-pipeline-sa@{PROJECT_ID}.iam.gserviceaccount.com",
)
PIPELINE_URI = f"gs://{BUCKET}/code/spark/pipeline.py"
CONFIG_URI = f"gs://{BUCKET}/config/entities.json"
CLUSTER_NAME = "bank-demo-{{ ts_nodash | lower }}"
RUN_UUID_TEMPLATE = "{{ ti.xcom_pull(task_ids='create_pipeline_run_id') }}"

# The SQL file is shipped beside this DAG in the Composer DAG bucket. Airflow reads
# the SQL definition; Spark, not Airflow, reads and transforms the GCS data.
GOLD_SQL = (Path(__file__).parent / "gold_views.sql").read_text(encoding="utf-8").replace(
    "__PROJECT__", PROJECT_ID
)

CLUSTER_CONFIG = {
    "config_bucket": TEMP_BUCKET,
    "temp_bucket": TEMP_BUCKET,
    "gce_cluster_config": {
        "service_account": PIPELINE_SA,
        "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
    },
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n4-standard-4",
        "disk_config": {"boot_disk_size_gb": 100, "boot_disk_type": "hyperdisk-balanced"},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n4-standard-4",
        "disk_config": {"boot_disk_size_gb": 100, "boot_disk_type": "hyperdisk-balanced"},
    },
    "software_config": {
        "image_version": "2.2-debian12",
        "optional_components": ["JUPYTER"],
    },
    "endpoint_config": {"enable_http_port_access": True},
    # Safety backstop if the explicit cleanup task cannot run.
    "lifecycle_config": {"auto_delete_ttl": {"seconds": 3600}},
}


def make_job(day: str, full_refresh: bool) -> dict:
    """Build one PySpark job; each task waits for Dataproc completion."""
    return {
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PIPELINE_URI,
            "args": [
                f"--input-uri=gs://{BUCKET}/inbound/{day}/*.txt",
                f"--config-uri={CONFIG_URI}",
                f"--project={PROJECT_ID}",
                f"--dataset={DATASET}",
                f"--batch-id={day}",
                f"--run-id={RUN_UUID_TEMPLATE}",
                f"--full-refresh={str(full_refresh).lower()}",
                f"--temporary-gcs-bucket={TEMP_BUCKET}",
            ],
        },
    }


def require_successful_pipeline(**context):
    """Keep cleanup from masking an upstream DAG failure as a successful run."""
    required = [
        "create_pipeline_run_id",
        "create_ephemeral_dataproc_cluster",
        "process_day0_full_baseline",
        "process_day1_incremental",
        "process_day2_incremental",
        "publish_gold_kpi_views",
        "verify_medallion_replay_dq_and_scd2",
        "delete_ephemeral_dataproc_cluster",
        "record_pipeline_failures",
    ]
    if ENABLE_REPLAY_CHECKS:
        required.extend(["replay_day1_idempotency_check", "replay_day2_idempotency_check"])
    dag_run = context["dag_run"]
    unsuccessful = []
    for task_id in required:
        task_instance = dag_run.get_task_instance(task_id)
        if task_instance is None or task_instance.state != "success":
            unsuccessful.append(f"{task_id}={getattr(task_instance, 'state', 'missing')}")
    if unsuccessful:
        raise AirflowException("Pipeline did not complete successfully: " + ", ".join(unsuccessful))


def create_pipeline_run_id(**context):
    """Return a deterministic UUID for this Airflow run, stable across task retries."""
    dag_run = context["dag_run"]
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{context['dag'].dag_id}:{dag_run.run_id}"))


def record_pipeline_failures(**context):
    """Persist Composer task failures to BigQuery; Airflow remains the fallback log."""
    from google.cloud import bigquery

    dag_run = context["dag_run"]
    dag = context["dag"]
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.pipeline_orchestration_failures"
    schema = [
        bigquery.SchemaField("dag_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("airflow_run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("task_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("task_state", "STRING"),
        bigquery.SchemaField("try_number", "INT64"),
        bigquery.SchemaField("started_at", "TIMESTAMP"),
        bigquery.SchemaField("ended_at", "TIMESTAMP"),
        bigquery.SchemaField("error_message", "STRING"),
        bigquery.SchemaField("log_url", "STRING"),
        bigquery.SchemaField("recorded_at", "TIMESTAMP", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="recorded_at")
    table.clustering_fields = ["dag_id", "task_id"]
    client.create_table(table, exists_ok=True)

    rows = []
    for ti in dag_run.get_task_instances():
        if ti.task_id in {"record_pipeline_failures", "confirm_pipeline_success"}:
            continue
        if ti.state not in {"failed", "upstream_failed"}:
            continue
        error = getattr(ti, "error", None)
        rows.append({
            "dag_id": dag.dag_id,
            "run_id": context["ti"].xcom_pull(task_ids="create_pipeline_run_id"),
            "airflow_run_id": dag_run.run_id,
            "task_id": ti.task_id,
            "task_state": ti.state,
            "try_number": ti.try_number,
            "started_at": ti.start_date.isoformat() if ti.start_date else None,
            "ended_at": ti.end_date.isoformat() if ti.end_date else None,
            "error_message": str(error)[:8000] if error else "Task failed; inspect the Airflow task log.",
            "log_url": ti.log_url,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        })
    if rows:
        row_ids = [hashlib.sha256(f"{row['dag_id']}:{row['run_id']}:{row['task_id']}".encode()).hexdigest()
                   for row in rows]
        errors = client.insert_rows_json(table_id, rows, row_ids=row_ids)
        if errors:
            raise AirflowException(f"Could not persist Composer failure records: {errors[:3]}")


with DAG(
    dag_id="bank_customer_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=MAX_ACTIVE_RUNS,
    dagrun_timeout=timedelta(hours=8),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["bank-analytics", "pyspark", "medallion", "dataproc"],
    doc_md=(
        "Creates a short-lived standard Dataproc cluster, processes Day 0 baseline then "
        "Day 1/Day 2 deltas, optionally replays both deltas for idempotency, builds Gold KPI views, "
        "runs acceptance checks, and deletes the cluster even when an upstream task fails."
    ),
) as dag:
    run_identity = PythonOperator(
        task_id="create_pipeline_run_id",
        python_callable=create_pipeline_run_id,
    )
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_ephemeral_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
        labels={"pipeline": "bank-customer-analytics", "lifecycle": "ephemeral"},
        delete_on_error=True,
        use_if_exists=False,
        deferrable=True,
    )

    day0 = DataprocSubmitJobOperator(
        task_id="process_day0_full_baseline",
        project_id=PROJECT_ID,
        region=REGION,
        job=make_job("day0", True),
        deferrable=True,
    )
    day1 = DataprocSubmitJobOperator(
        task_id="process_day1_incremental",
        project_id=PROJECT_ID,
        region=REGION,
        job=make_job("day1", False),
        deferrable=True,
    )
    day2 = DataprocSubmitJobOperator(
        task_id="process_day2_incremental",
        project_id=PROJECT_ID,
        region=REGION,
        job=make_job("day2", False),
        deferrable=True,
    )

    # Replay Day 1 and Day 2 after the latest snapshot; the downstream checks prove
    # the same business keys and row hashes remain unique.
    replay_day1 = DataprocSubmitJobOperator(
        task_id="replay_day1_idempotency_check",
        project_id=PROJECT_ID,
        region=REGION,
        job=make_job("day1", False),
        deferrable=True,
    )
    replay_day2 = DataprocSubmitJobOperator(
        task_id="replay_day2_idempotency_check",
        project_id=PROJECT_ID,
        region=REGION,
        job=make_job("day2", False),
        deferrable=True,
    )

    publish_gold_views = BigQueryInsertJobOperator(
        task_id="publish_gold_kpi_views",
        project_id=PROJECT_ID,
        location=REGION,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        configuration={"query": {"query": GOLD_SQL, "useLegacySql": False}},
    )

    acceptance_checks = BigQueryCheckOperator(
        task_id="verify_medallion_replay_dq_and_scd2",
        project_id=PROJECT_ID,
        location=REGION,
        use_legacy_sql=False,
        sql=f"""
        SELECT
          (SELECT COUNT(*) > 0 AND COUNT(*) = COUNT(DISTINCT TransactionID)
             FROM `{PROJECT_ID}.{DATASET}.fact_transaction`) AS fact_transaction_keys_unique,
          (SELECT COUNT(*) = COUNT(DISTINCT lineage_id)
             FROM `{PROJECT_ID}.{DATASET}.bronze_transactions`) AS bronze_source_records_unique,
          (SELECT NOT EXISTS (
             SELECT CustomerID FROM `{PROJECT_ID}.{DATASET}.dim_customer`
             WHERE _is_current GROUP BY CustomerID HAVING COUNT(*) > 1
           )) AS one_current_customer_version,
          (SELECT NOT EXISTS (
             SELECT AccountID FROM `{PROJECT_ID}.{DATASET}.dim_account`
             WHERE _is_current GROUP BY AccountID HAVING COUNT(*) > 1
           )) AS one_current_account_version,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.dim_customer`) AS customer_dimension_populated,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.dim_account`) AS account_dimension_populated,
          (SELECT COUNTIF(_is_current) > 0 FROM `{PROJECT_ID}.{DATASET}.dim_customer`) AS current_customer_versions_present,
          (SELECT COUNTIF(_is_current) > 0 FROM `{PROJECT_ID}.{DATASET}.dim_account`) AS current_account_versions_present,
          (SELECT NOT EXISTS (
             SELECT CustomerID FROM `{PROJECT_ID}.{DATASET}.dim_customer`
             GROUP BY CustomerID HAVING COUNTIF(_is_current) > 1
           )) AS no_overlapping_current_customer_keys,
          (SELECT COUNTIF(_effective_from IS NULL OR
                          (_effective_to IS NOT NULL AND _effective_to <= _effective_from) OR
                          (_is_current AND _effective_to IS NOT NULL) OR
                          (NOT _is_current AND _effective_to IS NULL)) = 0
             FROM `{PROJECT_ID}.{DATASET}.dim_customer`) AS customer_scd_intervals_valid,
          (SELECT NOT EXISTS (
             SELECT AccountID FROM `{PROJECT_ID}.{DATASET}.dim_account`
             GROUP BY AccountID HAVING COUNTIF(_is_current) > 1
           )) AS no_overlapping_current_account_keys,
          (SELECT COUNTIF(_effective_from IS NULL OR
                          (_effective_to IS NOT NULL AND _effective_to <= _effective_from) OR
                          (_is_current AND _effective_to IS NOT NULL) OR
                          (NOT _is_current AND _effective_to IS NULL)) = 0
             FROM `{PROJECT_ID}.{DATASET}.dim_account`) AS account_scd_intervals_valid,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.dim_date`) AS date_dimension_populated,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.dim_branch`) AS branch_dimension_populated,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.dim_account_type`) AS account_type_dimension_populated,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.dim_product`) AS product_dimension_populated,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.dim_transaction_type`) AS transaction_type_dimension_populated,
          (SELECT COUNTIF(customer_sk IS NULL OR account_sk IS NULL OR branch_sk IS NULL
                          OR account_type_sk IS NULL OR transaction_type_sk IS NULL OR date_sk IS NULL) = 0
             FROM `{PROJECT_ID}.{DATASET}.fact_transaction`) AS fact_dimension_keys_populated,
          (SELECT COUNTIF(_dq_reason IS NULL OR TRIM(_dq_reason) = '') = 0
             FROM `{PROJECT_ID}.{DATASET}.quarantine_rows`) AS rejected_rows_have_reasons,
          (SELECT COUNTIF(severity IS NULL OR severity NOT IN ('WARNING','ERROR')) = 0
             FROM `{PROJECT_ID}.{DATASET}.data_quality_findings`) AS anomaly_flags_well_formed,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.pipeline_audit`
             WHERE status = 'SUCCEEDED' AND batch_id = 'day0' AND run_id = '{RUN_UUID_TEMPLATE}' AND raw_record_count > 0) AS day0_audited,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.pipeline_audit`
             WHERE status = 'SUCCEEDED' AND batch_id = 'day1' AND run_id = '{RUN_UUID_TEMPLATE}' AND raw_record_count > 0) AS day1_audited,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.pipeline_audit`
             WHERE status = 'SUCCEEDED' AND batch_id = 'day2' AND run_id = '{RUN_UUID_TEMPLATE}' AND raw_record_count > 0) AS day2_audited
        """,
    )

    cleanup_cluster = DataprocDeleteClusterOperator(
        task_id="delete_ephemeral_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
        deferrable=True,
    )
    failure_audit = PythonOperator(
        task_id="record_pipeline_failures",
        python_callable=record_pipeline_failures,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )
    final_status = PythonOperator(
        task_id="confirm_pipeline_success",
        python_callable=require_successful_pipeline,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    replay_gate = ShortCircuitOperator(
        task_id="enable_demo_replay_checks",
        python_callable=lambda: ENABLE_REPLAY_CHECKS,
        ignore_downstream_trigger_rules=False,
    )

    create_cluster >> day0 >> day1 >> day2 >> replay_gate >> replay_day1 >> replay_day2
    day2 >> publish_gold_views
    replay_day2 >> publish_gold_views
    publish_gold_views >> acceptance_checks
    acceptance_checks >> cleanup_cluster
    # Ensure cleanup runs even if processing, replay, KPI publication, or checks fail.
    for task in (create_cluster, day0, day1, day2, replay_gate, replay_day1, replay_day2, publish_gold_views, acceptance_checks):
        task >> cleanup_cluster
    run_identity >> create_cluster
    cleanup_cluster >> failure_audit >> final_status

