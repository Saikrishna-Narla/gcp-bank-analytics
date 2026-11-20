"""Ephemeral-cluster Composer orchestration for the DummyBank hackathon demo."""
from datetime import datetime
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
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


PROJECT_ID = Variable.get("BANK_PROJECT_ID", default_var="mygcpproject-515308")
REGION = Variable.get("BANK_REGION", default_var="us-east1")
BUCKET = Variable.get("BANK_BUCKET", default_var="id-bank-analytics")
DATASET = Variable.get("BANK_DATASET", default_var="bank_analytics")
PIPELINE_SA = Variable.get(
    "BANK_PIPELINE_SA",
    default_var=f"bank-pipeline-sa@{PROJECT_ID}.iam.gserviceaccount.com",
)
PIPELINE_URI = f"gs://{BUCKET}/code/spark/pipeline.py"
CONFIG_URI = f"gs://{BUCKET}/config/entities.json"
CLUSTER_NAME = "bank-demo-{{ ts_nodash | lower }}"

# The SQL file is shipped beside this DAG in the Composer DAG bucket. Airflow reads
# the SQL definition; Spark, not Airflow, reads and transforms the GCS data.
GOLD_SQL = (Path(__file__).parent / "gold_views.sql").read_text(encoding="utf-8").replace(
    "__PROJECT__", PROJECT_ID
)

CLUSTER_CONFIG = {
    "config_bucket": BUCKET,
    "temp_bucket": BUCKET,
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
                f"--full-refresh={str(full_refresh).lower()}",
                f"--temporary-gcs-bucket={BUCKET}",
            ],
        },
    }


def require_successful_pipeline(**context):
    """Keep cleanup from masking an upstream DAG failure as a successful run."""
    required = [
        "create_ephemeral_dataproc_cluster",
        "process_day0_full_baseline",
        "process_day1_incremental",
        "process_day2_incremental",
        "replay_day1_idempotency_check",
        "replay_day2_idempotency_check",
        "publish_gold_kpi_views",
        "verify_medallion_replay_dq_and_scd2",
        "delete_ephemeral_dataproc_cluster",
    ]
    dag_run = context["dag_run"]
    unsuccessful = []
    for task_id in required:
        task_instance = dag_run.get_task_instance(task_id)
        if task_instance is None or task_instance.state != "success":
            unsuccessful.append(f"{task_id}={getattr(task_instance, 'state', 'missing')}")
    if unsuccessful:
        raise AirflowException("Pipeline did not complete successfully: " + ", ".join(unsuccessful))


with DAG(
    dag_id="bank_customer_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["hackathon", "pyspark", "medallion", "dataproc", "synthetic-demo"],
    doc_md=(
        "Creates a short-lived standard Dataproc cluster, processes Day 0 baseline then "
        "Day 1/Day 2 deltas, replays both deltas to prove idempotency, builds Gold KPI views, "
        "runs acceptance checks, and deletes the cluster even when an upstream task fails."
    ),
) as dag:
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
        configuration={"query": {"query": GOLD_SQL, "useLegacySql": False}},
    )

    acceptance_checks = BigQueryCheckOperator(
        task_id="verify_medallion_replay_dq_and_scd2",
        project_id=PROJECT_ID,
        location=REGION,
        use_legacy_sql=False,
        sql=f"""
        SELECT
          (SELECT COUNT(*) = COUNT(DISTINCT TransactionID)
             FROM `{PROJECT_ID}.{DATASET}.fact_transaction`) AS fact_transaction_keys_unique,
          (SELECT COUNT(*) = COUNT(DISTINCT CONCAT(_source_uri, ':', _source_record_number, ':', _row_hash))
             FROM `{PROJECT_ID}.{DATASET}.bronze_transactions`) AS bronze_source_records_unique,
          (SELECT NOT EXISTS (
             SELECT CustomerID FROM `{PROJECT_ID}.{DATASET}.dim_customer`
             WHERE _is_current GROUP BY CustomerID HAVING COUNT(*) > 1
           )) AS one_current_customer_version,
          (SELECT NOT EXISTS (
             SELECT AccountID FROM `{PROJECT_ID}.{DATASET}.dim_account`
             WHERE _is_current GROUP BY AccountID HAVING COUNT(*) > 1
           )) AS one_current_account_version,
          (SELECT COUNT(DISTINCT CustomerID) = 4
             FROM `{PROJECT_ID}.{DATASET}.dim_customer`
             WHERE _is_current AND CustomerID IN ('369','370','371','372')) AS expected_new_customers,
          (SELECT COUNT(DISTINCT AccountID) = 4
             FROM `{PROJECT_ID}.{DATASET}.dim_account`
             WHERE _is_current AND AccountID IN ('481','482','483','484')) AS expected_new_accounts,
          (SELECT COUNT(*) >= 2 FROM `{PROJECT_ID}.{DATASET}.dim_customer` WHERE CustomerID = '1') AS customer_scd2_history_retained,
          (SELECT COUNT(*) >= 2 FROM `{PROJECT_ID}.{DATASET}.dim_account` WHERE AccountID = '1') AS account_scd2_history_retained,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.dim_date`) AS date_dimension_populated,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.dim_branch`) AS branch_dimension_populated,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.dim_account_type`) AS account_type_dimension_populated,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.dim_product`) AS product_dimension_populated,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.dim_transaction_type`) AS transaction_type_dimension_populated,
          (SELECT COUNTIF(customer_sk IS NULL OR account_sk IS NULL OR branch_sk IS NULL
                          OR account_type_sk IS NULL OR transaction_type_sk IS NULL OR date_sk IS NULL) = 0
             FROM `{PROJECT_ID}.{DATASET}.fact_transaction`) AS fact_dimension_keys_populated,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.quarantine_rows`) AS rejected_rows_have_reasons,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.data_quality_findings`) AS anomaly_flags_present,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.pipeline_audit`
             WHERE status = 'SUCCEEDED' AND batch_id = 'day0') AS day0_audited,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.pipeline_audit`
             WHERE status = 'SUCCEEDED' AND batch_id = 'day1') AS day1_audited,
          (SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.{DATASET}.pipeline_audit`
             WHERE status = 'SUCCEEDED' AND batch_id = 'day2') AS day2_audited
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
    final_status = PythonOperator(
        task_id="confirm_pipeline_success",
        python_callable=require_successful_pipeline,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_cluster >> day0 >> day1 >> day2 >> replay_day1 >> replay_day2 >> publish_gold_views >> acceptance_checks
    acceptance_checks >> cleanup_cluster
    # Ensure cleanup runs even if processing, replay, KPI publication, or checks fail.
    for task in (create_cluster, day0, day1, day2, replay_day1, replay_day2, publish_gold_views, acceptance_checks):
        task >> cleanup_cluster
        task >> final_status
    cleanup_cluster >> final_status
