# Bank customer transaction analytics — GCP First Project

This project implements the brief's repeatable, metadata-driven pipeline with GCS, PySpark on a regular Dataproc cluster, BigQuery medallion tables, and Cloud Composer orchestration. The sample extracts are synthetic because the actual BankEvalDB extracts were not supplied.

## Architecture

GCS inbound Day 0/Day 1/Day 2 → Composer DAG → ephemeral Dataproc cluster (PySpark) → BigQuery Bronze/Silver/Gold.

Composer creates the cluster, runs Day 0 as a full baseline, applies Day 1 and Day 2 as incremental Customer/Account loads, replays both delta days, publishes KPI views, checks the acceptance conditions, and deletes the cluster. A one-hour Dataproc auto-delete TTL backs up the cleanup task. Airflow orchestrates Spark jobs and view creation; it does not read source rows from GCS or move data between tables.

All three layers are in the single BigQuery dataset `bank_analytics`, with `bronze_*`/`silver_*` table prefixes and Gold dimension, fact, and KPI names. Separate datasets are optional; the brief asks for layers, not three datasets.

## Composer flow

The DAG is [dags/bank_customer_pipeline.py](dags/bank_customer_pipeline.py). It uses `DataprocCreateClusterOperator`, sequential `DataprocSubmitJobOperator` tasks, `BigQueryInsertJobOperator`, `BigQueryCheckOperator`, and an all-done cluster cleanup task. It runs the Day 1/Day 2 files twice to demonstrate replay safety. Spark errors fail the Dataproc task; `pipeline_audit` records STARTED/SUCCEEDED/FAILED, while failed rows/reasons are saved in `quarantine_rows` and anomaly warnings in `data_quality_findings`.

The DAG expects these project resources and permissions:

- Project `mygcpproject-515308`, region `us-east1`, bucket `id-bank-analytics`, dataset `bank_analytics`.
- Pipeline service account `bank-pipeline-sa@mygcpproject-515308.iam.gserviceaccount.com` with Dataproc worker, BigQuery job/data, and bucket permissions.
- Composer service account `bank-composer-sa@mygcpproject-515308.iam.gserviceaccount.com` with Composer worker and Dataproc permissions, plus permission to use the pipeline service account.
- The GCS `code/spark/pipeline.py`, `config/entities.json`, and `inbound/day0`, `inbound/day1`, and `inbound/day2` inputs.

Upload both `dags/bank_customer_pipeline.py` and `sql/gold_views.sql` to the same folder in the Composer DAG bucket. The DAG reads the SQL file beside itself. Wait for `bank_customer_pipeline` to appear in the Airflow UI, then trigger it manually with no run configuration. Watch task states and logs there; Dataproc job details and BigQuery outputs are also linked in Google Cloud Console.

Composer environment creation is not included in a run of `scripts/run_demo.sh`; that old script is a Serverless alternative. Composer charges while the environment exists. Agree on its lifetime before creating the environment, then delete it after the demonstration if it is no longer needed.

## Layers and outputs

- **Bronze:** raw source values, source URI, batch ID, entity, ingestion time, and deterministic row hash. Replays merge on row hash.
- **Silver:** metadata-driven casts, required fields, email/phone checks, key deduplication, configured reference checks, currency/date/sort-code/overdraft rules, and SCD Type 2 history for Customers and Accounts.
- **Gold star schema:** SCD Type 2 `dim_customer` and `dim_account`, conformed `dim_date`, `dim_branch`, `dim_account_type`, `dim_product`, and `dim_transaction_type`, plus `fact_transaction` at one row per accepted transaction. The fact stores dimension surrogate keys and resolves Customer/Account versions as of each transaction date. Reusable KPI views cover top customers, product value, transaction category/channel, branch trends, inactive accounts, suspicious transactions, loan repayment, card adoption, SCD2 history, and DQ summary.
- **Operational/DQ outputs:** `metadata_entity_config`, `metadata_validation_rules`, `quarantine_rows`, `data_quality_findings`, `data_quality_summary`, and `pipeline_audit`.

Structuring and dormant-reactivation outputs are documented heuristic proxies, not fraud determinations. Inactive-account analysis uses the latest transaction date in the dataset as its analysis date so historical demo data remains interpretable.

## Source file format and assumptions

Each UTF-8 file has interleaved entity blocks: `#ENTITY:<EntityName>`, a pipe-delimited header, and pipe-delimited rows. Quoting uses `"`. Entity order can vary; entity names, columns, keys, types, references, and rules live in `config/entities.json`. If actual source headers differ, update that config and the clearly marked parser only when the file syntax itself differs.

The synthetic baseline includes ten entities: Customers, Accounts, Transactions, Branches, AccountTypes, TransactionTypes, Loans, LoanPayments, Cards, and Products. Day 1 adds Customers 369/370 and Accounts 481/482; Day 2 adds Customers 371/372 and Accounts 483/484. Day 1 and Day 2 contain only Customer and Account blocks, as stated in the brief. Their changed existing keys create SCD2 versions; unseen keys are inserted. Bronze/Silver retain prior data, and the deterministic Gold star schema is refreshed from the accumulated Silver state after each batch.

Synthetic DQ cases exercise invalid/missing customer fields, conflicting customer key, exact duplicate transaction, currency mismatch, transaction date before account open, account/branch sort-code mismatch, overdraft breach, near-threshold deposit clustering, and dormant reactivation after a long inactivity gap. Results from these generated rows are demo evidence only, not profiling findings from real bank extracts.

## Current status

The project is deployed in Google Cloud project `mygcpproject-515308`. Composer environment `bank-analytics-composer` is retained for continued use. The end-to-end DAG is being rerun with the corrected GoogleSQL acceptance check, SCD2 version expectation, and inner joins that keep orphaned transactions out of the Gold fact. The first attempt completed all Spark batches, both replay checks, Gold view publication, and explicit Dataproc cleanup; its final acceptance check exposed those two issues and failed. Do not treat its output as a successful acceptance run.

The environment and project resources incur Google Cloud charges while they exist. The project owner can delete the Composer environment when it is no longer needed.
