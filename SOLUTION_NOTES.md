# Hackathon solution notes

## Requirement fit

The attached brief requires a repeatable, metadata-driven pipeline for interleaved Day 0/1/2 extracts; GCS, Dataproc or Dataflow, and Python/PySpark; Bronze/Silver/Gold outputs; Composer orchestration; replay safety; SCD Type 2; validation/quarantine; and the listed customer, transaction, branch, product, loan, card, and anomaly analyses.

This implementation uses GCS → a regular, short-lived Dataproc cluster running PySpark → BigQuery. A Composer DAG creates the cluster, runs Day 0 as a full baseline, applies Day 1 and Day 2 as incremental customer/account deltas, optionally replays both deltas to demonstrate idempotency, publishes KPI views, verifies acceptance checks, then deletes the cluster. A one-hour Dataproc auto-delete TTL is the cleanup backstop. Composer orchestrates jobs; it does not move source data from GCS.

The three BigQuery medallion layers are represented by table families in the `bank_analytics` dataset: `bronze_*`, `silver_*`, and Gold dimensions/facts/KPI views. Three separate BigQuery datasets are not required by the brief.

## Processing and recovery

- Metadata tables expose entity names, keys, types, required fields, relationships, SCD settings, and validation rules for all ten source entities.
- Bronze retains raw values, input URI, batch ID, timestamp, entity, and deterministic row hash. Incremental/replay writes merge on row hash.
- Silver casts metadata-defined types, validates required fields and references, deduplicates business keys deterministically, quarantines rejected records with reasons, and maintains customer/account SCD Type 2 history.
- `quarantine_rows` stores rejected or duplicate records and reasons. `data_quality_findings` stores warning-level risk/anomaly findings while preserving those transactions in Silver for analysis. `pipeline_audit` records STARTED, SUCCEEDED, or FAILED outcomes and counts.
- The Composer DAG replays Day 1/Day 2 and checks unique transaction keys/hashes, one current SCD row per key, expected new IDs, non-empty quarantine/findings, and successful batch audit rows.
- The DAG uses a unique cluster name per run and a teardown task with an all-done trigger, plus Dataproc auto-delete TTL if teardown itself fails.
- Each DAG run has a stable UUIDv5 `run_id`; each physical source record has a deterministic UUIDv5 `lineage_id` derived from source URI, record number, and row hash. Both support tracing retries and replays across Bronze, Silver, Gold facts, and operational records.
- Composer failures are written to `pipeline_orchestration_failures` with task state and Airflow log URL. Spark failures remain in `pipeline_audit`. Airflow task logs remain the fallback when BigQuery is unavailable.
- Demo replay checks are disabled by default and enabled with `BANK_ENABLE_REPLAY_CHECKS=true`. The deployment bootstrap separates source and temporary buckets, enforces public access prevention, scopes bucket access, removes legacy project-level data grants, and applies a seven-day temporary-object lifecycle.

## Checks represented

The metadata and pipeline implement duplicate customer identity handling, required customer fields, invalid email/phone, exact duplicate transaction suppression with evidence, currency mismatch, transaction dates outside account lifecycle, account/branch sort-code prefix mismatch, overdraft limit violations, foreign-key orphans, near-threshold deposit structuring proxy, dormant reactivation burst proxy, and high-value transaction flags. Structuring and dormant-reactivation checks are explicitly heuristics, not fraud determinations.

## Gold outputs

The Gold star schema contains SCD2 customer/account dimensions, date, branch, account-type, product, and transaction-type dimensions, plus a transaction fact at one accepted row per TransactionID. The fact resolves customer/account surrogate keys using the effective date interval for the transaction and carries the other dimension keys for analytical joins. Incremental Day 1/Day 2 Customer/Account changes append SCD2 versions; new keys insert. Gold is deterministically rebuilt from accumulated Silver after each batch, so replaying a file does not add duplicate business records. Reusable views cover top five customers by transaction volume, product contribution, inactive accounts, suspicious transactions, transaction category/channel mix, branch trends, loan repayment health, card adoption, SCD2 history, and data-quality summary.

## Data and validation limits

The actual BankEvalDB Day 0/Day 1/Day 2 extracts were not provided. The local files are fictional synthetic records created to exercise the processing path, including validation and anomaly examples. Supporting-entity fields and several rule details are inferred from the brief and documented in `config/entities.json`; they must be matched to real extract headers and business definitions if those files become available. No demo row count or KPI should be described as a result from actual bank data.

The Composer environment `bank-analytics-composer` is deployed and retained. Run `manual__hackathon_demo_20261120_1740` completed successfully: Day 0 baseline, Day 1/Day 2 incremental loads, both delta replays, Gold KPI view publication, acceptance checks, ephemeral Dataproc cleanup, and final success confirmation all passed. That execution predates the current production-hardening changes. The actual BankEvalDB extracts were not supplied, so source-specific column mappings and business thresholds remain assumptions to validate against the real data before production use. The current fixed Day 0/1/2 DAG is demo-only and must not be scheduled for recurring production loads; see `docs/production_runbook.md` for release gates and operator steps. Composer remains active and incurs ongoing charges.

