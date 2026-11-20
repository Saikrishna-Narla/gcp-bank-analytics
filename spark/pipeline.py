"""Metadata-driven GCS -> Dataproc PySpark -> BigQuery medallion pipeline.

Input contract: UTF-8 files with #ENTITY:<name> blocks and pipe-delimited,
headered rows. See README.md and SOLUTION_NOTES.md for demo assumptions.
"""
import argparse
import csv
import hashlib
import json
import re
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window, functions as F, types as T
from pyspark.storagelevel import StorageLevel


PROCESS_ORDER = [
    "Customers", "Branches", "Products", "AccountTypes", "TransactionTypes",
    "Accounts", "Transactions", "Loans", "LoanPayments", "Cards",
]


def parse_interleaved_file(text, marker_regex, delimiter="|", quotechar='"'):
    """Parse entity blocks without assuming block order; retain malformed rows."""
    marker = re.compile(marker_regex)
    entities, current, header, rows = {}, None, None, []

    def flush():
        nonlocal rows
        if current and header is not None:
            entities.setdefault(current, []).extend(rows)
        rows = []

    for line_number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        match = marker.match(line.strip())
        if match:
            flush()
            current = match.group("entity")
            header = None
            continue
        if current is None:
            continue
        fields = next(csv.reader([line], delimiter=delimiter, quotechar=quotechar))
        if header is None:
            header = [name.strip() for name in fields]
            continue
        if len(fields) != len(header):
            record = dict(zip(header, fields[:len(header)]))
            record["__parse_error"] = f"wrong_field_count:expected={len(header)}:actual={len(fields)}:line={line_number}"
            record["__raw_record"] = line
            record["__source_record_number"] = str(line_number)
            rows.append(record)
        else:
            record = dict(zip(header, fields))
            record["__source_record_number"] = str(line_number)
            rows.append(record)
    flush()
    return entities


def stable_hash(entity, record):
    business_record = {key: value for key, value in record.items() if key != "__source_record_number"}
    raw = json.dumps({"entity": entity, "record": business_record}, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_file_rows(file_record, marker_regex, delimiter, quotechar, batch_id, run_id, ingested_at,
                    max_source_file_bytes):
    """Parse one source object on a Spark executor and emit compact JSON rows."""
    uri, content = file_record
    if len(content.encode("utf-8")) > max_source_file_bytes:
        raise ValueError(
            f"Source object exceeds the configured {max_source_file_bytes}-byte limit: {uri}"
        )
    blocks = parse_interleaved_file(content, marker_regex, delimiter, quotechar)
    for entity, rows in blocks.items():
        for row in rows:
            row_hash = stable_hash(entity, row)
            source_record_number = row.get("__source_record_number")
            lineage_id = str(uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{uri}:{source_record_number}:{row_hash}",
            ))
            yield (
                entity,
                uri,
                str(source_record_number) if source_record_number is not None else None,
                json.dumps(row, sort_keys=True, ensure_ascii=False),
                row_hash,
                lineage_id,
                batch_id,
                run_id,
                ingested_at,
            )


def append_reason(df, condition, reason):
    if "_dq_reason" not in df.columns:
        df = df.withColumn("_dq_reason", F.lit(None).cast("string"))
    return df.withColumn(
        "_dq_reason",
        F.when(
            condition,
            F.when(F.col("_dq_reason").isNull(), F.lit(reason)).otherwise(
                F.concat_ws(";", F.col("_dq_reason"), F.lit(reason))
            ),
        ).otherwise(F.col("_dq_reason")),
    )


def is_missing(column):
    return F.col(column).isNull() | (F.trim(F.col(column).cast("string")) == "")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-uri", required=True)
    parser.add_argument("--config-uri", required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", default="bank_analytics")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--full-refresh", default="false")
    parser.add_argument("--temporary-gcs-bucket", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("bank-customer-pipeline").getOrCreate()
    now = datetime.now(timezone.utc).isoformat()
    full_refresh = args.full_refresh.lower() == "true"
    bq = lambda table: f"{args.project}.{args.dataset}.{table}"

    def write(df, table, mode="append"):
        (df.write.format("bigquery").option("table", bq(table))
         .option("temporaryGcsBucket", args.temporary_gcs_bucket)
         .mode(mode).save())

    def read_existing(table):
        try:
            return spark.read.format("bigquery").option("table", bq(table)).load()
        except Exception as exc:
            message = str(exc).lower()
            if "not found" in message or "notfound" in message or "does not exist" in message:
                return None
            raise

    def audit(status, file_count, raw_count, parsed_count, quarantine_count=0, error=None):
        schema = T.StructType([
            T.StructField("batch_id", T.StringType()),
            T.StructField("run_id", T.StringType()),
            T.StructField("input_uri", T.StringType()),
            T.StructField("processed_at", T.StringType()),
            T.StructField("status", T.StringType()),
            T.StructField("file_count", T.LongType()),
            T.StructField("raw_record_count", T.LongType()),
            T.StructField("parsed_row_count", T.LongType()),
            T.StructField("quarantine_row_count", T.LongType()),
            T.StructField("error_type", T.StringType()),
            T.StructField("error_message", T.StringType()),
        ])
        record = [(args.batch_id, args.run_id, args.input_uri, datetime.now(timezone.utc).isoformat(), status,
                   int(file_count), int(raw_count), int(parsed_count), int(quarantine_count),
                   type(error).__name__ if error else None, str(error)[:4000] if error else None)]
        # Audit is append-only even for a baseline refresh. Overwriting this table
        # would erase STARTED and prior run evidence before the run has succeeded.
        write(spark.createDataFrame(record, schema), "pipeline_audit", "append")

    try:
        config_text = "\n".join(row[0] for row in spark.read.text(args.config_uri).collect())
        config = json.loads(config_text)
        marker = config["entity_marker_regex"]
        raw_files = spark.sparkContext.wholeTextFiles(args.input_uri).persist(StorageLevel.DISK_ONLY)
        file_count = raw_files.count()
        audit("STARTED", file_count, 0, 0)
        if not file_count:
            raise ValueError(f"No input files matched {args.input_uri}")

        parsed_at = datetime.now(timezone.utc).isoformat()
        row_schema = T.StructType([
            T.StructField("_entity", T.StringType(), False),
            T.StructField("_source_uri", T.StringType(), False),
            T.StructField("_source_record_number", T.StringType(), True),
            T.StructField("_record_json", T.StringType(), False),
            T.StructField("_row_hash", T.StringType(), False),
            T.StructField("lineage_id", T.StringType(), False),
            T.StructField("_batch_id", T.StringType(), False),
            T.StructField("_run_id", T.StringType(), False),
            T.StructField("_ingested_at", T.StringType(), False),
        ])
        record_rdd = raw_files.flatMap(lambda item: parse_file_rows(
            item, marker, config.get("delimiter", "|"), config.get("quotechar", '"'),
            args.batch_id, args.run_id, parsed_at,
            int(config.get("max_source_file_bytes", 67108864)),
        ))
        records = spark.createDataFrame(record_rdd, row_schema).persist(StorageLevel.DISK_ONLY)
        raw_record_count = records.count()
        if not raw_record_count:
            raise ValueError(f"Input files matched {args.input_uri}, but contained no entity records")

        # Publish active metadata so the parser, schema and rule registry are inspectable.
        entity_records = [
            (name, meta["table"], json.dumps(meta["key"]), json.dumps(meta["columns"]),
             json.dumps(meta.get("required", [])), bool(meta.get("scd2", False)),
             json.dumps(meta.get("references", [])), json.dumps(meta.get("rules", {})))
            for name, meta in config["entities"].items()
        ]
        entity_schema = T.StructType([
            T.StructField("entity", T.StringType()), T.StructField("table_name", T.StringType()),
            T.StructField("key_json", T.StringType()), T.StructField("columns_json", T.StringType()),
            T.StructField("required_json", T.StringType()), T.StructField("scd2", T.BooleanType()),
            T.StructField("references_json", T.StringType()), T.StructField("rules_json", T.StringType()),
        ])
        write(spark.createDataFrame(entity_records, entity_schema), "metadata_entity_config", "overwrite")
        rules = [(name, json.dumps(value)) for name, value in config.get("rules", {}).items()]
        rule_schema = T.StructType([T.StructField("rule_name", T.StringType()), T.StructField("rule_value", T.StringType())])
        write(spark.createDataFrame(rules, rule_schema), "metadata_validation_rules", "overwrite")

        # Parse each file on a Spark executor and keep row processing distributed.
        # The source object itself is still one Spark input partition; publishers must
        # deliver bounded files and enough objects to provide parallelism.
        entity_frames = {}
        known_entities = list(config["entities"])
        for entity, meta in config["entities"].items():
            entity_records = records.filter(F.col("_entity") == entity)
            if entity_records.limit(1).count() == 0:
                continue
            source_fields = T.StructType(
                [T.StructField(name, T.StringType(), True) for name in meta["columns"]]
                + [T.StructField("__parse_error", T.StringType(), True),
                   T.StructField("__raw_record", T.StringType(), True),
                   T.StructField("__source_record_number", T.StringType(), True)]
            )
            parsed_record = F.from_json(F.col("_record_json"), source_fields)
            lineage_columns = [
                "_source_uri", "_source_record_number", "_batch_id", "_run_id",
                "lineage_id", "_row_hash", "_ingested_at",
            ]
            data_columns = [F.col(f"_parsed.`{name}`").alias(name) for name in meta["columns"]]
            source = entity_records.withColumn("_parsed", parsed_record).select(
                *data_columns,
                *[F.col(name) for name in lineage_columns],
                F.col("_parsed.__parse_error").alias("_parse_error"),
                F.col("_parsed.__raw_record").alias("_raw_record"),
                F.col("_record_json").alias("_raw_payload"),
            ).persist(StorageLevel.DISK_ONLY)
            entity_frames[entity] = source
            table = "bronze_" + meta["table"]
            incoming = source.dropDuplicates(["_source_uri", "_source_record_number", "_row_hash"])
            if full_refresh:
                write(incoming, table, "overwrite")
            else:
                old = read_existing(table)
                if old is not None:
                    result = old.unionByName(incoming, allowMissingColumns=True).dropDuplicates(["_source_uri", "_source_record_number", "_row_hash"])
                else:
                    result = incoming
                write(result, table, "overwrite")

        # Convert and validate one entity at a time in dependency order so referential checks
        # can see same-run parent records as well as previously loaded parent records.
        configs = config["entities"]
        order = [name for name in PROCESS_ORDER if name in configs]
        order.extend(name for name in configs if name not in order)
        written_current = {}
        quarantine_frames = []
        parsed_count = records.filter(F.col("_entity").isin(known_entities)).count()
        unknown = records.filter(~F.col("_entity").isin(known_entities)).select(
            F.col("_entity"), "_source_uri", "_source_record_number", "_batch_id", "_run_id",
            "lineage_id", "_row_hash", "_ingested_at", F.col("_record_json").alias("_raw_payload"),
        ).withColumn("_dq_reason", F.lit("unknown_entity")).withColumn("_dq_severity", F.lit("ERROR"))
        if unknown.limit(1).count():
            quarantine_frames.append(unknown)

        def current_reference(entity):
            if entity in written_current:
                return written_current[entity]
            meta = configs[entity]
            existing = None if full_refresh else read_existing("silver_" + meta["table"])
            if existing is not None and "_is_current" in existing.columns:
                existing = existing.filter(F.col("_is_current"))
            return existing

        for entity in order:
            meta = configs[entity]
            df = entity_frames.get(entity)
            if df is None:
                continue
            column_names = list(meta["columns"])
            for column, type_name in meta["columns"].items():
                df = df.withColumn("__raw_" + column, F.col(column))
                df = df.withColumn(column, F.col(column).cast(type_name))
            df = df.withColumn("_dq_reason", F.lit(None).cast("string"))
            for required in meta.get("required", []):
                df = append_reason(df, is_missing(required), "required:" + required)
            for column in meta.get("email_columns", []):
                df = append_reason(df, F.col(column).isNotNull() & ~F.col(column).rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), "invalid_email:" + column)
            for column in meta.get("phone_columns", []):
                df = append_reason(df, F.col(column).isNotNull() & ~F.regexp_replace(F.col(column), r"[+ ()-]", "").rlike(r"^\d{8,15}$"), "invalid_phone:" + column)
            for column, type_name in meta["columns"].items():
                raw_column = F.col("__raw_" + column)
                if type_name.upper() not in ("STRING", "BOOL", "BOOLEAN"):
                    df = append_reason(df, raw_column.isNotNull() & (F.trim(raw_column) != "") & F.col(column).isNull(), "invalid_type:" + column)
            df = append_reason(df, F.col("_parse_error").isNotNull(), "malformed_record")

            # Cross-entity referential integrity is configuration-driven.
            for reference in meta.get("references", []):
                source_column = reference["column"]
                parent = reference["entity"]
                target_column = reference.get("target_column", source_column)
                if source_column not in df.columns:
                    continue
                target_df = current_reference(parent)
                if target_df is None or target_column not in target_df.columns:
                    df = append_reason(df, F.col(source_column).isNotNull(), "orphan:" + source_column + "->" + parent)
                else:
                    if "_is_current" in target_df.columns:
                        target_df = target_df.filter(F.col("_is_current"))
                    target_keys = target_df.select(F.col(target_column).alias("__ref_key")).distinct()
                    df = df.join(target_keys, F.col(source_column) == F.col("__ref_key"), "left")
                    df = append_reason(df, F.col(source_column).isNotNull() & F.col("__ref_key").isNull(), "orphan:" + source_column + "->" + parent)
                    df = df.drop("__ref_key")

            # Account-level consistency checks.
            if entity == "Accounts":
                branches = current_reference("Branches")
                if meta.get("rules", {}).get("sort_code_prefix_match") and branches is not None and "SortCode" in df.columns and "SortCode" in branches.columns and "BranchID" in df.columns:
                    branch_keys = branches.select("BranchID", F.col("SortCode").alias("__branch_sort_code")).distinct()
                    df = df.join(branch_keys, "BranchID", "left")
                    prefix_length = int(config.get("rules", {}).get("sort_code_prefix_length", 2))
                    df = append_reason(df, F.col("SortCode").isNotNull() & F.col("__branch_sort_code").isNotNull() &
                                       (F.substring("SortCode", 1, prefix_length) != F.substring("__branch_sort_code", 1, prefix_length)),
                                       "branch_sort_code_mismatch")
                    df = df.drop("__branch_sort_code")
                account_types = current_reference("AccountTypes")
                if meta.get("rules", {}).get("overdraft_limit_check") and account_types is not None and "Balance" in df.columns and "AccountTypeID" in df.columns:
                    limits = account_types.select("AccountTypeID", F.col("OverdraftLimit").alias("__overdraft_limit")).distinct()
                    df = df.join(limits, "AccountTypeID", "left")
                    df = append_reason(df, F.col("Balance").isNotNull() & F.col("__overdraft_limit").isNotNull() &
                                       (F.col("Balance") < -F.col("__overdraft_limit")), "overdraft_limit_exceeded")
                    df = df.drop("__overdraft_limit")

            # Transaction/account currency and valid account-date windows.
            if entity == "Transactions":
                accounts = current_reference("Accounts")
                if accounts is not None and "AccountID" in df.columns:
                    account_cols = ["AccountID"]
                    for name in ("Currency", "OpenDate", "CloseDate"):
                        if name in accounts.columns:
                            account_cols.append(name)
                    account_ref = accounts.select(*account_cols).dropDuplicates(["AccountID"])
                    for name in account_cols:
                        if name != "AccountID":
                            account_ref = account_ref.withColumnRenamed(name, "__account_" + name)
                    df = df.join(account_ref, "AccountID", "left")
                    if meta.get("rules", {}).get("currency_match_to_account") and "Currency" in df.columns and "__account_Currency" in df.columns:
                        df = append_reason(df, F.col("Currency").isNotNull() & F.col("__account_Currency").isNotNull() &
                                           (F.upper(F.col("Currency")) != F.upper(F.col("__account_Currency"))), "currency_mismatch")
                    if meta.get("rules", {}).get("transaction_date_within_account_lifecycle") and "TransactionDate" in df.columns and "__account_OpenDate" in df.columns:
                        df = append_reason(df, F.col("TransactionDate").isNotNull() & F.col("__account_OpenDate").isNotNull() &
                                           (F.col("TransactionDate") < F.col("__account_OpenDate")), "transaction_before_account_open")
                    if meta.get("rules", {}).get("transaction_date_within_account_lifecycle") and "TransactionDate" in df.columns and "__account_CloseDate" in df.columns:
                        df = append_reason(df, F.col("TransactionDate").isNotNull() & F.col("__account_CloseDate").isNotNull() &
                                           (F.col("TransactionDate") > F.col("__account_CloseDate")), "transaction_after_account_close")
                    df = df.drop(*[column for column in df.columns if column.startswith("__account_")])

            keys = meta["key"]
            key_counts = df.groupBy(*keys).count().filter(F.col("count") > 1).select(*keys).withColumn("__duplicate_key", F.lit(True))
            df = df.join(key_counts, keys, "left")
            window = Window.partitionBy(*keys).orderBy(
                F.col("effective_at").desc_nulls_last() if "effective_at" in df.columns else F.col("_row_hash").asc(),
                F.col("_row_hash").asc(),
            )
            df = df.withColumn("__key_rank", F.row_number().over(window))
            df = append_reason(df, F.col("__duplicate_key").isNotNull() & (F.col("__key_rank") > 1), "duplicate_business_key")
            bad = df.filter(F.col("_dq_reason").isNotNull()).drop("__key_rank", "__duplicate_key")
            if bad.limit(1).count():
                bad = bad.withColumn("_entity", F.lit(entity)).withColumn("_dq_severity", F.lit("ERROR"))
                quarantine_frames.append(bad)
            good = df.filter(F.col("_dq_reason").isNull() & (F.col("__key_rank") == 1)).drop("__key_rank", "__duplicate_key", "_dq_reason")
            good = good.drop(*[column for column in good.columns if column.startswith("__raw_")])

            table = "silver_" + meta["table"]
            if meta.get("scd2"):
                effective = F.coalesce(F.col("effective_at"), F.current_timestamp()) if "effective_at" in good.columns else F.current_timestamp()
                staged = good.withColumn("_effective_from", effective).withColumn("_is_current", F.lit(True)).withColumn("_effective_to", F.lit(None).cast("timestamp"))
                old = None if full_refresh else read_existing(table)
                if old is None:
                    result = staged
                else:
                    if "_row_hash" in old.columns:
                        staged = staged.join(old.select("_row_hash").distinct(), ["_row_hash"], "left_anti")
                    current_old = old.filter(F.col("_is_current")).alias("o")
                    incoming_new = staged.alias("n")
                    late = (incoming_new.join(current_old, keys, "inner")
                            .filter(F.col("n._effective_from") <= F.col("o._effective_from"))
                            .select("n.*")
                            .withColumn("_dq_reason", F.lit("scd2_out_of_order_effective_at"))
                            .withColumn("_dq_severity", F.lit("ERROR"))
                            .withColumn("_entity", F.lit(entity)))
                    if late.limit(1).count():
                        quarantine_frames.append(late)
                    staged = staged.join(late.select(*keys).distinct(), keys, "left_anti")
                    compare = [name for name in meta["columns"] if name not in keys and name != "effective_at"]
                    joined = old.filter(F.col("_is_current")).alias("o").join(staged.alias("n"), keys, "inner")
                    changed = F.lit(False)
                    for name in compare:
                        changed = changed | ~F.col("o." + name).eqNullSafe(F.col("n." + name))
                    changed_keys = (joined.filter(changed)
                                    .select(*[F.col("n." + name).alias(name) for name in keys],
                                            F.col("n._effective_from").alias("__close_at"))
                                    .dropDuplicates(keys))
                    closed = old.join(changed_keys, keys, "left")
                    closed = (closed.withColumn("_effective_to", F.when(F.col("_is_current") & F.col("__close_at").isNotNull(), F.col("__close_at")).otherwise(F.col("_effective_to")))
                              .withColumn("_is_current", F.when(F.col("__close_at").isNotNull(), F.lit(False)).otherwise(F.col("_is_current")))
                              .drop("__close_at"))
                    inserts = staged.join(changed_keys.select(*keys), keys, "inner")
                    current_keys = old.filter(F.col("_is_current")).select(*keys).distinct()
                    new_keys = staged.join(current_keys, keys, "left_anti")
                    result = closed.unionByName(inserts, allowMissingColumns=True).unionByName(new_keys, allowMissingColumns=True)
                write(result, table, "overwrite")
                written_current[entity] = result.filter(F.col("_is_current"))
            else:
                if entity == "Transactions":
                    old = None if full_refresh else read_existing(table)
                    if old is None:
                        result = good
                    else:
                        existing = old.select(*keys, F.col("_row_hash").alias("__existing_row_hash")).dropDuplicates(keys)
                        conflicts = (good.alias("incoming").join(existing.alias("stored"), keys, "inner")
                                     .filter(F.col("incoming._row_hash") != F.col("stored.__existing_row_hash"))
                                     .select("incoming.*")
                                     .withColumn("_dq_reason", F.lit("transaction_business_key_conflict"))
                                     .withColumn("_dq_severity", F.lit("ERROR"))
                                     .withColumn("_entity", F.lit(entity)))
                        if conflicts.limit(1).count():
                            quarantine_frames.append(conflicts)
                        new_rows = good.join(existing.select(*keys).distinct(), keys, "left_anti")
                        result = old.unionByName(new_rows, allowMissingColumns=True)
                    write(result, table, "overwrite")
                    written_current[entity] = result
                else:
                    old = None if full_refresh else read_existing(table)
                    if old is None:
                        result = good
                    else:
                        merged = old.unionByName(good, allowMissingColumns=True)
                        latest = Window.partitionBy(*keys).orderBy(
                            F.col("_ingested_at").desc_nulls_last(),
                            F.col("_batch_id").desc_nulls_last(),
                            F.col("_row_hash").desc(),
                        )
                        result = (merged.withColumn("__latest_rank", F.row_number().over(latest))
                                  .filter(F.col("__latest_rank") == 1).drop("__latest_rank"))
                    write(result, table, "overwrite")
                    written_current[entity] = result

        quarantine_count = 0
        if quarantine_frames:
            quarantine = quarantine_frames[0]
            for frame in quarantine_frames[1:]:
                quarantine = quarantine.unionByName(frame, allowMissingColumns=True)
            quarantine_count = quarantine.count()
            old_quarantine = None if full_refresh else read_existing("quarantine_rows")
            if old_quarantine is not None:
                quarantine = old_quarantine.unionByName(quarantine, allowMissingColumns=True)
            quarantine = quarantine.dropDuplicates(["_entity", "_source_uri", "_source_record_number", "_row_hash", "_dq_reason"])
            write(quarantine, "quarantine_rows", "overwrite")
        elif full_refresh:
            empty = spark.createDataFrame([], T.StructType([
                T.StructField("_entity", T.StringType()), T.StructField("_row_hash", T.StringType()),
                T.StructField("_dq_reason", T.StringType()), T.StructField("_dq_severity", T.StringType()),
            ]))
            write(empty, "quarantine_rows", "overwrite")

        # Build Gold star schema from the complete persisted Silver state. Source
        # processing remains incremental; rebuilding these compact demo Gold tables
        # from Silver makes each publication deterministic and replay-safe.
        silver = {}
        for entity in configs:
            frame = read_existing("silver_" + configs[entity]["table"])
            if frame is not None:
                silver[entity] = frame
        def with_surrogate(frame, natural_key, surrogate_key, versioned=False):
            identity = F.concat_ws("||", F.col(natural_key), F.col("_effective_from").cast("string")) if versioned else F.col(natural_key)
            return frame.withColumn(surrogate_key, F.sha2(identity, 256))

        if "Customers" in silver:
            customers = with_surrogate(silver["Customers"], "CustomerID", "customer_sk", True)
            write(customers, "dim_customer", "overwrite")
            silver["Customers"] = customers
        if "Accounts" in silver:
            accounts = with_surrogate(silver["Accounts"], "AccountID", "account_sk", True)
            write(accounts, "dim_account", "overwrite")
            silver["Accounts"] = accounts
        if "Branches" in silver:
            branches = with_surrogate(silver["Branches"], "BranchID", "branch_sk")
            write(branches, "dim_branch", "overwrite")
            silver["Branches"] = branches
        if "TransactionTypes" in silver:
            tx_types = with_surrogate(silver["TransactionTypes"], "TransactionTypeID", "transaction_type_sk")
            write(tx_types, "dim_transaction_type", "overwrite")
            silver["TransactionTypes"] = tx_types
        if "AccountTypes" in silver:
            account_types = silver["AccountTypes"]
            account_types = with_surrogate(account_types, "AccountTypeID", "account_type_sk")
            write(account_types, "dim_account_type", "overwrite")
            if "Products" in silver:
                products = with_surrogate(silver["Products"], "ProductID", "product_sk")
                write(products, "dim_product", "overwrite")
                silver["Products"] = products
            silver["AccountTypes"] = account_types
        if "Transactions" in silver:
            tx = silver["Transactions"]
            dates = tx.select(F.to_date("TransactionDate").alias("full_date")).where(F.col("full_date").isNotNull()).distinct()
            date_dim = (dates.withColumn("date_sk", F.date_format("full_date", "yyyyMMdd").cast("int"))
                        .withColumn("year", F.year("full_date"))
                        .withColumn("quarter", F.quarter("full_date"))
                        .withColumn("month", F.month("full_date"))
                        .withColumn("day", F.dayofmonth("full_date"))
                        .withColumn("day_of_week", F.dayofweek("full_date")))
            write(date_dim, "dim_date", "overwrite")

        if all(name in silver for name in ("Accounts", "Customers", "Transactions")):
            # One fact row per accepted transaction. Dimension versions are selected
            # as they were effective on the event date, preserving SCD2 history.
            tx = silver["Transactions"].alias("t")
            account_versions = silver["Accounts"].alias("a")
            fact = tx.join(
                account_versions,
                (F.col("t.AccountID") == F.col("a.AccountID")) &
                (F.to_timestamp(F.col("t.TransactionDate")) >= F.col("a._effective_from")) &
                (F.col("a._effective_to").isNull() | (F.to_timestamp(F.col("t.TransactionDate")) < F.col("a._effective_to"))),
                "inner",
            )
            customer_versions = silver["Customers"].alias("c")
            fact = fact.join(
                customer_versions,
                (F.col("a.CustomerID") == F.col("c.CustomerID")) &
                (F.to_timestamp(F.col("t.TransactionDate")) >= F.col("c._effective_from")) &
                (F.col("c._effective_to").isNull() | (F.to_timestamp(F.col("t.TransactionDate")) < F.col("c._effective_to"))),
                "inner",
            )
            if "AccountTypes" in silver:
                account_types = silver["AccountTypes"].select("AccountTypeID", "ProductID").alias("at")
                fact = fact.join(account_types, F.col("a.AccountTypeID") == F.col("at.AccountTypeID"), "left")
            fact = fact.select(
                F.col("t.TransactionID").alias("TransactionID"),
                F.sha2(F.col("t.TransactionID"), 256).alias("transaction_sk"),
                F.col("t.TransactionDate").alias("TransactionDate"),
                F.date_format("t.TransactionDate", "yyyyMMdd").cast("int").alias("date_sk"),
                F.col("t.AccountID").alias("AccountID"), F.col("a.account_sk").alias("account_sk"),
                F.col("a.CustomerID").alias("CustomerID"), F.col("c.customer_sk").alias("customer_sk"),
                F.col("a.BranchID").alias("BranchID"), F.col("a.AccountTypeID").alias("AccountTypeID"),
                F.sha2(F.col("a.BranchID"), 256).alias("branch_sk"),
                F.sha2(F.col("a.AccountTypeID"), 256).alias("account_type_sk"),
                F.sha2(F.col("at.ProductID"), 256).alias("product_sk") if "AccountTypes" in silver else F.lit(None).cast("string").alias("product_sk"),
                F.col("a.Currency").alias("AccountCurrency"),
                F.col("t.TransactionTypeID").alias("TransactionTypeID"),
                F.sha2(F.col("t.TransactionTypeID"), 256).alias("transaction_type_sk"),
                F.col("t.Amount").alias("Amount"), F.col("t.Currency").alias("Currency"),
                F.col("t.Channel").alias("Channel"), F.col("t._batch_id").alias("_batch_id"),
                F.col("t._run_id").alias("_run_id"), F.col("t.lineage_id").alias("lineage_id"),
                F.col("t._source_uri").alias("_source_uri"), F.col("t._row_hash").alias("_row_hash"),
            )
            write(fact.dropDuplicates(["TransactionID"]), "fact_transaction", "overwrite")

        # Risk/anomaly findings are visible separately; flagged transactions remain queryable in Silver/Gold.
        if "Transactions" in silver:
            tx = silver["Transactions"]
            rules = config.get("rules", {})
            suspicious_threshold = float(rules.get("suspicious_amount_gbp", 100000))
            finding_frames = []

            def add_findings(frame, condition, rule_name):
                finding_frames.append(
                    frame.filter(condition).select(
                        F.lit("Transactions").alias("entity"), F.lit(rule_name).alias("rule_name"),
                        F.lit("WARNING").alias("severity"), "_batch_id", "_source_uri", "_row_hash",
                        "_run_id", "lineage_id",
                        F.to_json(F.struct("TransactionID", "AccountID", "Amount", "Currency", "Channel", "TransactionDate")).alias("record_json"),
                        F.current_timestamp().cast("string").alias("detected_at"),
                    )
                )

            add_findings(tx, F.abs(F.col("Amount")) >= suspicious_threshold, "high_value_transaction")

            # Structuring is an explicitly heuristic proxy: repeated, near-threshold credits
            # through ATM/branch channels for the same account within the configured window.
            transaction_types = silver.get("TransactionTypes")
            if transaction_types is not None:
                type_cols = ["TransactionTypeID"]
                for column in ("TransactionTypeName", "Category", "IsCredit"):
                    if column in transaction_types.columns:
                        type_cols.append(column)
                typed_tx = tx.join(transaction_types.select(*type_cols).dropDuplicates(["TransactionTypeID"]), "TransactionTypeID", "left")
                is_deposit = F.lower(F.coalesce(F.col("TransactionTypeName"), F.lit(""))).contains("deposit") | F.lower(F.coalesce(F.col("Category"), F.lit(""))).contains("deposit")
                is_credit = F.lower(F.coalesce(F.col("IsCredit").cast("string"), F.lit("false"))).isin("true", "1", "yes")
                channels = [str(value).upper() for value in rules.get("structuring_channels", ["ATM", "BRANCH"])]
                allowed_channel = F.upper(F.col("Channel")).isin(channels)
                threshold = float(rules.get("structuring_threshold_gbp", 9000))
                ratio = float(rules.get("structuring_near_threshold_ratio", 0.9))
                near_threshold = (F.abs(F.col("Amount")) >= threshold * ratio) & (F.abs(F.col("Amount")) < threshold)
                qualifies = near_threshold & allowed_channel & (is_deposit | is_credit)
                window_days = int(rules.get("structuring_window_days", 7))
                account_window = Window.partitionBy("AccountID").orderBy(F.col("TransactionDate").cast("timestamp").cast("long")).rangeBetween(-window_days * 86400, 0)
                typed_tx = typed_tx.withColumn("__qualifying_near_txn", F.when(qualifies, 1).otherwise(0))
                typed_tx = typed_tx.withColumn("__near_count", F.sum("__qualifying_near_txn").over(account_window))
                add_findings(typed_tx, qualifies & (F.col("__near_count") >= int(rules.get("structuring_min_count", 3))), "structuring_proxy")

            # Dormant reactivation proxy: an account is quiet for the configured period,
            # then has a burst of transactions inside the rolling window.
            seconds = int(rules.get("structuring_window_days", 7)) * 86400
            account_window = Window.partitionBy("AccountID").orderBy(F.col("TransactionDate").cast("timestamp").cast("long")).rangeBetween(-seconds, 0)
            sequence = Window.partitionBy("AccountID").orderBy(F.col("TransactionDate").cast("timestamp").cast("long"))
            dormant_tx = (tx.withColumn("__previous_date", F.lag("TransactionDate").over(sequence))
                          .withColumn("__recent_count", F.count(F.lit(1)).over(account_window)))
            dormant_condition = (F.datediff(F.col("TransactionDate"), F.col("__previous_date")) > int(rules.get("inactive_days", 90))) & (F.col("__recent_count") >= int(rules.get("dormant_burst_min_count", 3)))
            add_findings(dormant_tx, dormant_condition, "dormant_reactivation_proxy")
            if finding_frames:
                findings = finding_frames[0]
                for frame in finding_frames[1:]:
                    findings = findings.unionByName(frame)
                write(findings.dropDuplicates(["entity", "rule_name", "_row_hash"]), "data_quality_findings", "overwrite")

        all_quarantine = read_existing("quarantine_rows")
        if all_quarantine is not None and all(name in all_quarantine.columns for name in ("_entity", "_dq_reason", "_dq_severity")):
            summary = all_quarantine.groupBy("_entity", "_dq_reason", "_dq_severity").count().withColumnRenamed("count", "row_count")
            write(summary, "data_quality_summary", "overwrite")
        audit("SUCCEEDED", file_count, raw_record_count, parsed_count, quarantine_count)
    except Exception as exc:
        try:
            audit("FAILED", locals().get("file_count", 0), locals().get("raw_record_count", 0),
                  locals().get("parsed_count", 0), locals().get("quarantine_count", 0), exc)
        except Exception as audit_exc:
            print("Failed to persist failure audit:", str(audit_exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

