"""Serverless Kafka ingestion, incremental Delta transforms and report reconciliation."""

import json
from datetime import datetime, timezone

from pyspark import cloudpickle
from pyspark.sql import functions as F, types as T

from milestone4 import observations as obs

CATALOG = "ieso_dev"
CHECKPOINT_ROOT = "/Volumes/ieso_dev/ops/checkpoints/milestone4"
BROKER = "evhns-ieso-dev-024ed7af.servicebus.windows.net:9093"
CREDENTIAL = "svc_ieso_eventhubs"

BRONZE_DDL = """
delivery_id STRING, origin STRING, payload BINARY, message_key BINARY,
topic STRING, partition INT, offset BIGINT, kafka_timestamp TIMESTAMP,
kafka_timestamp_type INT, ingested_at TIMESTAMP, archive_input_path STRING
"""
LINEAGE = ("delivery_id", "origin", "topic", "partition", "offset", "ingested_at", "archive_input_path")


def table(catalog, schema, name):
    return f"{catalog}.{schema}.{name}"


def bronze_table(catalog, source):
    return table(catalog, "bronze", source + "_events")


def silver_table(catalog, source):
    return table(catalog, "silver", source + "_deliveries")


def silver_fields(source):
    strings = (
        "report_id", "report_event_id", "event_id", "pricing_location", "units",
        "flag", "archive_path", "publication_run_id", "publication_adf_run_id", "rejection_reason",
    )
    fields = [T.StructField(name, T.StringType()) for name in strings]
    fields.extend([
        T.StructField("delivery_date", T.DateType()),
        T.StructField("delivery_hour", T.IntegerType()),
        T.StructField("interval", T.IntegerType()),
    ])
    fields.extend(T.StructField(name, T.TimestampType()) for name in
                  ("interval_start_utc", "interval_end_utc", "source_version_utc", "first_seen_utc"))
    fields.extend(T.StructField(name, T.DecimalType(38, 12)) for name in obs.FIELDS[source])
    return T.StructType(fields)


def kafka_reader(spark, source, streaming):
    reader = spark.readStream if streaming else spark.read
    reader = (reader.format("kafka")
              .option("kafka.bootstrap.servers", BROKER)
              .option("subscribe", obs.TOPICS[source])
              .option("databricks.serviceCredential", CREDENTIAL)
              .option("startingOffsets", "earliest")
              .option("failOnDataLoss", "true"))
    if streaming:
        reader = reader.option("maxOffsetsPerTrigger", 20000)
    else:
        reader = reader.option("endingOffsets", "latest")
    return reader.load()


def kafka_deliveries(frame):
    return frame.select(
        F.concat_ws(":", F.lit("kafka"), "topic", "partition", "offset").alias("delivery_id"),
        F.lit("kafka").alias("origin"),
        F.col("value").alias("payload"), F.col("key").alias("message_key"),
        "topic", "partition", "offset", F.col("timestamp").alias("kafka_timestamp"),
        F.col("timestampType").alias("kafka_timestamp_type"),
        F.current_timestamp().alias("ingested_at"),
        F.lit(None).cast("string").alias("archive_input_path"),
    )


def classify(frame, source):
    # Serialize the two pure Python modules with the UDF for Spark Connect workers.
    cloudpickle.register_pickle_by_value(obs)
    cloudpickle.register_pickle_by_value(obs.events)
    validate = F.udf(lambda payload, key: obs.decode(source, payload, key), silver_fields(source))
    return (frame.withColumn("decoded", validate("payload", "message_key"))
            .select(*LINEAGE, "decoded.*"))


def probe(spark, source, expected=None):
    events = kafka_deliveries(kafka_reader(spark, source, streaming=False))
    if expected is None:
        sample = events.select("payload", "message_key", "partition", "offset").limit(16).collect()
        if not sample:
            raise RuntimeError(f"{source} has no retained test events")
        for row in sample:
            result = obs.decode(source, row.payload, row.message_key)
            if result["rejection_reason"]:
                raise RuntimeError(f"{source} read-back failed: {result['rejection_reason']}")
        return {"source": source, "sample_size": len(sample), "topic": obs.TOPICS[source],
                "partitions_seen": sorted({row.partition for row in sample})}
    report_id, count = expected
    selected = events.where(F.get_json_object(F.col("payload").cast("string"), "$.report.report_id") == report_id)
    return report_counts(classify(selected, source), source, report_id, count, "kafka")


def ensure_bronze(spark, source, catalog=CATALOG):
    name = bronze_table(catalog, source)
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {name} ({BRONZE_DDL})
                  USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')""")
    return name


def ingest(spark, source, catalog=CATALOG, checkpoint_root=CHECKPOINT_ROOT):
    name = ensure_bronze(spark, source, catalog)
    deliveries = kafka_deliveries(kafka_reader(spark, source, streaming=True))
    query = (deliveries.writeStream.format("delta").outputMode("append")
             .option("checkpointLocation", f"{checkpoint_root}/bronze_{source}")
             .trigger(availableNow=True).toTable(name))
    query.awaitTermination()


def merge_new(spark, frame, name, key, view):
    frame.createOrReplaceTempView(view)
    spark.sql(f"""MERGE INTO {name} AS target
                  USING {view} AS incoming
                  ON target.{key} = incoming.{key}
                  WHEN NOT MATCHED THEN INSERT *""").collect()


def land_archive(spark, record, content=None, catalog=CATALOG):
    source, report_id, path, _ = obs.replay_input(record)
    if content is None:
        objects = (spark.read.format("binaryFile").load(obs.RAW + path)
                   .select("length", "content").where("length <= 5242880").limit(2).collect())
        if len(objects) != 1:
            raise RuntimeError("Expected one archived file of at most 5 MiB")
        content = bytes(objects[0].content)
    now = datetime.now(timezone.utc)
    rows = [(f"archive:{event_id}", "archive", payload, key, None, None, None, None, None, now, path)
            for event_id, payload, key in obs.replay_events(record, content)]
    name = ensure_bronze(spark, source, catalog)
    merge_new(spark, spark.createDataFrame(rows, BRONZE_DDL), name, "delivery_id", "m4_archive_incoming")
    return {"source": source, "report_id": report_id, "archive_observations": len(rows)}


def ensure_silver(spark, source, catalog, bronze, silver):
    lineage_ddl = """
        delivery_id STRING, origin STRING, topic STRING, partition INT,
        offset BIGINT, ingested_at TIMESTAMP, archive_input_path STRING
    """
    typed_ddl = ", ".join(f"{field.name} {field.dataType.simpleString()}" for field in silver_fields(source))
    spark.sql(f"CREATE TABLE IF NOT EXISTS {silver} ({lineage_ddl}, {typed_ddl}) USING DELTA")
    history = table(catalog, "silver", source + "_history")
    current = table(catalog, "silver", source + "_current")
    quarantine = table(catalog, "ops", source + "_quarantine")
    columns = ", ".join([*LINEAGE, *silver_fields(source).fieldNames()])
    spark.sql(f"""CREATE VIEW IF NOT EXISTS {history} AS
        SELECT {columns} FROM (
            SELECT *, row_number() OVER (
                PARTITION BY report_event_id
                ORDER BY CASE WHEN origin = 'kafka' THEN 0 ELSE 1 END, delivery_id
            ) AS delivery_rank
            FROM {silver} WHERE rejection_reason IS NULL
        ) WHERE delivery_rank = 1""")
    spark.sql(f"""CREATE VIEW IF NOT EXISTS {current} AS
        SELECT {columns} FROM (
            SELECT *, row_number() OVER (
                PARTITION BY delivery_date, delivery_hour, interval, pricing_location
                ORDER BY source_version_utc DESC, first_seen_utc DESC, report_id DESC, report_event_id DESC
            ) AS version_rank
            FROM {history}
        ) WHERE version_rank = 1""")
    spark.sql(f"""CREATE VIEW IF NOT EXISTS {quarantine} AS
        SELECT s.delivery_id, s.report_id, s.rejection_reason, s.origin,
               b.payload, b.message_key, b.topic, b.partition, b.offset, b.ingested_at
        FROM {silver} s JOIN {bronze} b ON s.delivery_id = b.delivery_id
        WHERE s.rejection_reason IS NOT NULL""")


def update_silver(spark, source, fault="none", catalog=CATALOG):
    bronze = ensure_bronze(spark, source, catalog)
    silver = silver_table(catalog, source)
    ensure_silver(spark, source, catalog, bronze, silver)
    progress = table(catalog, "ops", "m4_silver_progress")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {progress}
                  (source STRING, bronze_id STRING, processed_version BIGINT) USING DELTA""")
    table_id = spark.sql(f"DESCRIBE DETAIL {bronze}").select("id").first().id
    latest = spark.sql(f"DESCRIBE HISTORY {bronze} LIMIT 1").first().version
    previous = spark.table(progress).where(F.col("source") == source).first()
    if previous and (previous.bronze_id != table_id or previous.processed_version > latest):
        raise RuntimeError("Bronze table identity or version changed. Investigate before advancing Silver.")
    if previous and previous.processed_version == latest:
        return {"source": source, "processed_version": latest, "new_versions": 0}
    if previous:
        first = previous.processed_version + 1
        changes = spark.sql(f"SELECT * FROM table_changes('{bronze}', {first}, {latest})")
        if changes.where(F.col("_change_type") != "insert").limit(1).count():
            raise RuntimeError("Bronze is append-only. Investigate the changed rows before processing.")
    else:
        first = 0
        changes = spark.read.option("versionAsOf", latest).table(bronze)
    merge_new(spark, classify(changes, source).dropDuplicates(["delivery_id"]),
              silver, "delivery_id", "m4_silver_incoming")
    if fault == "silver_after_merge":
        raise RuntimeError("IESO M4 intentional failure after Silver merge, before saving progress")
    completed = spark.createDataFrame([(source, table_id, latest)],
                                      "source STRING, bronze_id STRING, processed_version BIGINT")
    completed.createOrReplaceTempView("m4_silver_completed")
    spark.sql(f"""MERGE INTO {progress} AS target
                  USING m4_silver_completed AS incoming ON target.source = incoming.source
                  WHEN MATCHED THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""").collect()
    return {"source": source, "processed_version": latest, "new_versions": latest - first + 1}


def report_counts(frame, source, report_id, expected_count, origin):
    rows = frame.where((F.col("report_id") == report_id) & (F.col("origin") == origin))
    valid = F.col("rejection_reason").isNull()
    stats = rows.agg(
        F.count("*").alias("deliveries"),
        F.count(F.when(~valid, 1)).alias("rejected"),
        F.countDistinct(F.when(valid, F.col("report_event_id"))).alias("unique_events"),
        F.countDistinct(F.when(valid, F.struct("delivery_date", "delivery_hour", "interval",
                                             "pricing_location"))).alias("unique_observations"),
    ).first().asDict()
    stats.update(source=source, report_id=report_id, expected_count=expected_count, origin=origin)
    stats["duplicate_deliveries"] = stats["deliveries"] - stats["rejected"] - stats["unique_events"]
    stats["passed"] = (stats["unique_events"] == stats["unique_observations"] == expected_count
                       and stats["rejected"] == 0)
    return stats


def verify(spark, expectations, origin, job_run_id, adf_run_id, revision, catalog=CATALOG):
    metrics = []
    for source, (report_id, count) in expectations.items():
        bronze = spark.table(bronze_table(catalog, source))
        silver = spark.table(silver_table(catalog, source))
        results = report_counts(silver, source, report_id, count, origin)
        counts = bronze.agg(F.count("*").alias("bronze_rows"),
                            F.countDistinct("delivery_id").alias("bronze_unique_deliveries")).first().asDict()
        totals = silver.agg(F.count("*").alias("classified_rows"),
                            F.count("rejection_reason").alias("quarantined_rows"),
                            F.countDistinct(F.when(F.col("rejection_reason").isNull(),
                                                   F.col("report_event_id"))).alias("valid_distinct")).first().asDict()
        totals["valid_duplicates"] = totals["classified_rows"] - totals["quarantined_rows"] - totals["valid_distinct"]
        results.update(counts, **totals)
        results["passed"] = results["passed"] and (
            counts["bronze_rows"] == counts["bronze_unique_deliveries"] == totals["classified_rows"]
            and bronze.select("delivery_id").join(silver.select("delivery_id"), "delivery_id", "left_anti")
            .limit(1).count() == 0)
        metrics.append(results)
    passed = all(row["passed"] for row in metrics)
    payload = json.dumps(metrics, sort_keys=True)
    print(payload)
    audit = table(catalog, "ops", "m4_validation_runs")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {audit} (
        job_run_id STRING, adf_run_id STRING, revision STRING, origin STRING,
        checked_at TIMESTAMP, passed BOOLEAN, metrics_json STRING
    ) USING DELTA""")
    evidence = spark.createDataFrame(
        [(job_run_id, adf_run_id, revision, origin, datetime.now(timezone.utc), passed, payload)],
        "job_run_id STRING, adf_run_id STRING, revision STRING, origin STRING, checked_at TIMESTAMP, passed BOOLEAN, metrics_json STRING",
    )
    evidence.write.format("delta").mode("append").saveAsTable(audit)
    if not passed:
        raise RuntimeError("IESO M4 reconciliation failed. Inspect m4_validation_runs and task output.")
    return metrics
