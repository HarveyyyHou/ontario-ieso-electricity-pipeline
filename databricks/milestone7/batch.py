"""Process a sealed ADF report inventory with recoverable, durable stage progress."""

import hashlib
import json
import re
from datetime import date, datetime, timedelta, timezone
from time import monotonic

from pyspark.sql import functions as F
from pyspark.errors import AnalysisException
from milestone4 import lakehouse as m4
from milestone5 import gold as m5
from milestone6 import hourly_batch as m6

RAW = "abfss://raw@iesodev001.dfs.core.windows.net/"
CHECKPOINTS = "/Volumes/ieso_dev/ops/checkpoints/milestone7"
CONTROL = "ieso_dev.ops.m7_steps"


def read_inventory(spark, batch_id, expected_sha, raw=RAW):
    if not re.fullmatch(r"[0-9a-f]{24}", batch_id) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha):
        raise ValueError("Invalid batch ID or inventory hash.")
    objects = spark.read.format("binaryFile").load(
        raw + f"ieso/ops/m7/{batch_id}/inventory.json"
    ).select("length", "content").collect()
    if len(objects) != 1 or objects[0].length > 1024 * 1024:
        raise ValueError("Expected one sealed inventory of at most one MiB.")
    content = bytes(objects[0].content)
    if hashlib.sha256(content).hexdigest() != expected_sha:
        raise ValueError("Inventory SHA-256 differs from ADF.")
    inventory = json.loads(content)
    if inventory["plan"]["batch_id"] != batch_id:
        raise ValueError("Inventory refers to another batch.")
    expected = {task["key"] for task in inventory["plan"]["tasks"]}
    if expected != {task["key"] for task in inventory["reports"]} or len(expected) != len(inventory["reports"]):
        raise ValueError("Inventory has missing or duplicate reports.")
    return inventory


def run_stage(spark, batch_id, revision, stage, callback):
    previous = spark.sql(
        f"SELECT revision, metrics FROM {CONTROL} WHERE batch_id='{batch_id}' AND stage='{stage}'"
    ).first()
    if previous:
        if previous.revision != revision:
            raise ValueError("The batch already has completed stages from another revision. Use a new refresh_token.")
        return json.loads(previous.metrics)
    result = callback()
    frame = spark.createDataFrame(
        [(batch_id, stage, revision, datetime.now(timezone.utc), json.dumps(result, default=str))],
        "batch_id STRING, stage STRING, revision STRING, completed_at TIMESTAMP, metrics STRING",
    )
    frame.createOrReplaceTempView("m7_completed")
    spark.sql(f"""
        MERGE INTO {CONTROL} t USING m7_completed s
        ON t.batch_id=s.batch_id AND t.stage=s.stage
        WHEN NOT MATCHED THEN INSERT *
    """).collect()
    return json.loads(json.dumps(result, default=str))


def is_retention_error(exc):
    message = str(exc)
    return any(term in message for term in (
        "KAFKA_DATA_LOSS", "OFFSET_OUT_OF_RANGE", "Some data may have been lost",
        "OffsetOutOfRangeException",
    ))


def read_stage(spark, path):
    try:
        return spark.read.format("delta").load(path)
    except AnalysisException as exc:
        if exc.getErrorClass() in {"PATH_NOT_FOUND", "DELTA_MISSING_DELTA_TABLE", "DELTA_PATH_DOES_NOT_EXIST"}:
            return None
        raise


def consume(spark, source, report_ids, batch_id, checkpoints=CHECKPOINTS):
    staging = f"{checkpoints}/{batch_id}/staging/{source}"
    checkpoint = f"{checkpoints}/{batch_id}/checkpoints/{source}"
    reader = m4.kafka_reader(spark, source, streaming=True)
    matching = reader.where(F.get_json_object(F.col("value").cast("string"), "$.report.report_id").isin(report_ids))
    recovery = ""
    try:
        query = (m4.kafka_deliveries(matching).writeStream.format("delta").outputMode("append")
                 .option("checkpointLocation", checkpoint).trigger(availableNow=True).start(staging))
        query.awaitTermination()
    except Exception as exc:
        if not is_retention_error(exc):
            raise
        recovery = "Kafka offsets expired. Reconcile the staged data and recover missing observations from ADLS."
        print(recovery)
    staged = read_stage(spark, staging)
    target = m4.ensure_bronze(spark, source)
    if staged is not None:
        m4.merge_new(spark, staged.dropDuplicates(["delivery_id"]), target, "delivery_id", "m7_staged")
    return {"checkpoint": checkpoint, "retention_recovery": recovery}


def report_metrics(spark, source, reports):
    ids = [report["record"]["RowKey"] for report in reports]
    silver = spark.table(m4.silver_table("ieso_dev", source)).where(F.col("report_id").isin(ids))
    valid = F.col("rejection_reason").isNull()
    stats = silver.groupBy("report_id").agg(
        F.count("*").alias("deliveries"),
        F.count(F.col("rejection_reason")).alias("rejected"),
        F.countDistinct(F.when(valid, F.col("report_event_id"))).alias("accepted_unique"),
        F.countDistinct(F.when(valid, F.struct(
            "delivery_date", "delivery_hour", "interval", "pricing_location"
        ))).alias("observation_keys"),
        F.countDistinct(F.when(valid & (F.col("origin") == "kafka"),
                               F.col("report_event_id"))).alias("kafka_unique"),
    ).collect()
    return {row.report_id: row.asDict() for row in stats}


def reconcile(spark, source, reports):
    m4.update_silver(spark, source)
    metrics = report_metrics(spark, source, reports)
    recovered = []
    for report in reports:
        identity = report["record"]["RowKey"]
        current = metrics.get(identity, {})
        if current.get("rejected", 0):
            raise RuntimeError(f"Silver rejected events for {source}, {identity}.")
        if current.get("accepted_unique", 0) < report["expected_count"]:
            m4.land_archive(spark, report["record"])
            recovered.append(identity)
    if recovered:
        m4.update_silver(spark, source)
    final = report_metrics(spark, source, reports)
    for report in reports:
        identity = report["record"]["RowKey"]
        current = final.get(identity, {})
        expected = report["expected_count"]
        if (current.get("accepted_unique") != expected or current.get("observation_keys") != expected
                or current.get("rejected", 0)):
            raise RuntimeError(f"Unreconciled report: {source}, {identity}, {current}, expected={expected}")
        current["expected"] = expected
        current["published_count"] = expected
        current["accepted_without_kafka_delivery"] = expected - current["kafka_unique"]
        current["valid_duplicate_deliveries"] = current["deliveries"] - expected
    return {"source": source, "reports": list(final.values()), "archive_recovered_reports": recovered}


def completeness(spark, source, reports):
    selected = spark.createDataFrame(
        sorted({(report["day"], report["hour"]) for report in reports}),
        "day STRING, delivery_hour INT",
    ).select(F.to_date("day").alias("delivery_date"), "delivery_hour")
    current = spark.table(f"ieso_dev.silver.{source}_current").join(
        selected, ["delivery_date", "delivery_hour"], "inner"
    )
    groups = current.groupBy("delivery_date", "delivery_hour", "pricing_location").agg(
        F.count("*").alias("rows"), F.countDistinct("interval").alias("intervals")
    )
    result = groups.agg(F.count("*").alias("location_hours"),
                       F.count(F.when((F.col("rows") != 12) | (F.col("intervals") != 12), 1))
                       .alias("incomplete")).first().asDict()
    observed_hours = current.select("delivery_date", "delivery_hour").distinct().count()
    if result["incomplete"] or observed_hours != len(reports) or not result["location_hours"]:
        raise RuntimeError(f"Source has an incomplete selected hour: {source}, {result}")
    return {**result, "report_hours": observed_hours}


def refresh(spark, inventory, revision, adf_run_id, job_run_id, consumer=consume):
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("source_revision must be the reviewed full Git SHA.")
    started = monotonic()
    plan, reports = inventory["plan"], inventory["reports"]
    batch_id = plan["batch_id"]
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {CONTROL} (
        batch_id STRING, stage STRING, revision STRING, completed_at TIMESTAMP, metrics STRING
    ) USING DELTA""")
    result = {"batch_id": batch_id, "source_revision": revision, "adf_run_id": adf_run_id,
              "job_run_id": job_run_id, "real_time": {}, "gold": [], "hourly": []}
    for source in ("rt_lmp", "rt_totals"):
        selected = [report for report in reports if report["source"] == source]
        if not selected:
            raise ValueError(f"Inventory has no {source} reports.")
        ids = [report["record"]["RowKey"] for report in selected]
        result["real_time"][source] = {
            "ingest": run_stage(spark, batch_id, revision, "ingest_" + source,
                lambda: consumer(spark, source, ids, batch_id)),
            "reconcile": run_stage(spark, batch_id, revision, "silver_" + source,
                lambda: reconcile(spark, source, selected)),
            "coverage": run_stage(spark, batch_id, revision, "coverage_" + source,
                lambda: completeness(spark, source, selected)),
        }
    day, end = date.fromisoformat(plan["data_start_date"]), date.fromisoformat(plan["end_date"])
    while day <= end:
        text = day.isoformat()
        result["gold"].append(run_stage(spark, batch_id, revision, "gold_" + text,
            lambda: m5.refresh_gold(spark, "ieso_dev.silver", "ieso_dev.gold", text, "")))
        pair = {report["source"]: report["report"] for report in reports
                if report["day"] == text and report["source"] in ("da_lmp", "generator")}
        if set(pair) != {"da_lmp", "generator"}:
            raise ValueError("Each processed date needs both hourly reports.")
        result["hourly"].append(run_stage(spark, batch_id, revision, "hourly_" + text,
            lambda: m6.refresh(spark, m6.RAW, ("ieso_dev.bronze", "ieso_dev.silver", "ieso_dev.gold"),
                               text, json.dumps(pair["da_lmp"]), json.dumps(pair["generator"]))))
        day += timedelta(days=1)
    result.update(passed=True, runtime_seconds=round(monotonic() - started, 1))
    audit = spark.createDataFrame(
        [(batch_id, job_run_id, adf_run_id, revision, datetime.now(timezone.utc), json.dumps(result, default=str))],
        "batch_id STRING, job_run_id STRING, adf_run_id STRING, revision STRING, "
        "completed_at TIMESTAMP, metrics STRING",
    )
    audit.write.format("delta").mode("append").saveAsTable(m4.table("ieso_dev", "ops", "m7_runs"))
    return result
