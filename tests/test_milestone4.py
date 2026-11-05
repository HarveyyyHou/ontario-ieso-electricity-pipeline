"""Verify the publisher/replay contract and exercise real local Spark/Delta when enabled."""

import hashlib
import json
import os
import sys
import tempfile
import unittest
import uuid
from unittest.mock import patch
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "databricks"))
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "functions" / "ieso_ingest"))

import collector
import events
import publisher
import totals
from milestone4 import observations as obs
from test_ingestion import report
from test_totals import csv_fixture, xml_fixture

NOW = "2026-11-05T15:00:00+00:00"


def fixture(source, value="-1.00", created="2026/11/01 00:01:00"):
    record = {"PartitionKey": source + "_v1", "ValidationStatus": "accepted",
              "ArchiveState": "archived", "FirstSeenUtc": NOW}
    if source == "rt_lmp":
        content = report(rows=[f"1,1,TEST-LT.AG,{value},0,0"], created=created)
        digest = hashlib.sha256(content).hexdigest()
        record.update(RowKey=digest, Sha256=digest, BlobPath=collector.archive_path(digest),
                      **collector.validate_report(content))
    else:
        original_csv, xml = csv_fixture(), xml_fixture()
        csv_hash, xml_hash = (hashlib.sha256(data).hexdigest() for data in (original_csv, xml))
        content, metadata, _ = totals.combine(original_csv, xml)
        prepared_hash = hashlib.sha256(content).hexdigest()
        record.update(RowKey=totals.report_id(csv_hash, xml_hash),
                      Sha256=csv_hash, CsvSha256=csv_hash, XmlSha256=xml_hash,
                      BlobPath=totals.raw_path("csv", csv_hash), PreparedState="ready",
                      PreparedCsvSha256=prepared_hash, PreparedCsvBlobPath=totals.prepared_path(prepared_hash),
                      **metadata)
    return record, content


class ObservationTests(unittest.TestCase):
    def test_replay_reproduces_live_publisher_contract(self):
        for source in obs.TOPICS:
            with self.subTest(source=source):
                record, content = fixture(source)
                live = publisher.report_events(source, record["RowKey"], record, content,
                                               str(uuid.uuid4()), "", NOW)
                replay = obs.replay_events(record, content)
                self.assertEqual(len(live), len(replay))
                for (location, expected), (identity, payload, key) in zip(live, replay):
                    actual = json.loads(payload)
                    expected.pop("publication")
                    actual.pop("publication")
                    self.assertEqual(actual, expected)
                    self.assertEqual(key, location.encode())
                    self.assertEqual(identity, expected["report_event_id"])
                    self.assertIsNone(obs.decode(source, payload, key)["rejection_reason"])

    def test_bad_key_event_id_and_archive_hash_are_rejected(self):
        record, content = fixture("rt_lmp")
        _, payload, key = obs.replay_events(record, content)[0]
        self.assertIn("key", obs.decode("rt_lmp", payload, b"OTHER")["rejection_reason"])
        event = json.loads(payload)
        event["event_id"] = "0" * 64
        self.assertIn("IDs", obs.decode("rt_lmp", events.canonical(event).encode(), key)["rejection_reason"])
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            obs.replay_events(record, content + b"\n")


@unittest.skipUnless(os.environ.get("IESO_M4_DELTA_JARS"), "set IESO_M4_DELTA_JARS for local Spark/Delta")
class DeltaIntegrationTests(unittest.TestCase):
    def test_incremental_recovery_correction_and_reconciliation(self):
        from pyspark.sql import SparkSession
        from milestone4 import lakehouse as m4

        with tempfile.TemporaryDirectory() as tmp:
            spark = (SparkSession.builder.master("local[2]").appName("ieso-m4")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.warehouse.dir", tmp)
                     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                     .config("spark.databricks.delta.snapshotPartitions", "2")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.jars", os.environ["IESO_M4_DELTA_JARS"]).getOrCreate())
            try:
                self.exercise_delta(spark, m4, Path(tmp))
            finally:
                spark.stop()

    def exercise_delta(self, spark, m4, tmp):
        from datetime import datetime, timezone
        catalog = "spark_catalog"
        for schema in ("bronze", "silver", "ops"):
            spark.sql(f"CREATE SCHEMA {schema}")
        record, content = fixture("rt_lmp")
        m4.land_archive(spark, record, content, catalog)
        m4.land_archive(spark, record, content, catalog)
        m4.update_silver(spark, "rt_lmp", catalog=catalog)
        self.assertEqual(spark.table("bronze.rt_lmp_events").count(), 1)
        self.assertEqual(m4.update_silver(spark, "rt_lmp", catalog=catalog)["new_versions"], 0)

        corrected, corrected_csv = fixture("rt_lmp", "3.00", "2026/11/01 00:05:00")
        m4.land_archive(spark, corrected, corrected_csv, catalog)
        _, payload, key = obs.replay_events(corrected, corrected_csv)[0]
        now = datetime.now(timezone.utc)
        # Same report occurrence delivered again by Kafka, plus one invalid payload.
        # Exercise the Bronze streaming sink and checkpoint with a local stand-in for Kafka.
        schema = "key BINARY, value BINARY, topic STRING, partition INT, offset BIGINT, timestamp TIMESTAMP, timestampType INT"
        local_input = str(tmp / "kafka_fixture")
        spark.createDataFrame([
            (key, payload, "ieso-rt-lmp", 0, 1, now, 0),
            (key, b"bad", "ieso-rt-lmp", 0, 2, now, 0),
        ], schema).write.parquet(local_input)

        def local_stream(session, source, streaming):
            return session.readStream.schema(schema).parquet(local_input)

        with patch.object(m4, "kafka_reader", local_stream):
            m4.ingest(spark, "rt_lmp", catalog, str(tmp / "checkpoints"))
            self.assertEqual(spark.table("bronze.rt_lmp_events").count(), 4)
            m4.ingest(spark, "rt_lmp", catalog, str(tmp / "checkpoints"))
            self.assertEqual(spark.table("bronze.rt_lmp_events").count(), 4)
            spark.createDataFrame([(key, payload, "ieso-rt-lmp", 0, 3, now, 0)], schema) \
                .write.mode("append").parquet(local_input)
            m4.ingest(spark, "rt_lmp", catalog, str(tmp / "checkpoints"))
        with self.assertRaisesRegex(RuntimeError, "intentional failure"):
            m4.update_silver(spark, "rt_lmp", "silver_after_merge", catalog)
        m4.update_silver(spark, "rt_lmp", catalog=catalog)
        self.assertEqual(spark.table("silver.rt_lmp_history").count(), 2)
        self.assertEqual(spark.table("ops.rt_lmp_quarantine").count(), 1)
        self.assertEqual(spark.table("silver.rt_lmp_current").first().lmp, 3)
        metrics = m4.verify(spark, {"rt_lmp": (corrected["RowKey"], 1)},
                            "kafka", "local-test", "", "local", catalog)[0]
        self.assertEqual(metrics["bronze_rows"], 5)
        self.assertEqual(metrics["valid_duplicates"], 2)
        self.assertEqual(metrics["quarantined_rows"], 1)

        totals_record, totals_content = fixture("rt_totals")
        m4.land_archive(spark, totals_record, totals_content, catalog)
        m4.update_silver(spark, "rt_totals", catalog=catalog)
        demand = spark.table("silver.rt_totals_current").orderBy("interval").first()
        self.assertEqual((demand.ontario_demand, demand.units), (90, "MW"))
