"""Focused operations tests and an opt-in local Spark recovery integration test."""

import base64
import copy
import json
import os
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "functions/ieso_ingest"))
import batches
import collector


class Entity(dict):
    metadata = {"etag": "test-etag"}


class Table:
    def __init__(self):
        self.rows = {}

    def get_entity(self, partition, row):
        try:
            return Entity(copy.deepcopy(self.rows[partition, row]))
        except KeyError as exc:
            raise ResourceNotFoundError("Missing entity") from exc

    def create_entity(self, entity):
        key = entity["PartitionKey"], entity["RowKey"]
        if key in self.rows:
            raise ResourceExistsError("Entity exists")
        self.rows[key] = copy.deepcopy(entity)

    def update_entity(self, entity, **kwargs):
        key = entity["PartitionKey"], entity["RowKey"]
        self.rows[key].update(copy.deepcopy(entity))

    def submit_transaction(self, operations):
        before = copy.deepcopy(self.rows)
        try:
            for kind, entity, *options in operations:
                getattr(self, kind + "_entity")(entity, **(options[0] if options else {}))
        except Exception:
            self.rows = before
            raise


class Archive:
    def __init__(self):
        self.files = {}

    def put(self, path, content, metadata, **kwargs):
        if path in self.files:
            assert self.files[path] == content
            return "already_archived"
        self.files[path] = content
        return "archived"


class BatchControlTests(unittest.TestCase):
    def test_window_bounds_lookback_and_hour_preview(self):
        body = {"start_date": "2026-11-12", "end_date": "2026-11-13"}
        plan = batches.plan_window(body, today=date(2026, 11, 18))
        self.assertEqual(len(plan["tasks"]), 100)
        auto = batches.plan_window({"start_date": "auto", "advance_cursor": True},
                                   "2026-11-13", date(2026, 11, 18))
        self.assertEqual((auto["data_start_date"], auto["start_date"], auto["end_date"]),
                         ("2026-11-13", "2026-11-14", "2026-11-15"))
        self.assertEqual(len(auto["tasks"]), 150)
        one = batches.plan_window({"start_date": "2026-11-12", "delivery_hour": 1})
        self.assertEqual(len(one["tasks"]), 4)
        for invalid in (
            {"start_date": "2026-11-12", "delivery_hour": 1, "advance_cursor": True},
            {"start_date": "auto"},
            {"start_date": "2026-11-12", "end_date": "2026-11-15"},
        ):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                batches.plan_window(invalid, today=date(2026, 11, 18))

    def test_inventory_resume_failure_and_atomic_cursor(self):
        table = Table()
        request = {"start_date": "2026-11-12", "advance_cursor": True}
        result = batches.start(table, request, "run-1")
        identity = result["batch_id"]
        entity = table.get_entity(batches.CONTROL, "batch_" + identity)
        plan = json.loads(entity["PlanJson"])
        archive = Archive()
        with self.assertRaisesRegex(ValueError, "has not completed"):
            batches.seal(table, archive, entity, plan)
        for task in plan["tasks"]:
            table.create_entity({"PartitionKey": identity, "RowKey": task["key"],
                                 "Published": True, "DetailsJson": '{"expected_count":12}'})
        sealed = batches.seal(table, archive, entity, plan)
        self.assertEqual(sealed["reports"], 50)
        entity = table.get_entity(batches.CONTROL, "batch_" + identity)
        batches.state(table, entity, "failed", FailedStage="process_window")
        resume = batches.start(table, request, "run-2")
        self.assertEqual(resume["tasks"], [])
        self.assertIsNone(batches.get(table, batches.CONTROL, "cursor"))
        entity = table.get_entity(batches.CONTROL, "batch_" + identity)
        batches.seal(table, archive, entity, plan)
        entity = table.get_entity(batches.CONTROL, "batch_" + identity)
        completed = batches.complete(table, entity, plan, {"spark_output": "successful job"})
        self.assertTrue(completed["cursor_advanced"])
        self.assertEqual(table.get_entity(batches.CONTROL, "cursor")["EndDate"], "2026-11-12")
        self.assertTrue(batches.start(table, request, "run-3")["already_complete"])

    def test_collected_reports_are_reused_and_adf_failure_stays_failed(self):
        table = Table()
        plan = batches.plan_window({"start_date": "2026-11-12", "delivery_hour": 1})
        task = next(task for task in plan["tasks"] if task["source"] == "da_lmp")
        fixture = {"status": "archived", "row_count": 2, "report": {
            "sha256": "a" * 64, "prepared_sha256": "b" * 64,
            "row_count": 2, "delivery_date": "2026-11-12",
            "source_created_utc": "2026-11-11T18:00:00+00:00",
        }}
        with patch.object(batches.hourly, "collect", return_value=(fixture, 200)) as collect:
            batches.collect_report(table, Archive(), plan, task, "adf")
            repeated, code = batches.collect_report(table, Archive(), plan, task, "adf")
        self.assertEqual((repeated["status"], code, collect.call_count), ("already_collected", 200, 1))
        parent = json.loads((ROOT / "adf/pipeline/pl_ieso_m7_execute.json").read_text())
        steps = parent["properties"]["activities"]
        for name in ("collect_reports", "seal_inventory", "process_window", "complete_batch"):
            failure = next(step for step in steps if step["name"] == "fail_" + name)
            self.assertEqual(failure["type"], "Fail")
        child = json.loads((ROOT / "adf/pipeline/pl_ieso_m7_report.json").read_text())
        self.assertEqual(child["properties"]["activities"][1]["typeProperties"]["timeout"], "0.00:20:00")


@unittest.skipUnless(os.environ.get("IESO_M4_DELTA_JARS"), "requires local Spark and Delta jars")
class SparkRecoveryTests(unittest.TestCase):
    def test_complete_hour_archive_recovery_and_stage_resume(self):
        from pyspark.sql import SparkSession

        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            runtime = folder / "runtime.zip"
            runtime.write_bytes(base64.b64decode(
                (ROOT / "databricks/milestone7/runtime.b64").read_text().strip()))
            sys.path.insert(0, str(runtime))
            from milestone7 import batch
            from milestone4 import lakehouse as m4
            spark = (SparkSession.builder.master("local[2]").appName("ieso-m7")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.warehouse.dir", (folder / "warehouse").as_uri())
                     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                     .config("spark.databricks.delta.snapshotPartitions", "2")
                     .config("spark.jars", os.environ["IESO_M4_DELTA_JARS"]).getOrCreate())
            try:
                spark.sparkContext.addPyFile(str(runtime))
                self.exercise(spark, folder, batch, m4)
            finally:
                spark.stop()

    def exercise(self, spark, folder, batch, m4):
        import hourly
        import totals
        from datetime import datetime, timezone

        day = "2026-11-12"
        now = "2026-11-13T06:00:00+00:00"
        for schema in ("bronze", "silver", "gold", "ops"):
            spark.sql(f"CREATE SCHEMA {schema}")
        # Map the production catalog qualifiers to the local Spark catalog.
        batch.CONTROL = "spark_catalog.ops.m7_steps"
        original_table = m4.table
        m4.table = lambda catalog, schema, name: original_table("spark_catalog", schema, name)
        original_sql = spark.sql
        spark.sql = lambda query, *a, **kw: original_sql(query.replace("ieso_dev.", "spark_catalog."), *a, **kw)
        original_exists = spark.catalog.tableExists
        spark.catalog.tableExists = lambda name: original_exists(name.replace("ieso_dev.", "spark_catalog."))
        original_table_read = spark.table
        spark.table = lambda name: original_table_read(name.replace("ieso_dev.", "spark_catalog."))
        raw = (folder / "raw").as_uri() + "/"
        m4.obs.RAW = raw
        batch.m6.RAW = raw + "ieso/milestone6"
        reports = []
        lmp_bytes = (
            "CREATED AT 2026/11/12 01:01:00 FOR 2026/11/12\n" + ",".join(collector.HEADER) + "\n"
            + "".join(f"1,{i},TEST.A,20,0,0\n" for i in range(1, 13))
        ).encode()
        lmp_hash = collector.sha256(lmp_bytes)
        lmp = {"PartitionKey": "rt_lmp_v1", "RowKey": lmp_hash, "Sha256": lmp_hash,
               "BlobPath": collector.archive_path(lmp_hash), "ValidationStatus": "accepted",
               "ArchiveState": "archived", "FirstSeenUtc": now, **collector.validate_report(lmp_bytes)}
        target = folder / "raw" / lmp["BlobPath"]
        target.parent.mkdir(parents=True)
        target.write_bytes(lmp_bytes)
        reports.append({"source": "rt_lmp", "day": day, "hour": 1, "record": lmp, "expected_count": 12})

        prepared = ("DELIVERY DATE," + ",".join(totals.CSV_HEADER) + ",ONTARIO DEMAND\n" +
                    "".join(f"{day},1,{i},100,2,98,1,11,22,33,DSO-RD,90\n" for i in range(1, 13))).encode()
        csv_hash, xml_hash = "a" * 64, "b" * 64
        prepared_hash = collector.sha256(prepared)
        record = {"PartitionKey": "rt_totals_v1", "RowKey": totals.report_id(csv_hash, xml_hash),
                  "Sha256": csv_hash, "CsvSha256": csv_hash, "XmlSha256": xml_hash,
                  "BlobPath": totals.raw_path("csv", csv_hash), "PreparedState": "ready",
                  "PreparedCsvSha256": prepared_hash, "PreparedCsvBlobPath": totals.prepared_path(prepared_hash),
                  "ValidationStatus": "accepted", "ArchiveState": "archived", "FirstSeenUtc": now,
                  "DeliveryDate": day, "DeliveryHour": 1, "RowCount": 12,
                  "CsvCreatedLocal": "2026-11-12T01:01:00", "XmlCreatedLocal": "2026-11-12T01:01:00"}
        target = folder / "raw" / record["PreparedCsvBlobPath"]
        target.parent.mkdir(parents=True)
        target.write_bytes(prepared)
        reports.append({"source": "rt_totals", "day": day, "hour": 1, "record": record, "expected_count": 12})
        da = ("CREATED AT 2026/11/11 13:00:00 FOR 2026/11/12\n" + ",".join(hourly.DA_HEADER) +
              "\n1,TEST.A,10,0,0\n").encode()
        gen = ("""<IMODocument xmlns="http://www.theIMO.com/schema" docID="GenOutputCapability">
<IMODocHeader><CreatedAt>2026-11-13T00:20:00</CreatedAt></IMODocHeader>
<IMODocBody><Date>2026-11-12</Date><Generators><Generator><GeneratorName>GAS-1</GeneratorName>
<FuelType>GAS</FuelType><Outputs><Output><Hour>1</Hour><EnergyMW>80</EnergyMW></Output></Outputs>
<Capabilities><Capability><Hour>1</Hour><EnergyMW>100</EnergyMW></Capability></Capabilities>
</Generator></Generators></IMODocBody></IMODocument>""").encode()
        for source, content in (("da_lmp", da), ("generator", gen)):
            created, rows, data = hourly.prepare(source, content, date.fromisoformat(day))
            digest = collector.sha256(data)
            path = folder / "raw/ieso/milestone6" / source / "prepared/v1" / ("sha256=" + digest)
            path.mkdir(parents=True)
            (path / (source + ".csv")).write_bytes(data)
            reports.append({"source": source, "day": day, "hour": 0,
                            "report": {"sha256": collector.sha256(content), "prepared_sha256": digest,
                                       "row_count": rows, "delivery_date": day, "source_created_utc": created}})
        plan = {"batch_id": "f" * 24, "data_start_date": day, "end_date": day}
        calls = []
        def expired(spark, source, ids, batch_id):
            calls.append(source)
            m4.ensure_bronze(spark, source)
            return {"retention_recovery": "simulated expired offset"}
        inventory = {"plan": plan, "reports": reports}
        result = batch.refresh(spark, inventory, "0" * 40, "adf-1", "job-1", consumer=expired)
        self.assertTrue(result["passed"])
        stats = result["real_time"]["rt_lmp"]["reconcile"]["reports"][0]
        self.assertEqual((stats["published_count"], stats["accepted_unique"], stats["accepted_without_kafka_delivery"]),
                         (12, 12, 12))
        self.assertEqual(result["real_time"]["rt_lmp"]["coverage"]["incomplete"], 0)
        self.assertEqual(result["hourly"][0]["marts"]["complete_spreads"], 1)
        self.assertEqual(result["hourly"][0]["marts"]["complete_demand_hours"], 1)
        self.assertEqual(len(result["real_time"]["rt_lmp"]["reconcile"]["archive_recovered_reports"]), 1)
        second = batch.refresh(spark, inventory, "0" * 40, "adf-2", "job-2", consumer=expired)
        self.assertTrue(second["passed"])
        self.assertEqual(calls, ["rt_lmp", "rt_totals"])
        self.assertEqual(spark.table("bronze.rt_lmp_events").count(), 12)
        self.assertEqual(spark.table("bronze.rt_totals_events").count(), 12)
        self.assertTrue(batch.is_retention_error(RuntimeError("[KAFKA_DATA_LOSS]")))
        self.assertFalse(batch.is_retention_error(RuntimeError("authentication failed")))
