"""Focused collector tests and an opt-in local Spark/Delta integration test."""

import importlib.util
import json
import os
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "functions" / "ieso_ingest"))
from collector import SourceError, sha256
import hourly

SPEC = importlib.util.spec_from_file_location(
    "hourly_batch", ROOT / "databricks" / "milestone6" / "hourly_batch.py"
)
batch = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(batch)
DAY = date(2026, 11, 12)
DA = """CREATED AT 2026/11/11 13:00:00 FOR 2026/11/12
Delivery Hour,Pricing Location,LMP,Energy Loss Price,Energy Congestion Price
1,TEST.A,-20.00,0.25,-1.25
2,TEST.A,10,0,0
"""
GEN = """<IMODocument xmlns="http://www.theIMO.com/schema" docID="GenOutputCapability">
<IMODocHeader><CreatedAt>2026-11-13T00:20:00</CreatedAt></IMODocHeader>
<IMODocBody><Date>2026-11-12</Date><Generators>
<Generator><GeneratorName>WIND-1</GeneratorName><FuelType>WIND</FuelType>
<Outputs><Output><Hour>1</Hour><EnergyMW>12</EnergyMW></Output>
<Output><Hour>2</Hour></Output></Outputs>
<Capabilities><Capability><Hour>1</Hour><EnergyMW>13</EnergyMW></Capability>
<Capability><Hour>2</Hour><EnergyMW>15</EnergyMW></Capability></Capabilities>
<Capacities><AvailCapacity><Hour>1</Hour><EnergyMW>20</EnergyMW></AvailCapacity></Capacities>
</Generator>
<Generator><GeneratorName>GAS-1</GeneratorName><FuelType>GAS</FuelType>
<Outputs><Output><Hour>1</Hour><EnergyMW>22</EnergyMW></Output></Outputs>
<Capabilities><Capability><Hour>1</Hour><EnergyMW>25</EnergyMW></Capability></Capabilities>
</Generator>
</Generators></IMODocBody></IMODocument>"""


class Archive:
    def __init__(self):
        self.files = {}

    def put(self, name, content, metadata, content_type="text/csv"):
        if name in self.files:
            assert self.files[name] == content
            return "already_archived"
        self.files[name] = content
        return "archived"

    def read_path(self, name, digest):
        content = self.files[name]
        assert sha256(content) == digest
        return content


class Records:
    def __init__(self):
        self.attempts = {}
        self.reports = {}

    def start_attempt(self, run, requested, now, adf):
        attempt = {"PartitionKey": "test", "requested": requested, "adf": adf}
        self.attempts[run] = attempt
        return attempt

    def update_attempt(self, attempt, **fields):
        attempt.update(fields)

    def record_report(self, entity):
        self.reports[entity["RowKey"]] = entity


class HourlySourceTests(unittest.TestCase):
    def test_csv_xml_semantics_and_bad_inputs(self):
        created, count, output = hourly.prepare("da_lmp", DA.encode(), DAY)
        self.assertEqual((created, count), ("2026-11-11T18:00:00+00:00", 2))
        self.assertIn(b"1,TEST.A,-20.00,0.25,-1.25", output)
        _, count, output = hourly.prepare("generator", GEN.encode(), DAY)
        self.assertEqual(count, 3)
        self.assertIn(b"1,WIND-1,WIND,12,,13,20", output)
        self.assertIn(b"1,GAS-1,GAS,22,25,,", output)
        self.assertIn(b"2,WIND-1,WIND,,,15,", output)
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            hourly.prepare("da_lmp", (DA + "1,TEST.A,1,2,3").encode(), DAY)
        with self.assertRaisesRegex(ValueError, "delivery date"):
            hourly.prepare("generator", GEN.encode(), date(2026, 11, 13))
        with self.assertRaises(ValueError):
            hourly.request_options({"source": "da_lmp", "delivery_date": "https://example.com"})

    def test_archive_repeat_replay_revision_and_rejected_or_missing_report(self):
        archive, records = Archive(), Records()
        body = {"source": "da_lmp", "delivery_date": DAY.isoformat()}

        def collect(content, request=body):
            return hourly.collect(request, archive, records, "test-adf",
                                  downloader=lambda url, deadline: (content.encode(), {}))

        first, code = collect(DA)
        self.assertEqual((first["status"], code), ("archived", 200))
        repeat, code = collect(DA)
        self.assertEqual((repeat["status"], code), ("already_archived", 200))
        self.assertEqual(first["report"], repeat["report"])
        replay, code = collect("", {**body, "replay_sha": first["report"]["sha256"]})
        self.assertEqual((replay["report"], code), (first["report"], 200))
        newer, code = collect(DA.replace("13:00:00", "15:00:00").replace("-20.00", "-21.00"))
        self.assertEqual(code, 200)
        self.assertNotEqual(newer["report"]["sha256"], first["report"]["sha256"])
        rejected, code = collect("bad header")
        self.assertEqual((rejected["status"], code), ("rejected", 422))

        def missing(url, deadline):
            raise SourceError("IESO returned HTTP 404", 404)

        failed, code = hourly.collect(body, archive, records, downloader=missing)
        self.assertEqual((failed["status"], code), ("failed", 404))
        self.assertEqual(len(records.reports), 2)
        self.assertEqual([row["Outcome"] for row in records.attempts.values()],
                         ["archived", "already_archived", "already_archived", "archived", "rejected", "failed"])

    def test_adf_handoff_matches_databricks_inputs(self):
        pipeline = json.loads((ROOT / "adf" / "pipeline" / "pl_ieso_hourly_batch.json").read_text())
        props = pipeline["properties"]
        collectors = props["activities"][:2]
        job = props["activities"][2]
        self.assertEqual(props["concurrency"], 1)
        self.assertEqual({row["activity"] for row in job["dependsOn"]}, {row["name"] for row in collectors})
        self.assertTrue(all(row["dependencyConditions"] == ["Succeeded"] for row in job["dependsOn"]))
        self.assertEqual(set(job["typeProperties"]["jobParameters"]),
                         {"delivery_date", "da_report", "generator_report", "adf_run_id"})


@unittest.skipUnless(os.environ.get("IESO_M4_DELTA_JARS"), "requires local Spark and Delta jars")
class HourlyDeltaTests(unittest.TestCase):
    def test_hourly_models_and_revision_ordering(self):
        from pyspark.sql import SparkSession

        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch)
            spark = (SparkSession.builder.master("local[2]").appName("ieso-m6")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.warehouse.dir", (folder / "warehouse").as_uri())
                     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                     .config("spark.databricks.delta.snapshotPartitions", "2")
                     .config("spark.jars", os.environ["IESO_M4_DELTA_JARS"]).getOrCreate())
            try:
                self.exercise(spark, folder)
            finally:
                spark.stop()

    def exercise(self, spark, folder):
        def report(source, text):
            raw = text.encode()
            stamp, rows, normalized = hourly.prepare(source, raw, DAY)
            digest = sha256(normalized)
            dest = folder / source / "prepared" / "v1" / ("sha256=" + digest)
            dest.mkdir(parents=True, exist_ok=True)
            (dest / (source + ".csv")).write_bytes(normalized)
            return json.dumps({"sha256": sha256(raw), "prepared_sha256": digest, "row_count": rows,
                               "source_created_utc": stamp, "delivery_date": DAY.isoformat()})

        for schema in ("bronze", "silver", "gold"):
            spark.sql(f"CREATE SCHEMA {schema}")
        spark.sql("""
            CREATE VIEW gold.fact_rt_lmp_5m AS
            SELECT DATE '2026-11-12' AS delivery_date, 1 AS delivery_hour,
                   CAST(id + 1 AS INT) AS interval, 'TEST.A' AS pricing_location,
                   CAST(20 AS DECIMAL(38,12)) AS lmp FROM range(12)
            UNION ALL SELECT DATE '2026-11-12', 2, 1, 'TEST.A', 200
        """)
        spark.sql("""
            CREATE VIEW gold.fact_ontario_demand_5m AS
            SELECT DATE '2026-11-12' AS delivery_date, 1 AS delivery_hour,
                   CAST(id + 1 AS INT) AS interval, CAST(100 AS DECIMAL(38,12)) AS ontario_demand
            FROM range(12)
            UNION ALL SELECT DATE '2026-11-12', 2, 1, 200
        """)
        da, gen = report("da_lmp", DA), report("generator", GEN)
        args = (spark, folder.as_uri(), ("bronze", "silver", "gold"), DAY.isoformat())
        first = batch.refresh(*args, da, gen)
        self.assertTrue(first["passed"])
        self.assertEqual(first["marts"]["complete_spreads"], 1)
        self.assertEqual(first["marts"]["facility_hours_with_output"], 2)
        self.assertEqual(first["marts"]["complete_demand_hours"], 1)
        prices = spark.sql("""
            SELECT rt_minus_da_cad_per_mwh FROM gold.mart_hourly_price_spread ORDER BY delivery_hour
        """).collect()
        self.assertEqual([row[0] for row in prices], [40, None])
        mix = spark.sql("""
            SELECT reported_output_mw, ontario_demand_mw FROM gold.mart_generation_demand_hourly
            WHERE delivery_hour <= 2 ORDER BY delivery_hour
        """).collect()
        self.assertEqual([tuple(row) for row in mix], [(34, 100), (None, None)])

        revision = DA.replace("13:00:00", "14:00:00").replace("-20.00", "-21.00")
        updated = batch.refresh(*args, report("da_lmp", revision), gen)
        self.assertEqual(updated["facts"]["da_lmp"]["history_rows"], 4)
        self.assertEqual(updated["facts"]["generator"]["history_rows"], 3)
        replay = batch.refresh(*args, da, gen)
        self.assertEqual(replay["facts"]["da_lmp"]["selected_sha256"],
                         updated["facts"]["da_lmp"]["selected_sha256"])
        self.assertEqual(replay["facts"]["da_lmp"]["history_rows"], 4)
        self.assertEqual(replay["facts"]["generator"]["history_rows"], 3)
        latest = spark.sql("""
            SELECT rt_minus_da_cad_per_mwh FROM gold.mart_hourly_price_spread WHERE delivery_hour = 1
        """).first()[0]
        self.assertEqual(latest, 41)
