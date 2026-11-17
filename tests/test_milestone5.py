"""Test Gold delivery windows and, when enabled, a local Spark/Delta refresh."""

import importlib.util
import os
import tempfile
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "ieso_gold", ROOT / "databricks" / "milestone5" / "gold.py"
)
gold = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gold)


class DeliveryWindowTests(unittest.TestCase):
    def test_accepts_one_or_two_dates(self):
        first, last = date(2026, 11, 1), date(2026, 11, 2)
        self.assertEqual(gold.date_window("auto", "", first), (first, first))
        self.assertEqual(gold.date_window("2026-11-01", "", None), (first, first))
        self.assertEqual(gold.date_window("2026-11-01", "2026-11-02", None), (first, last))

    def test_rejects_missing_or_unbounded_periods(self):
        for start, end in (
            ("auto", ""), ("auto", "2026-11-01"), ("20261101", ""),
            ("2026-11-01", "2026-11-03"), ("2026-11-02", "2026-11-01"),
        ):
            with self.subTest(start=start, end=end), self.assertRaises(ValueError):
                gold.date_window(start, end, None)


@unittest.skipUnless(os.environ.get("IESO_M4_DELTA_JARS"), "requires local Spark and Delta jars")
class GoldDeltaTests(unittest.TestCase):
    def test_refresh_repeat_and_correct_one_day(self):
        from pyspark.sql import SparkSession

        with tempfile.TemporaryDirectory() as warehouse:
            spark = (SparkSession.builder.master("local[2]").appName("ieso-m5")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "2")
                     .config("spark.sql.warehouse.dir", warehouse)
                     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                     .config("spark.databricks.delta.snapshotPartitions", "2")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.jars", os.environ["IESO_M4_DELTA_JARS"]).getOrCreate())
            try:
                self.exercise_refresh(spark)
            finally:
                spark.stop()

    def exercise_refresh(self, spark):
        spark.sql("CREATE SCHEMA silver")
        spark.sql("CREATE SCHEMA gold")
        spark.sql("""
            CREATE TABLE silver.prices USING DELTA AS
            SELECT to_date(day) AS delivery_date, 1 AS delivery_hour, slot AS interval,
                   loc AS pricing_location, cast(price AS decimal(38,12)) AS lmp,
                   cast(2 AS decimal(38,12)) AS energy_loss_price,
                   cast(3 AS decimal(38,12)) AS energy_congestion_price,
                   to_timestamp(concat(day, ' 05:00:00')) + (slot-1)*INTERVAL 5 MINUTES AS interval_start_utc,
                   to_timestamp(concat(day, ' 05:00:00')) + slot*INTERVAL 5 MINUTES AS interval_end_utc,
                   'price-report' AS report_id, concat(day, loc, slot) AS event_id,
                   to_timestamp(concat(day, ' 05:05:00')) AS source_version_utc,
                   'ieso/lmp/fixture.csv' AS archive_path
            FROM VALUES ('2026-11-01', 1, 'A', -10), ('2026-11-01', 2, 'A', 10),
                        ('2026-11-01', 1, 'B', 30), ('2026-11-02', 1, 'A', 50)
                         AS src(day, slot, loc, price)
        """)
        spark.sql("CREATE VIEW silver.rt_lmp_current AS SELECT * FROM silver.prices")
        spark.sql("""
            CREATE VIEW silver.rt_totals_current AS
            SELECT delivery_date, delivery_hour, interval, 'ONTARIO' AS pricing_location,
                   cast(15000 AS decimal(38,12)) AS ontario_demand, interval_start_utc,
                   interval_end_utc, 'demand-report' AS report_id, event_id,
                   source_version_utc, 'ieso/totals/fixture.csv' AS archive_path
            FROM silver.prices WHERE pricing_location = 'A' AND interval = 1
        """)
        initial = gold.refresh_gold(spark, "silver", "gold", "2026-11-01", "2026-11-02")
        self.assertTrue(initial["passed"])
        self.assertEqual(initial["gold"]["overview_rows"], 576)
        self.assertEqual(initial["gold"]["overview_demand_rows"], 2)
        what = spark.sql("""
            SELECT observed_lmp_locations, avg_location_lmp_cad_per_mwh, ontario_demand_mw
            FROM gold.mart_market_overview
            WHERE delivery_date = DATE '2026-11-01' AND delivery_hour = 1 AND interval = 1
        """).first()
        self.assertEqual(tuple(what), (2, 10, 15000))
        null_slots = spark.sql("""
            SELECT count(*) FROM gold.mart_market_overview
            WHERE observed_lmp_locations = 0 AND NOT demand_present
        """).first()[0]
        self.assertEqual(null_slots, 573)
        volatility = spark.sql("""
            SELECT pricing_location, observed_intervals, incomplete_hour, lmp_stddev_cad_per_mwh
              FROM gold.mart_price_volatility
             WHERE delivery_date = DATE '2026-11-01' ORDER BY pricing_location
        """).collect()
        self.assertEqual([tuple(row) for row in volatility], [
            ("A", 2, True, 10.0), ("B", 1, True, None)
        ])
        endpoint = spark.sql("""
            SELECT date_format(interval_start_utc, 'yyyy-MM-dd HH:mm') AS start_utc
            FROM gold.dim_time WHERE delivery_date = DATE '2026-11-01'
              AND delivery_hour = 24 AND interval = 12
        """).first()[0]
        self.assertEqual(endpoint, "2026-11-02 04:55")

        def gold_rows(day):
            return [tuple(row) for row in spark.sql(f"""
                SELECT * FROM gold.mart_market_overview WHERE delivery_date = DATE '{day}'
                ORDER BY delivery_hour, interval
            """).collect()]

        day_one = gold_rows("2026-11-01")
        day_two = gold_rows("2026-11-02")
        repeat = gold.refresh_gold(spark, "silver", "gold", "auto", "")
        self.assertEqual(repeat["delivery_start_date"], date(2026, 11, 2))
        self.assertEqual(gold_rows("2026-11-01"), day_one)
        self.assertEqual(gold_rows("2026-11-02"), day_two)
        spark.sql("""
            UPDATE silver.prices SET lmp = 20, report_id = 'corrected'
            WHERE delivery_date = DATE '2026-11-01' AND interval = 1 AND pricing_location = 'A'
        """)
        corrected = gold.refresh_gold(spark, "silver", "gold", "2026-11-01", "")
        self.assertEqual(corrected["silver_rows"], {"lmp": 3, "demand": 1})
        self.assertEqual(corrected["samples"][0]["gold_value"], 20)
        new_day_one = gold_rows("2026-11-01")
        self.assertEqual(new_day_one[0][6], 25)
        self.assertEqual(new_day_one[1:], day_one[1:])
        self.assertEqual(gold_rows("2026-11-02"), day_two)
