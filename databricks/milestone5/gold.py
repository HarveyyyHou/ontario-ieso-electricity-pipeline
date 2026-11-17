# Databricks notebook source
"""Refresh bounded real-time Gold datasets from the current Silver observations."""

import json
from datetime import date, timedelta
from time import monotonic

SLOT = "delivery_date, delivery_hour, interval"
LINEAGE = "interval_start_utc, interval_end_utc, report_id, event_id, source_version_utc, archive_path"
LMP = "fact_rt_lmp_5m"
DEMAND = "fact_ontario_demand_5m"
TIME = "dim_time"
LOCATION = "dim_pricing_location"
OVERVIEW = "mart_market_overview"
VOLATILITY = "mart_price_volatility"


def date_window(start, end, latest_common_date):
    """Return a validated inclusive range of one or two market dates."""
    start, end = start.strip(), end.strip()
    if start == "auto":
        if end:
            raise ValueError("Leave end_date blank when start_date is auto.")
        if latest_common_date is None:
            raise ValueError("No delivery date has both current Silver feeds.")
        return latest_common_date, latest_common_date

    first = date.fromisoformat(start)
    last = date.fromisoformat(end) if end else first
    if start != first.isoformat() or (end and end != last.isoformat()):
        raise ValueError("Use YYYY-MM-DD dates.")
    if not timedelta(0) <= last - first <= timedelta(days=1):
        raise ValueError("Select one date or two consecutive dates.")
    return first, last


def predicate(first, last):
    return f"delivery_date BETWEEN DATE '{first.isoformat()}' AND DATE '{last.isoformat()}'"


def write_window(spark, target, query, condition):
    if spark.catalog.tableExists(target):
        spark.sql(f"INSERT INTO {target} REPLACE WHERE {condition} {query}")
    else:
        spark.sql(f"CREATE TABLE {target} USING DELTA AS {query}")


def fact_sample(spark, sources, gold, condition):
    """Compare one deterministic source row from each feed to its stored Gold row."""
    sample_queries = []
    for source, fact, value, lmp in (
        (sources[0], LMP, "lmp", True),
        (sources[1], DEMAND, "ontario_demand", False),
    ):
        join_keys = f"{SLOT}, pricing_location" if lmp else SLOT
        location = "s.pricing_location" if lmp else "'ONTARIO'"
        sample_queries.append(f"""
            SELECT '{fact}' AS fact, s.delivery_date, s.delivery_hour, s.interval,
                   {location} AS pricing_location, s.report_id, s.archive_path,
                   s.{value} AS silver_value, g.{value} AS gold_value,
                   g.{value} <=> s.{value} AND g.report_id <=> s.report_id
                       AND g.event_id <=> s.event_id AS matches
            FROM (
                SELECT * FROM {source} WHERE {condition}
                ORDER BY {join_keys} LIMIT 1
            ) s
            LEFT JOIN (SELECT * FROM {gold}.{fact} WHERE {condition}) g
                USING ({join_keys})
        """)
    return [row.asDict() for row in spark.sql(" UNION ALL ".join(sample_queries)).collect()]


def refresh_gold(spark, silver, gold, start, end):
    """Refresh the requested dates and return reconciliation data for the job output."""
    lmp_source = f"{silver}.rt_lmp_current"
    demand_source = f"{silver}.rt_totals_current"
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    latest = None
    if start.strip() == "auto" and not end.strip():
        latest = spark.sql(f"""
            SELECT max(delivery_date) FROM (
                SELECT delivery_date FROM {lmp_source}
                INTERSECT
                SELECT delivery_date FROM {demand_source}
            )
        """).first()[0]
    first, last = date_window(start, end, latest)
    where = predicate(first, last)
    dates = (last - first).days + 1
    common_checks = """
        delivery_hour BETWEEN 1 AND 24 AND interval BETWEEN 1 AND 12
        AND report_id IS NOT NULL AND event_id IS NOT NULL
        AND interval_start_utc IS NOT NULL AND interval_end_utc IS NOT NULL
    """
    preflight = spark.sql(f"""
        SELECT 'lmp' AS feed, delivery_date, count(*) AS rows,
               count(DISTINCT struct(delivery_hour, interval, pricing_location)) AS keys,
               count_if(({common_checks} AND lmp IS NOT NULL
                   AND trim(pricing_location) <> '') IS NOT TRUE) AS invalid
        FROM {lmp_source} WHERE {where} GROUP BY delivery_date
        UNION ALL
        SELECT 'demand', delivery_date, count(*),
               count(DISTINCT struct(delivery_hour, interval)),
               count_if(({common_checks} AND ontario_demand IS NOT NULL
                   AND pricing_location = 'ONTARIO') IS NOT TRUE)
        FROM {demand_source} WHERE {where} GROUP BY delivery_date
    """).collect()
    counts = {"lmp": 0, "demand": 0}
    for row in preflight:
        if row.rows != row.keys or row.invalid:
            raise ValueError(f"Silver has invalid or duplicate observations: {row.asDict()}")
        counts[row.feed] += row.rows
    if not all(counts.values()) or len({row.delivery_date for row in preflight}) != dates:
        raise ValueError("The selected period needs both feeds and at least one observation on every date.")
    print(f"Gold window {first} through {last}. Silver counts: {counts}")

    write_window(spark, f"{gold}.{LMP}", f"""
        SELECT {SLOT}, pricing_location, lmp, energy_loss_price, energy_congestion_price,
               {LINEAGE}
        FROM {lmp_source} WHERE {where}
    """, where)
    write_window(spark, f"{gold}.{DEMAND}", f"""
        SELECT {SLOT}, ontario_demand, {LINEAGE}
        FROM {demand_source} WHERE {where}
    """, where)
    write_window(spark, f"{gold}.{TIME}", f"""
        WITH slots AS (
            SELECT day AS delivery_date, cast(slot DIV 12 + 1 AS INT) AS delivery_hour,
                   cast(pmod(slot, 12) + 1 AS INT) AS interval,
                   make_timestamp(year(day), month(day), dayofmonth(day), 5, 0, 0, 'UTC')
                       + make_interval(0, 0, 0, 0, 0, cast(slot * 5 AS INT), 0)
                       AS interval_start_utc
            FROM (
                SELECT explode(sequence(DATE '{first}', DATE '{last}', INTERVAL 1 DAY)) AS day
            ) CROSS JOIN (SELECT id AS slot FROM range(288))
        )
        SELECT {SLOT}, interval_start_utc,
               interval_start_utc + INTERVAL 5 MINUTES AS interval_end_utc
        FROM slots
    """, where)
    spark.sql(f"""
        CREATE VIEW IF NOT EXISTS {gold}.{LOCATION} AS
        SELECT DISTINCT pricing_location FROM {gold}.{LMP}
    """)
    write_window(spark, f"{gold}.{OVERVIEW}", f"""
        WITH prices AS (
            SELECT {SLOT}, count(*) AS observed_lmp_locations,
                   avg(lmp) AS avg_location_lmp_cad_per_mwh,
                   min(lmp) AS min_location_lmp_cad_per_mwh,
                   max(lmp) AS max_location_lmp_cad_per_mwh
            FROM {gold}.{LMP} WHERE {where} GROUP BY {SLOT}
        )
        SELECT t.delivery_date, t.delivery_hour, t.interval, t.interval_start_utc, t.interval_end_utc,
               coalesce(p.observed_lmp_locations, 0) AS observed_lmp_locations,
               p.avg_location_lmp_cad_per_mwh, p.min_location_lmp_cad_per_mwh,
               p.max_location_lmp_cad_per_mwh, d.ontario_demand AS ontario_demand_mw,
               d.ontario_demand IS NOT NULL AS demand_present
        FROM (SELECT * FROM {gold}.{TIME} WHERE {where}) t
        LEFT JOIN prices p USING ({SLOT})
        LEFT JOIN (SELECT * FROM {gold}.{DEMAND} WHERE {where}) d USING ({SLOT})
    """, where)
    write_window(spark, f"{gold}.{VOLATILITY}", f"""
        SELECT delivery_date, delivery_hour, pricing_location,
               count(*) AS observed_intervals, cast(12 AS INT) AS expected_intervals,
               count(*) < 12 AS incomplete_hour,
               min(lmp) AS min_lmp_cad_per_mwh, max(lmp) AS max_lmp_cad_per_mwh,
               CASE WHEN count(*) >= 2 THEN stddev_pop(lmp) END AS lmp_stddev_cad_per_mwh
        FROM {gold}.{LMP} WHERE {where}
        GROUP BY delivery_date, delivery_hour, pricing_location
    """, where)

    summary = spark.sql(f"""
        SELECT
            (SELECT count(*) FROM {gold}.{LMP} WHERE {where}) AS lmp_fact_rows,
            (SELECT count(*) FROM {gold}.{DEMAND} WHERE {where}) AS demand_fact_rows,
            (SELECT count(*) FROM {gold}.{TIME} WHERE {where}) AS time_rows,
            (SELECT coalesce(sum(observed_intervals), 0)
                FROM {gold}.{VOLATILITY} WHERE {where}) AS volatility_lmp_rows,
            count(*) AS overview_rows, count_if(demand_present) AS overview_demand_rows,
            coalesce(sum(observed_lmp_locations), 0) AS overview_lmp_rows,
            count_if(observed_lmp_locations = 0) AS intervals_without_lmp,
            count_if(NOT demand_present) AS intervals_without_demand
        FROM {gold}.{OVERVIEW} WHERE {where}
    """).first().asDict()
    samples = fact_sample(spark, (lmp_source, demand_source), gold, where)
    if not (
        summary["lmp_fact_rows"] == summary["volatility_lmp_rows"]
        == summary["overview_lmp_rows"] == counts["lmp"]
        and summary["demand_fact_rows"] == summary["overview_demand_rows"] == counts["demand"]
        and summary["time_rows"] == summary["overview_rows"] == dates * 288
        and len(samples) == 2 and all(sample["matches"] for sample in samples)
    ):
        raise RuntimeError(f"Gold reconciliation failed: Silver={counts}, Gold={summary}, samples={samples}")
    return {
        "delivery_start_date": first,
        "delivery_end_date": last,
        "silver_rows": counts,
        "gold": summary,
        "samples": samples,
        "passed": True,
    }


if "dbutils" in globals():
    dbutils.widgets.text("start_date", "auto")
    dbutils.widgets.text("end_date", "")
    dbutils.widgets.text("job_run_id", "")
    dbutils.widgets.text("source_revision", "")
    started = monotonic()
    result = refresh_gold(spark, "ieso_dev.silver", "ieso_dev.gold",
                          dbutils.widgets.get("start_date"), dbutils.widgets.get("end_date"))
    result.update(
        run_id=dbutils.widgets.get("job_run_id"),
        source_revision=dbutils.widgets.get("source_revision"),
        runtime_seconds=round(monotonic() - started, 1),
    )
    evidence = json.dumps(result, default=str, indent=2)
    print(evidence)
    dbutils.notebook.exit(evidence)
