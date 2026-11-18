# Databricks notebook source
"""Publish one delivery day of revision-aware IESO hourly observations."""

import json
import re
from datetime import date, datetime, timezone
from time import monotonic

RAW = "abfss://raw@iesodev001.dfs.core.windows.net/ieso/milestone6"
HASH = re.compile(r"[0-9a-f]{64}")
SPECS = {
    "da_lmp": (
        "pricing_location",
        "delivery_date DATE, delivery_hour INT, pricing_location STRING, "
        "lmp DECIMAL(30,9), energy_loss_price DECIMAL(30,9), energy_congestion_price DECIMAL(30,9)",
        "fact_da_lmp_hourly",
    ),
    "generator": (
        "generator_name",
        "delivery_date DATE, delivery_hour INT, generator_name STRING, fuel_type STRING, "
        "output_mw DECIMAL(30,9), capability_mw DECIMAL(30,9), forecast_mw DECIMAL(30,9), "
        "available_capacity_mw DECIMAL(30,9)",
        "fact_generator_hourly",
    ),
}


def report_options(text, day):
    value = json.loads(text)
    if not isinstance(value, dict) or value.get("delivery_date") != day.isoformat():
        raise ValueError("Collection metadata must match the requested delivery date.")
    for key in ("sha256", "prepared_sha256"):
        digest = value.get(key)
        if not isinstance(digest, str) or not HASH.fullmatch(digest):
            raise ValueError(f"Malformed {key} in collection metadata.")
    rows = value.get("row_count")
    if type(rows) is not int or rows <= 0:
        raise ValueError("Collection must report a positive row count.")
    stamp = datetime.fromisoformat(value["source_created_utc"])
    if stamp.utcoffset() != timezone.utc.utcoffset(None):
        raise ValueError("Collection metadata must use a UTC creation timestamp.")
    value["stamp"] = stamp.strftime("%Y-%m-%d %H:%M:%S")
    return value


def read_prepared(spark, source, meta, raw, date_literal):
    key, ddl, _ = SPECS[source]
    path = f"{raw}/{source}/prepared/v1/sha256={meta['prepared_sha256']}/{source}.csv"
    payload = (
        spark.read.format("binaryFile").load(path)
        .selectExpr("length", "sha2(content, 256) as sha256").first()
    )
    if not payload or payload.sha256 != meta["prepared_sha256"]:
        raise ValueError(f"{source}: prepared bytes do not match the collection SHA-256.")
    frame = spark.read.schema(ddl).options(
        header=True, enforceSchema=False, mode="FAILFAST"
    ).csv(path)
    frame.createOrReplaceTempView("m6_" + source)
    rule = f"delivery_date = {date_literal} AND delivery_hour BETWEEN 1 AND 24 AND trim({key}) <> ''"
    rule += " AND lmp IS NOT NULL" if source == "da_lmp" else " AND trim(fuel_type) <> ''"
    result = spark.sql(f"""
        SELECT count(*) AS rows, count(DISTINCT struct(delivery_hour, {key})) AS unique_keys,
               count_if(({rule}) IS NOT TRUE) AS invalid
        FROM m6_{source}
    """).first()
    if result.rows != meta["row_count"] or result.rows != result.unique_keys or result.invalid:
        raise ValueError(f"{source}: collection reconciliation failed: {result.asDict()}")
    return frame.columns


def replace_day(spark, table, query, dates):
    if spark.catalog.tableExists(table):
        spark.sql(f"INSERT INTO {table} REPLACE WHERE {dates} {query}")
    else:
        spark.sql(f"CREATE TABLE {table} USING DELTA AS {query}")


def load_revision(spark, source, meta, columns, schemas, dates):
    bronze, silver, gold = schemas
    history = f"{bronze}.m6_{source}_reports"
    current = f"{silver}.m6_{source}_current"
    fact = f"{gold}.{SPECS[source][2]}"
    listed = ", ".join(columns)
    data = f"""
        SELECT {listed}, '{meta['sha256']}' AS source_sha256,
               '{meta['prepared_sha256']}' AS prepared_sha256,
               TIMESTAMP '{meta['stamp']}' AS source_created_utc
        FROM m6_{source}
    """
    if spark.catalog.tableExists(history):
        existing = spark.sql(f"""
            SELECT count_if(source_sha256 = '{meta['sha256']}') AS same,
                   count_if(source_created_utc = TIMESTAMP '{meta['stamp']}'
                            AND source_sha256 <> '{meta['sha256']}') AS ambiguous
            FROM {history} WHERE {dates}
        """).first()
        if existing.ambiguous:
            raise ValueError(f"{source}: distinct report bytes have the same IESO timestamp; inspect the reports.")
        if not existing.same:
            spark.sql(f"INSERT INTO {history} {data}")
    else:
        spark.sql(f"CREATE TABLE {history} USING DELTA AS {data}")

    # Select the whole latest snapshot so a removed source key also leaves Silver and Gold.
    query = f"""
        SELECT {listed}, source_sha256, prepared_sha256, source_created_utc
        FROM {history}
        WHERE {dates} AND source_created_utc = (
            SELECT max(source_created_utc) FROM {history} WHERE {dates}
        )
    """
    replace_day(spark, current, query, dates)
    replace_day(spark, fact, f"SELECT * FROM {current} WHERE {dates}", dates)
    counts = spark.sql(f"""
        SELECT count(*) AS rows, count(DISTINCT source_sha256) AS revisions,
               first(source_sha256) AS selected_sha256,
               (SELECT count(*) FROM {current} WHERE {dates}) AS silver_rows,
               (SELECT count(*) FROM {history} WHERE {dates}) AS history_rows
        FROM {fact} WHERE {dates}
    """).first()
    if counts.rows != counts.silver_rows or counts.revisions != 1 or (
        counts.selected_sha256 == meta["sha256"] and counts.rows != meta["row_count"]
    ):
        raise RuntimeError(f"{source}: Gold totals do not reconcile with the selected snapshot.")
    sample = spark.sql(f"""
        SELECT {listed} FROM {fact} WHERE {dates}
        ORDER BY delivery_hour, {SPECS[source][0]} LIMIT 1
    """).first()
    return {**counts.asDict(), "sample": sample.asDict(),
            "input_rows": meta["row_count"], "input_sha256": meta["sha256"]}


def build_marts(spark, gold, day, dates):
    replace_day(spark, f"{gold}.mart_hourly_price_spread", f"""
        WITH rt AS (
            SELECT delivery_date, delivery_hour, pricing_location,
                   count(DISTINCT interval) AS rt_intervals, avg(lmp) AS rt_lmp
            FROM {gold}.fact_rt_lmp_5m WHERE {dates}
            GROUP BY delivery_date, delivery_hour, pricing_location
        ), da AS (SELECT * FROM {gold}.fact_da_lmp_hourly WHERE {dates})
        SELECT delivery_date, delivery_hour, pricing_location,
               da.lmp AS da_lmp_cad_per_mwh, rt.rt_lmp AS observed_rt_mean_cad_per_mwh,
               coalesce(rt.rt_intervals, 0) AS rt_intervals,
               CASE WHEN rt.rt_intervals = 12 AND da.lmp IS NOT NULL
                    THEN rt.rt_lmp - da.lmp END AS rt_minus_da_cad_per_mwh
        FROM da FULL OUTER JOIN rt USING (delivery_date, delivery_hour, pricing_location)
    """, dates)
    replace_day(spark, f"{gold}.mart_generation_fuel_hourly", f"""
        SELECT delivery_date, delivery_hour, fuel_type,
               count(*) AS reported_facilities, count(output_mw) AS facilities_with_output,
               sum(output_mw) AS reported_output_mw,
               sum(capability_mw) AS reported_capability_mw,
               sum(forecast_mw) AS reported_forecast_mw,
               sum(available_capacity_mw) AS reported_available_capacity_mw
        FROM {gold}.fact_generator_hourly WHERE {dates}
        GROUP BY delivery_date, delivery_hour, fuel_type
    """, dates)
    replace_day(spark, f"{gold}.mart_generation_demand_hourly", f"""
        WITH hours AS (SELECT DATE '{day}' AS delivery_date, cast(id + 1 AS INT) AS delivery_hour
                       FROM range(24)), gen AS (
            SELECT delivery_date, delivery_hour,
                   sum(reported_facilities) AS reported_facilities,
                   sum(facilities_with_output) AS facilities_with_output,
                   sum(reported_output_mw) AS reported_output_mw
            FROM {gold}.mart_generation_fuel_hourly WHERE {dates} GROUP BY delivery_date, delivery_hour
        ), demand AS (
            SELECT delivery_date, delivery_hour, count(DISTINCT interval) AS demand_intervals,
                   avg(ontario_demand) AS avg_demand_mw
            FROM {gold}.fact_ontario_demand_5m WHERE {dates} GROUP BY delivery_date, delivery_hour
        )
        SELECT delivery_date, delivery_hour,
               coalesce(gen.reported_facilities, 0) AS reported_facilities,
               coalesce(gen.facilities_with_output, 0) AS facilities_with_output,
               gen.reported_output_mw,
               coalesce(demand.demand_intervals, 0) AS demand_intervals,
               CASE WHEN demand.demand_intervals = 12 THEN demand.avg_demand_mw END AS ontario_demand_mw
        FROM hours LEFT JOIN gen USING (delivery_date, delivery_hour)
                   LEFT JOIN demand USING (delivery_date, delivery_hour)
    """, dates)


def refresh(spark, raw, schemas, day_text, da_report, generator_report):
    day = date.fromisoformat(day_text)
    if day.isoformat() != day_text:
        raise ValueError("Use a YYYY-MM-DD delivery date.")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    literal = f"DATE '{day}'"
    dates = f"delivery_date = {literal}"
    inputs = {"da_lmp": report_options(da_report, day),
              "generator": report_options(generator_report, day)}
    for source, meta in inputs.items():
        meta["columns"] = read_prepared(spark, source, meta, raw, literal)
    facts = {source: load_revision(spark, source, meta, meta["columns"], schemas, dates)
             for source, meta in inputs.items()}
    gold = schemas[2]
    build_marts(spark, gold, day, dates)
    result = spark.sql(f"""
        SELECT
          (SELECT count(*) FROM {gold}.mart_hourly_price_spread WHERE {dates}) AS spread_keys,
          (SELECT count(*) FROM {gold}.mart_hourly_price_spread WHERE {dates}
           AND rt_intervals BETWEEN 1 AND 11) AS partial_rt_keys,
          (SELECT count(rt_minus_da_cad_per_mwh) FROM {gold}.mart_hourly_price_spread WHERE {dates})
                AS complete_spreads,
          (SELECT count(*) FROM {gold}.mart_hourly_price_spread WHERE {dates}
           AND rt_minus_da_cad_per_mwh IS NOT NULL AND (rt_intervals <> 12 OR da_lmp_cad_per_mwh IS NULL))
                AS invalid_spreads,
          (SELECT sum(reported_facilities) FROM {gold}.mart_generation_fuel_hourly WHERE {dates})
                AS fuel_facility_hours,
          (SELECT sum(facilities_with_output) FROM {gold}.mart_generation_fuel_hourly WHERE {dates})
                AS facility_hours_with_output,
          (SELECT sum(reported_output_mw) FROM {gold}.mart_generation_fuel_hourly WHERE {dates}) <=>
          (SELECT sum(output_mw) FROM {gold}.fact_generator_hourly WHERE {dates}) AS fuel_output_reconciles,
          (SELECT count(ontario_demand_mw) FROM {gold}.mart_generation_demand_hourly WHERE {dates})
                AS complete_demand_hours,
          (SELECT count(*) FROM {gold}.mart_generation_demand_hourly WHERE {dates}
           AND ontario_demand_mw IS NOT NULL AND demand_intervals <> 12) AS invalid_demand_hours,
          (SELECT count(*) FROM {gold}.mart_generation_demand_hourly WHERE {dates}) AS generation_hours
    """).first().asDict()
    if (result["fuel_facility_hours"] != facts["generator"]["rows"]
            or result["generation_hours"] != 24
            or result["spread_keys"] < facts["da_lmp"]["rows"]
            or result["invalid_spreads"] or result["invalid_demand_hours"]
            or not result["fuel_output_reconciles"]):
        raise RuntimeError(f"Hourly mart totals do not reconcile: {result}")
    return {"passed": True, "delivery_date": day_text, "facts": facts, "marts": result}


if "dbutils" in globals():
    fields = ("delivery_date", "da_report", "generator_report", "adf_run_id",
              "source_revision", "job_run_id")
    for field in fields:
        dbutils.widgets.text(field, "")
    values = {field: dbutils.widgets.get(field) for field in fields}
    started = monotonic()
    evidence = refresh(spark, RAW, ("ieso_dev.bronze", "ieso_dev.silver", "ieso_dev.gold"),
                       values["delivery_date"], values["da_report"], values["generator_report"])
    evidence.update({key: values[key] for key in ("adf_run_id", "source_revision", "job_run_id")})
    evidence["runtime_seconds"] = round(monotonic() - started, 1)
    output = json.dumps(evidence, default=str, indent=2)
    print(output)
    dbutils.notebook.exit(output)
