"""Bounded, read-only reporting queries and desktop export for IESO Milestone 8.

No collector, job, trigger, table write, warehouse creation, or Azure login is called.
Only explicit coverage/export commands can submit SQL. CLI OAuth stays in its profile.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import subprocess
import sys
import tempfile
import time
import statistics
from decimal import Decimal
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BASE_REVISION = "c607aa598318e187f8e2a32fe8ef21b6b00289c1"
DEFAULT_BATCH = "d99f4e0459d28079c2680094"
HOST = "https://adb-7405605219777405.5.azuredatabricks.net"

# These types also define the Power BI model and the CSV contracts. Null CSV cells stay null.
TABLES = {
    "Calendar": [("delivery_date", "date"), ("week_start", "date"), ("week_end", "date"),
                 ("weekday_order", "int"), ("weekday", "text")],
    "MarketIntervals": [("delivery_date", "date"), ("delivery_hour", "int"), ("interval", "int"),
        ("interval_start_utc", "datetime"), ("interval_end_utc", "datetime"),
        ("observed_lmp_locations", "int"), ("price_cad_mwh", "number"),
        ("min_price_cad_mwh", "number"), ("max_price_cad_mwh", "number"), ("demand_mw", "number")],
    "PriceHourly": [("delivery_date", "date"), ("delivery_hour", "int"), ("pricing_location", "text"),
        ("day_ahead_cad_mwh", "number"), ("rt_mean_cad_mwh", "number"),
        ("spread_cad_mwh", "number"), ("rt_intervals", "int")],
    "DemandGeneration": [("delivery_date", "date"), ("delivery_hour", "int"),
        ("reported_facilities", "int"), ("facilities_with_output", "int"),
        ("reported_output_mw", "number"), ("demand_intervals", "int"), ("demand_mw", "number")],
    "FuelHourly": [("delivery_date", "date"), ("delivery_hour", "int"), ("fuel_type", "text"),
        ("reported_facilities", "int"), ("facilities_with_output", "int"),
        ("output_mw", "number"), ("capability_mw", "number"), ("forecast_mw", "number"),
        ("available_capacity_mw", "number")],
    "GeneratorHourly": [("delivery_date", "date"), ("delivery_hour", "int"), ("generator_name", "text"),
        ("fuel_type", "text"), ("output_mw", "number"), ("capability_mw", "number"),
        ("forecast_mw", "number"), ("available_capacity_mw", "number")],
    "Coverage": [("delivery_date", "date"), ("source", "text"), ("observation_rows", "int"),
        ("observed_slots", "int"), ("expected_slots", "int"), ("missing_slots", "int"),
        ("reported_entities", "int"), ("coverage_note", "text")],
    "HealthRuns": [("batch_id", "text"), ("job_run_id", "text"), ("adf_run_id", "text"),
        ("revision", "text"), ("spark_completed_utc", "datetime"), ("spark_seconds", "number"),
        ("spark_passed", "text"), ("requested_start", "date"), ("requested_end", "date"),
        ("data_start", "date"), ("selected_delivery_hour", "int"),
        ("expected_reports", "int"), ("received_reports", "int"),
        ("final_batch_status", "text"), ("final_batch_completed_utc", "datetime"), ("scope_note", "text")],
    "HealthReports": [("batch_id", "text"), ("source", "text"), ("delivery_date", "date"),
        ("delivery_hour", "int"), ("report_id", "text"), ("expected_observations", "int"),
        ("accepted_unique", "int"), ("rejected", "int"), ("repeated_deliveries", "int"),
        ("archive_recovered", "text"), ("without_kafka_delivery", "int")],
    "HealthSteps": [("batch_id", "text"), ("stage", "text"), ("revision", "text"),
        ("completed_utc", "datetime")],
}


def window(start: str, end: str) -> tuple[date, date]:
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first.isoformat() != start or last.isoformat() != end or not 0 <= (last-first).days < 14:
        raise ValueError("Choose 1 to 14 delivery dates, using YYYY-MM-DD. This reads existing data only.")
    return first, last


def literal(value: str) -> str:
    if not value or any(ord(ch) < 32 for ch in value):
        raise ValueError("A non-empty printable pricing location is required.")
    return "'" + value.replace("'", "''") + "'"


def calendar_rows(first: date, last: date):
    day = first
    while day <= last:
        monday = day-timedelta(days=day.weekday())
        yield [day.isoformat(), monday.isoformat(), (monday+timedelta(days=6)).isoformat(),
               day.weekday()+1, day.strftime("%A")]
        day += timedelta(days=1)


def queries(start: str, end: str, location: str, batch_id: str) -> dict[str, str]:
    window(start, end)
    loc = literal(location)
    if not re.fullmatch(r"[0-9a-f]{24}", batch_id):
        raise ValueError("batch_id must be the 24-character ID of an existing M7 batch.")
    pred = f"delivery_date BETWEEN DATE '{start}' AND DATE '{end}'"
    days = f"SELECT explode(sequence(DATE '{start}', DATE '{end}', INTERVAL 1 DAY)) AS delivery_date"
    raw = f"abfss://raw@iesodev001.dfs.core.windows.net/ieso/ops/m7/{batch_id}/inventory.json"
    inventory = f"""SELECT from_json(cast(content AS STRING),
      'plan STRUCT<start_date:STRING,end_date:STRING,data_start_date:STRING,delivery_hour:INT,
       tasks:ARRAY<STRUCT<key:STRING,source:STRING,day:STRING,hour:INT>>>,
       reports ARRAY<STRUCT<key:STRING,source:STRING,day:STRING,hour:INT,expected_count:BIGINT,
       record:STRUCT<RowKey:STRING>>>') AS inventory
      FROM read_files('{raw}', format => 'binaryFile')"""
    run = f"SELECT * FROM ieso_dev.ops.m7_runs WHERE batch_id='{batch_id}'"
    # Reconciliation is stored only for the two real-time sources. The hourly feeds' source
    # counts reconcile in their M6 metrics, not in these real-time rejection/delivery fields.
    stats_type = """MAP<STRING,STRUCT<reconcile:STRUCT<reports:ARRAY<STRUCT<report_id:STRING,
      expected:BIGINT,accepted_unique:BIGINT,rejected:BIGINT,valid_duplicate_deliveries:BIGINT,
      accepted_without_kafka_delivery:BIGINT>>,archive_recovered_reports:ARRAY<STRING>>>>"""
    hourly_type = """ARRAY<STRUCT<delivery_date:STRING,facts:STRUCT<da_lmp:STRUCT<rows:BIGINT>,
      generator:STRUCT<rows:BIGINT>>>>"""
    return {
        "Calendar": f"""WITH days AS ({days})
          SELECT delivery_date, cast(date_trunc('week',delivery_date) AS DATE) AS week_start,
          date_add(cast(date_trunc('week',delivery_date) AS DATE),6) AS week_end,
          pmod(dayofweek(delivery_date)+5,7)+1 AS weekday_order,
          date_format(delivery_date,'EEEE') AS weekday FROM days""",
        "MarketIntervals": f"""SELECT delivery_date, delivery_hour, interval,
          interval_start_utc, interval_end_utc, observed_lmp_locations,
          avg_location_lmp_cad_per_mwh AS price_cad_mwh,
          min_location_lmp_cad_per_mwh AS min_price_cad_mwh,
          max_location_lmp_cad_per_mwh AS max_price_cad_mwh,
          ontario_demand_mw AS demand_mw
          FROM ieso_dev.gold.mart_market_overview WHERE {pred}
          ORDER BY delivery_date,delivery_hour,interval""",
        "PriceHourly": f"""SELECT delivery_date, delivery_hour, pricing_location,
          da_lmp_cad_per_mwh AS day_ahead_cad_mwh,
          CASE WHEN rt_intervals=12 THEN observed_rt_mean_cad_per_mwh END AS rt_mean_cad_mwh,
          rt_minus_da_cad_per_mwh AS spread_cad_mwh, rt_intervals
          FROM ieso_dev.gold.mart_hourly_price_spread
          WHERE {pred} AND pricing_location={loc} ORDER BY delivery_date,delivery_hour""",
        "DemandGeneration": f"""SELECT delivery_date,delivery_hour,reported_facilities,
          facilities_with_output,reported_output_mw,demand_intervals,
          CASE WHEN demand_intervals=12 THEN ontario_demand_mw END AS demand_mw
          FROM ieso_dev.gold.mart_generation_demand_hourly WHERE {pred}
          ORDER BY delivery_date,delivery_hour""",
        "FuelHourly": f"""SELECT delivery_date,delivery_hour,fuel_type,reported_facilities,
          facilities_with_output,reported_output_mw AS output_mw,
          reported_capability_mw AS capability_mw,reported_forecast_mw AS forecast_mw,
          reported_available_capacity_mw AS available_capacity_mw
          FROM ieso_dev.gold.mart_generation_fuel_hourly WHERE {pred}
          ORDER BY delivery_date,delivery_hour,fuel_type""",
        "GeneratorHourly": f"""SELECT delivery_date,delivery_hour,generator_name,fuel_type,
          output_mw,capability_mw,forecast_mw,available_capacity_mw
          FROM ieso_dev.gold.fact_generator_hourly WHERE {pred}
          ORDER BY delivery_date,delivery_hour,generator_name""",
        "Coverage": f"""WITH days AS ({days}),
          sources AS (SELECT 'rt_lmp' AS source,288 AS expected_slots UNION ALL
            SELECT 'rt_totals',288 UNION ALL SELECT 'da_lmp',24 UNION ALL SELECT 'generator',24),
          observed AS (
            SELECT delivery_date,'rt_lmp' AS source,count(*) AS observation_rows,
              count(DISTINCT struct(delivery_hour,interval)) AS observed_slots,
              count(DISTINCT pricing_location) AS reported_entities
              FROM ieso_dev.gold.fact_rt_lmp_5m WHERE {pred} AND lmp IS NOT NULL GROUP BY delivery_date
            UNION ALL
            SELECT delivery_date,'rt_totals',count(*),
              count(DISTINCT struct(delivery_hour,interval)),cast(1 AS BIGINT)
              FROM ieso_dev.gold.fact_ontario_demand_5m WHERE {pred} AND ontario_demand IS NOT NULL GROUP BY delivery_date
            UNION ALL
            SELECT delivery_date,'da_lmp',count(*),count(DISTINCT delivery_hour),count(DISTINCT pricing_location)
              FROM ieso_dev.gold.fact_da_lmp_hourly WHERE {pred} AND lmp IS NOT NULL GROUP BY delivery_date
            UNION ALL
            SELECT delivery_date,'generator',count(*),count(DISTINCT delivery_hour),count(DISTINCT generator_name)
              FROM ieso_dev.gold.fact_generator_hourly WHERE {pred} GROUP BY delivery_date)
          SELECT d.delivery_date,s.source,coalesce(o.observation_rows,0) AS observation_rows,
            coalesce(o.observed_slots,0) AS observed_slots,s.expected_slots,
            s.expected_slots-coalesce(o.observed_slots,0) AS missing_slots,
            coalesce(o.reported_entities,0) AS reported_entities,
            CASE WHEN s.source IN ('rt_lmp','da_lmp') THEN 'Any-location time coverage; not all-location completeness'
                 WHEN s.source='generator' THEN 'Reported facility-hour coverage; measurements may still be missing'
                 ELSE 'Non-null Ontario demand interval coverage' END AS coverage_note
          FROM days d CROSS JOIN sources s LEFT JOIN observed o
          ON d.delivery_date=o.delivery_date AND s.source=o.source ORDER BY d.delivery_date,s.source""",
        "HealthRuns": f"""WITH inv AS ({inventory}), ranked AS (
          SELECT *,row_number() OVER (PARTITION BY batch_id ORDER BY completed_at DESC) AS n FROM ({run})),
          r AS (SELECT * FROM ranked WHERE n=1)
          SELECT r.batch_id,r.job_run_id,r.adf_run_id,r.revision,r.completed_at AS spark_completed_utc,
            cast(get_json_object(r.metrics,'$.runtime_seconds') AS DOUBLE) AS spark_seconds,
            get_json_object(r.metrics,'$.passed') AS spark_passed,
            cast(i.inventory.plan.start_date AS DATE) AS requested_start,
            cast(i.inventory.plan.end_date AS DATE) AS requested_end,
            cast(i.inventory.plan.data_start_date AS DATE) AS data_start,
            i.inventory.plan.delivery_hour AS selected_delivery_hour,
            size(i.inventory.plan.tasks) AS expected_reports,size(i.inventory.reports) AS received_reports,
            'Unknown - check ADF/control record' AS final_batch_status,
            cast(NULL AS TIMESTAMP) AS final_batch_completed_utc,
            'Sealed inventory and successful Spark records only; not a complete attempt/failure history' AS scope_note
          FROM r CROSS JOIN inv i""",
        "HealthReports": f"""WITH inv AS ({inventory}), ranked AS (
          SELECT *,row_number() OVER (PARTITION BY batch_id ORDER BY completed_at DESC) AS n FROM ({run})),
          r AS (SELECT * FROM ranked WHERE n=1),
          sources AS (SELECT r.batch_id,explode(from_json(get_json_object(r.metrics,'$.real_time'),
            '{stats_type}')) AS (source,detail) FROM r),
          stats AS (SELECT batch_id,source,detail.reconcile.archive_recovered_reports AS recovered,
            explode(detail.reconcile.reports) AS report FROM sources),
          hourly AS (SELECT explode(from_json(get_json_object(metrics,'$.hourly'),'{hourly_type}')) AS item FROM r),
          wanted AS (SELECT explode(inventory.reports) AS item FROM inv)
          SELECT '{batch_id}' AS batch_id,w.item.source,cast(w.item.day AS DATE) AS delivery_date,
            w.item.hour AS delivery_hour,coalesce(w.item.record.RowKey,w.item.key) AS report_id,
            w.item.expected_count AS expected_observations,
            CASE WHEN w.item.source='da_lmp' THEN h.item.facts.da_lmp.rows
                 WHEN w.item.source='generator' THEN h.item.facts.generator.rows
                 ELSE s.report.accepted_unique END AS accepted_unique,
            s.report.rejected,s.report.valid_duplicate_deliveries AS repeated_deliveries,
            CASE WHEN w.item.source NOT IN ('rt_lmp','rt_totals') THEN 'Not applicable to batch route'
              WHEN s.report.report_id IS NULL THEN 'Unknown'
              WHEN array_contains(s.recovered,s.report.report_id) THEN 'Yes' ELSE 'No' END AS archive_recovered,
            s.report.accepted_without_kafka_delivery AS without_kafka_delivery
          FROM wanted w LEFT JOIN stats s ON w.item.source=s.source AND w.item.record.RowKey=s.report.report_id
          LEFT JOIN hourly h ON w.item.day=h.item.delivery_date
          ORDER BY delivery_date,delivery_hour,source""",
        "HealthSteps": f"""SELECT batch_id,stage,revision,completed_at AS completed_utc
          FROM ieso_dev.ops.m7_steps WHERE batch_id='{batch_id}' ORDER BY completed_at,stage""",
    }


def cli_api(method: str, endpoint: str, profile: str, payload=None):
    """Use the desktop CLI's existing OAuth profile; never print credentials."""
    args = ["databricks", "api", method.lower(), endpoint, "--profile", profile, "--output", "json"]
    if payload is not None:
        # Temp file avoids PowerShell/Windows command-line escaping and contains SQL only.
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".json", delete=False) as f:
            json.dump(payload, f)
            filename = f.name
        args += ["--json", "@"+filename]
    else:
        filename = None
    try:
        result = subprocess.run(args, capture_output=True, text=True, encoding="utf-8", timeout=90)
        if result.returncode:
            raise RuntimeError(result.stderr.strip() or "Databricks CLI request failed.")
        return json.loads(result.stdout)
    finally:
        if filename:
            Path(filename).unlink(missing_ok=True)


def warehouse_guard(info):
    allowed = {"2X-Small", "X-Small", "Small"}
    if info.get("cluster_size") not in allowed or info.get("max_num_clusters", 1) != 1:
        raise ValueError("Use a project warehouse of size Small or smaller, with maximum clusters=1.")
    if not 0 < info.get("auto_stop_mins", 0) <= 5:
        raise ValueError("Set warehouse auto-stop to 5 minutes or less before submitting queries.")
    if not info.get("enable_serverless_compute"):
        raise ValueError("This batch expects the approved Serverless SQL approach; review other types separately.")


def result_rows(response, profile):
    meta = response.get("manifest", {})
    if meta.get("truncated"):
        raise RuntimeError("Query results were truncated. No partial export will be published; narrow the read window.")
    names = [c["name"] for c in meta["schema"]["columns"]]
    result = response.get("result", {})
    rows = list(result.get("data_array") or [])
    while result.get("next_chunk_internal_link"):
        link = result["next_chunk_internal_link"]
        if not link.startswith("/api/2.0/sql/statements/") or "://" in link:
            raise RuntimeError("Unexpected result endpoint.")
        result = cli_api("get", link, profile)
        rows.extend(result.get("data_array") or [])
    expected = meta.get("total_row_count")
    if expected is not None and len(rows) != expected:
        raise RuntimeError("Result chunk count does not match the returned row count.")
    return names, rows


def execute(sql, warehouse_id, profile, timeout=180):
    response = cli_api("post", "/api/2.0/sql/statements", profile, {
        "warehouse_id": warehouse_id, "statement": sql, "format": "JSON_ARRAY", "disposition": "INLINE",
        "wait_timeout": "10s", "on_wait_timeout": "CONTINUE", "row_limit": 150000, "byte_limit": 20000000,
    })
    statement_id = response["statement_id"]
    print(f"  SQL statement ID: {statement_id}", flush=True)
    deadline = time.monotonic()+timeout
    try:
        while response["status"]["state"] in {"PENDING", "RUNNING"}:
            if time.monotonic() >= deadline:
                raise TimeoutError("SQL exceeded the 180-second reporting limit; inspect before retrying.")
            time.sleep(2)
            response = cli_api("get", f"/api/2.0/sql/statements/{statement_id}", profile)
        if response["status"]["state"] != "SUCCEEDED":
            raise RuntimeError(json.dumps(response["status"].get("error", response["status"])))
        return result_rows(response, profile)
    except BaseException:
        try:
            cli_api("post", f"/api/2.0/sql/statements/{statement_id}/cancel", profile, {})
        except Exception:
            pass
        raise


def write_csv(path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(fields)
        writer.writerows(rows)


def validate_export(data):
    """Validate source rules independently of the report's formulas."""
    issues = []
    for row in data.get("PriceHourly", []):
        if row.get("spread_cad_mwh") not in (None, "") and (
            int(row["rt_intervals"]) != 12 or row.get("day_ahead_cad_mwh") in (None, "")):
            issues.append("An hourly spread lacks all 12 real-time intervals or a day-ahead price.")
    for row in data.get("DemandGeneration", []):
        if row.get("demand_mw") not in (None, "") and int(row["demand_intervals"]) != 12:
            issues.append("An hourly demand average lacks all 12 intervals.")
    keys = {
        "Calendar": ["delivery_date"], "MarketIntervals": ["delivery_date", "delivery_hour", "interval"],
        "PriceHourly": ["delivery_date", "delivery_hour", "pricing_location"],
        "DemandGeneration": ["delivery_date", "delivery_hour"],
        "FuelHourly": ["delivery_date", "delivery_hour", "fuel_type"],
        "GeneratorHourly": ["delivery_date", "delivery_hour", "generator_name"],
        "Coverage": ["delivery_date", "source"],
        "HealthRuns": ["batch_id"], "HealthReports": ["batch_id", "source", "delivery_date", "delivery_hour", "report_id"],
        "HealthSteps": ["batch_id", "stage"],
    }
    for name, columns in keys.items():
        seen = set()
        for row in data.get(name, []):
            key = tuple(row[c] for c in columns)
            if key in seen:
                issues.append(f"Duplicate reporting key in {name}: {key}")
            seen.add(key)
    return sorted(set(issues))


def snapshot_init(destination: Path, start: str, end: str):
    first, last = window(start, end)
    if any(destination.glob("*.csv")):
        raise ValueError("Snapshot folder already contains CSVs. Choose an empty folder; existing exports are preserved.")
    for name, fields in TABLES.items():
        rows = calendar_rows(first, last) if name == "Calendar" else []
        write_csv(destination/f"{name}.csv", [f[0] for f in fields], rows)
    write_csv(destination/"Snapshot.csv", ["exported_utc", "source_state", "start_date", "end_date", "reviewed_baseline_revision"],
              [[datetime.now(timezone.utc).isoformat(), "EMPTY - NO MEASUREMENTS LOADED", start, end, BASE_REVISION]])


def read_csv(path):
    with Path(path).open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def market_clock(now=None):
    """Existing IESO convention is fixed UTC-05, not the desktop's DST time zone."""
    return (now or datetime.now(timezone.utc)).astimezone(timezone(timedelta(hours=-5)))


def weekly_reference(rows, monday, now=None):
    """Independent decimal reference for the displayed provincial interval-average series."""
    if monday.weekday() != 0:
        raise ValueError("The reference week must start on Monday.")
    clock = market_clock(now)
    week_start = datetime.combine(monday, datetime.min.time(), clock.tzinfo)
    elapsed = max(0, min(168, int((clock-week_start).total_seconds()//3600)))
    by_stamp = {}
    for row in rows:
        key = (date.fromisoformat(str(row["delivery_date"])), int(row["delivery_hour"]), int(row["interval"]))
        if key in by_stamp:
            raise ValueError("Duplicate market observation keys cannot be compared.")
        by_stamp[key] = row
    current, previous, demand_current, demand_previous, observed_prices, observed_demand = [], [], [], [], [], []
    for key, row in by_stamp.items():
        day, hour, interval = key
        offset = (day-monday).days*24+hour-1
        if not (monday <= day < monday+timedelta(days=7) and 0 <= offset < elapsed):
            continue
        p, d = row.get("price_cad_mwh"), row.get("demand_mw")
        if p not in (None, ""): observed_prices.append(Decimal(str(p)))
        if d not in (None, ""): observed_demand.append(Decimal(str(d)))
        old = by_stamp.get((day-timedelta(days=7), hour, interval), {})
        op, od = old.get("price_cad_mwh"), old.get("demand_mw")
        if p not in (None, "") and op not in (None, ""):
            current.append(Decimal(str(p))); previous.append(Decimal(str(op)))
        if d not in (None, "") and od not in (None, ""):
            demand_current.append(Decimal(str(d))); demand_previous.append(Decimal(str(od)))
    mean = lambda values: sum(values)/len(values) if values else None
    a,b = mean(current),mean(previous)
    return {
        "week_start": monday.isoformat(), "week_end": (monday+timedelta(days=6)).isoformat(),
        "reference_as_of_utc": clock.astimezone(timezone.utc).isoformat(),
        "elapsed_complete_hours": elapsed, "expected_intervals": elapsed*12,
        "observed_price_intervals": len(observed_prices), "observed_demand_intervals": len(observed_demand),
        "week_price_cad_mwh": mean(observed_prices),
        "week_population_sd": statistics.pstdev(observed_prices) if len(observed_prices)>=2 else None,
        "week_observed_demand_peak_mw": max(observed_demand) if observed_demand else None,
        "paired_price_intervals":len(current), "paired_current_price":a,"paired_previous_price":b,
        "paired_price_change":a-b if a is not None and b is not None else None,
        "paired_demand_intervals":len(demand_current),
        "paired_current_demand_peak":max(demand_current) if demand_current else None,
        "paired_previous_demand_peak":max(demand_previous) if demand_previous else None,
    }


def check_local(destination):
    data={name:read_csv(destination/f"{name}.csv") for name in TABLES}
    issues=validate_export(data)
    if issues: raise ValueError("; ".join(issues))
    mondays=sorted({date.fromisoformat(row["week_start"]) for row in data["Calendar"]})
    references=[weekly_reference(data["MarketIntervals"],monday) for monday in mondays]
    if references:
        write_csv(destination/"weekly_reference.csv",list(references[0]),[list(r.values()) for r in references])
    for r in references:
        print(json.dumps(r,default=str,indent=2))
    print("Local checks passed: unique row keys and published hourly 12-interval rules. No cloud calls.")
    print("Compare weekly_reference.csv with Power BI using the same selected week and reporting hour.")


def history_plan(destination):
    coverage=read_csv(destination/"Coverage.csv")
    if not coverage:raise ValueError("Run the bounded existing-data coverage check first.")
    missing=[r for r in coverage if int(r["missing_slots"])>0]
    days=sorted({r["delivery_date"] for r in missing})
    result={"approved_to_collect":False,"scope":"Planning only; existing coverage is not a source-availability check",
        "dates_with_missing_time_slots":days,"missing_by_feed_and_day":missing,
        "upper_bound_full_day_report_tasks":len(days)*50,
        "note":"50 tasks/day is 24 RT LMP + 24 RT Totals + 1 DA + 1 generator. It is not HTTP requests, rows, or cost. Check each location/facility and retained archives before any collection approval."}
    (destination/"history_request.json").write_text(json.dumps(result,indent=2),encoding="utf-8")
    print(json.dumps(result,indent=2))
    print("No collection commands were generated or executed. Obtain separate approval before historical collection.")


def attach_control(destination, source_file):
    """Accept a read-only Azure Table JSON export; do not guess final completion from Spark."""
    payload=json.loads(Path(source_file).read_text(encoding="utf-8-sig"))
    if isinstance(payload,dict):
        marker=payload.get("nextMarker") or payload.get("next_marker")
        if marker and any(marker.values() if isinstance(marker,dict) else [marker]):
            raise ValueError("Azure Table result is paginated. Export the remaining pages before attaching evidence.")
        records=payload.get("items",payload.get("value",[]))
    else:records=payload
    if not isinstance(records,list):raise ValueError("Expected an Azure Table entity JSON list or an object containing items.")
    by_id={r.get("RowKey","").removeprefix("batch_"):r for r in records
           if r.get("PartitionKey")=="m7_control" and r.get("RowKey","").startswith("batch_")}
    runs=read_csv(destination/"HealthRuns.csv")
    updates=0
    for row in runs:
        control=by_id.get(row["batch_id"])
        if not control:continue
        row["final_batch_status"]=control.get("Status") or "Unknown"
        row["final_batch_completed_utc"]=control.get("UpdatedUtc","") if control.get("Status")=="succeeded" else ""
        updates+=1
    if not updates:raise ValueError("No matching imported batch IDs were found. Existing report evidence is unchanged.")
    write_csv(destination/"HealthRuns.csv",[f[0] for f in TABLES["HealthRuns"]],[[r.get(f[0],"") for f in TABLES["HealthRuns"]] for r in runs])
    print(f"Attached final control status for {updates} imported batch(s). This snapshot is not live monitoring.")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["access", "coverage", "export", "init-data", "configure", "stop", "check-local", "history-plan", "attach-control"])
    parser.add_argument("--profile", default="IESO_DATABRICKS")
    parser.add_argument("--warehouse-id", default="")
    parser.add_argument("--start", default="2026-11-02")
    parser.add_argument("--end", default="2026-11-15")
    parser.add_argument("--location", default="CHOOSE_FROM_COVERAGE_OUTPUT")
    parser.add_argument("--batch-id", default=DEFAULT_BATCH)
    parser.add_argument("--output", default=str(ROOT/".local"/"data"))
    parser.add_argument("--mode", choices=["CSV", "Databricks"], default="CSV")
    parser.add_argument("--allow-compute", action="store_true")
    parser.add_argument("--control-file")
    args = parser.parse_args(argv)
    first, last = window(args.start, args.end)
    destination = Path(args.output).resolve()
    if args.command=="check-local":
        check_local(destination);return
    if args.command=="history-plan":
        history_plan(destination);return
    if args.command=="attach-control":
        if not args.control_file:raise ValueError("Pass --control-file with your downloaded Azure Table JSON export.")
        attach_control(destination,args.control_file);return
    if args.command == "access":
        info = cli_api("get", "/api/2.0/sql/warehouses", args.profile)
        print(json.dumps([{k: w.get(k) for k in ("id", "name", "state", "cluster_size", "max_num_clusters",
            "auto_stop_mins", "enable_serverless_compute")} for w in info.get("warehouses", [])], indent=2))
        print("Metadata only. No warehouse start or SQL query was requested.")
        return
    if args.command == "init-data":
        snapshot_init(destination, args.start, args.end)
        print(f"Created header-only data files in {destination}. No synthetic measurements or live values.")
        return
    if args.command == "configure":
        from build_project import configure
        configure(args.mode, str(destination), args.start, args.end, args.location, args.batch_id, args.warehouse_id)
        print("Updated Power BI parameters. Keep Desktop closed while changing these files. No SQL submitted.")
        return
    if not re.fullmatch(r"[0-9a-fA-F]{8,32}", args.warehouse_id):
        raise ValueError("Copy the SQL warehouse ID from its Connection details page.")
    if args.command == "stop":
        cli_api("post", f"/api/2.0/sql/warehouses/{args.warehouse_id}/stop", args.profile, {})
        print("Warehouse stop requested. Confirm Stopped in the Databricks SQL Warehouses page.")
        return
    if not args.allow_compute:
        raise ValueError("SQL can start billable compute. Review the window/settings, then explicitly pass --allow-compute.")
    info = cli_api("get", f"/api/2.0/sql/warehouses/{args.warehouse_id}", args.profile)
    warehouse_guard(info)
    sqls = queries(args.start, args.end, args.location, args.batch_id)
    if args.command == "coverage":
        sqls = {"Coverage": sqls["Coverage"], "Locations": f"""SELECT DISTINCT pricing_location
          FROM ieso_dev.gold.mart_hourly_price_spread WHERE delivery_date BETWEEN DATE '{args.start}'
          AND DATE '{args.end}' ORDER BY pricing_location"""}
    elif args.location == "CHOOSE_FROM_COVERAGE_OUTPUT":
        raise ValueError("Choose one real pricing location from the coverage output before export.")
    data = {}
    stop_error = None
    try:
        with tempfile.TemporaryDirectory(prefix="ieso_m8_") as tmp:
            staging = Path(tmp)
            for name, sql in sqls.items():
                print(f"Reading {name}: {first} through {last}; existing data only.", flush=True)
                names, rows = execute(sql, args.warehouse_id, args.profile)
                expected = [f[0] for f in TABLES[name]] if name in TABLES else ["pricing_location"]
                if names != expected:
                    raise RuntimeError(f"{name} returned unexpected columns: {names}")
                data[name] = [dict(zip(names, row)) for row in rows]
                write_csv(staging/f"{name}.csv", names, rows)
                print(f"  {len(rows):,} rows", flush=True)
            issues = validate_export(data)
            if args.command == "export" and len(data.get("HealthRuns",[])) != 1:
                issues.append("The chosen batch has no single recorded successful Spark result. Select an existing batch ID.")
            if issues:
                raise ValueError("Export validation failed: " + "; ".join(issues))
            if args.command == "export":
                write_csv(staging/"Snapshot.csv", ["exported_utc", "source_state", "start_date", "end_date", "reviewed_baseline_revision"],
                    [[datetime.now(timezone.utc).isoformat(), "LIVE GOLD EXPORT", args.start, args.end, BASE_REVISION]])
            destination.mkdir(parents=True, exist_ok=True)
            # All SQL and row checks pass before replacing any prior export file.
            for path in staging.iterdir():
                (destination/path.name).write_bytes(path.read_bytes())
            if args.command == "coverage":
                for row in data["Coverage"]:
                    print(f"{row['delivery_date']} {row['source']:10s} slots={row['observed_slots']}/{row['expected_slots']} "
                          f"missing={row['missing_slots']} rows={row['observation_rows']}")
                locations = [r["pricing_location"] for r in data["Locations"]]
                print("Pricing locations (first 30):", ", ".join(locations[:30]))
                print(f"All {len(locations)} names saved in Locations.csv.")
            print(f"Saved validated {args.command} output to {destination}.")
    finally:
        try:
            cli_api("post", f"/api/2.0/sql/warehouses/{args.warehouse_id}/stop", args.profile, {})
            print("Warehouse stop requested. Confirm Stopped in the UI.", flush=True)
        except Exception as exc:
            stop_error = exc
            print("STOP REQUEST FAILED. Open SQL Warehouses and click Stop now.", file=sys.stderr)
    if stop_error:
        raise RuntimeError("Data operation finished, but the warehouse stop needs manual confirmation.")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError, TimeoutError, FileNotFoundError, subprocess.TimeoutExpired) as exc:
        print(f"STOP: {exc}", file=sys.stderr)
        raise SystemExit(1)
