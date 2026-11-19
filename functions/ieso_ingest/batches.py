"""ADF batch planning, resumable collection inventory, and atomic completion progress."""

import code
import hashlib
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone

from azure.core import MatchConditions
from azure.core.exceptions import ResourceNotFoundError
from azure.data.tables import UpdateMode

import collector
import totals
import hourly
import publisher

CONTROL = "m7_control"
PREFIX = "ieso/ops/m7"
EST = timezone(timedelta(hours=-5))


def iso_day(value):
    if not isinstance(value, str):
        raise ValueError("Use YYYY-MM-DD dates.")
    day = date.fromisoformat(value)
    if day.isoformat() != value:
        raise ValueError("Use YYYY-MM-DD dates.")
    return day


def plan_window(body, cursor="", today=None):
    """Bound automatic catch-up to two new completed dates plus one revision date."""
    today = today or datetime.now(EST).date()
    start, end = body.get("start_date", ""), body.get("end_date", "")
    selected_hour = body.get("delivery_hour", 0)
    if type(selected_hour) is not int or not 0 <= selected_hour <= 24:
        raise ValueError("delivery_hour is 0 for all hours or 1 through 24.")
    advance = body.get("advance_cursor", False)
    if type(advance) is not bool:
        raise ValueError("advance_cursor must be boolean.")
    if type(body.get("preview_only", False)) is not bool:
        raise ValueError("preview_only must be boolean.")
    if start == "auto":
        if end or selected_hour or not advance or not cursor:
            raise ValueError("Auto needs an initialized cursor, all hours, and advance_cursor=true.")
        first = iso_day(cursor) + timedelta(days=1)
        last = min(first + timedelta(days=1), today - timedelta(days=1))
        if last < first:
            raise ValueError("No new completed delivery dates are ready.")
        data_start = first - timedelta(days=1)
    else:
        first, last = iso_day(start), iso_day(end or start)
        if not timedelta(0) <= last - first <= timedelta(days=1) or last >= today:
            raise ValueError("Select one or two completed delivery dates.")
        data_start = first
        if advance and selected_hour:
            raise ValueError("An hourly acceptance batch cannot advance the daily cursor.")
        if advance and cursor and first != iso_day(cursor) + timedelta(days=1) and last != iso_day(cursor):
            raise ValueError("Cursor advancement requires the next consecutive delivery date.")
    tasks = []
    day = data_start
    while day <= last:
        hours = [selected_hour] if selected_hour else range(1, 25)
        for number in hours:
            for source in ("rt_lmp", "rt_totals"):
                tasks.append({"key": f"{source}_{day:%Y%m%d}_{number:02d}", "source": source,
                              "day": day.isoformat(), "hour": number})
        for source in ("da_lmp", "generator"):
            tasks.append({"key": f"{source}_{day:%Y%m%d}", "source": source,
                          "day": day.isoformat(), "hour": 0})
        day += timedelta(days=1)
    token = body.get("refresh_token", "")
    if not isinstance(token, str) or len(token) > 60 or any(ord(c) < 32 for c in token):
        raise ValueError("refresh_token must be a short printable string.")
    plan = {"start_date": first.isoformat(), "end_date": last.isoformat(),
            "data_start_date": data_start.isoformat(), "delivery_hour": selected_hour,
            "advance_cursor": advance, "refresh_token": token, "tasks": tasks}
    plan["batch_id"] = hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()[:24]
    return plan


def get(table, partition, key):
    try:
        return table.get_entity(partition, key)
    except ResourceNotFoundError:
        return None


def json_clean(record):
    return json.loads(json.dumps(dict(record), default=str))


def validate(body):
    if not isinstance(body, dict) or body.get("source") != "m7_control":
        raise ValueError("Expected source=m7_control.")
    if body.get("action") not in {"start", "collect", "publish", "seal", "complete", "failed", "status"}:
        raise ValueError("Unknown M7 action.")


def current(table, body, adf_run_id):
    batch_id = body.get("batch_id", "")
    if not isinstance(batch_id, str) or len(batch_id) != 24 or any(c not in "0123456789abcdef" for c in batch_id):
        raise ValueError("Invalid batch ID.")
    entity = get(table, CONTROL, "batch_" + batch_id)
    if not entity:
        raise ValueError("Batch was not planned.")
    if entity["LastAdfRun"] != adf_run_id:
        raise ValueError("This batch belongs to a different ADF run. Resume through the parent pipeline.")
    return entity, json.loads(entity["PlanJson"])


def state(table, entity, status, **extra):
    table.update_entity(
        {"PartitionKey": CONTROL, "RowKey": entity["RowKey"], "Status": status,
         "UpdatedUtc": collector.utc_now(), **extra}, mode=UpdateMode.MERGE
    )


def start(table, body, adf_run_id):
    cursor = get(table, CONTROL, "cursor")
    value = cursor["EndDate"] if cursor else ""
    plan = plan_window(body, value)
    if body.get("preview_only", False):
        return {"batch_id": plan["batch_id"], "already_complete": True, "tasks": [],
                "expected_reports": len(plan["tasks"]), "status": "preview",
                "start_date": plan["start_date"], "end_date": plan["end_date"],
                "data_start_date": plan["data_start_date"]}
    key = "batch_" + plan["batch_id"]
    existing = get(table, CONTROL, key)
    if existing and existing["Status"] == "succeeded":
        return {"batch_id": plan["batch_id"], "already_complete": True, "tasks": [],
                "status": "already_complete"}
    entity = {
        "PartitionKey": CONTROL, "RowKey": key, "Status": "collecting",
        "PlanJson": json.dumps(plan, sort_keys=True), "ExpectedCursor": value,
        "LastAdfRun": adf_run_id, "UpdatedUtc": collector.utc_now(),
    }
    if existing:
        if existing["ExpectedCursor"] != value:
            raise ValueError("Cursor moved since this unfinished batch. Inspect it before resuming.")
        entity["ExpectedCursor"] = existing["ExpectedCursor"]
        table.update_entity(entity, mode=UpdateMode.MERGE, etag=existing.metadata["etag"],
                            match_condition=MatchConditions.IfNotModified)
    else:
        table.create_entity(entity)
    pending = []
    for task in plan["tasks"]:
        report = get(table, plan["batch_id"], task["key"])
        if not report or not report.get("Published"):
            pending.append(task)
    logging.info("ieso_m7_start batch=%s reports=%s pending=%s adf=%s",
                 plan["batch_id"], len(plan["tasks"]), len(pending), adf_run_id)
    return {"batch_id": plan["batch_id"], "already_complete": False, "tasks": pending,
            "expected_reports": len(plan["tasks"]), "status": "collecting"}


def task_for(plan, task_key):
    for task in plan["tasks"]:
        if task["key"] == task_key:
            return task
    raise ValueError("Report is outside this batch's inventory.")


def collect_report(table, archive, plan, task, adf_run_id):
    existing = get(table, plan["batch_id"], task["key"])
    if existing:
        return {"status": "already_collected", "rows": existing["Rows"]}, 200
    source = task["source"]
    day = iso_day(task["day"])
    records = collector.IngestionRecords(table, source=source)
    if source in ("rt_lmp", "rt_totals"):
        kind = "RealtimeEnergyLMP" if source == "rt_lmp" else "RealtimeTotals"
        request = {"source": source, "report_file": f"PUB_{kind}_{day:%Y%m%d}{task['hour']:02d}.csv"}
        run_collect = collector.collect if source == "rt_lmp" else totals.collect
    else:
        request = {"source": source, "delivery_date": day.isoformat()}
        run_collect = hourly.collect

    result, code = run_collect(request, archive, records, adf_run_id=adf_run_id)

    accepted_statuses = (
        {"prepared", "already_prepared"}
        if source == "rt_totals"
        else {"archived", "already_archived"}
    )

    if code != 200 or result.get("status") not in accepted_statuses:
        return result, code if code != 200 else 422

    if source in ("rt_lmp", "rt_totals"):
        record = table.get_entity(result["records_partition"], result["records_row"])
        details = {"record": json_clean(record), "expected_count": int(record["RowCount"])}
    else:
        details = {"report": result["report"], "expected_count": result["row_count"]}
    saved = {"PartitionKey": plan["batch_id"], "RowKey": task["key"], "Source": source,
             "DetailsJson": json.dumps(details, sort_keys=True), "Rows": details["expected_count"],
             "Published": source not in ("rt_lmp", "rt_totals")}
    table.create_entity(saved)
    return {"status": result["status"], "rows": saved["Rows"]}, 200


def publish_report(table, archive, plan, task, credential, adf_run_id):
    saved = get(table, plan["batch_id"], task["key"])
    if not saved:
        raise ValueError("Collect the report before publishing it.")
    if saved["Published"]:
        return {"complete": True, "status": "already_complete"}, 200
    if os.environ.get("IESO_PUBLISH_ENABLED", "false").lower() != "true":
        return {"complete": False, "error": "IESO_PUBLISH_ENABLED must be true."}, 503
    source = task["source"]
    record = json.loads(saved["DetailsJson"])["record"]
    namespace = os.environ["IESO_EVENTHUB_NAMESPACE"]
    topic = os.environ.get("IESO_EVENTHUB_" + source.upper(), publisher.DEFAULT_TOPICS[source])
    publisher.validate_destination(namespace, topic)
    sender = lambda run_id: publisher.KafkaBatchSender(credential, namespace, topic, run_id)
    result, code = publisher.publish(
        {"source": source, "report_id": record["RowKey"], "batch_size": 1000},
        archive, publisher.PublicationRecords(table), sender, namespace, topic, adf_run_id
    )
    if code == 200 and result.get("complete"):
        table.update_entity({"PartitionKey": plan["batch_id"], "RowKey": task["key"], "Published": True},
                            mode=UpdateMode.MERGE)
    return result, code


def seal(table, archive, entity, plan):
    reports = []
    for task in plan["tasks"]:
        saved = get(table, plan["batch_id"], task["key"])
        if not saved or not saved.get("Published"):
            raise ValueError(f"Report has not completed collection/publication: {task['key']}")
        reports.append({**task, **json.loads(saved["DetailsJson"])})
    content = json.dumps({"plan": plan, "reports": reports}, sort_keys=True).encode()
    digest = collector.sha256(content)
    path = f"{PREFIX}/{plan['batch_id']}/inventory.json"
    archive.put(path, content, {"sha256": digest}, content_type="application/json")
    state(table, entity, "ready", InventorySha256=digest, ExpectedReports=len(reports))
    return {"batch_id": plan["batch_id"], "inventory_sha256": digest,
            "reports": len(reports), "status": "ready"}


def complete(table, entity, plan, body):
    if entity["Status"] == "succeeded":
        return {"status": "already_complete"}
    if entity["Status"] != "ready":
        raise ValueError("Only a sealed batch with a successful Spark activity can complete.")
    updated = {"PartitionKey": CONTROL, "RowKey": entity["RowKey"], "Status": "succeeded",
               "UpdatedUtc": collector.utc_now(), "SparkOutput": str(body.get("spark_output", ""))[:20000]}
    operations = [("update", updated, {"mode": UpdateMode.MERGE, "etag": entity.metadata["etag"],
                                     "match_condition": MatchConditions.IfNotModified})]
    if plan["advance_cursor"]:
        cursor = get(table, CONTROL, "cursor")
        old = cursor["EndDate"] if cursor else ""
        if old != entity["ExpectedCursor"]:
            raise ValueError("Another completed batch moved the cursor.")
        progress = {"PartitionKey": CONTROL, "RowKey": "cursor", "EndDate": plan["end_date"],
                    "BatchId": plan["batch_id"], "UpdatedUtc": collector.utc_now()}
        if cursor:
            operations.append(("update", progress, {"mode": UpdateMode.MERGE,
                              "etag": cursor.metadata["etag"],
                              "match_condition": MatchConditions.IfNotModified}))
        else:
            operations.append(("create", progress))
    table.submit_transaction(operations)
    logging.info("ieso_m7_complete batch=%s cursor_advanced=%s", plan["batch_id"], plan["advance_cursor"])
    return {"status": "succeeded", "batch_id": plan["batch_id"],
            "cursor_advanced": plan["advance_cursor"]}


def dispatch(body, container, table, credential, adf_run_id):
    validate(body)
    if not adf_run_id:
        raise ValueError("An ADF run ID is required.")
    action = body["action"]
    if action == "start":
        return start(table, body, adf_run_id), 200
    if action == "status":
        return {"cursor": json_clean(get(table, CONTROL, "cursor") or {}),
                "batches": [json_clean(row) for row in table.query_entities(
                    "PartitionKey eq 'm7_control'") if row["RowKey"].startswith("batch_")]}, 200
    entity, plan = current(table, body, adf_run_id)
    archive = collector.BlobArchive(container)
    if action == "collect":
        return collect_report(table, archive, plan, task_for(plan, body.get("task_key")), adf_run_id)
    if action == "publish":
        return publish_report(table, archive, plan, task_for(plan, body.get("task_key")), credential, adf_run_id)
    if action == "seal":
        return seal(table, archive, entity, plan), 200
    if action == "failed":
        if entity["Status"] == "succeeded":
            return {"status": "already_succeeded"}, 200
        state(table, entity, "failed", FailedStage=str(body.get("stage", ""))[:100])
        return {"status": "failed_recorded"}, 200
    return complete(table, entity, plan, body), 200
