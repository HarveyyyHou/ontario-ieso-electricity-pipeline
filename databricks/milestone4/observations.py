"""Validate v1 events and reconstruct them from accepted archive records."""

import csv
import hashlib
import io
import json
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from functions.ieso_ingest import events

TOPICS = {"rt_lmp": "ieso-rt-lmp", "rt_totals": "ieso-rt-totals"}
FIELDS = {"rt_lmp": events.LMP_FIELDS, "rt_totals": events.TOTAL_FIELDS}
HASH = re.compile(r"[0-9a-f]{64}")
DECIMAL = re.compile(r"-?(0|[1-9][0-9]{0,25})(\.[0-9]{1,12})?")
RAW = "abfss://raw@iesodev001.dfs.core.windows.net/"
REPORT_FIELDS = {
    "report_id", "record_partition", "csv_sha256", "csv_blob_path",
    "first_seen_utc", "source_time_convention", "source_version_utc",
    "source_created_csv_local",
}
TOTAL_REPORT_FIELDS = {
    "xml_sha256", "prepared_csv_sha256", "prepared_csv_blob_path",
    "source_created_xml_local",
}


def require_hash(value):
    if not isinstance(value, str) or not HASH.fullmatch(value):
        raise ValueError("Expected a lowercase SHA-256 identifier")
    return value


def utc_datetime(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.utcoffset() != timedelta(0):
        raise ValueError("Expected an explicit UTC timestamp")
    return result


def raw_csv_path(source, digest):
    if source == "rt_lmp":
        return f"ieso/rt_lmp/sha256={digest}/PUB_RealtimeEnergyLMP.csv"
    return f"ieso/rt_totals/original/csv/sha256={digest}/PUB_RealtimeTotals.csv"


def prepared_path(digest):
    return f"ieso/rt_totals/prepared/sha256={digest}/PUB_RealtimeTotals_with_OntarioDemand.csv"


def totals_id(csv_hash, xml_hash):
    return events.digest({
        "source": "rt_totals", "csv_sha256": csv_hash, "xml_sha256": xml_hash,
        "transform": "totals_csv_with_ontario_demand_v1",
    })


def unique_object(pairs):
    obj = dict(pairs)
    if len(obj) != len(pairs):
        raise ValueError("Duplicate JSON field")
    return obj


def decode(source, payload, key):
    """Return typed Silver columns or a rejection reason for one delivery."""
    event = None
    try:
        event = json.loads(bytes(payload).decode("utf-8"), object_pairs_hook=unique_object)
        if not isinstance(event, dict):
            raise ValueError("Event must be a JSON object")
        if set(event) != set(events.CORE_FIELDS) | {"event_id", "report_event_id", "report", "publication"}:
            raise ValueError("Unexpected envelope fields")
        if event["source"] != source:
            raise ValueError("Event source does not match the feed")
        events.validate_event(event)
        if key is None or bytes(key).decode("utf-8") != event["pricing_location"]:
            raise ValueError("Message key differs from the pricing location")

        report = event["report"]
        expected_fields = REPORT_FIELDS | (TOTAL_REPORT_FIELDS if source == "rt_totals" else set())
        if set(report) != expected_fields or report["record_partition"] != source + "_v1":
            raise ValueError("Invalid report metadata fields")
        if report["csv_blob_path"] != raw_csv_path(source, report["csv_sha256"]):
            raise ValueError("CSV archive path differs from its content hash")
        version = events.source_utc(report["source_created_csv_local"])
        report_id = report["csv_sha256"]
        archive_path = report["csv_blob_path"]
        if source == "rt_totals":
            report_id = totals_id(report["csv_sha256"], report["xml_sha256"])
            version = max(version, events.source_utc(report["source_created_xml_local"]))
            archive_path = prepared_path(report["prepared_csv_sha256"])
            if report["prepared_csv_blob_path"] != archive_path:
                raise ValueError("Prepared CSV archive path differs from its content hash")
        if report["report_id"] != report_id or utc_datetime(version) != utc_datetime(report["source_version_utc"]):
            raise ValueError("Report identity or source version does not match report metadata")

        pub = event["publication"]
        if set(pub) != {"run_id", "adf_run_id", "attempted_at_utc"} or not isinstance(pub["adf_run_id"], str):
            raise ValueError("Invalid publication metadata fields")
        uuid.UUID(pub["run_id"])
        utc_datetime(pub["attempted_at_utc"])

        measurements = {}
        for name, value in event["measurements"].items():
            if not DECIMAL.fullmatch(value):
                raise ValueError(f"{name} cannot be represented exactly as DECIMAL(38,12)")
            measurements[name] = Decimal(value)
        return {
            "report_id": report_id, "report_event_id": event["report_event_id"],
            "event_id": event["event_id"], "delivery_date": date.fromisoformat(event["delivery_date"]),
            "delivery_hour": event["delivery_hour"], "interval": event["interval"],
            "pricing_location": event["pricing_location"], "units": event["units"],
            "flag": event["flag"], "interval_start_utc": utc_datetime(event["interval_start_utc"]),
            "interval_end_utc": utc_datetime(event["interval_end_utc"]),
            "source_version_utc": utc_datetime(version),
            "first_seen_utc": utc_datetime(report["first_seen_utc"]),
            "archive_path": archive_path, "publication_run_id": pub["run_id"],
            "publication_adf_run_id": pub["adf_run_id"], "rejection_reason": None,
            **measurements,
        }
    except (ValueError, KeyError, TypeError, AttributeError, OverflowError) as exc:
        report = event.get("report") if isinstance(event, dict) else None
        report_id = report.get("report_id") if isinstance(report, dict) else None
        return {
            "report_id": report_id if isinstance(report_id, str) and HASH.fullmatch(report_id) else None,
            "rejection_reason": (str(exc) or type(exc).__name__)[:500],
        }


def replay_input(record):
    """Return source, ID and exact archive file for one accepted ingestion record."""
    source = {"rt_lmp_v1": "rt_lmp", "rt_totals_v1": "rt_totals"}.get(record.get("PartitionKey"))
    if not source or record.get("ValidationStatus") != "accepted" or record.get("ArchiveState") != "archived":
        raise ValueError("Replay requires an accepted archived report row from iesoingestion")
    report_id = require_hash(record["RowKey"])
    csv_hash = require_hash(record["Sha256"])
    if record["BlobPath"] != raw_csv_path(source, csv_hash):
        raise ValueError("Unexpected original CSV path")
    if source == "rt_lmp":
        if report_id != csv_hash:
            raise ValueError("LMP report ID differs from its CSV hash")
        return source, report_id, record["BlobPath"], csv_hash
    if record.get("PreparedState") != "ready" or record["CsvSha256"] != csv_hash:
        raise ValueError("Totals requires a ready prepared CSV and consistent original hash")
    if report_id != totals_id(csv_hash, require_hash(record["XmlSha256"])):
        raise ValueError("Totals report ID differs from its source hashes")
    digest = require_hash(record["PreparedCsvSha256"])
    if record["PreparedCsvBlobPath"] != prepared_path(digest):
        raise ValueError("Unexpected prepared CSV path")
    return source, report_id, record["PreparedCsvBlobPath"], digest


def replay_events(record, content):
    """Rebuild v1 events from verified bytes without contacting IESO or Event Hubs."""
    source, report_id, path, digest = replay_input(record)
    if len(content) > 5 * 1024 * 1024 or hashlib.sha256(content).hexdigest() != digest:
        raise ValueError("Archive size or SHA-256 differs from the accepted file")
    expected_count = int(record["RowCount"])
    if not 0 < expected_count <= 50_000:
        raise ValueError("Replay is limited to one accepted report of up to 50,000 rows per feed")
    day, hour = record["DeliveryDate"], int(record["DeliveryHour"])
    date.fromisoformat(day)
    created = record["SourceCreatedLocal"] if source == "rt_lmp" else record["CsvCreatedLocal"]
    report = {
        "report_id": report_id, "record_partition": record["PartitionKey"],
        "csv_sha256": record["Sha256"], "csv_blob_path": record["BlobPath"],
        "first_seen_utc": record["FirstSeenUtc"], "source_time_convention": "EST_UTC_MINUS_05",
        "source_created_csv_local": created, "source_version_utc": events.source_utc(created),
    }
    contents = io.StringIO(content.decode("utf-8-sig"), newline="")
    if source == "rt_lmp":
        expected_header = ["Delivery Hour", "Interval", "Pricing Location", *FIELDS[source].values()]
        hour_column, interval_column = "Delivery Hour", "Interval"
        next(contents)
    else:
        expected_header = ["DELIVERY DATE", "HOUR", "INTERVAL",
                           *list(FIELDS[source].values())[:-1], "FLAG", "ONTARIO DEMAND"]
        hour_column, interval_column = "HOUR", "INTERVAL"
        report.update(
            xml_sha256=record["XmlSha256"], prepared_csv_sha256=digest, prepared_csv_blob_path=path,
            source_created_xml_local=record["XmlCreatedLocal"],
            source_version_utc=max(report["source_version_utc"], events.source_utc(record["XmlCreatedLocal"])),
        )
    reader = csv.DictReader(contents, strict=True)
    if reader.fieldnames != expected_header:
        raise ValueError("Archived CSV header differs from the accepted contract")
    provenance = {
        "run_id": str(uuid.uuid4()), "adf_run_id": "",
        "attempted_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    result, seen = [], set()
    for row in reader:
        location = row["Pricing Location"] if source == "rt_lmp" else "ONTARIO"
        interval = int(row[interval_column])
        if int(row[hour_column]) != hour or row.get("DELIVERY DATE", day) != day:
            raise ValueError("Archive row differs from the accepted report period")
        if (interval, location) in seen:
            raise ValueError("Archive contains a duplicate observation key")
        seen.add((interval, location))
        event = events.build(source, day, hour, interval, location,
                             {name: row[column] for name, column in FIELDS[source].items()},
                             row.get("FLAG"), report, provenance)
        payload = events.canonical(event).encode("utf-8")
        key = location.encode("utf-8")
        check = decode(source, payload, key)
        if check["rejection_reason"]:
            raise ValueError(f"Reconstructed event failed validation: {check['rejection_reason']}")
        result.append((event["report_event_id"], payload, key))
    if len(result) != expected_count:
        raise ValueError("Archived row count differs from the accepted report record")
    return result
