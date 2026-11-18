"""Collect dated IESO hourly files and write validated, replayable CSV snapshots."""

import csv
import io
import logging
import re
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone

from collector import EST, MAX_BYTES, DeferredSource, SourceError, fetch_url, sha256, utc_now

ROOT = "ieso/milestone6"
HOST = "https://reports-public.ieso.ca/public"
SOURCES = {
    "da_lmp": ("DAHourlyEnergyLMP", "csv"),
    "generator": ("GenOutputCapability", "xml"),
}
COLUMNS = {
    "da_lmp": (
        "delivery_date", "delivery_hour", "pricing_location", "lmp",
        "energy_loss_price", "energy_congestion_price",
    ),
    "generator": (
        "delivery_date", "delivery_hour", "generator_name", "fuel_type",
        "output_mw", "capability_mw", "forecast_mw", "available_capacity_mw",
    ),
}
DA_HEADER = ("Delivery Hour", "Pricing Location", "LMP", "Energy Loss Price", "Energy Congestion Price")
DA_PREAMBLE = re.compile(r"CREATED AT (\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) FOR (\d{4}/\d{2}/\d{2})")
XML_NS = {"i": "http://www.theIMO.com/schema"}
DECIMAL = re.compile(r"[+-]?\d{1,15}(?:\.\d{1,9})?")
HASH = re.compile(r"[0-9a-f]{64}")


def request_options(body):
    if not isinstance(body, dict) or set(body) - {"source", "delivery_date", "replay_sha"}:
        raise ValueError("Use source, delivery_date and optional replay_sha.")
    source = body.get("source")
    if not isinstance(source, str) or source not in SOURCES:
        raise ValueError("Hourly source must be da_lmp or generator.")
    value = body.get("delivery_date")
    if not isinstance(value, str):
        raise ValueError("delivery_date must use YYYY-MM-DD.")
    day = date.fromisoformat(value)
    if day.isoformat() != value:
        raise ValueError("delivery_date must use YYYY-MM-DD.")
    digest = body.get("replay_sha", "")
    if not isinstance(digest, str) or (digest and not HASH.fullmatch(digest)):
        raise ValueError("replay_sha must be a lowercase SHA-256 digest.")
    return source, day, digest


def filename(source, day):
    kind, ext = SOURCES[source]
    return f"PUB_{kind}_{day:%Y%m%d}.{ext}"


def original_path(source, day, digest):
    return f"{ROOT}/{source}/original/sha256={digest}/{filename(source, day)}"


def measurement(value, label, optional=False):
    value = (value or "").strip()
    if optional and value.upper() in {"", "N/A"}:
        return None
    if not DECIMAL.fullmatch(value):
        raise ValueError(f"Invalid decimal for {label}.")
    return value


def label(value, field):
    value = (value or "").strip()
    if not value or len(value) > 200 or any(ord(char) < 32 for char in value):
        raise ValueError(f"Invalid {field}.")
    return value


def hour(value):
    if not re.fullmatch(r"(?:0?[1-9]|1[0-9]|2[0-4])", (value or "").strip()):
        raise ValueError("Delivery hour must be 1 through 24.")
    return int(value)


def utc_created(value):
    stamp = datetime.fromisoformat(value)
    return stamp.replace(tzinfo=stamp.tzinfo or EST).astimezone(timezone.utc)


def parse_da(text, day):
    reader = csv.reader(io.StringIO(text, newline=""), strict=True)
    preamble = next(reader, [])
    match = DA_PREAMBLE.fullmatch(preamble[0]) if len(preamble) == 1 else None
    if not match or tuple(next(reader, [])) != DA_HEADER:
        raise ValueError("Day-ahead CSV preamble or header does not match IESO.")
    if datetime.strptime(match[2], "%Y/%m/%d").date() != day:
        raise ValueError("Day-ahead delivery date differs from the requested date.")
    created = utc_created(datetime.strptime(match[1], "%Y/%m/%d %H:%M:%S").isoformat())
    rows = {}
    for row in reader:
        if not row:
            continue
        if len(row) != len(DA_HEADER):
            raise ValueError("Day-ahead CSV row has the wrong number of fields.")
        key = (hour(row[0]), label(row[1], "pricing location"))
        if key in rows:
            raise ValueError("Duplicate day-ahead location-hour.")
        rows[key] = (day.isoformat(), *key, *(measurement(x, "price") for x in row[2:]))
    return created, rows


def xml_hours(generator, path):
    hours = {}
    for row in generator.findall(path, XML_NS):
        index = hour(row.findtext("i:Hour", namespaces=XML_NS))
        if index in hours:
            raise ValueError("Duplicate hour in generator measurement series.")
        hours[index] = measurement(row.findtext("i:EnergyMW", namespaces=XML_NS), path, optional=True)
    return hours


def parse_generator(text, day):
    if re.search(r"<!\s*(DOCTYPE|ENTITY)\b", text, re.IGNORECASE):
        raise ValueError("XML document types and entities are unsupported.")
    root = ET.fromstring(text)
    if root.tag != f"{{{XML_NS['i']}}}IMODocument" or root.get("docID") != "GenOutputCapability":
        raise ValueError("XML is not an IESO Generator Output and Capability report.")
    body = root.find("i:IMODocBody", XML_NS)
    if body is None or body.findtext("i:Date", namespaces=XML_NS) != day.isoformat():
        raise ValueError("Generator delivery date differs from the requested date.")
    created = utc_created(root.findtext("i:IMODocHeader/i:CreatedAt", "", XML_NS))
    rows, seen = {}, set()
    for gen in body.findall("i:Generators/i:Generator", XML_NS):
        name = label(gen.findtext("i:GeneratorName", namespaces=XML_NS), "generator name")
        fuel = label(gen.findtext("i:FuelType", namespaces=XML_NS), "fuel type").upper()
        if name in seen:
            raise ValueError("Duplicate generator in the report.")
        seen.add(name)
        outputs = xml_hours(gen, "i:Outputs/i:Output")
        capabilities = xml_hours(gen, "i:Capabilities/i:Capability")
        capacities = xml_hours(gen, "i:Capacities/i:AvailCapacity")
        variable = fuel in {"WIND", "SOLAR"}
        for index in outputs.keys() | capabilities.keys() | capacities.keys():
            reported = capabilities.get(index)
            rows[(index, name)] = (
                day.isoformat(), index, name, fuel, outputs.get(index),
                None if variable else reported, reported if variable else None, capacities.get(index),
            )
    return created, rows


def prepare(source, content, day):
    if len(content) > MAX_BYTES:
        raise ValueError("Report exceeds the existing source-file size limit.")
    try:
        parse = parse_da if source == "da_lmp" else parse_generator
        created, rows = parse(content.decode("utf-8-sig"), day)
    except (csv.Error, ET.ParseError, UnicodeError, OverflowError) as exc:
        raise ValueError(f"Could not parse {source}: {exc}") from exc
    if not rows:
        raise ValueError("The hourly report contains no observations.")
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(COLUMNS[source])
    writer.writerows(rows[key] for key in sorted(rows))
    return created.isoformat(timespec="seconds"), len(rows), output.getvalue().encode("utf-8")


def collect(body, archive, records, adf_run_id="", downloader=fetch_url):
    source, day, replay_sha = request_options(body)
    run_id = str(uuid.uuid4())
    now = utc_now()
    archive.deadline = records.deadline = time.monotonic() + 120
    report_file = filename(source, day)
    requested = f"sha256:{replay_sha}" if replay_sha else report_file
    result = {"source": source, "delivery_date": day.isoformat(), "run_id": run_id,
              "adf_run_id": adf_run_id, "report_file": report_file}
    attempt = records.start_attempt(run_id, requested, now, adf_run_id)
    result.update(attempt_partition=attempt["PartitionKey"], attempt_row=run_id)
    stage = "source"
    try:
        if replay_sha:
            content = archive.read_path(original_path(source, day, replay_sha), replay_sha)
        else:
            kind, _ = SOURCES[source]
            content, _ = downloader(f"{HOST}/{kind}/{report_file}", archive.deadline)
        raw_hash = sha256(content)
        ext = SOURCES[source][1]
        mime = "application/xml" if ext == "xml" else "text/csv; charset=utf-8"
        stage = "validation"
        try:
            created, count, prepared = prepare(source, content, day)
        except ValueError as exc:
            path = f"ieso/quarantine/milestone6/{source}/sha256={raw_hash}/{report_file}"
            archive.put(path, content, {"sha256": raw_hash, "source": source}, content_type=mime)
            records.update_attempt(attempt, Stage="validation", Outcome="rejected", BlobPath=path,
                                   Sha256=raw_hash, ValidationError=str(exc), CompletedUtc=utc_now())
            return {**result, "status": "rejected", "stage": "validation", "error": str(exc),
                    "sha256": raw_hash, "blob_name": path}, 422
        stage = "archive"
        path = original_path(source, day, raw_hash)
        archive_status = archive.put(path, content, {"sha256": raw_hash, "source": source}, content_type=mime)
        prepared_hash = sha256(prepared)
        prepared_path = f"{ROOT}/{source}/prepared/v1/sha256={prepared_hash}/{source}.csv"
        archive.put(prepared_path, prepared, {"sha256": prepared_hash, "source": source})
        stage = "records"
        records.record_report({
            "PartitionKey": source + "_v1", "RowKey": raw_hash, "Sha256": raw_hash,
            "Bytes": len(content), "BlobPath": path, "PreparedBlobPath": prepared_path,
            "PreparedSha256": prepared_hash, "SourceCreatedUtc": created, "DeliveryDate": day.isoformat(),
            "RowCount": count, "ArchiveState": "archived", "ValidationStatus": "accepted",
            "FirstSeenUtc": now, "LastSeenUtc": now, "FirstRunId": run_id, "LastRunId": run_id,
        })
        records.update_attempt(attempt, Stage="complete", Outcome=archive_status,
                               Sha256=raw_hash, RowCount=count, BlobPath=path, CompletedUtc=utc_now())
        report = {"sha256": raw_hash, "prepared_sha256": prepared_hash, "source_created_utc": created,
                  "row_count": count, "delivery_date": day.isoformat()}
        logging.info("ieso_hourly_collection source=%s delivery_date=%s status=%s rows=%s adf_run_id=%s",
                     source, day, archive_status, count, adf_run_id)
        return {**result, "status": archive_status, "row_count": count, "report": report,
                "blob_name": path, "prepared_blob_name": prepared_path}, 200
    except Exception as exc:
        if isinstance(exc, DeferredSource):
            code, error = 503, str(exc)
        elif isinstance(exc, SourceError):
            code, error = (404 if exc.status == 404 else 502), str(exc)
        elif replay_sha and stage == "source" and isinstance(exc, ValueError):
            code, error = 422, str(exc)
        else:
            logging.exception("ieso_hourly_failure source=%s stage=%s", source, stage)
            code, error = 500, "Check Application Insights for this collection run."
        try:
            records.update_attempt(attempt, Stage=stage, Outcome="failed",
                                   Error=error, CompletedUtc=utc_now())
        except Exception:
            logging.exception("ieso_hourly_audit_failure run_id=%s", run_id)
        return {**result, "status": "failed", "stage": stage, "error": error}, code
