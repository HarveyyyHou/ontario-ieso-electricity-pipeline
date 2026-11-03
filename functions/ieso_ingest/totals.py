"""Match original Real-Time Totals CSV/XML and prepare a CSV with Ontario Demand."""
import csv
import io
import json
import logging
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from xml.etree.ElementTree import ParseError

from defusedxml import ElementTree
from defusedxml.common import DefusedXmlException

import collector as common

SOURCE = "rt_totals"
CONTRACT = "rt_totals_v1"
TRANSFORM = "totals_csv_with_ontario_demand_v1"
BASE_URL = "https://reports-public.ieso.ca/public/RealtimeTotals/"
LATEST_FILE = "PUB_RealtimeTotals.csv"
OUTPUT_FILE = "PUB_RealtimeTotals_with_OntarioDemand.csv"
FILE_RE = re.compile(r"PUB_RealtimeTotals_(\d{8})(\d{2})(?:_v\d+)?\.csv\Z")
CSV_HEADER = ["HOUR", "INTERVAL", "TOTAL ENERGY", "TOTAL LOSS", "TOTAL LOAD",
              "TOTAL DISP LOAD SCHED OFF", "TOTAL 10S", "TOTAL 10N", "TOTAL 30R", "FLAG"]
QUANTITIES = dict(zip(CSV_HEADER[2:-1], ["Total Energy", "Total Loss", "Total Load",
                      "Total Dispatch Load Scheduled OFF", "Total 10S", "Total 10N", "Total 30R"]))
OUTPUT_HEADER = ["DELIVERY DATE", *CSV_HEADER, "ONTARIO DEMAND"]
NS = {"i": "http://www.ieso.ca/schema"}


class PairMismatch(ValueError):
    """Individually readable reports that cannot safely be combined."""


def validate_filename(name):
    if not isinstance(name, str):
        raise ValueError("report_file must be a CSV filename")
    if name == LATEST_FILE:
        return name
    match = FILE_RE.fullmatch(name)
    if not match:
        raise ValueError("Use a Real-Time Totals CSV filename; matching XML is fetched automatically")
    datetime.strptime(match[1], "%Y%m%d")
    if not 1 <= int(match[2]) <= 24:
        raise ValueError("Filename delivery hour must be 01 through 24")
    return name


def request_options(body):
    if not isinstance(body, dict) or body.get("source") != SOURCE:
        raise ValueError("source must be rt_totals")
    if set(body) - {"source", "report_file", "csv_sha256", "xml_sha256"}:
        raise ValueError("Unknown rt_totals request fields")
    replay = "csv_sha256" in body or "xml_sha256" in body
    if replay:
        if "report_file" in body:
            raise ValueError("Use report_file or the two replay hashes, not both")
        hashes = tuple(body.get(key) for key in ("csv_sha256", "xml_sha256"))
        if not all(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) for value in hashes):
            raise ValueError("Replay needs both csv_sha256 and xml_sha256 as 64 lowercase hex characters")
        return None, hashes
    return validate_filename(body.get("report_file", LATEST_FILE)), None


def download(filename, deadline):
    csv_name = filename[:-4] + ".csv" if filename.endswith(".xml") else filename
    validate_filename(csv_name)
    if filename not in (csv_name, csv_name[:-4] + ".xml"):
        raise ValueError("Unsupported source filename")
    return common.fetch_url(BASE_URL + filename, deadline)


def raw_path(kind, digest):
    return f"ieso/rt_totals/original/{kind}/sha256={digest}/PUB_RealtimeTotals.{kind}"


def prepared_path(digest):
    return f"ieso/rt_totals/prepared/sha256={digest}/{OUTPUT_FILE}"


def report_id(csv_digest, xml_digest):
    parents = {"source": SOURCE, "csv_sha256": csv_digest, "xml_sha256": xml_digest,
               "transform": TRANSFORM}
    return common.sha256(json.dumps(parents, sort_keys=True, separators=(",", ":")).encode())


def number(value, label):
    try:
        parsed = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{label}: expected a decimal number") from exc
    if not parsed.is_finite():
        raise ValueError(f"{label}: expected a finite number")
    return parsed


def integer(value, maximum, label):
    if not isinstance(value, str) or not re.fullmatch(r"\d{1,2}", value) or not 1 <= int(value) <= maximum:
        raise ValueError(f"{label}: expected an integer from 1 to {maximum}")
    return int(value)


def text_bytes(content):
    if not content or len(content) > common.MAX_BYTES:
        raise ValueError("Report is empty or exceeds 5 MiB")
    return content.decode("utf-8-sig")


def check_filename_period(filename, date, hour):
    if not filename:
        return
    match = FILE_RE.fullmatch(filename)
    if match and (match[1] != date.replace("-", "") or int(match[2]) != hour):
        raise ValueError("Requested filename does not match report delivery date/hour")


def parse_csv(content, filename=None):
    try:
        lines = list(csv.reader(io.StringIO(text_bytes(content), newline=""), strict=True))
    except (UnicodeError, csv.Error) as exc:
        raise ValueError(f"Invalid source CSV: {exc}") from exc
    header_index = next((i for i, row in enumerate(lines) if row == CSV_HEADER), None)
    if header_index is None:
        raise ValueError("Real-Time Totals CSV header changed or is missing")
    if ["RTEM_TOTALS;"] not in lines[:header_index]:
        raise ValueError("CSV is missing the RTEM_TOTALS identifier")
    timestamps = [common.PREAMBLE_RE.fullmatch(row[0].lstrip("\\").strip())
                  for row in lines[:header_index] if len(row) == 1]
    timestamps = [match for match in timestamps if match]
    if len(timestamps) != 1:
        raise ValueError("CSV needs one CREATED AT / FOR preamble")
    created = datetime.strptime(timestamps[0][1], "%Y/%m/%d %H:%M:%S").isoformat()
    date = datetime.strptime(timestamps[0][2], "%Y/%m/%d").date().isoformat()
    rows, hours = {}, set()
    for line, values in enumerate(lines[header_index + 1:], header_index + 2):
        if not values:
            continue
        if len(values) != len(CSV_HEADER):
            raise ValueError(f"CSV line {line}: expected ten fields")
        cells = dict(zip(CSV_HEADER, values))
        hour = integer(cells["HOUR"], 24, "CSV hour")
        interval = integer(cells["INTERVAL"], 12, "CSV interval")
        key = date, hour, interval
        if key in rows:
            raise ValueError("Duplicate date/hour/interval in CSV")
        numbers = {name: number(cells[name], name) for name in QUANTITIES}
        if not cells["FLAG"].strip():
            raise ValueError("CSV FLAG is empty")
        rows[key] = {"cells": cells, "numbers": numbers, "flag": cells["FLAG"].strip()}
        hours.add(hour)
    if not rows or len(hours) != 1:
        raise ValueError("CSV must contain nonempty data for one delivery hour")
    hour = next(iter(hours))
    check_filename_period(filename, date, hour)
    return rows, {"DeliveryDate": date, "DeliveryHour": hour, "CsvCreatedLocal": created}


def parse_xml(content, filename=None):
    try:
        root = ElementTree.fromstring(text_bytes(content), forbid_dtd=True, forbid_entities=True, forbid_external=True)
    except (UnicodeError, ParseError, DefusedXmlException) as exc:
        raise ValueError(f"Invalid source XML: {type(exc).__name__}") from exc
    if root.tag != "{http://www.ieso.ca/schema}Document" or root.get("docID") != "RealtimeTotals":
        raise ValueError("Unexpected XML document or namespace")
    created = root.findtext("i:DocHeader/i:CreatedAt", namespaces=NS)
    if not created:
        raise ValueError("XML CreatedAt is missing")
    datetime.fromisoformat(created)
    date_text = root.findtext("i:DocBody/i:DeliveryDate", namespaces=NS)
    if not date_text:
        raise ValueError("XML DeliveryDate is missing")
    date = datetime.strptime(date_text, "%Y-%m-%d").date().isoformat()
    hour = integer(root.findtext("i:DocBody/i:DeliveryHour", namespaces=NS), 24, "XML hour")
    check_filename_period(filename, date, hour)
    rows = {}
    for item in root.findall("i:DocBody/i:Energies/i:IntervalEnergy", NS):
        interval = integer(item.findtext("i:Interval", namespaces=NS), 12, "XML interval")
        key = date, hour, interval
        if key in rows:
            raise ValueError("Duplicate date/hour/interval in XML")
        values = {}
        for quantity in item.findall("i:MQ", NS):
            name = quantity.findtext("i:MarketQuantity", namespaces=NS)
            if not name or name in values:
                raise ValueError("Missing or duplicate XML MarketQuantity")
            values[name] = number(quantity.findtext("i:EnergyMW", namespaces=NS), name)
        missing = (set(QUANTITIES.values()) | {"ONTARIO DEMAND"}) - set(values)
        if missing:
            raise ValueError("Missing XML quantities: " + ", ".join(sorted(missing)))
        flag = item.findtext("i:Flag", default="", namespaces=NS).strip()
        if not flag:
            raise ValueError("XML Flag is missing")
        rows[key] = {"values": values, "flag": flag}
    if not rows:
        raise ValueError("XML contains no interval measurements")
    return rows, {"DeliveryDate": date, "DeliveryHour": hour, "XmlCreatedLocal": created}


def decimal_text(value):
    if value == 0:
        return "0"
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def combine(csv_content, xml_content, filename=None):
    csv_rows, csv_meta = parse_csv(csv_content, filename)
    xml_rows, xml_meta = parse_xml(xml_content, filename)
    if set(csv_rows) != set(xml_rows):
        raise PairMismatch("CSV and XML do not contain the same delivery date/hour/interval keys")
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=OUTPUT_HEADER, lineterminator="\n")
    writer.writeheader()
    preview = []
    for key in sorted(csv_rows):
        left, right = csv_rows[key], xml_rows[key]
        for csv_name, xml_name in QUANTITIES.items():
            if left["numbers"][csv_name] != right["values"][xml_name]:
                raise PairMismatch(f"CSV/XML shared measurement differs at {key}: {csv_name}")
        if left["flag"] != right["flag"]:
            raise PairMismatch(f"CSV/XML flags differ at {key}")
        row = {"DELIVERY DATE": key[0], **left["cells"],
               "HOUR": str(key[1]), "INTERVAL": str(key[2]), "FLAG": left["flag"],
               "ONTARIO DEMAND": decimal_text(right["values"]["ONTARIO DEMAND"])}
        for name in QUANTITIES:
            row[name] = decimal_text(left["numbers"][name])
        writer.writerow(row)
        if len(preview) < 2:
            preview.append(row)
    intervals = sorted(key[2] for key in csv_rows)
    day = datetime.strptime(csv_meta["DeliveryDate"], "%Y-%m-%d").replace(tzinfo=common.EST)
    start = day + timedelta(hours=csv_meta["DeliveryHour"] - 1, minutes=5 * (min(intervals) - 1))
    end = day + timedelta(hours=csv_meta["DeliveryHour"] - 1, minutes=5 * max(intervals))
    details = {**csv_meta, **xml_meta, "RowCount": len(csv_rows), "IntervalsPresent": ",".join(map(str, intervals)),
               "EarliestIntervalStartUtc": start.astimezone(timezone.utc).isoformat(),
               "LatestIntervalEndUtc": end.astimezone(timezone.utc).isoformat(),
               "Units": "MW", "TransformVersion": TRANSFORM}
    return output.getvalue().encode("utf-8"), details, preview


def collect(body, archive, records, downloader=download, adf_run_id=""):
    filename, replay = request_options(body)
    run_id, started = str(uuid.uuid4()), common.utc_now()
    clock = time.monotonic()
    deadline = clock + 120
    archive.deadline = records.deadline = deadline
    result = {"source": SOURCE, "run_id": run_id, "adf_run_id": adf_run_id,
              "contract_version": CONTRACT, "transform_version": TRANSFORM,
              "collection_mode": "archive_replay" if replay else "http"}
    attempt = None
    try:
        attempt = records.start_attempt(run_id, filename or "archive:" + "+".join(replay), started, adf_run_id)
        result.update(attempt_partition=attempt["PartitionKey"], attempt_row=run_id)
        contents = {}
        for index, kind in enumerate(("csv", "xml")):
            result["stage"] = "archive_replay_" + kind if replay else "source_" + kind
            common.check_budget(deadline)
            asset_name = filename if kind == "csv" else filename[:-4] + ".xml" if filename else ""
            if replay:
                digest = replay[index]
                path = raw_path(kind, digest)
                content, headers = archive.read_path(path, digest), {}
                url = "adls:" + path
            else:
                url = BASE_URL + asset_name
                result["source_url"] = url
                content, headers = downloader(asset_name, deadline)
                digest = common.sha256(content)
                path = raw_path(kind, digest)
                result[kind + "_retrieved_at_utc"] = common.utc_now()
            result.update({kind + "_sha256": digest, kind + "_blob_name": path,
                           kind + "_bytes": len(content), kind + "_source_url": url})
            records.update_attempt(attempt, Stage=kind + "_downloaded", **{
                kind.title() + "Sha256": digest, kind.title() + "BlobPath": path,
                "SourceHttpStatus": 0 if replay else 200})
            result["stage"] = "archive_" + kind
            metadata = {"source": SOURCE, "artifact_kind": "source_" + kind, "sha256": digest, "source_url": url}
            if not replay:
                metadata["retrieved_at_utc"] = result[kind + "_retrieved_at_utc"]
            lower = {key.lower(): value for key, value in headers.items()}
            for header, field in [("etag", "source_etag"), ("last-modified", "source_last_modified")]:
                if lower.get(header):
                    metadata[field] = lower[header]
            result[kind + "_archive_status"] = archive.put(path, content, metadata,
                                                          content_type="application/xml" if kind == "xml" else "text/csv; charset=utf-8")
            contents[kind] = content
        result["report_id"] = report_id(result["csv_sha256"], result["xml_sha256"])
        records.update_attempt(attempt, Stage="matching", ReportId=result["report_id"])
        result["stage"] = "matching"
        prepared, details, preview, error = None, {}, [], ""
        validation = "accepted"
        try:
            prepared, details, preview = combine(contents["csv"], contents["xml"], filename)
        except PairMismatch as exc:
            validation, error = "deferred", str(exc)
        except ValueError as exc:
            validation, error = "rejected", str(exc)
        output_path, output_digest, output_status = "", "", "not_created"
        if prepared is not None:
            result["stage"] = "prepared_csv"
            output_digest = common.sha256(prepared)
            output_path = prepared_path(output_digest)
            output_status = archive.put(output_path, prepared,
                                        {"source": SOURCE, "artifact_kind": "prepared_csv",
                                         "sha256": output_digest, "transform_version": TRANSFORM})
        result["stage"] = "records"
        seen = common.utc_now()
        entity = {"PartitionKey": CONTRACT, "RowKey": result["report_id"], "ReportId": result["report_id"],
                  "Source": SOURCE, "Sha256": result["csv_sha256"], "Bytes": len(contents["csv"]),
                  "BlobPath": result["csv_blob_name"], "CsvSha256": result["csv_sha256"],
                  "XmlSha256": result["xml_sha256"], "XmlBlobPath": result["xml_blob_name"],
                  "CsvSourceUrl": result["csv_source_url"], "XmlSourceUrl": result["xml_source_url"],
                  "PreparedCsvBlobPath": output_path, "PreparedCsvSha256": output_digest,
                  "PreparedState": "ready" if prepared is not None else "blocked",
                  "FirstSeenUtc": seen, "LastSeenUtc": seen, "FirstRunId": run_id, "LastRunId": run_id,
                  "ContractVersion": CONTRACT, "ValidationStatus": validation, "ValidationError": error[:2000],
                  "ArchiveState": "archived", "EventPublishStatus": "not_enabled" if validation == "accepted" else "not_applicable",
                  **details}
        record_status = records.record_report(entity)
        status = ("prepared" if output_status == "archived" else "already_prepared") if validation == "accepted" else validation
        result.update(status=status, stage="complete", validation_status=validation, validation_error=error,
                      records_partition=CONTRACT, records_row=result["report_id"], records_status=record_status,
                      prepared_csv_blob_name=output_path, prepared_csv_sha256=output_digest,
                      prepared_csv_bytes=len(prepared) if prepared is not None else 0, prepared_csv_status=output_status,
                      row_count=details.get("RowCount", 0), report_metadata=details, preview=preview,
                      duration_ms=round((time.monotonic() - clock) * 1000))
        if replay:
            result["replayed_at_utc"] = common.utc_now()
        records.update_attempt(attempt, Stage="complete", Outcome=status, ValidationStatus=validation,
                               CompletedUtc=common.utc_now(), PreparedCsvBlobPath=output_path,
                               PreparedCsvSha256=output_digest, RowCount=result["row_count"], ValidationError=error[:2000])
        logging.info("ieso_collection %s", json.dumps(result))
        return result, 200
    except Exception as exc:
        stage = result.get("stage", "records_start")
        deferred = isinstance(exc, common.DeferredSource)
        if deferred:
            logging.warning("ieso_collection_deferred source=rt_totals run_id=%s stage=%s wait=%s", run_id, stage, exc.wait)
        else:
            logging.exception("ieso_collection_failed source=rt_totals run_id=%s stage=%s", run_id, stage)
        if attempt is not None:
            try:
                records.update_attempt(attempt, Stage=stage, Outcome="deferred" if deferred else "failed",
                                       CompletedUtc=common.utc_now(), ErrorType=type(exc).__name__,
                                       SourceHttpStatus=(exc.status or 0) if isinstance(exc, common.SourceError) else attempt.get("SourceHttpStatus", 0))
            except Exception:
                logging.exception("ieso_attempt_update_failed source=rt_totals run_id=%s", run_id)
        result.update(status="failed", error_type=type(exc).__name__,
                      error="Collection failed; inspect the run_id in Application Insights and the ingestion table")
        if isinstance(exc, common.SourceError):
            result["error"] = str(exc)
            if exc.status is not None:
                result["source_http_status"] = exc.status
        if deferred:
            result.update(status="deferred", validation_status="not_checked", retry_after_seconds=exc.wait)
            return result, 200
        return result, 502 if isinstance(exc, common.SourceError) else 500
