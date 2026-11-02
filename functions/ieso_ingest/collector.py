"""IESO LMP contract, content-addressed archive, and durable ingestion state."""
import csv
import hashlib
import io
import json
import logging
import math
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime

import requests
from azure.core import MatchConditions
from azure.core.exceptions import ResourceExistsError, HttpResponseError
from azure.data.tables import UpdateMode
from azure.storage.blob import ContentSettings

BASE_URL = "https://reports-public.ieso.ca/public/RealtimeEnergyLMP/"
LATEST_FILE = "PUB_RealtimeEnergyLMP.csv"
FILE_RE = re.compile(r"PUB_RealtimeEnergyLMP_(\d{8})(\d{2})(?:_v\d+)?\.csv\Z")
PREAMBLE_RE = re.compile(r"CREATED AT (\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) FOR (\d{4}/\d{2}/\d{2})\Z")
HEADER = ["Delivery Hour", "Interval", "Pricing Location", "LMP", "Energy Loss Price", "Energy Congestion Price"]
CONTRACT = "rt_lmp_v1"
EST = timezone(timedelta(hours=-5))
MAX_BYTES = 5 * 1024 * 1024
TRANSIENT = {429, 500, 502, 503, 504}


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def sha256(content):
    return hashlib.sha256(content).hexdigest()


def archive_path(digest):
    return f"ieso/rt_lmp/sha256={digest}/{LATEST_FILE}"


class SourceError(Exception):
    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status


class DeferredSource(SourceError):
    def __init__(self, status, wait):
        super().__init__(f"IESO requested a retry after {wait:g} seconds; wait before a manual rerun", status)
        self.wait = wait


def check_budget(deadline):
    if time.monotonic() >= deadline:
        raise TimeoutError("Collection work budget exhausted; retry this single report")


def validate_filename(name):
    if not isinstance(name, str):
        raise ValueError("report_file must be a string")
    if name == LATEST_FILE:
        return name
    match = FILE_RE.fullmatch(name)
    if not match:
        raise ValueError("report_file must be an IESO LMP filename, not a URL or path")
    datetime.strptime(match[1], "%Y%m%d")
    if not 1 <= int(match[2]) <= 24:
        raise ValueError("Filename delivery hour must be 01 through 24")
    return name


def request_options(body):
    if not isinstance(body, dict) or body.get("source") != "rt_lmp":
        raise ValueError("Send a JSON object with source set to rt_lmp")
    extra = set(body) - {"source", "report_file", "archive_sha256"}
    if extra:
        raise ValueError("Unknown request fields: " + ", ".join(sorted(extra)))
    if "archive_sha256" in body:
        if "report_file" in body:
            raise ValueError("Use report_file or archive_sha256, not both")
        digest = body["archive_sha256"]
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("archive_sha256 must be 64 lowercase hexadecimal characters")
        return None, digest
    return validate_filename(body.get("report_file", LATEST_FILE)), None


def validate_report(content, report_file=LATEST_FILE):
    """Return metadata after checking every row; preserve numeric source strings."""
    if not content or len(content) > MAX_BYTES:
        raise ValueError("Report is empty or exceeds the 5 MiB contract limit")
    try:
        rows = csv.reader(io.StringIO(content.decode("utf-8-sig"), newline=""), strict=True)
        preamble = next(rows, [])
        match = PREAMBLE_RE.fullmatch(preamble[0]) if len(preamble) == 1 else None
        if not match:
            raise ValueError("Unexpected CREATED AT / FOR preamble")
        created = datetime.strptime(match[1], "%Y/%m/%d %H:%M:%S")
        delivery_date = datetime.strptime(match[2], "%Y/%m/%d")
        if next(rows, []) != HEADER:
            raise ValueError("CSV header does not match rt_lmp_v1")
        seen, hours, intervals, locations = set(), set(), set(), set()
        for line, row in enumerate(rows, 3):
            if len(row) != len(HEADER):
                raise ValueError(f"Line {line}: expected six fields")
            if not re.fullmatch(r"\d{1,2}", row[0]) or not 1 <= int(row[0]) <= 24:
                raise ValueError(f"Line {line}: delivery hour must be 1 through 24")
            if not re.fullmatch(r"\d{1,2}", row[1]) or not 1 <= int(row[1]) <= 12:
                raise ValueError(f"Line {line}: interval must be 1 through 12")
            location = row[2]
            if not location or location != location.strip():
                raise ValueError(f"Line {line}: missing or padded pricing location")
            for column, value in zip(HEADER[3:], row[3:]):
                try:
                    number = Decimal(value)
                except InvalidOperation as exc:
                    raise ValueError(f"Line {line}: {column} is not numeric") from exc
                if not number.is_finite():
                    raise ValueError(f"Line {line}: {column} must be finite")
            hour, interval = int(row[0]), int(row[1])
            key = (hour, interval, location)
            if key in seen:
                raise ValueError(f"Line {line}: duplicate observation key")
            seen.add(key)
            hours.add(hour)
            intervals.add(interval)
            locations.add(location)
        if not seen or len(hours) != 1:
            raise ValueError("Expected nonempty data for one delivery hour")
        hour = next(iter(hours))
        named = FILE_RE.fullmatch(report_file) if report_file else None
        if named and (named[1] != delivery_date.strftime("%Y%m%d") or int(named[2]) != hour):
            raise ValueError("Report period does not match the requested dated filename")
        start = delivery_date.replace(tzinfo=EST) + timedelta(hours=hour - 1, minutes=5 * (min(intervals) - 1))
        end = delivery_date.replace(tzinfo=EST) + timedelta(hours=hour - 1, minutes=5 * max(intervals))
        return {
            "DeliveryDate": delivery_date.date().isoformat(), "DeliveryHour": hour,
            "SourceCreatedLocal": created.isoformat(), "SourceTimeConvention": "EST_UTC_MINUS_05",
            "RowCount": len(seen), "LocationCount": len(locations),
            "IntervalsPresent": ",".join(map(str, sorted(intervals))),
            "EarliestIntervalStartUtc": start.astimezone(timezone.utc).isoformat(),
            "LatestIntervalEndUtc": end.astimezone(timezone.utc).isoformat(),
        }
    except (UnicodeError, csv.Error) as exc:
        raise ValueError(f"Invalid UTF-8 CSV: {exc}") from exc


def retry_delay(value):
    """Parse Retry-After without shortening the server's requested wait."""
    try:
        delay = float(value)
    except (TypeError, ValueError):
        try:
            delay = (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()
        except (TypeError, ValueError, OverflowError):
            delay = 1
    return max(0, delay) if math.isfinite(delay) else 1


def fetch_report(report_file, deadline, session_factory=requests.Session, sleep=time.sleep):
    url = BASE_URL + validate_filename(report_file)
    with session_factory() as session:
        for attempt in range(2):
            check_budget(deadline)
            delay = 1
            try:
                with session.get(url, timeout=(5, 20), stream=True, allow_redirects=False) as response:
                    status = response.status_code
                    if status in TRANSIENT:
                        delay = retry_delay(response.headers.get("Retry-After"))
                        if delay > 5 or (status == 429 and attempt == 1):
                            raise DeferredSource(status, delay)
                        if attempt == 1:
                            raise SourceError(f"IESO returned HTTP {status}", status)
                    elif status != 200:
                        raise SourceError(f"IESO returned HTTP {status}", status)
                    else:
                        chunks, size = [], 0
                        for chunk in response.iter_content(64 * 1024):
                            check_budget(deadline)
                            size += len(chunk)
                            if size > MAX_BYTES:
                                raise SourceError("IESO response exceeds 5 MiB; investigate before raising the limit")
                            chunks.append(chunk)
                        return b"".join(chunks), dict(response.headers)
            except requests.RequestException as exc:
                if attempt == 1:
                    raise SourceError(f"IESO network request failed: {type(exc).__name__}") from exc
            check_budget(deadline)
            sleep(min(delay, max(0, deadline - time.monotonic())))
    raise SourceError("IESO download did not complete")


class Manifest:
    def __init__(self, table):
        self.table = table
        self.deadline = float("inf")

    def start_attempt(self, run_id, requested, now, adf_run_id):
        entity = {"PartitionKey": "attempt_rt_lmp_" + now[:10].replace("-", ""),
                  "RowKey": run_id, "RunId": run_id, "RequestedReport": requested,
                  "StartedUtc": now, "Outcome": "started", "Stage": "started",
                  "AdfRunId": adf_run_id, "ContractVersion": CONTRACT}
        check_budget(self.deadline)
        self.table.create_entity(entity)
        return entity

    def update_attempt(self, entity, **fields):
        check_budget(self.deadline)
        entity.update(fields)
        self.table.update_entity(entity, mode=UpdateMode.MERGE)

    def record_report(self, entity):
        """Insert immutable report details, then advance last-seen with ETag checks."""
        try:
            check_budget(self.deadline)
            self.table.create_entity(entity)
            return "created"
        except ResourceExistsError:
            pass
        for _ in range(4):
            check_budget(self.deadline)
            current = self.table.get_entity(entity["PartitionKey"], entity["RowKey"])
            if current["Sha256"] != entity["Sha256"] or current["Bytes"] != entity["Bytes"]:
                raise RuntimeError("Manifest hash or byte-count conflict")
            if current["LastSeenUtc"] >= entity["LastSeenUtc"]:
                return "existing"
            patch = {"PartitionKey": entity["PartitionKey"], "RowKey": entity["RowKey"],
                     "LastSeenUtc": entity["LastSeenUtc"], "LastRunId": entity["LastRunId"]}
            try:
                check_budget(self.deadline)
                self.table.update_entity(patch, mode=UpdateMode.MERGE,
                                         etag=current.metadata["etag"],
                                         match_condition=MatchConditions.IfNotModified)
                return "updated"
            except HttpResponseError as exc:
                if exc.status_code != 412:
                    raise
        raise RuntimeError("Manifest remained busy after four conditional update attempts")


class BlobArchive:
    def __init__(self, container):
        self.container = container
        self.deadline = float("inf")

    def put(self, path, content, metadata):
        check_budget(self.deadline)
        blob = self.container.get_blob_client(path)
        try:
            blob.upload_blob(content, overwrite=False, metadata=metadata,
                             content_settings=ContentSettings(content_type="text/csv; charset=utf-8"))
            return "archived"
        except ResourceExistsError:
            check_budget(self.deadline)
            existing = blob.get_blob_properties()
            if existing.size != len(content) or existing.metadata.get("sha256") != sha256(content):
                raise RuntimeError("Existing archive metadata or size does not match its content hash")
            return "already_archived"

    def read(self, digest):
        check_budget(self.deadline)
        blob = self.container.get_blob_client(archive_path(digest))
        if blob.get_blob_properties().size > MAX_BYTES:
            raise ValueError("Archived file exceeds the 5 MiB replay limit")
        check_budget(self.deadline)
        content = blob.download_blob(max_concurrency=1).readall()
        if len(content) > MAX_BYTES or sha256(content) != digest:
            raise ValueError("Archived bytes do not match the requested SHA-256")
        return content


def collect(body, archive, manifest, downloader=fetch_report, adf_run_id=""):
    """One source report per call. Blob and Table writes are deliberately recoverable."""
    report_file, replay_digest = request_options(body)
    run_id, now = str(uuid.uuid4()), utc_now()
    started_monotonic = time.monotonic()
    deadline = started_monotonic + 120
    archive.deadline = manifest.deadline = deadline
    result = {"source": "rt_lmp", "run_id": run_id, "adf_run_id": adf_run_id,
              "contract_version": CONTRACT,
              "collection_mode": "http" if report_file else "archive_replay"}
    attempt = None
    try:
        attempt = manifest.start_attempt(run_id, report_file or "archive:" + replay_digest, now, adf_run_id)
        result.update(attempt_partition=attempt["PartitionKey"], attempt_row=run_id)
        result["stage"] = "source" if report_file else "archive_replay"
        if report_file:
            content, headers = downloader(report_file, deadline)
            source_url = BASE_URL + report_file
        else:
            content, headers = archive.read(replay_digest), {}
            source_url = "adls:" + archive_path(replay_digest)
        digest = sha256(content)
        observed_at = utc_now()
        result["retrieved_at_utc" if report_file else "replayed_at_utc"] = observed_at
        result.update(sha256=digest, bytes=len(content), source_url=source_url,
                      report_file=report_file or LATEST_FILE)
        manifest.update_attempt(attempt, Stage="downloaded", Sha256=digest, Bytes=len(content),
                                SourceUrl=source_url, SourceHttpStatus=200 if report_file else 0,
                                ObservedUtc=observed_at)
        result["stage"] = "validation"
        error = ""
        try:
            details = validate_report(content, report_file)
            accepted = True
        except ValueError as exc:
            details, accepted, error = {}, False, str(exc)
        path = archive_path(digest) if accepted else f"ieso/quarantine/rt_lmp/sha256={digest}/{LATEST_FILE}"
        manifest.update_attempt(attempt, Stage="validated", BlobPath=path,
                                ValidationStatus="accepted" if accepted else "rejected",
                                ValidationError=error)
        result["stage"] = "archive"
        check_budget(deadline)
        headers = {k.lower(): v for k, v in headers.items()}
        metadata = {"source": "rt_lmp", "source_url": source_url, "sha256": digest,
                    "observed_at_utc": observed_at, "collection_mode": result["collection_mode"],
                    "validation_status": "accepted" if accepted else "rejected"}
        if report_file:
            metadata["retrieved_at_utc"] = observed_at
        for source, target in [("etag", "source_etag"), ("last-modified", "source_last_modified")]:
            if headers.get(source):
                metadata[target] = headers[source]
        archive_status = archive.put(path, content, metadata)
        result["stage"] = "manifest"
        check_budget(deadline)
        entity = {"PartitionKey": CONTRACT, "RowKey": digest, "Sha256": digest,
                  "Bytes": len(content), "BlobPath": path, "SourceUrl": source_url,
                  "FirstSeenUtc": observed_at, "LastSeenUtc": observed_at, "FirstRunId": run_id,
                  "LastRunId": run_id, "ArchiveState": "archived", "ContractVersion": CONTRACT,
                  "ValidationStatus": "accepted" if accepted else "rejected",
                  "ValidationError": error, "EventPublishStatus": "not_enabled" if accepted else "not_applicable",
                  "SourceEtag": headers.get("etag", ""), "SourceLastModified": headers.get("last-modified", ""),
                  **details}
        manifest_status = manifest.record_report(entity)
        result.update(status=archive_status if accepted else "rejected", stage="complete",
                      validation_status=entity["ValidationStatus"], validation_error=error,
                      archive_status=archive_status, manifest_status=manifest_status,
                      manifest_partition=CONTRACT, manifest_row=digest, blob_name=path,
                      row_count=details.get("RowCount", 0), report_metadata=details)
        result["duration_ms"] = round((time.monotonic() - started_monotonic) * 1000)
        manifest.update_attempt(attempt, Stage="complete", Outcome=result["status"],
                                CompletedUtc=utc_now(), RowCount=result["row_count"])
        logging.info("ieso_collection %s", json.dumps(result))
        return result, 200
    except Exception as exc:
        # A failed attempt to persist the audit outcome never conceals the original failure.
        stage = result.get("stage", "manifest_start")
        if isinstance(exc, DeferredSource):
            logging.warning("ieso_collection_deferred run_id=%s stage=%s wait=%s", run_id, stage, exc.wait)
        else:
            logging.exception("ieso_collection_failed run_id=%s stage=%s sha256=%s", run_id, stage, result.get("sha256", ""))
        if attempt is not None:
            try:
                manifest.update_attempt(attempt, Outcome="deferred" if isinstance(exc, DeferredSource) else "failed", Stage=stage,
                                        CompletedUtc=utc_now(), ErrorType=type(exc).__name__,
                                        SourceHttpStatus=(exc.status or 0) if isinstance(exc, SourceError) else attempt.get("SourceHttpStatus", 0))
            except Exception:
                logging.exception("ieso_attempt_update_failed run_id=%s", run_id)
        result.update(status="failed", error_type=type(exc).__name__,
                      error="Collection failed; inspect this run_id in Application Insights and the attempt record")
        if isinstance(exc, SourceError):
            result["error"] = str(exc)
            if exc.status is not None:
                result["source_http_status"] = exc.status
        if isinstance(exc, DeferredSource):
            result.update(status="deferred", validation_status="not_checked",
                          retry_after_seconds=exc.wait)
            return result, 200
        return result, 502 if isinstance(exc, SourceError) else 500
