"""Publish accepted archived CSV observations with acknowledged, resumable batches."""
import csv
import io
import json
import logging
import re
import time
import uuid
from datetime import datetime, timedelta, timezone

from azure.core import MatchConditions
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.data.tables import UpdateMode

import collector as common
import events
import totals

DEFAULT_TOPICS = {"rt_lmp": "ieso-rt-lmp", "rt_totals": "ieso-rt-totals"}
HASH = re.compile(r"[0-9a-f]{64}\Z")


class Busy(Exception):
    pass


class DeliveryError(RuntimeError):
    def __init__(self, message, acknowledged=0):
        super().__init__(message)
        self.acknowledged = acknowledged


def request_options(body):
    if not isinstance(body, dict) or body.get("source") not in DEFAULT_TOPICS:
        raise ValueError("source must be rt_lmp or rt_totals")
    if set(body) - {"source", "report_id", "batch_size"}:
        raise ValueError("Unknown publishing request fields")
    if not isinstance(body.get("report_id"), str) or not HASH.fullmatch(body["report_id"]):
        raise ValueError("report_id must be a 64-character lowercase SHA-256/report-pair ID")
    size = body.get("batch_size", 500)
    if type(size) is not int or not 1 <= size <= 1000:
        raise ValueError("batch_size must be an integer from 1 through 1000")
    return body["source"], body["report_id"], size


def validate_destination(namespace, topic):
    if not isinstance(namespace, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]{4,48}[a-z0-9]\.servicebus\.windows\.net", namespace):
        raise ValueError("IESO_EVENTHUB_NAMESPACE must be the namespace FQDN without a protocol or port")
    if not isinstance(topic, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,255}", topic):
        raise ValueError("Invalid configured event hub name")


def report_events(source, report_id, record, content, run_id, adf_run_id, now):
    """Revalidate the archived CSV and deterministically order observations for the cursor."""
    if record.get("ValidationStatus") != "accepted" or record.get("ArchiveState") != "archived":
        raise ValueError("Only an accepted archived report can be published")
    if source == "rt_lmp" and record["Sha256"] != report_id:
        raise ValueError("LMP report ID differs from the archived CSV hash")
    if source == "rt_totals" and (record["Sha256"] != record["CsvSha256"] or
                                  totals.report_id(record["CsvSha256"], record["XmlSha256"]) != report_id):
        raise ValueError("Totals report-pair ID differs from the recorded source hashes")
    report = {"report_id": report_id, "record_partition": source + "_v1",
              "csv_sha256": record["Sha256"], "csv_blob_path": record["BlobPath"],
              "first_seen_utc": record["FirstSeenUtc"], "source_time_convention": "EST_UTC_MINUS_05"}
    if source == "rt_lmp":
        details = common.validate_report(content)
        rows = list(csv.DictReader(io.StringIO(content.decode("utf-8-sig").split("\n", 1)[1])))
        report["source_version_utc"] = events.source_utc(details["SourceCreatedLocal"])
        report["source_created_csv_local"] = details["SourceCreatedLocal"]
        keys = lambda row: (int(row["Delivery Hour"]), int(row["Interval"]), row["Pricing Location"])
    else:
        if record.get("PreparedState") != "ready":
            raise ValueError("Totals prepared CSV is not ready")
        reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig"), newline=""), strict=True)
        if reader.fieldnames != totals.OUTPUT_HEADER:
            raise ValueError("Unexpected prepared Totals CSV header")
        rows = list(reader)
        if not rows or any(set(row) != set(totals.OUTPUT_HEADER) or None in row.values() for row in rows):
            raise ValueError("Malformed or empty prepared Totals CSV")
        details = {"DeliveryDate": record["DeliveryDate"], "DeliveryHour": record["DeliveryHour"], "RowCount": len(rows)}
        for row in rows:
            if row["DELIVERY DATE"] != details["DeliveryDate"] or int(row["HOUR"]) != details["DeliveryHour"]:
                raise ValueError("Prepared CSV period differs from the ingestion record")
        report.update(xml_sha256=record["XmlSha256"], prepared_csv_sha256=record["PreparedCsvSha256"],
                      prepared_csv_blob_path=record["PreparedCsvBlobPath"],
                      source_created_csv_local=record["CsvCreatedLocal"], source_created_xml_local=record["XmlCreatedLocal"])
        report["source_version_utc"] = max(events.source_utc(record["CsvCreatedLocal"]), events.source_utc(record["XmlCreatedLocal"]))
        keys = lambda row: (int(row["HOUR"]), int(row["INTERVAL"]), "ONTARIO")
    if any(record.get(field) != details[field] for field in ("DeliveryDate", "DeliveryHour", "RowCount")):
        raise ValueError("Archived CSV metadata differs from the accepted ingestion record")
    ordered = sorted(rows, key=keys)
    if len({keys(row) for row in ordered}) != len(ordered):
        raise ValueError("Duplicate observation key in archived CSV")
    publication = {"run_id": run_id, "adf_run_id": adf_run_id, "attempted_at_utc": now}
    output = []
    for row in ordered:
        hour, interval, location = keys(row)
        fields = events.LMP_FIELDS if source == "rt_lmp" else events.TOTAL_FIELDS
        event = events.build(source, details["DeliveryDate"], hour, interval, location,
                             {name: row[column] for name, column in fields.items()},
                             None if source == "rt_lmp" else row["FLAG"], report, publication)
        output.append((location, event))
    return output


class PublicationRecords:
    """Short report leases and compare-and-set updates protect concurrent invocations."""
    def __init__(self, table):
        self.table = table
        self.deadline = float("inf")

    def get(self, source, report_id):
        common.check_budget(self.deadline)
        return self.table.get_entity(source + "_v1", report_id)

    def patch(self, current, **fields):
        common.check_budget(self.deadline)
        patch = {"PartitionKey": current["PartitionKey"], "RowKey": current["RowKey"], **fields}
        self.table.update_entity(patch, mode=UpdateMode.MERGE, etag=current.metadata["etag"],
                                 match_condition=MatchConditions.IfNotModified)

    def claim(self, source, report_id, owner, namespace, topic, now):
        for _ in range(4):
            current = self.get(source, report_id)
            if current.get("ValidationStatus") != "accepted" or current.get("ArchiveState") != "archived":
                raise ValueError("Report was not accepted and archived")
            configured = current.get("EventHubNamespace")
            if configured and (configured != namespace or current.get("EventHubName") != topic or current.get("EventSchemaVersion") != events.SCHEMA):
                raise ValueError("Report publication destination/schema differs; do not reuse its cursor")
            if current.get("EventPublishStatus") == "published":
                if current.get("EventPublishCursor") != current.get("RowCount"):
                    raise ValueError("Published record has an inconsistent event count")
                return current, False
            if current.get("EventPublishLeaseOwner") and current.get("EventPublishLeaseUntilUtc", "") > now:
                raise Busy("Another publisher owns this report; retry after its lease is released or expires")
            until = (datetime.fromisoformat(now) + timedelta(seconds=300)).isoformat(timespec="microseconds")
            try:
                self.patch(current, EventPublishLeaseOwner=owner, EventPublishLeaseUntilUtc=until,
                           EventPublishStatus="in_progress", EventHubNamespace=namespace, EventHubName=topic,
                           EventSchemaVersion=events.SCHEMA, EventPublishLastRunId=owner,
                           EventPublishStartedUtc=current.get("EventPublishStartedUtc") or now)
                return self.get(source, report_id), True
            except HttpResponseError as exc:
                if exc.status_code != 412:
                    raise
        raise Busy("Report changed repeatedly while claiming publication")

    def finish(self, source, report_id, owner, **fields):
        for _ in range(4):
            current = self.get(source, report_id)
            if current.get("EventPublishLeaseOwner") != owner:
                raise RuntimeError("Publication lease changed; cursor must not advance")
            try:
                self.patch(current, EventPublishLeaseOwner="", EventPublishLeaseUntilUtc="", **fields)
                return
            except HttpResponseError as exc:
                if exc.status_code != 412:
                    raise
        raise RuntimeError("Could not persist publication progress after four conditional updates")


class KafkaBatchSender:
    def __init__(self, credential, namespace, topic, run_id):
        from confluent_kafka import Producer
        self.topic = topic
        def oauth_cb(_):
            token = credential.get_token(f"https://{namespace}/.default")
            return token.token, token.expires_on
        self.producer = Producer({"bootstrap.servers": namespace + ":9093", "security.protocol": "SASL_SSL",
                                  "sasl.mechanism": "OAUTHBEARER", "oauth_cb": oauth_cb,
                                  "client.id": "ieso-publisher-" + run_id, "enable.idempotence": True,
                                  "acks": "all", "request.timeout.ms": 60000, "delivery.timeout.ms": 70000,
                                  "metadata.max.age.ms": 180000, "compression.type": "none",
                                  "allow.auto.create.topics": False})

    def send(self, batch, deadline):
        acknowledged, errors = [], []
        def delivered(error, message):
            if error:
                errors.append(str(error))
            else:
                acknowledged.append((message.partition(), message.offset()))
        try:
            for key, event in batch:
                common.check_budget(deadline)
                self.producer.produce(self.topic, key=key.encode("utf-8"),
                                      value=events.canonical(event).encode("utf-8"), on_delivery=delivered)
                self.producer.poll(0)
            wait = min(75, max(0, deadline - time.monotonic() - 15))
            remaining = self.producer.flush(wait)
            if remaining or errors or len(acknowledged) != len(batch):
                raise DeliveryError("Kafka batch was not fully acknowledged; retry the same report", len(acknowledged))
            return {"acknowledged_count": len(acknowledged),
                    "partitions": sorted({item[0] for item in acknowledged})}
        except Exception:
            self.producer.purge(in_queue=True, in_flight=True, blocking=True)
            self.producer.poll(0)
            raise


def publish(body, archive, state, sender_factory, namespace, topic, adf_run_id=""):
    source, report_id, size = request_options(body)
    validate_destination(namespace, topic)
    run_id, now, started = str(uuid.uuid4()), common.utc_now(), time.monotonic()
    deadline = started + 110
    archive.deadline = state.deadline = deadline
    result = {"source": source, "report_id": report_id, "run_id": run_id, "adf_run_id": adf_run_id,
              "namespace": namespace, "event_hub": topic, "schema_version": events.SCHEMA,
              "complete": False, "acknowledged_count": 0, "records_partition": source + "_v1", "records_row": report_id}
    attempt = {"PartitionKey": "publish_" + source + "_" + now[:10].replace("-", ""), "RowKey": run_id,
               "RunId": run_id, "AdfRunId": adf_run_id, "ReportId": report_id, "Source": source,
               "StartedUtc": now, "Outcome": "started", "Stage": "claim", "EventHubNamespace": namespace,
               "EventHubName": topic, "RequestedBatchSize": size}
    claimed, audit_started = False, False
    status_code = 200
    try:
        state.table.create_entity(attempt)
        audit_started = True
        result.update(attempt_partition=attempt["PartitionKey"], attempt_row=run_id)
        record, claimed = state.claim(source, report_id, run_id, namespace, topic, now)
        count = record["RowCount"]
        cursor = record.get("EventPublishCursor", 0)
        if type(cursor) is not int or not 0 <= cursor <= count:
            raise ValueError("Invalid publication cursor")
        result.update(row_count=count, batch_start=cursor, published_count=cursor)
        if not claimed:
            result.update(status="already_published", complete=True)
        else:
            result["stage"] = "read_csv"
            if source == "rt_lmp":
                path, digest = record["BlobPath"], record["Sha256"]
                expected_path = common.archive_path(digest)
            else:
                path, digest = record["PreparedCsvBlobPath"], record["PreparedCsvSha256"]
                expected_path = totals.prepared_path(digest)
            if path != expected_path or not HASH.fullmatch(digest):
                raise ValueError("Unexpected accepted CSV archive path/hash")
            content = archive.read_path(path, digest)
            rows = report_events(source, report_id, record, content, run_id, adf_run_id, now)
            batch = rows[cursor:cursor + size]
            result.update(batch_count=len(batch), stage="send")
            delivery = sender_factory(run_id).send(batch, deadline)
            if delivery["acknowledged_count"] != len(batch):
                raise DeliveryError("Sender did not acknowledge the complete batch", delivery["acknowledged_count"])
            result.update(delivery)
            end = cursor + len(batch)
            complete = end == count
            result["stage"] = "save_progress"
            fields = {"EventPublishCursor": end, "EventPublishRowCount": count,
                      "EventPublishStatus": "published" if complete else "in_progress",
                      "EventPublishLastRunId": run_id, "EventPublishLastAdfRunId": adf_run_id,
                      "EventPublishLastBatchCount": len(batch), "EventPublishLastError": "",
                      "EventPublishUpdatedUtc": common.utc_now()}
            if complete:
                fields["EventPublishedUtc"] = common.utc_now()
            state.finish(source, report_id, run_id, **fields)
            claimed = False
            result.update(status=fields["EventPublishStatus"], complete=complete, published_count=end)
    except Busy:
        result.update(status="busy", retry_after_seconds=5, stage="claim")
    except Exception as exc:
        logging.exception("ieso_publish_failed source=%s report_id=%s run_id=%s", source, report_id, run_id)
        result.update(status="failed", error_type=type(exc).__name__,
                      error="Publishing failed; inspect this run_id in Application Insights and the publishing attempt row")
        if isinstance(exc, DeliveryError):
            result["acknowledged_count"] = exc.acknowledged
        status_code = 404 if isinstance(exc, ResourceNotFoundError) else 409 if isinstance(exc, ValueError) else 502
        if claimed:
            try:
                # Cleanup is best effort. A host crash leaves a five-minute lease, then a retry can resume.
                state.deadline = time.monotonic() + 15
                state.finish(source, report_id, run_id, EventPublishStatus="failed",
                             EventPublishLastError=type(exc).__name__, EventPublishUpdatedUtc=common.utc_now())
            except Exception:
                logging.exception("ieso_publish_lease_release_failed run_id=%s", run_id)
    finally:
        result["duration_ms"] = round((time.monotonic() - started) * 1000)
        if audit_started:
            try:
                attempt.update(Outcome=result.get("status", "failed"), Stage=result.get("stage", "complete"),
                               CompletedUtc=common.utc_now(), AcknowledgedCount=result["acknowledged_count"],
                               ErrorType=result.get("error_type", ""), BatchStart=result.get("batch_start", 0),
                               PublishedCount=result.get("published_count", 0))
                state.table.update_entity(attempt, mode=UpdateMode.MERGE)
            except Exception:
                logging.exception("ieso_publish_attempt_update_failed run_id=%s", run_id)
                # An unaudited attempt must be visible to the caller, even if the report cursor was saved.
                result.update(status="failed", complete=False, error="Publishing attempt update failed; inspect run_id")
                status_code = 500
        logging.info("ieso_publication %s", json.dumps(result))
    return result, status_code
