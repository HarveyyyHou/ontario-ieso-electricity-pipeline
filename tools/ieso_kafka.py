"""Inspect a report, publish bounded batches, and verify Kafka messages. Keys are never printed."""
import argparse
from datetime import datetime, timezone
from getpass import getpass
import json
from pathlib import Path
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "functions" / "ieso_ingest"))
import events

DEFAULT_NAMESPACE = "evhns-ieso-dev-024ed7af.servicebus.windows.net"
TOPICS = {"rt_lmp": "ieso-rt-lmp", "rt_totals": "ieso-rt-totals"}
PUBLISH_URL = "https://func-ieso-ingest-dev-024ed7af.azurewebsites.net/api/publish_ieso"


def save(path, value):
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    print(f"Evidence saved to {destination}; check_passed={value.get('check_passed', False)}", flush=True)


def report_record(args):
    from azure.data.tables import TableClient
    from azure.identity import AzureCliCredential
    with AzureCliCredential() as credential, TableClient(
            endpoint=f"https://{args.account}.table.core.windows.net", table_name=args.table,
            credential=credential, retry_total=1, connection_timeout=5, read_timeout=10) as table:
        entity = table.get_entity(args.source + "_v1", args.report_id)
        selected = {k: v for k, v in entity.items()
                    if k in ("PartitionKey", "RowKey", "ValidationStatus", "ArchiveState", "PreparedState",
                             "RowCount", "DeliveryDate", "DeliveryHour", "BlobPath", "PreparedCsvBlobPath",
                             "PreparedCsvSha256", "Sha256", "CsvSha256", "XmlSha256") or k.startswith("Event")}
    print(json.dumps(selected, indent=2), flush=True)
    good = entity.get("ValidationStatus") == "accepted" and entity.get("ArchiveState") == "archived"
    if args.source == "rt_totals":
        good = good and entity.get("PreparedState") == "ready"
    if args.require_published:
        good = good and entity.get("EventPublishStatus") == "published" and \
            entity.get("EventPublishCursor") == entity.get("RowCount") == entity.get("EventPublishRowCount")
    return {"command": "record", "check_passed": good, "record": selected}


def publish_report(args):
    import requests
    key = getpass("Paste the publish_ieso function key named adf (input hidden), then press Enter: ")
    if not key:
        raise ValueError("Publisher function key was empty")
    body = {"source": args.source, "report_id": args.report_id, "batch_size": args.batch_size}
    results = []
    limit = args.max_calls if args.until_complete else 1
    for _ in range(limit):
        try:
            response = requests.post(args.function_url, json=body, headers={"x-functions-key": key}, timeout=(5, 170))
        except requests.RequestException as exc:
            return {"command": "publish", "request": body, "results": results, "check_passed": False,
                    "complete": False, "error_type": type(exc).__name__,
                    "error": "HTTP request failed; inspect the last run and resume the same report"}
        try:
            payload = response.json()
        except ValueError as exc:
            raise ValueError(f"Publisher returned HTTP {response.status_code} without a JSON response") from exc
        item = {"http_status": response.status_code, "response": payload}
        results.append(item)
        print(json.dumps(item, indent=2), flush=True)
        save(args.output, {"command": "publish", "request": body, "results": results,
                           "request_succeeded": response.status_code == 200,
                           "complete": payload.get("complete") is True,
                           "check_passed": response.status_code == 200 and payload.get("status") in
                           ("in_progress", "published", "already_published", "busy")})
        if response.status_code != 200 or payload.get("status") not in ("in_progress", "published", "already_published", "busy"):
            return {"command": "publish", "request": body, "results": results, "check_passed": False,
                    "complete": False, "error": "Publishing request failed; inspect its run_id before continuing"}
        if payload.get("complete") is True:
            return {"command": "publish", "request": body, "results": results,
                    "request_succeeded": True, "complete": True, "check_passed": True}
        if not args.until_complete:
            return {"command": "publish", "request": body, "results": results,
                    "request_succeeded": True, "complete": False, "check_passed": True}
        time.sleep(5 if payload["status"] == "busy" else 1)
    return {"command": "publish", "request": body, "results": results, "check_passed": False, "complete": False,
            "error": "Stopped at max-calls before publication completed; the next invocation resumes saved progress"}


def consume_report(args):
    from azure.identity import AzureCliCredential
    from confluent_kafka import Consumer, KafkaException
    if args.expected_count < 1:
        raise ValueError("expected-count must be positive")
    topic = TOPICS[args.source]
    group = "ieso-verify-" + uuid.uuid4().hex
    received, checked, seen, sample, positions = 0, 0, set(), [], {}
    started, reached = time.monotonic(), None
    with AzureCliCredential(process_timeout=20) as credential:
        def oauth_cb(_):
            token = credential.get_token(f"https://{args.namespace}/.default")
            return token.token, token.expires_on
        consumer = Consumer({"bootstrap.servers": args.namespace + ":9093", "security.protocol": "SASL_SSL",
                             "sasl.mechanism": "OAUTHBEARER", "oauth_cb": oauth_cb,
                             "group.id": group, "auto.offset.reset": "earliest", "enable.auto.commit": False,
                             "metadata.max.age.ms": 180000, "allow.auto.create.topics": False})
        try:
            consumer.subscribe([topic])
            while time.monotonic() - started < args.timeout:
                message = consumer.poll(1)
                if message is None:
                    if reached is not None and time.monotonic() - reached >= 5:
                        break
                    continue
                if message.error():
                    raise KafkaException(message.error())
                received += 1
                event = json.loads(message.value().decode("utf-8"))
                if event.get("source") != args.source or event.get("report", {}).get("report_id") != args.report_id:
                    continue
                events.validate_event(event)
                key = message.key().decode("utf-8") if message.key() is not None else None
                if key != event["pricing_location"]:
                    raise ValueError("Kafka key differs from the event pricing location")
                checked += 1
                seen.add(event["report_event_id"])
                positions[str(message.partition())] = message.offset()
                if len(sample) < 2:
                    sample.append({"key": key, "event": event, "partition": message.partition(), "offset": message.offset()})
                if len(seen) > args.expected_count:
                    raise ValueError("More unique observations than the accepted report row count")
                if len(seen) == args.expected_count and reached is None:
                    reached = time.monotonic()
            good = len(seen) == args.expected_count
            result = {"command": "consume", "check_passed": good, "source": args.source,
                      "report_id": args.report_id, "topic": topic, "consumer_group": group,
                      "expected_count": args.expected_count, "matching_messages": checked,
                      "unique_report_events": len(seen), "duplicate_deliveries": checked - len(seen),
                      "all_messages_scanned": received, "last_offsets_by_partition": positions, "sample": sample}
            print(json.dumps(result, indent=2), flush=True)
            return result
        finally:
            consumer.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    record = sub.add_parser("record", help="Read the accepted report and saved publishing progress")
    record.add_argument("--account", default="iesodev001")
    record.add_argument("--table", default="iesoingestion")
    record.add_argument("--require-published", action="store_true")
    publish = sub.add_parser("publish", help="Send one batch, or resume until complete")
    publish.add_argument("--batch-size", type=int, choices=range(1, 1001), default=500)
    publish.add_argument("--until-complete", action="store_true")
    publish.add_argument("--max-calls", type=int, default=500)
    publish.add_argument("--function-url", default=PUBLISH_URL)
    consume = sub.add_parser("consume", help="Verify one report using a fresh Kafka consumer group")
    consume.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    consume.add_argument("--expected-count", type=int, required=True)
    consume.add_argument("--timeout", type=int, default=180)
    for command in (record, publish, consume):
        command.add_argument("--source", choices=TOPICS, required=True)
        command.add_argument("--report-id", required=True)
        command.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    summary = {"command": args.command, "check_passed": False}
    try:
        if len(args.report_id) != 64 or any(c not in "0123456789abcdef" for c in args.report_id):
            raise ValueError("report-id must be 64 lowercase hexadecimal characters")
        if args.command == "publish" and args.function_url != PUBLISH_URL:
            raise ValueError("Unexpected publisher URL; verify before sending a function key")
        if args.command == "publish" and args.max_calls < 1:
            raise ValueError("max-calls must be positive")
        summary = {"record": report_record, "publish": publish_report, "consume": consume_report}[args.command](args)
        return 0 if summary["check_passed"] else 1
    except Exception as exc:
        summary.update(check_passed=False, error_type=type(exc).__name__, error=str(exc))
        print(f"STOP: {exc}", file=sys.stderr)
        return 1
    finally:
        save(args.output, summary)


if __name__ == "__main__":
    sys.exit(main())
