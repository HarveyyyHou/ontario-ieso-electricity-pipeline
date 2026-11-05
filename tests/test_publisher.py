import copy
from contextlib import redirect_stdout
import io
import json
import logging
from pathlib import Path
import sys
from threading import Event, Thread
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch, Mock
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "functions" / "ieso_ingest"))
import collector as c
import events as e
import publisher as p
import totals
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import ieso_kafka as cli
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from test_ingestion import Table, report
from test_totals import csv_fixture, xml_fixture

NAMESPACE = "evhns-ieso-dev-024ed7af.servicebus.windows.net"


class FakeTable(Table):
    def get_entity(self, partition_key, row_key):
        try:
            return super().get_entity(partition_key, row_key)
        except KeyError as exc:
            raise ResourceNotFoundError("not found") from exc


class Archive:
    def __init__(self):
        self.data = {}
    def read_path(self, path, digest):
        content = self.data[path]
        if c.sha256(content) != digest:
            raise ValueError("Archived bytes differ from the recorded hash")
        return content


class Sender:
    def __init__(self):
        self.sent, self.fail, self.entered, self.release = [], False, None, None
    def send(self, batch, deadline):
        self.sent.extend(copy.deepcopy(batch))
        if self.entered:
            self.entered.set()
            self.release.wait(3)
        if self.fail:
            raise p.DeliveryError("partial delivery", 1)
        return {"acknowledged_count": len(batch), "partitions": [0]}


class PublisherTests(unittest.TestCase):
    def setUp(self):
        self.table, self.archive, self.sender = FakeTable(), Archive(), Sender()
        self.state = p.PublicationRecords(self.table)
        self.logs = patch.object(logging, "exception")
        self.logs.start()
        self.addCleanup(self.logs.stop)

    def lmp(self, content=None):
        content = content or report()
        digest = c.sha256(content)
        row = {"PartitionKey": "rt_lmp_v1", "RowKey": digest, "Sha256": digest,
               "Bytes": len(content), "BlobPath": c.archive_path(digest), "ArchiveState": "archived",
               "FirstSeenUtc": "2026-11-01T05:02:00+00:00", "LastSeenUtc": "2026-11-01T05:02:00+00:00",
               "ValidationStatus": "accepted", "EventPublishStatus": "not_enabled", **c.validate_report(content)}
        self.table.create_entity(row)
        self.archive.data[row["BlobPath"]] = content
        return digest, row

    def total(self):
        csv, xml = csv_fixture(), xml_fixture()
        content, details, _ = totals.combine(csv, xml)
        csv_hash, xml_hash, prepared_hash = c.sha256(csv), c.sha256(xml), c.sha256(content)
        rid = totals.report_id(csv_hash, xml_hash)
        row = {"PartitionKey": "rt_totals_v1", "RowKey": rid, "Sha256": csv_hash, "CsvSha256": csv_hash,
               "XmlSha256": xml_hash, "BlobPath": totals.raw_path("csv", csv_hash),
               "PreparedCsvBlobPath": totals.prepared_path(prepared_hash), "PreparedCsvSha256": prepared_hash,
               "FirstSeenUtc": "2026-11-01T05:02:00+00:00", "ValidationStatus": "accepted",
               "ArchiveState": "archived", "PreparedState": "ready", "EventPublishStatus": "not_enabled", **details}
        self.table.create_entity(row)
        self.archive.data[row["PreparedCsvBlobPath"]] = content
        return rid, row

    def call(self, rid, source="rt_lmp", size=500):
        return p.publish({"source": source, "report_id": rid, "batch_size": size}, self.archive,
                         self.state, lambda _: self.sender, NAMESPACE, p.DEFAULT_TOPICS[source], "adf-test-run")

    def test_batches_resume_and_completed_rerun_sends_nothing(self):
        rid, _ = self.lmp()
        first, code = self.call(rid, size=1)
        self.assertEqual((code, first["status"], first["complete"], first["published_count"]), (200, "in_progress", False, 1))
        second, _ = self.call(rid, size=1)
        self.assertEqual((second["status"], second["published_count"]), ("published", 2))
        again, _ = self.call(rid)
        self.assertEqual((again["status"], again["acknowledged_count"], len(self.sender.sent)), ("already_published", 0, 2))
        self.assertEqual(self.state.get("rt_lmp", rid)["EventPublishLastAdfRunId"], "adf-test-run")

    def test_partial_ack_does_not_advance_cursor_and_retry_has_same_ids(self):
        rid, _ = self.lmp()
        self.sender.fail = True
        result, code = self.call(rid)
        self.assertEqual((code, result["acknowledged_count"]), (502, 1))
        self.assertEqual(self.state.get("rt_lmp", rid).get("EventPublishCursor", 0), 0)
        prior = [x[1]["report_event_id"] for x in self.sender.sent]
        self.sender.fail = False
        result, code = self.call(rid)
        self.assertEqual((code, result["complete"]), (200, True))
        self.assertEqual(prior, [x[1]["report_event_id"] for x in self.sender.sent[2:]])

    def test_ack_before_cursor_write_failure_can_duplicate_but_not_lose_rows(self):
        rid, _ = self.lmp()
        original = self.state.finish
        def finish(*args, **kwargs):
            if "EventPublishCursor" in kwargs:
                raise HttpResponseError("storage unavailable")
            return original(*args, **kwargs)
        with patch.object(self.state, "finish", side_effect=finish):
            result, code = self.call(rid)
        self.assertEqual(code, 502)
        self.assertEqual(self.state.get("rt_lmp", rid).get("EventPublishCursor", 0), 0)
        self.call(rid)
        self.assertEqual(len(self.sender.sent), 4)
        self.assertEqual([x[1]["event_id"] for x in self.sender.sent[:2]], [x[1]["event_id"] for x in self.sender.sent[2:]])

    def test_rejected_report_cannot_publish(self):
        rid, _ = self.lmp()
        entity = self.state.get("rt_lmp", rid)
        self.state.patch(entity, ValidationStatus="rejected")
        result, code = self.call(rid)
        self.assertEqual((code, result["complete"], len(self.sender.sent)), (409, False, 0))

    def test_unknown_report_returns_404_and_failed_attempt(self):
        result, code = self.call("0" * 64)
        self.assertEqual((code, result["error_type"]), (404, "ResourceNotFoundError"))
        attempt = self.table.get_entity(result["attempt_partition"], result["attempt_row"])
        self.assertEqual(attempt["Outcome"], "failed")

    def test_tampered_archive_is_not_sent(self):
        rid, row = self.lmp()
        self.archive.data[row["BlobPath"]] += b"bad"
        result, code = self.call(rid)
        self.assertEqual((code, len(self.sender.sent)), (409, 0))

    def test_metadata_mismatch_is_not_sent(self):
        rid, _ = self.lmp()
        self.state.patch(self.state.get("rt_lmp", rid), RowCount=99)
        result, code = self.call(rid)
        self.assertEqual((code, len(self.sender.sent)), (409, 0))

    def test_cas_retry_preserves_collection_fields(self):
        rid, _ = self.lmp()
        self.table.conflicts = 2
        result, code = self.call(rid)
        self.assertEqual((code, result["complete"]), (200, True))
        self.assertEqual(self.state.get("rt_lmp", rid)["LastSeenUtc"], "2026-11-01T05:02:00+00:00")

    def test_concurrent_request_observes_lease(self):
        rid, _ = self.lmp()
        self.sender.entered, self.sender.release = Event(), Event()
        result = []
        thread = Thread(target=lambda: result.append(self.call(rid)))
        thread.start()
        self.assertTrue(self.sender.entered.wait(2))
        busy, code = self.call(rid)
        self.assertEqual((code, busy["status"], busy["complete"]), (200, "busy", False))
        self.sender.release.set()
        thread.join(4)
        self.assertFalse(thread.is_alive())
        self.assertTrue(result[0][0]["complete"])
        self.assertEqual(len(self.sender.sent), 2)

    def test_expired_lease_is_recoverable(self):
        rid, _ = self.lmp()
        self.state.patch(self.state.get("rt_lmp", rid), EventPublishLeaseOwner="dead-host",
                         EventPublishLeaseUntilUtc="2000-01-01T00:00:00+00:00")
        result, code = self.call(rid)
        self.assertEqual((code, result["complete"]), (200, True))

    def test_changed_destination_cannot_reuse_a_published_cursor(self):
        rid, _ = self.lmp()
        self.call(rid)
        result, code = p.publish({"source": "rt_lmp", "report_id": rid}, self.archive, self.state,
                                 lambda _: self.sender, NAMESPACE, "different-topic")
        self.assertEqual((code, result["complete"], len(self.sender.sent)), (409, False, 2))

    def test_totals_publisher_reads_prepared_csv_and_carries_demand(self):
        rid, _ = self.total()
        result, code = self.call(rid, source="rt_totals")
        self.assertEqual((code, result["complete"]), (200, True))
        key, event = self.sender.sent[0]
        self.assertEqual(key, "ONTARIO")
        self.assertIn("ontario_demand", event["measurements"])
        self.assertEqual(event["units"], "MW")
        self.assertTrue(e.validate_event(event))

    def test_attempt_update_failure_is_visible_after_cursor_saved(self):
        rid, _ = self.lmp()
        original = self.table.update_entity
        def update(entity, **kwargs):
            if entity["PartitionKey"].startswith("publish_"):
                raise HttpResponseError("attempt audit unavailable")
            return original(entity, **kwargs)
        with patch.object(self.table, "update_entity", side_effect=update):
            result, code = self.call(rid)
        self.assertEqual((code, result["complete"]), (500, False))
        self.assertEqual(self.state.get("rt_lmp", rid)["EventPublishStatus"], "published")
        self.call(rid)
        self.assertEqual(len(self.sender.sent), 2)

    def test_out_of_order_source_rows_have_deterministic_batch_order(self):
        content = report(["1,2,B,2,0,0", "1,1,Z,1,0,0", "1,1,A,1,0,0"])
        rid, row = self.lmp(content)
        messages = p.report_events("rt_lmp", rid, row, content, "run", "adf", c.utc_now())
        self.assertEqual([(m[1]["interval"], m[0]) for m in messages], [(1, "A"), (1, "Z"), (2, "B")])


class EventTests(unittest.TestCase):
    def build(self, value="1.00", report_id="a" * 64):
        return e.build("rt_lmp", "2026-07-01", 1, 1, "TEST", {"lmp": value, "energy_loss_price": "0", "energy_congestion_price": "0"},
                       None, {"report_id": report_id, "csv_sha256": report_id, "csv_blob_path": "raw.csv",
                              "source_version_utc": "2026-07-01T05:01:00Z", "source_time_convention": "EST_UTC_MINUS_05"}, {})

    def test_normalized_values_share_id_across_reports_and_retrieval_times(self):
        a, b = self.build("1.00"), self.build("1", "b" * 64)
        b["publication"] = {"attempted_at_utc": "later"}
        self.assertEqual(a["event_id"], b["event_id"])
        self.assertNotEqual(a["report_event_id"], b["report_event_id"])

    def test_correction_and_return_to_previous_value_retain_report_history(self):
        a, b, again = self.build("1", "a" * 64), self.build("2", "b" * 64), self.build("1", "c" * 64)
        self.assertNotEqual(a["event_id"], b["event_id"])
        self.assertEqual(a["event_id"], again["event_id"])
        self.assertEqual(len({x["report_event_id"] for x in (a, b, again)}), 3)

    def test_fixed_est_does_not_apply_summer_dst(self):
        self.assertEqual(self.build()["interval_start_utc"], "2026-07-01T05:00:00Z")
        self.assertEqual(e.interval_times("2026-07-01", 24, 12)[1], "2026-07-02T05:00:00Z")

    def test_negative_price_and_negative_zero(self):
        self.assertEqual(self.build("-2.500")["measurements"]["lmp"], "-2.5")
        self.assertEqual(e.decimal_text("-0.00"), "0")

    def test_invalid_decimal_is_rejected(self):
        for value in ("NaN", "Infinity", "1e9999999", "bad", 1):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.build(value)

    def test_event_validation_detects_modified_price_timestamp_units_and_id(self):
        good = self.build()
        self.assertTrue(e.validate_event(good))
        for field, value in [("interval_end_utc", "bad"), ("units", "MW"), ("event_id", "b" * 64),
                             ("measurements", {"lmp": "2", "energy_loss_price": "0", "energy_congestion_price": "0"})]:
            bad = copy.deepcopy(good);bad[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                e.validate_event(bad)

    def test_bad_request_never_accepts_arbitrary_urls_or_batch_sizes(self):
        for body in ({"source": "rt_lmp", "report_id": "http://example.com"},
                     {"source": "rt_lmp", "report_id": "a" * 64, "batch_size": True},
                     {"source": "rt_totals", "report_id": "a" * 64, "batch_size": 1001},
                     {"source": "rt_lmp", "report_id": "a" * 64, "force": True}):
            with self.subTest(body=body), self.assertRaises(ValueError):
                p.request_options(body)


class KafkaAdapterTests(unittest.TestCase):
    def test_partial_delivery_callbacks_raise_even_after_flush_returns_zero(self):
        credential = Mock()
        class Producer:
            def __init__(self, config): self.config = config;self.i = 0
            def produce(self, topic, key, value, on_delivery):
                self.i += 1
                on_delivery(None if self.i == 1 else "denied", Mock(partition=lambda: 0, offset=lambda: 3))
            def poll(self, _): pass
            def flush(self, _): return 0
            def purge(self, **kwargs): pass
        with patch("confluent_kafka.Producer", Producer):
            sender = p.KafkaBatchSender(credential, NAMESPACE, "ieso-rt-lmp", "run")
            self.assertTrue(sender.producer.config["enable.idempotence"])
            self.assertEqual(sender.producer.config["acks"], "all")
            with self.assertRaises(p.DeliveryError) as caught:
                sender.send([("TEST", {}), ("TEST", {})], float("inf"))
            self.assertEqual(caught.exception.acknowledged, 1)


class PipelineTests(unittest.TestCase):
    def test_each_source_has_independent_gate_bounded_loop_and_failure_propagation(self):
        path = Path(__file__).resolve().parents[1] / "adf/pipeline/pl_ieso_datacollect.json"
        pipeline = json.loads(path.read_text())["properties"]
        activities = {a["name"]: a for a in pipeline["activities"]}
        databricks_gate = activities.pop("if_run_databricks")
        self.assertEqual(len(activities), 8)
        self.assertFalse(pipeline["parameters"]["run_databricks"]["defaultValue"])
        self.assertEqual(
            {dep["activity"]: dep["dependencyConditions"] for dep in databricks_gate["dependsOn"]},
            {"if_rt_lmp_published": ["Succeeded"], "if_rt_totals_published": ["Succeeded"]},
        )
        job = databricks_gate["typeProperties"]["ifTrueActivities"][0]
        self.assertEqual(job["type"], "DatabricksJob")
        params = job["typeProperties"]["jobParameters"]
        self.assertIn(".output.sha256", params["lmp_report_id"]["value"])
        self.assertIn(".output.report_id", params["totals_report_id"]["value"])
        for source in ("rt_lmp", "rt_totals"):
            self.assertEqual(activities["act_collect_" + source]["policy"]["timeout"], "0.00:10:00")
            loop = activities["until_" + source + "_published"]
            self.assertEqual(loop["typeProperties"]["timeout"], "0.00:30:00")
            self.assertEqual(loop["dependsOn"][0]["activity"], "if_" + source + "_accepted")
            children = loop["typeProperties"]["activities"]
            self.assertEqual(children[0]["linkedServiceName"]["referenceName"], "ls_ieso_publish_function")
            self.assertEqual(children[1]["dependsOn"][0]["dependencyConditions"], ["Completed"])
            failed_var = children[1]["typeProperties"]["ifFalseActivities"][0]["typeProperties"]
            self.assertEqual(failed_var, {"variableName": source + "_publish_failed", "value": True})
            final = activities["if_" + source + "_published"]
            self.assertEqual(final["typeProperties"]["ifFalseActivities"][0]["type"], "Fail")
            for suffix in ("complete", "failed"):
                self.assertFalse(pipeline["variables"][source + "_publish_" + suffix]["defaultValue"])


class RouteAndToolTests(unittest.TestCase):
    def test_publisher_rejects_bad_request_before_azure_clients(self):
        import azure.functions as func
        import function_app
        request = func.HttpRequest(method="POST", url=cli.PUBLISH_URL, body=b'{"source":"rt_lmp","report_id":"bad"}')
        response = function_app.publish_ieso.build().get_user_function()(request)
        self.assertEqual(response.status_code, 400)
        self.assertFalse(json.loads(response.get_body())["complete"])

    def test_publisher_is_disabled_without_explicit_setting(self):
        import azure.functions as func
        import function_app
        request = func.HttpRequest(method="POST", url=cli.PUBLISH_URL,
                                  body=json.dumps({"source": "rt_lmp", "report_id": "a" * 64}).encode())
        with patch.dict("os.environ", {"IESO_PUBLISH_ENABLED": "false"}):
            response = function_app.publish_ieso.build().get_user_function()(request)
        self.assertEqual(response.status_code, 503)
        self.assertEqual(json.loads(response.get_body())["status"], "disabled")

    def test_cli_keeps_prior_progress_and_never_prints_key_on_network_failure(self):
        import requests
        response = Mock(status_code=200)
        response.json.return_value = {"status": "in_progress", "complete": False, "published_count": 1}
        with TemporaryDirectory() as directory:
            args = SimpleNamespace(source="rt_lmp", report_id="a" * 64, batch_size=1, until_complete=True,
                                   max_calls=3, output=str(Path(directory) / "result.json"), function_url=cli.PUBLISH_URL)
            captured = io.StringIO()
            with patch.object(cli, "getpass", return_value="do-not-print-key"), patch.object(cli.time, "sleep"), \
                 patch("requests.post", side_effect=[response, requests.Timeout("network")]), redirect_stdout(captured):
                result = cli.publish_report(args)
            self.assertFalse(result["check_passed"])
            self.assertEqual(len(result["results"]), 1)
            self.assertNotIn("do-not-print-key", captured.getvalue())

    def test_consumer_counts_duplicates_and_verifies_exact_report(self):
        first = EventTests().build()
        second = e.build("rt_lmp", "2026-07-01", 1, 2, "TEST", first["measurements"], None, first["report"], {})
        messages = [Mock(error=lambda: None, value=lambda x=x: e.canonical(x).encode(), key=lambda: b"TEST",
                         partition=lambda: 0, offset=lambda i=i: i) for i, x in enumerate([first, first, second])]
        class Consumer:
            def __init__(self, config): self.config = config
            def subscribe(self, topics): pass
            def poll(self, _): return messages.pop(0) if messages else None
            def close(self): pass
        args = SimpleNamespace(expected_count=2, source="rt_lmp", report_id="a" * 64, namespace=NAMESPACE, timeout=100)
        with patch("azure.identity.AzureCliCredential"), patch("confluent_kafka.Consumer", Consumer), \
             patch.object(cli.time, "monotonic", side_effect=range(1000)), redirect_stdout(io.StringIO()):
            result = cli.consume_report(args)
        self.assertTrue(result["check_passed"])
        self.assertEqual((result["unique_report_events"], result["duplicate_deliveries"]), (2, 1))


if __name__ == "__main__":
    unittest.main()
