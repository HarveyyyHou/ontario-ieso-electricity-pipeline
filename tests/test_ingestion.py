import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import logging
from pathlib import Path
import sys
from threading import Lock
import time
import unittest
from unittest.mock import patch
from unittest.mock import Mock
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "functions" / "ieso_ingest"))
import collector as c
from azure.core import MatchConditions
from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.data.tables import TableEntity
import requests


def report(rows=None, created="2026/11/01 00:01:00", day="2026/11/01"):
    rows = rows if rows is not None else ["1,1,TEST-LT.AG,-1.00,0.00,0.00", "1,2,TEST-LT.AG,2.00,0.00,0.00"]
    return (f"CREATED AT {created} FOR {day}\r\n" + ",".join(c.HEADER) + "\r\n" + "\r\n".join(rows) + "\r\n").encode()


class Table:
    def __init__(self):
        self.rows, self.lock, self.conflicts = {}, Lock(), 0

    def create_entity(self, entity):
        key = entity["PartitionKey"], entity["RowKey"]
        with self.lock:
            if key in self.rows:
                raise ResourceExistsError("exists")
            self.rows[key] = (copy.deepcopy(entity), 1)

    def get_entity(self, partition_key, row_key):
        with self.lock:
            data, version = self.rows[partition_key, row_key]
            entity = TableEntity(copy.deepcopy(data))
            entity._metadata = {"etag": str(version)}
            return entity

    def update_entity(self, entity, mode=None, etag=None, match_condition=None):
        key = entity["PartitionKey"], entity["RowKey"]
        with self.lock:
            data, version = self.rows[key]
            if match_condition == MatchConditions.IfNotModified and (self.conflicts or etag != str(version)):
                self.conflicts = max(0, self.conflicts - 1)
                exc = HttpResponseError("changed")
                exc.status_code = 412
                raise exc
            data.update(copy.deepcopy(entity))
            self.rows[key] = data, version + 1


class Archive:
    def __init__(self):
        self.data, self.lock = {}, Lock()

    def put(self, path, content, metadata):
        with self.lock:
            if path in self.data:
                return "already_archived"
            self.data[path] = content
            return "archived"

    def read(self, digest):
        return self.data[c.archive_path(digest)]


class Response:
    def __init__(self, status=200, content=b"ok", headers=None):
        self.status_code, self.content, self.headers = status, content, headers or {}
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def iter_content(self, size): yield self.content


class Session:
    def __init__(self, replies): self.replies, self.calls = list(replies), 0
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def get(self, *args, **kwargs):
        self.calls += 1
        response = self.replies.pop(0)
        if isinstance(response, Exception): raise response
        return response


class ContractTests(unittest.TestCase):
    def test_all_rows_checked(self):
        with self.assertRaisesRegex(ValueError, "Line 4"):
            c.validate_report(report(["1,1,TEST,1,0,0", "1,2,TEST,bad,0,0"]))

    def test_negative_prices_and_partial_hour_valid(self):
        details = c.validate_report(report())
        self.assertEqual(details["RowCount"], 2)
        self.assertEqual(details["IntervalsPresent"], "1,2")

    def test_duplicate_key_rejected(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            c.validate_report(report(["1,1,TEST,1,0,0"] * 2))

    def test_missing_fields_and_bad_header(self):
        for data in [report(["1,1,,1,0,0"]), report().replace(b"Pricing Location", b"Other")]:
            with self.assertRaises(ValueError): c.validate_report(data)

    def test_nonfinite_and_invalid_intervals(self):
        for row in ["1,1,A,NaN,0,0", "1,13,A,1,0,0", "25,1,A,1,0,0", "1,1,A,1,0"]:
            with self.assertRaises(ValueError): c.validate_report(report([row]))

    def test_empty_and_invalid_utf8(self):
        for content in [b"", b"\xff", report([])]:
            with self.assertRaises(ValueError): c.validate_report(content)

    def test_filename_period_must_match(self):
        with self.assertRaisesRegex(ValueError, "period"):
            c.validate_report(report(), "PUB_RealtimeEnergyLMP_2026110102.csv")

    def test_fixed_est_on_dst_day_and_hour24(self):
        details = c.validate_report(report(["24,12,A,1,0,0"], day="2026/07/01"))
        self.assertEqual(details["LatestIntervalEndUtc"], "2026-07-02T05:00:00+00:00")

    def test_request_whitelist(self):
        for body in [{"source":"x"}, {"source":"rt_lmp","report_file":"https://evil/a"},
                     {"source":"rt_lmp","archive_sha256":"bad"},
                     {"source":"rt_lmp","report_file":c.LATEST_FILE,"archive_sha256":"a"*64}]:
            with self.assertRaises(ValueError): c.request_options(body)


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)
        self.table, self.archive = Table(), Archive()
        self.manifest = c.Manifest(self.table)
        self.bytes = report()

    def tearDown(self): logging.disable(logging.NOTSET)

    def collect(self, content=None, **kwargs):
        data = content if content is not None else self.bytes
        return c.collect({"source":"rt_lmp"}, self.archive, self.manifest,
                         downloader=lambda *_: (data, {}), **kwargs)

    def test_repeated_download_one_blob_and_report_two_attempts(self):
        first, status = self.collect()
        second, status2 = self.collect()
        self.assertEqual((status, status2), (200,200))
        self.assertEqual(first["sha256"], second["sha256"])
        self.assertTrue(first["attempt_partition"].startswith("attempt_rt_lmp_"))
        self.assertEqual(first["attempt_row"], first["run_id"])
        self.assertEqual(second["status"], "already_archived")
        self.assertEqual(len(self.archive.data), 1)
        self.assertEqual(sum(k[0] == c.CONTRACT for k in self.table.rows), 1)
        self.assertEqual(sum(k[0].startswith("attempt_") for k in self.table.rows), 2)

    def test_corrected_version_preserved(self):
        self.collect()
        self.collect(self.bytes.replace(b"-1.00",b"-2.00"))
        self.assertEqual(len(self.archive.data), 2)

    def test_rejected_payload_quarantined(self):
        outcome, status = self.collect(b"not csv")
        self.assertEqual(status, 200)
        self.assertEqual(outcome["validation_status"], "rejected")
        self.assertIn("/quarantine/", outcome["blob_name"])

    def test_failure_after_blob_write_resumes(self):
        with patch.object(self.manifest, "record_report", side_effect=RuntimeError("table unavailable")):
            outcome, status = self.collect()
        self.assertEqual(status, 500)
        self.assertEqual(len(self.archive.data), 1)
        recovered, status = self.collect()
        self.assertEqual(status, 200)
        self.assertEqual(recovered["status"], "already_archived")
        self.assertEqual(recovered["manifest_status"], "created")

    def test_archive_replay_does_not_contact_ieso(self):
        outcome, _ = self.collect()
        result, code = c.collect({"source":"rt_lmp","archive_sha256":outcome["sha256"]},
                                 self.archive, self.manifest, downloader=lambda *_: self.fail("HTTP used"))
        self.assertEqual(code, 200)
        self.assertEqual(result["status"], "already_archived")

    def test_source_404_is_durable_failed_attempt(self):
        def missing(*_): raise c.SourceError("not found", 404)
        outcome, status = c.collect({"source":"rt_lmp"},self.archive,self.manifest,downloader=missing)
        self.assertEqual((status,outcome["source_http_status"]), (502,404))
        self.assertEqual(next(iter(self.table.rows.values()))[0]["Outcome"], "failed")

    def test_long_source_wait_returns_a_gateable_deferred_result(self):
        def wait(*_): raise c.DeferredSource(429, 600)
        outcome, status = c.collect({"source":"rt_lmp"},self.archive,self.manifest,downloader=wait)
        self.assertEqual((status,outcome["status"],outcome["validation_status"]),(200,"deferred","not_checked"))
        self.assertEqual(outcome["retry_after_seconds"],600)
        self.assertFalse(self.archive.data)

    def test_overlapping_calls(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: self.collect(), range(2)))
        self.assertTrue(all(code == 200 for _,code in results))
        self.assertEqual(len(self.archive.data), 1)

    def test_conditional_update_retries_and_preserves_publish_state(self):
        result, _ = self.collect()
        key = c.CONTRACT, result["sha256"]
        self.table.rows[key][0]["EventPublishStatus"] = "future_test_value"
        self.table.conflicts = 1
        result, status = self.collect()
        self.assertEqual(status, 200)
        self.assertEqual(self.table.rows[key][0]["EventPublishStatus"], "future_test_value")

    def test_late_attempt_cannot_move_last_seen_backwards(self):
        result, _ = self.collect()
        key = c.CONTRACT, result["sha256"]
        current = copy.deepcopy(self.table.rows[key][0])
        current["LastSeenUtc"] = "2000-01-01T00:00:00+00:00"
        self.manifest.record_report(current)
        self.assertNotEqual(self.table.rows[key][0]["LastSeenUtc"], current["LastSeenUtc"])


class DownloadTests(unittest.TestCase):
    def test_429_then_success_and_retry_after(self):
        session = Session([Response(429,headers={"Retry-After":"2"}), Response()])
        delays=[]
        data, _ = c.fetch_report(c.LATEST_FILE,time.monotonic()+120,lambda:session,delays.append)
        self.assertEqual(data,b"ok")
        self.assertEqual(delays,[2])

    def test_long_retry_after_is_deferred_without_an_early_retry(self):
        session = Session([Response(429,headers={"Retry-After":"600"})])
        with self.assertRaises(c.DeferredSource):
            c.fetch_report(c.LATEST_FILE,time.monotonic()+120,lambda:session,lambda _:self.fail("slept"))
        self.assertEqual(session.calls,1)

    def test_404_not_retried_in_function(self):
        session = Session([Response(404)])
        with self.assertRaises(c.SourceError):
            c.fetch_report(c.LATEST_FILE,time.monotonic()+120,lambda:session,lambda _:None)
        self.assertEqual(session.calls,1)

    def test_network_timeout_retried_once(self):
        session = Session([requests.Timeout(),Response()])
        self.assertEqual(c.fetch_report(c.LATEST_FILE,time.monotonic()+120,lambda:session,lambda _:None)[0],b"ok")

    def test_oversize_and_budget(self):
        session = Session([Response(content=b"x"*(c.MAX_BYTES+1))])
        with self.assertRaises(c.SourceError):
            c.fetch_report(c.LATEST_FILE,time.monotonic()+120,lambda:session,lambda _:None)
        with self.assertRaises(TimeoutError):
            c.fetch_report(c.LATEST_FILE,time.monotonic()-1,lambda:session,lambda _:None)


class ArchiveAndHttpTests(unittest.TestCase):
    def test_existing_blob_checked_and_not_overwritten(self):
        blob, container = Mock(), Mock()
        container.get_blob_client.return_value = blob
        blob.upload_blob.side_effect = ResourceExistsError("exists")
        blob.get_blob_properties.return_value = SimpleNamespace(size=2,metadata={"sha256":c.sha256(b"ok")})
        self.assertEqual(c.BlobArchive(container).put("path",b"ok",{}),"already_archived")
        self.assertFalse(blob.upload_blob.call_args.kwargs["overwrite"])
        blob.get_blob_properties.return_value = SimpleNamespace(size=2,metadata={"sha256":"wrong"})
        with self.assertRaises(RuntimeError): c.BlobArchive(container).put("path",b"ok",{})

    def test_replay_integrity_failure(self):
        blob, container = Mock(), Mock()
        container.get_blob_client.return_value = blob
        blob.get_blob_properties.return_value = SimpleNamespace(size=2)
        blob.download_blob.return_value.readall.return_value = b"ok"
        with self.assertRaises(ValueError): c.BlobArchive(container).read("a"*64)

    def test_http_bad_request_rejected_before_azure(self):
        import azure.functions as func
        import function_app
        req=func.HttpRequest(method="POST",url="https://example/api/collect_ieso",body=b'{"source":"wrong"}')
        with patch.object(function_app,"ManagedIdentityCredential") as credential:
            response=function_app.collect_ieso(req)
        self.assertEqual(response.status_code,400)
        credential.assert_not_called()

    def test_http_rejection_result_stays_json_and_preserves_correlation(self):
        import azure.functions as func
        import function_app
        req=func.HttpRequest(method="POST",url="https://example/api/collect_ieso",body=b'{"source":"rt_lmp"}',
                             headers={"x-adf-run-id":"test-run"})
        with patch.dict("os.environ",{"IESO_STORAGE_ACCOUNT":"iesodev001","IESO_RAW_CONTAINER":"raw","IESO_MANIFEST_TABLE":"iesoingestionmanifest"}), \
             patch.object(function_app,"ManagedIdentityCredential"), \
             patch.object(function_app,"BlobServiceClient"), \
             patch.object(function_app,"TableClient"), \
             patch.object(function_app,"collect",return_value=({"status":"rejected","validation_status":"rejected"},200)) as call:
            response=function_app.collect_ieso(req)
        self.assertEqual(response.status_code,200)
        self.assertEqual(call.call_args.kwargs["adf_run_id"],"test-run")


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"tools"))
        import ieso_client
        self.client=ieso_client

    def test_inventory_is_one_hour_and_numeric_version_order(self):
        names=["PUB_RealtimeEnergyLMP_2026103112_v10.csv","PUB_RealtimeEnergyLMP_2026103112_v2.csv",
               "PUB_RealtimeEnergyLMP_2026103112.csv","PUB_RealtimeEnergyLMP_2026103113.csv"]
        response=Mock(text="".join(f'<a href="{name}">{name}</a>' for name in names))
        with patch.object(self.client.requests,"get",return_value=response):
            result=self.client.inventory("2026-10-31",12)
        self.assertEqual(result,[names[2],names[1],names[0]])

    def test_inventory_limit_never_silently_truncates(self):
        response=Mock(text='<a href="PUB_RealtimeEnergyLMP_2026103112.csv">x</a><a href="PUB_RealtimeEnergyLMP_2026103112_v1.csv">x</a>')
        with patch.object(self.client.requests,"get",return_value=response):
            with self.assertRaisesRegex(ValueError,"exceeding"):
                self.client.inventory("2026-10-31",12,1)

    def test_inventory_missing_hour_is_explicit(self):
        response=Mock(text="<html>No reports</html>")
        with patch.object(self.client.requests,"get",return_value=response):
            with self.assertRaisesRegex(ValueError,"No dated reports"):
                self.client.inventory("2026-10-31",12)


if __name__ == "__main__": unittest.main()
