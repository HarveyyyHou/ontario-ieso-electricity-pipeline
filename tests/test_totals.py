import copy
import csv
import io
import json
import logging
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from concurrent.futures import ThreadPoolExecutor
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "functions" / "ieso_ingest"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import collector as c
import totals as t
import ieso_client as client
from test_ingestion import Table


def csv_fixture(rows=None, date="2026/11/02", hour=12):
    rows = rows if rows is not None else [f"{hour},1,100,2,98,1,11,22,33,DSO-RD", f"{hour},2,101,2,99,1,11,22,33,DSO-RD"]
    return (f"PM {date.replace('/', '')} NORMAL DISPATCH TOTALS;\r\n\r\nRTEM_TOTALS;\r\n"
            f"\\\\CREATED AT 2026/11/02 11:03:00 FOR {date}\r\n" + ",".join(t.CSV_HEADER) + "\r\n" + "\r\n".join(rows) + "\r\n").encode()


def xml_fixture(demands=(90,91), order=(1,2), date="2026-11-02", hour=12):
    items=[]
    for interval in order:
        values=[("Total Energy",99+interval),("Total Loss",2),("Total Load",97+interval),
                ("Total Dispatch Load Scheduled OFF",1),("Total 10S",11),("Total 10N",22),("Total 30R",33),
                ("ONTARIO DEMAND",demands[interval-1])]
        quantities="".join(f"<MQ><MarketQuantity>{name}</MarketQuantity><EnergyMW>{value}</EnergyMW></MQ>" for name,value in values)
        items.append(f"<IntervalEnergy><Interval>{interval}</Interval>{quantities}<Flag>DSO-RD</Flag></IntervalEnergy>")
    return (f'<?xml version="1.0" encoding="UTF-8"?><Document xmlns="http://www.ieso.ca/schema" docID="RealtimeTotals">'
            '<DocHeader><CreatedAt>2026-11-02T11:03:02</CreatedAt></DocHeader>'
            f'<DocBody><DeliveryDate>{date}</DeliveryDate><DeliveryHour>{hour}</DeliveryHour><Energies>'
            + "".join(items) + '</Energies></DocBody></Document>').encode()


class PairTests(unittest.TestCase):
    def test_adds_exact_ontario_demand_and_delivery_date(self):
        output, details, _=t.combine(csv_fixture(),xml_fixture())
        rows=list(csv.DictReader(io.StringIO(output.decode())))
        self.assertEqual(rows[0]["ONTARIO DEMAND"],"90")
        self.assertEqual(rows[0]["TOTAL LOAD"],"98")
        self.assertEqual(rows[0]["DELIVERY DATE"],"2026-11-02")
        self.assertEqual(details["RowCount"],2)

    def test_xml_order_does_not_determine_join(self):
        output,_,_=t.combine(csv_fixture(),xml_fixture(order=(2,1)))
        rows=list(csv.DictReader(io.StringIO(output.decode())))
        self.assertEqual([r["ONTARIO DEMAND"] for r in rows],["90","91"])

    def test_missing_demand_is_rejected_not_substituted(self):
        data=xml_fixture().replace(b"ONTARIO DEMAND",b"OTHER DEMAND")
        with self.assertRaisesRegex(ValueError,"ONTARIO DEMAND"):
            t.combine(csv_fixture(),data)

    def test_duplicate_csv_key_rejected(self):
        row="12,1,100,2,98,1,11,22,33,DSO-RD"
        with self.assertRaisesRegex(ValueError,"Duplicate"):
            t.combine(csv_fixture([row,row]),xml_fixture())

    def test_duplicate_xml_key_rejected(self):
        with self.assertRaisesRegex(ValueError,"Duplicate"):
            t.combine(csv_fixture(),xml_fixture(order=(1,1)))

    def test_duplicate_xml_quantity_rejected(self):
        data=xml_fixture().replace(b"<Flag>",b"<MQ><MarketQuantity>ONTARIO DEMAND</MarketQuantity><EnergyMW>90</EnergyMW></MQ><Flag>",1)
        with self.assertRaisesRegex(ValueError,"duplicate"):
            t.combine(csv_fixture(),data)

    def test_missing_interval_never_silently_drops_a_row(self):
        with self.assertRaises(t.PairMismatch):
            t.combine(csv_fixture(),xml_fixture(order=(1,)))

    def test_different_day_never_merges(self):
        with self.assertRaises(t.PairMismatch):
            t.combine(csv_fixture(),xml_fixture(date="2026-11-03"))

    def test_shared_measurement_mismatch_never_merges(self):
        with self.assertRaisesRegex(t.PairMismatch,"measurement"):
            t.combine(csv_fixture(),xml_fixture().replace(b"<EnergyMW>100</EnergyMW>",b"<EnergyMW>999</EnergyMW>"))

    def test_flags_must_agree(self):
        with self.assertRaisesRegex(t.PairMismatch,"flags"):
            t.combine(csv_fixture(),xml_fixture().replace(b"DSO-RD",b"OTHER"))

    def test_nonfinite_value_rejected(self):
        with self.assertRaisesRegex(ValueError,"finite"):
            t.combine(csv_fixture().replace(b",100,",b",NaN,"),xml_fixture())

    def test_malformed_xml_and_dtd_rejected(self):
        for data in [b"<Document>",b'<!DOCTYPE Document [<!ENTITY x "1">]>'+xml_fixture()]:
            with self.assertRaises(ValueError): t.combine(csv_fixture(),data)

    def test_current_partial_hour_is_valid(self):
        output,details,_=t.combine(csv_fixture(["12,1,100,2,98,1,11,22,33,DSO-RD"]),xml_fixture(order=(1,)))
        self.assertEqual(details["RowCount"],1)

    def test_equivalent_decimal_notation_matches(self):
        output,_,_=t.combine(csv_fixture().replace(b",100,",b",100.000,"),xml_fixture())
        self.assertIn(b",100,2,98,",output)

    def test_filename_period_checked(self):
        with self.assertRaisesRegex(ValueError,"filename"):
            t.combine(csv_fixture(),xml_fixture(),"PUB_RealtimeTotals_2026110213.csv")

    def test_only_xml_demand_change_creates_new_csv(self):
        first=t.combine(csv_fixture(),xml_fixture())[0]
        changed=t.combine(csv_fixture(),xml_fixture(demands=(95,91)))[0]
        self.assertNotEqual(c.sha256(first),c.sha256(changed))

    def test_creation_timestamp_change_reuses_same_prepared_bytes(self):
        first=t.combine(csv_fixture(),xml_fixture())[0]
        changed=t.combine(csv_fixture(),xml_fixture().replace(b"11:03:02",b"11:04:02"))[0]
        self.assertEqual(first,changed)
        self.assertNotEqual(t.report_id(c.sha256(csv_fixture()),c.sha256(xml_fixture())),
                            t.report_id(c.sha256(csv_fixture()),c.sha256(xml_fixture().replace(b"11:03:02",b"11:04:02"))))

    def test_replay_requires_two_hashes_and_rejects_urls(self):
        for body in [{"source":"rt_totals","csv_sha256":"a"*64},
                     {"source":"rt_totals","report_file":"https://other/file.csv"},
                     {"source":"rt_totals","report_file":"PUB_RealtimeTotals.xml"}]:
            with self.assertRaises(ValueError): t.request_options(body)


class Archive:
    def __init__(self):
        from threading import Lock
        self.data,self.lock={},Lock()
        self.fail_output=False

    def put(self,path,content,metadata,content_type=None):
        if self.fail_output and "/prepared/" in path:
            raise RuntimeError("storage interrupted")
        with self.lock:
            if path in self.data: return "already_archived"
            self.data[path]=content
            return "archived"

    def read_path(self,path,digest):
        content=self.data[path]
        if c.sha256(content)!=digest: raise ValueError("hash mismatch")
        return content


class TotalsRecoveryTests(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)
        self.archive,self.table=Archive(),Table()
        self.records=c.IngestionRecords(self.table,source="rt_totals")

    def tearDown(self): logging.disable(logging.NOTSET)

    def collect(self,xml=None,body=None):
        return t.collect(body or {"source":"rt_totals"},self.archive,self.records,
                         downloader=lambda name,_:(xml if xml is not None else xml_fixture(),{}) if name.endswith(".xml") else (csv_fixture(),{}))

    def replay_body(self,result):
        return {"source":"rt_totals","csv_sha256":result["csv_sha256"],"xml_sha256":result["xml_sha256"]}

    def test_repeated_pair_has_three_objects_and_one_report_row(self):
        first,code=self.collect()
        second,code2=self.collect()
        self.assertEqual((code,code2),(200,200))
        self.assertEqual(second["status"],"already_prepared")
        self.assertEqual(len(self.archive.data),3)
        self.assertEqual(sum(key[0]==t.CONTRACT for key in self.table.rows),1)
        self.assertEqual(first["records_partition"],"rt_totals_v1")
        row=self.table.rows[t.CONTRACT,first["report_id"]][0]
        self.assertEqual(row["PreparedCsvSha256"],first["prepared_csv_sha256"])

    def test_xml_correction_changes_pair_id_when_csv_unchanged(self):
        first,_=self.collect()
        revised,_=self.collect(xml_fixture(demands=(95,91)))
        self.assertEqual(first["csv_sha256"],revised["csv_sha256"])
        self.assertNotEqual(first["report_id"],revised["report_id"])
        self.assertEqual(len(self.archive.data),5)

    def test_replay_uses_originals_without_http(self):
        result,_=self.collect()
        recovered,code=t.collect(self.replay_body(result),self.archive,self.records,
                                  downloader=lambda *_:self.fail("Unexpected HTTP"))
        self.assertEqual(code,200)
        self.assertEqual(recovered["status"],"already_prepared")

    def test_prepared_upload_failure_is_recoverable(self):
        self.archive.fail_output=True
        result,code=self.collect()
        self.assertEqual(code,500)
        self.assertEqual(len(self.archive.data),2)
        self.archive.fail_output=False
        recovered,code=self.collect(body=self.replay_body(result))
        self.assertEqual((code,recovered["status"]),(200,"prepared"))

    def test_table_failure_after_prepared_upload_is_recoverable(self):
        with patch.object(self.records,"record_report",side_effect=RuntimeError("table unavailable")):
            result,code=self.collect()
        self.assertEqual(code,500)
        self.assertEqual(len(self.archive.data),3)
        recovered,code=self.collect(body=self.replay_body(result))
        self.assertEqual((code,recovered["status"],recovered["records_status"]),(200,"already_prepared","created"))

    def test_mismatched_pair_has_no_prepared_csv(self):
        result,code=self.collect(xml_fixture(order=(1,)))
        self.assertEqual((code,result["validation_status"]),(200,"deferred"))
        self.assertEqual(len(self.archive.data),2)
        self.assertEqual(result["prepared_csv_blob_name"],"")
        self.assertEqual(self.table.rows[t.CONTRACT,result["report_id"]][0]["PreparedState"],"blocked")

    def test_missing_demand_has_rejected_record_and_no_prepared_csv(self):
        result,code=self.collect(xml_fixture().replace(b"ONTARIO DEMAND",b"OTHER"))
        self.assertEqual((code,result["status"]),(200,"rejected"))
        self.assertEqual(len(self.archive.data),2)

    def test_missing_xml_retains_csv_and_records_upstream_status(self):
        def download(name,_):
            if name.endswith(".xml"): raise c.SourceError("not found",404)
            return csv_fixture(),{}
        result,code=t.collect({"source":"rt_totals"},self.archive,self.records,downloader=download)
        self.assertEqual((code,result["source_http_status"]),(502,404))
        self.assertEqual(len(self.archive.data),1)
        row=self.table.rows[result["attempt_partition"],result["attempt_row"]][0]
        self.assertEqual((row["Outcome"],row["Stage"],row["SourceHttpStatus"]),("failed","source_xml",404))

    def test_long_source_wait_is_deferred_without_prepared_output(self):
        def download(name,_):
            if name.endswith(".xml"): raise c.DeferredSource(429,600)
            return csv_fixture(),{}
        result,code=t.collect({"source":"rt_totals"},self.archive,self.records,downloader=download)
        self.assertEqual((code,result["status"],result["retry_after_seconds"]),(200,"deferred",600))
        self.assertEqual(result["validation_status"],"not_checked")
        self.assertEqual(len(self.archive.data),1)

    def test_overlapping_requests_reuse_originals_and_prepared_csv(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:self.collect(),range(2)))
        self.assertTrue(all(code==200 for _,code in results))
        self.assertEqual(len(self.archive.data),3)
        self.assertEqual(sum(key[0]==t.CONTRACT for key in self.table.rows),1)


class ClientAndRouteTests(unittest.TestCase):
    def test_totals_inventory_requires_both_formats(self):
        response=Mock(text='<a href="PUB_RealtimeTotals_2026110212.csv">CSV</a>')
        with patch.object(client.requests,"get",return_value=response):
            with self.assertRaisesRegex(ValueError,"XML counterparts"):
                client.inventory("2026-11-02",12,source="rt_totals")

    def test_totals_inventory_returns_paired_csv_filenames(self):
        response=Mock(text='<a href="PUB_RealtimeTotals_2026110212.csv">CSV</a><a href="PUB_RealtimeTotals_2026110212.xml">XML</a>')
        with patch.object(client.requests,"get",return_value=response):
            self.assertEqual(client.inventory("2026-11-02",12,source="rt_totals"),["PUB_RealtimeTotals_2026110212.csv"])

    def test_offline_verifier_checks_exact_prepared_bytes(self):
        with TemporaryDirectory() as folder:
            root=Path(folder)
            (root/'input.csv').write_bytes(csv_fixture())
            (root/'input.xml').write_bytes(xml_fixture())
            (root/'output.csv').write_bytes(t.combine(csv_fixture(),xml_fixture())[0])
            args=['verify-files','--csv',str(root/'input.csv'),'--xml',str(root/'input.xml'),
                  '--prepared',str(root/'output.csv'),'--output',str(root/'result.json')]
            with patch.object(client,'getpass',side_effect=AssertionError('Unexpected key prompt')),patch('builtins.print'):
                self.assertEqual(client.main(args),0)
                (root/'output.csv').write_bytes(b"incorrect")
                self.assertEqual(client.main(args),1)

    def test_http_totals_dispatch_uses_existing_setting_and_totals_collector(self):
        import azure.functions as func
        import function_app
        req=func.HttpRequest(method="POST",url="https://example/api/collect_ieso",body=b'{"source":"rt_totals"}')
        with patch.dict('os.environ',{'IESO_STORAGE_ACCOUNT':'iesodev001','IESO_RAW_CONTAINER':'raw','IESO_TABLE':'iesoingestion'}), \
             patch.object(function_app,'ManagedIdentityCredential'),patch.object(function_app,'BlobServiceClient'), \
             patch.object(function_app,'TableClient') as table, \
             patch.object(function_app.totals,'collect',return_value=({'status':'prepared','validation_status':'accepted'},200)) as collect:
            response=function_app.collect_ieso(req)
        self.assertEqual(response.status_code,200)
        collect.assert_called_once()
        self.assertEqual(table.call_args.kwargs['table_name'],'iesoingestion')


if __name__ == "__main__": unittest.main()
