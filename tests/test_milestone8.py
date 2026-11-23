"""Local acceptance checks for reporting safety, alignment, contracts and project links."""
import copy
import importlib.util
import json
import statistics
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

SOURCE=Path(__file__).resolve().parents[1]/"powerbi"/"milestone8"
sys.path.insert(0,str(SOURCE))
import m8
import build_project


class ReportingRules(unittest.TestCase):
    def test_window_bounds_and_iso_dates(self):
        self.assertEqual((date(2026,11,9),date(2026,11,22)),m8.window("2026-11-09","2026-11-22"))
        for first,last in [("2026-11-09","2026-11-23"),("2026-11-10","2026-11-09"),("20261109","2026-11-10")]:
            with self.assertRaises(ValueError):m8.window(first,last)

    def test_calendar_monday_and_year_boundary(self):
        rows=list(m8.calendar_rows(date(2026,12,28),date(2027,1,3)))
        self.assertEqual(7,len(rows))
        self.assertTrue(all(r[1]=="2026-12-28" and r[2]=="2027-01-03" for r in rows))
        self.assertEqual(list(range(1,8)),[r[3] for r in rows])

    def test_market_clock_stays_fixed_utc_minus_five(self):
        clock=m8.market_clock(datetime(2026,7,1,4,30,tzinfo=timezone.utc))
        self.assertEqual(date(2026,6,30),clock.date())
        self.assertEqual(23,clock.hour)

    def test_location_literal_escapes_quotes(self):
        q=m8.queries("2026-11-09","2026-11-22","A'; DELETE FROM x; --",m8.DEFAULT_BATCH)["PriceHourly"]
        self.assertIn("pricing_location='A''; DELETE FROM x; --'",q)
        with self.assertRaises(ValueError):m8.queries("2026-11-09","2026-11-22","A","../../x")

    def test_queries_do_not_change_resources(self):
        for name,sql in m8.queries("2026-11-09","2026-11-22","A",m8.DEFAULT_BATCH).items():
            self.assertTrue(sql.lstrip().upper().startswith(("SELECT","WITH")),name)
            for verb in ("CREATE TABLE","INSERT INTO","MERGE INTO","DELETE FROM","DROP TABLE","ALTER TABLE"):
                self.assertNotIn(verb,sql.upper())

    def test_explicit_compute_opt_in_precedes_any_api_call(self):
        with patch.object(m8,"cli_api") as api:
            with self.assertRaises(ValueError):m8.main(["coverage","--warehouse-id","abcdef0123456789"])
            api.assert_not_called()

    def test_warehouse_cost_guard(self):
        good={"cluster_size":"2X-Small","max_num_clusters":1,"auto_stop_mins":5,"enable_serverless_compute":True}
        m8.warehouse_guard(good)
        for key,bad in [("cluster_size","X-Large"),("max_num_clusters",2),("auto_stop_mins",0),("auto_stop_mins",10),("enable_serverless_compute",False)]:
            settings={**good,key:bad}
            with self.assertRaises(ValueError):m8.warehouse_guard(settings)

    def test_truncated_results_cannot_be_used(self):
        with self.assertRaises(RuntimeError):m8.result_rows({"manifest":{"truncated":True}},"P")

    def test_all_result_chunks_are_read(self):
        response={"manifest":{"schema":{"columns":[{"name":"n"}]},"total_row_count":2},
          "result":{"data_array":[["1"]],"next_chunk_internal_link":"/api/2.0/sql/statements/abc/result/chunks/1"}}
        with patch.object(m8,"cli_api",return_value={"data_array":[["2"]]}):
            self.assertEqual((["n"],[["1"],["2"]]),m8.result_rows(response,"P"))

    def test_result_chunk_count_must_reconcile(self):
        response={"manifest":{"schema":{"columns":[{"name":"n"}]},"total_row_count":2},"result":{"data_array":[[1]]}}
        with self.assertRaises(RuntimeError):m8.result_rows(response,"P")

    def test_hourly_gates_and_duplicate_keys(self):
        data={"PriceHourly":[{"delivery_date":"2026-11-12","delivery_hour":"1","pricing_location":"A","rt_intervals":"11","day_ahead_cad_mwh":"0","spread_cad_mwh":"-5"}],
              "DemandGeneration":[{"delivery_date":"2026-11-12","delivery_hour":"1","demand_intervals":"11","demand_mw":"100"}]}
        self.assertEqual(2,len(m8.validate_export(data)))
        data["PriceHourly"][0]["rt_intervals"]="12"
        data["DemandGeneration"][0]["demand_intervals"]="12"
        self.assertEqual([],m8.validate_export(data))
        data["PriceHourly"].append(copy.deepcopy(data["PriceHourly"][0]))
        self.assertIn("Duplicate",m8.validate_export(data)[0])

    def test_missing_is_not_zero(self):
        rows=[self.row("2026-11-09",1,1,"0"),self.row("2026-11-09",1,2,None)]
        ref=m8.weekly_reference(rows,date(2026,11,9),datetime(2026,12,1,tzinfo=timezone.utc))
        self.assertEqual(1,ref["observed_price_intervals"])
        self.assertEqual(Decimal(0),ref["week_price_cad_mwh"])
        self.assertIsNone(ref["week_population_sd"])

    def row(self,day,hour,interval,price,demand="100"):
        return {"delivery_date":day,"delivery_hour":hour,"interval":interval,"price_cad_mwh":price,"demand_mw":demand}

    def test_partial_week_pairs_same_elapsed_hours_only(self):
        rows=[self.row("2026-11-09",1,1,"5"),self.row("2026-11-09",2,1,"100"),
              self.row("2026-11-16",1,1,"-5"),self.row("2026-11-16",2,1,"1000")]
        # Monday 01:30 in the IESO fixed UTC-05 clock: only HE1 is complete.
        ref=m8.weekly_reference(rows,date(2026,11,16),datetime(2026,11,16,6,30,tzinfo=timezone.utc))
        self.assertEqual(1,ref["elapsed_complete_hours"])
        self.assertEqual(1,ref["paired_price_intervals"])
        self.assertEqual(Decimal(-10),ref["paired_price_change"])

    def test_gaps_do_not_shrink_expected_period(self):
        rows=[self.row("2026-11-16",1,1,"10")]
        ref=m8.weekly_reference(rows,date(2026,11,16),datetime(2026,11,18,17,tzinfo=timezone.utc))
        self.assertEqual(60,ref["elapsed_complete_hours"])
        self.assertEqual(720,ref["expected_intervals"])
        self.assertEqual(1,ref["observed_price_intervals"])
        self.assertIsNone(ref["paired_price_change"])

    def test_paired_averages_use_only_intersection(self):
        rows=[self.row("2026-11-09",1,1,"1"),self.row("2026-11-09",1,2,"999"),
              self.row("2026-11-16",1,1,"3"),self.row("2026-11-16",1,3,"100")]
        ref=m8.weekly_reference(rows,date(2026,11,16),datetime(2026,12,1,tzinfo=timezone.utc))
        self.assertEqual(1,ref["paired_price_intervals"])
        self.assertEqual(Decimal(2),ref["paired_price_change"])

    def test_weekly_sd_is_calculated_from_observations(self):
        rows=[self.row("2026-11-09",1,1,"0"),self.row("2026-11-09",1,2,"0"),
              self.row("2026-11-10",1,1,"10"),self.row("2026-11-10",1,2,"10")]
        ref=m8.weekly_reference(rows,date(2026,11,9),datetime(2026,12,1,tzinfo=timezone.utc))
        self.assertEqual(Decimal(5),ref["week_population_sd"])
        self.assertEqual(Decimal(5),ref["week_price_cad_mwh"])

    def test_unequal_daily_counts_are_weighted_by_observation(self):
        rows=[self.row("2026-11-09",1,1,"0"),self.row("2026-11-10",1,1,"12"),
              self.row("2026-11-10",1,2,"12"),self.row("2026-11-10",1,3,"12")]
        ref=m8.weekly_reference(rows,date(2026,11,9),datetime(2026,12,1,tzinfo=timezone.utc))
        self.assertEqual(Decimal(9),ref["week_price_cad_mwh"])

    def test_init_does_not_overwrite_existing_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            m8.snapshot_init(root,"2026-11-09","2026-11-22")
            self.assertEqual([],m8.read_csv(root/"MarketIntervals.csv"))
            self.assertEqual(14,len(m8.read_csv(root/"Calendar.csv")))
            with self.assertRaises(ValueError):m8.snapshot_init(root,"2026-11-09","2026-11-22")

    def test_incomplete_control_export_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/"control.json"
            p.write_text(json.dumps({"nextMarker":{"next_partition_key":"x"},"items":[]}))
            with self.assertRaises(ValueError):m8.attach_control(Path(tmp),p)

    def test_model_has_import_partitions_and_no_credentials(self):
        model=build_project.build_model()
        for table in model["model"]["tables"]:
            for part in table["partitions"]:self.assertEqual("import",part["mode"])
        text=json.dumps(model).lower()
        self.assertNotIn("client_secret",text)
        self.assertNotIn("personal_access_token",text)
        params={e["name"]:e["expression"] for e in model["model"]["expressions"]}
        self.assertTrue(params["pSourceMode"].startswith('"CSV"'))

    def test_m_text_quoting_is_not_json_quoting(self):
        self.assertEqual('"a""b#(lf)C:\\folder"',build_project.quoted('a"b\nC:\\folder'))

    def test_configure_preserves_custom_measures(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            original=build_project.build_model()
            original['model']['tables'][-1]['measures'].append({'name':'User Measure','expression':'42'})
            build_project.dump(root/'model.bim',original)
            with patch.object(build_project,'MODEL',root):
                build_project.configure('CSV',r'C:\data','2026-11-02','2026-11-15','A',m8.DEFAULT_BATCH,'')
            changed=json.loads((root/'model.bim').read_text())
            self.assertIn('User Measure',[v['name'] for v in changed['model']['tables'][-1]['measures']])
            params={e['name']:e['expression'] for e in changed['model']['expressions']}
            self.assertTrue(params['pSnapshotFolder'].startswith('"C:\\data"'))

    def test_six_pages_and_all_visual_fields_exist(self):
        model = json.loads(
            (build_project.MODEL / "model.bim").read_text(encoding="utf-8-sig")
        )["model"]
        columns={t["name"]:{c["name"] for c in t["columns"]} for t in model["tables"]}
        measures={t["name"]:{m["name"] for m in t.get("measures",[])} for t in model["tables"]}
        pages=json.loads((build_project.REPORT/"definition/pages/pages.json").read_text())
        self.assertEqual(6,len(pages["pageOrder"]))
        for path in build_project.REPORT.rglob("visual.json"):
            obj=json.loads(path.read_text())
            for role in obj["visual"]["query"]["queryState"].values():
                for projection in role["projections"]:
                    f=projection["field"]
                    kind=next(iter(f));field=f[kind]
                    table=field["Expression"]["SourceRef"]["Entity"]
                    self.assertIn(field["Property"],measures[table] if kind=="Measure" else columns[table],str(path))


if __name__=="__main__":unittest.main()
