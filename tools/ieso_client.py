"""Manual collection and bounded reconciliation. Never prints the Function key."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from getpass import getpass
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import sys
import time
from urllib.parse import unquote, urlparse

import requests

DIRECTORY = "https://reports-public.ieso.ca/public/RealtimeEnergyLMP/"
SOURCES = {"rt_lmp": (DIRECTORY, "PUB_RealtimeEnergyLMP"),
           "rt_totals": ("https://reports-public.ieso.ca/public/RealtimeTotals/", "PUB_RealtimeTotals")}
FUNCTION_URL = "https://func-ieso-ingest-dev-024ed7af.azurewebsites.net/api/collect_ieso"


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.names = set()

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            for key, value in attrs:
                if key == "href" and value:
                    self.names.add(unquote(urlparse(value).path.rsplit("/", 1)[-1]))


def inventory(date, hour, maximum=20, source="rt_lmp"):
    parsed = datetime.strptime(date, "%Y-%m-%d")
    if not 1 <= hour <= 24:
        raise ValueError("Delivery hour must be 1 through 24")
    directory, basename = SOURCES[source]
    prefix = f"{basename}_{parsed:%Y%m%d}{hour:02d}"
    pattern = re.compile(re.escape(prefix) + r"(?:_v(\d+))?\.csv\Z")
    for attempt in range(2):
        try:
            response = requests.get(directory, timeout=(10, 30))
            response.raise_for_status()
            break
        except requests.RequestException:
            if attempt:
                raise
            time.sleep(1)
    parser = Links()
    parser.feed(response.text)
    names = [name for name in parser.names if pattern.fullmatch(name)]
    if source == "rt_totals":
        missing = [name[:-4] + ".xml" for name in names if name[:-4] + ".xml" not in parser.names]
        if missing:
            raise ValueError("CSV files have missing XML counterparts: " + ", ".join(sorted(missing)))
    names.sort(key=lambda name: (pattern.fullmatch(name)[1] is not None,
                                 int(pattern.fullmatch(name)[1] or 0)))
    if not names:
        raise ValueError("No dated reports listed for that hour; check date/hour and source availability")
    if len(names) > maximum:
        raise ValueError(f"Found {len(names)} files, exceeding --max-files {maximum}; review the inventory limit")
    return names


def invoke(body, key):
    response = requests.post(FUNCTION_URL, json=body, headers={"x-functions-key": key}, timeout=(5, 170))
    try:
        payload = response.json()
    except ValueError:
        payload = {"status": "failed", "error": "Non-JSON response; inspect Function logs"}
    record = {"request": body, "http_status": response.status_code, "response": payload}
    print(json.dumps(record, indent=2), flush=True)
    return record


def accepted(record):
    return record["http_status"] == 200 and record["response"].get("validation_status") == "accepted"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    one = sub.add_parser("collect", help="Collect latest, a dated filename, or replay an archive hash")
    mode = one.add_mutually_exclusive_group()
    mode.add_argument("--report-file")
    mode.add_argument("--archive-sha256")
    one.add_argument("--source", choices=SOURCES, default="rt_lmp")
    one.add_argument("--csv-sha256")
    one.add_argument("--xml-sha256")
    one.add_argument("--repeat", type=int, choices=range(1, 5), default=1)
    one.add_argument("--parallel", action="store_true")
    one.add_argument("--output", required=True)
    rec = sub.add_parser("reconcile", help="Inventory one source hour; --apply collects each available version")
    rec.add_argument("--source", choices=SOURCES, default="rt_lmp")
    rec.add_argument("--date", required=True)
    rec.add_argument("--hour", type=int, required=True)
    rec.add_argument("--max-files", type=int, choices=range(1, 51), default=20)
    rec.add_argument("--apply", action="store_true")
    rec.add_argument("--output", required=True)
    verify = sub.add_parser("verify-files", help="Check downloaded original CSV/XML against prepared CSV")
    verify.add_argument("--csv", required=True)
    verify.add_argument("--xml", required=True)
    verify.add_argument("--prepared", required=True)
    verify.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    results = []
    summary = {"command": args.command, "function_url": FUNCTION_URL, "results": results}
    try:
        if args.command == "verify-files":
            sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "functions" / "ieso_ingest"))
            from totals import combine
            from collector import sha256
            expected, details, preview = combine(Path(args.csv).read_bytes(), Path(args.xml).read_bytes())
            actual = Path(args.prepared).read_bytes()
            summary.update(check_passed=actual == expected, row_count=details["RowCount"],
                           expected_sha256=sha256(expected), actual_sha256=sha256(actual), preview=preview)
            print(json.dumps(summary, indent=2), flush=True)
            return 0 if summary["check_passed"] else 1
        summary["source"] = args.source
        if args.command == "reconcile":
            names = inventory(args.date, args.hour, args.max_files, source=args.source)
            summary.update(delivery_date=args.date, delivery_hour=args.hour, available_files=names, applied=args.apply)
            print(json.dumps({k: v for k, v in summary.items() if k != "results"}, indent=2), flush=True)
            if args.apply:
                key = getpass("Paste the Function key named adf (input hidden), then press Enter: ")
                if not key:
                    raise ValueError("Function key was empty")
                for name in names:
                    record = invoke({"source": args.source, "report_file": name}, key)
                    results.append(record)
                    if not accepted(record):
                        break
        else:
            body = {"source": args.source}
            if args.report_file:
                body["report_file"] = args.report_file
            if args.archive_sha256:
                body["archive_sha256"] = args.archive_sha256
            if args.csv_sha256 or args.xml_sha256:
                if args.source != "rt_totals" or not args.csv_sha256 or not args.xml_sha256 or args.report_file or args.archive_sha256:
                    raise ValueError("Totals replay needs --source rt_totals and both hashes, without report-file/archive-sha256")
                body.update(csv_sha256=args.csv_sha256, xml_sha256=args.xml_sha256)
            if args.source == "rt_totals" and args.archive_sha256:
                raise ValueError("Totals replay uses --csv-sha256 and --xml-sha256")
            key = getpass("Paste the Function key named adf (input hidden), then press Enter: ")
            if not key:
                raise ValueError("Function key was empty")
            if args.parallel:
                with ThreadPoolExecutor(max_workers=args.repeat) as pool:
                    results.extend(pool.map(lambda _: invoke(body, key), range(args.repeat)))
            else:
                for _ in range(args.repeat):
                    record = invoke(body, key)
                    results.append(record)
                    if not accepted(record):
                        break
        expected = len(summary.get("available_files", [])) if args.command == "reconcile" and args.apply else args.repeat if args.command == "collect" else 0
        good = len(results) == expected and all(accepted(r) for r in results)
        summary["check_passed"] = good
        if args.command == "collect" and args.repeat > 1 and results:
            fingerprints = {r["response"].get("report_id") or r["response"].get("sha256") for r in results}
            summary["same_report_version"] = len(fingerprints) == 1
            if args.report_file or args.archive_sha256 or args.csv_sha256:
                summary["check_passed"] = good and len(fingerprints) == 1
        return 0 if summary["check_passed"] else 1
    except (requests.RequestException, ValueError, OSError) as exc:
        summary.update(check_passed=False, error=str(exc))
        print(f"STOP: {exc}", file=sys.stderr)
        return 1
    finally:
        destination = Path(args.output)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        print(f"Evidence saved to {destination}; check_passed={summary.get('check_passed', False)}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
