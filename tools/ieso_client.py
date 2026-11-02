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


def inventory(date, hour, maximum=20):
    parsed = datetime.strptime(date, "%Y-%m-%d")
    if not 1 <= hour <= 24:
        raise ValueError("Delivery hour must be 1 through 24")
    prefix = f"PUB_RealtimeEnergyLMP_{parsed:%Y%m%d}{hour:02d}"
    pattern = re.compile(re.escape(prefix) + r"(?:_v(\d+))?\.csv\Z")
    for attempt in range(2):
        try:
            response = requests.get(DIRECTORY, timeout=(10, 30))
            response.raise_for_status()
            break
        except requests.RequestException:
            if attempt:
                raise
            time.sleep(1)
    parser = Links()
    parser.feed(response.text)
    names = [name for name in parser.names if pattern.fullmatch(name)]
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
    one.add_argument("--repeat", type=int, choices=range(1, 5), default=1)
    one.add_argument("--parallel", action="store_true")
    one.add_argument("--output", required=True)
    rec = sub.add_parser("reconcile", help="Inventory one source hour; --apply collects each available version")
    rec.add_argument("--date", required=True)
    rec.add_argument("--hour", type=int, required=True)
    rec.add_argument("--max-files", type=int, choices=range(1, 51), default=20)
    rec.add_argument("--apply", action="store_true")
    rec.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    results = []
    summary = {"command": args.command, "function_url": FUNCTION_URL, "results": results}
    try:
        if args.command == "reconcile":
            names = inventory(args.date, args.hour, args.max_files)
            summary.update(delivery_date=args.date, delivery_hour=args.hour, available_files=names, applied=args.apply)
            print(json.dumps({k: v for k, v in summary.items() if k != "results"}, indent=2), flush=True)
            if args.apply:
                key = getpass("Paste the Function key named adf (input hidden), then press Enter: ")
                if not key:
                    raise ValueError("Function key was empty")
                for name in names:
                    record = invoke({"source": "rt_lmp", "report_file": name}, key)
                    results.append(record)
                    if not accepted(record):
                        break
        else:
            body = {"source": "rt_lmp"}
            if args.report_file:
                body["report_file"] = args.report_file
            if args.archive_sha256:
                body["archive_sha256"] = args.archive_sha256
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
            fingerprints = {r["response"].get("sha256") for r in results}
            summary["same_report_version"] = len(fingerprints) == 1
            if args.report_file or args.archive_sha256:
                summary["check_passed"] = good and len(fingerprints) == 1
        return 0 if summary["check_passed"] else 1
    except (requests.RequestException, ValueError) as exc:
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
