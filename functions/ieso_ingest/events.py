"""Versioned IESO row events. Decimal measurements remain exact JSON strings."""
import hashlib
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

SCHEMA = "ieso_observation_v1"
EST = timezone(timedelta(hours=-5))
LMP_FIELDS = {"lmp": "LMP", "energy_loss_price": "Energy Loss Price",
              "energy_congestion_price": "Energy Congestion Price"}
TOTAL_FIELDS = {"total_energy": "TOTAL ENERGY", "total_loss": "TOTAL LOSS",
                "total_load": "TOTAL LOAD", "total_dispatch_load_scheduled_off": "TOTAL DISP LOAD SCHED OFF",
                "total_10s": "TOTAL 10S", "total_10n": "TOTAL 10N", "total_30r": "TOTAL 30R",
                "ontario_demand": "ONTARIO DEMAND"}
CORE_FIELDS = ("schema_version", "source", "delivery_date", "delivery_hour", "interval",
               "pricing_location", "interval_start_utc", "interval_end_utc", "units", "measurements", "flag")


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def decimal_text(value):
    if not isinstance(value, str) or len(value) > 128:
        raise ValueError("Measurement must be a decimal string of at most 128 characters")
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("Invalid decimal measurement") from exc
    if not number.is_finite() or abs(number.adjusted()) > 100 or abs(number.as_tuple().exponent) > 100:
        raise ValueError("Measurement is nonfinite or has an unreasonable decimal exponent")
    if number == 0:
        return "0"
    text = format(number, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def utc_text(value):
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def source_utc(value):
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=EST)
    return utc_text(parsed)


def interval_times(day, hour, interval):
    if type(hour) is not int or not 1 <= hour <= 24 or type(interval) is not int or not 1 <= interval <= 12:
        raise ValueError("Invalid delivery hour or five-minute interval")
    base = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=EST)
    start = base + timedelta(hours=hour - 1, minutes=5 * (interval - 1))
    return utc_text(start), utc_text(start + timedelta(minutes=5))


def build(source, day, hour, interval, location, measurements, flag, report, publication):
    if source not in ("rt_lmp", "rt_totals"):
        raise ValueError("Unknown event source")
    expected = LMP_FIELDS if source == "rt_lmp" else TOTAL_FIELDS
    if set(measurements) != set(expected):
        raise ValueError("Unexpected measurement fields")
    if not isinstance(location, str) or not location or location != location.strip():
        raise ValueError("Missing or padded pricing location")
    if source == "rt_lmp" and flag is not None:
        raise ValueError("LMP events do not have a report flag")
    if source == "rt_totals" and (location != "ONTARIO" or not isinstance(flag, str) or not flag.strip()):
        raise ValueError("Totals events need Ontario and a report flag")
    start, end = interval_times(day, hour, interval)
    core = {"schema_version": SCHEMA, "source": source, "delivery_date": day,
            "delivery_hour": hour, "interval": interval, "pricing_location": location,
            "interval_start_utc": start, "interval_end_utc": end,
            "units": "CAD/MWh" if source == "rt_lmp" else "MW",
            "measurements": {k: decimal_text(v) for k, v in measurements.items()}, "flag": flag}
    event_id = digest(core)
    return {**core, "event_id": event_id,
            "report_event_id": digest({"schema_version": SCHEMA, "report_id": report["report_id"], "event_id": event_id}),
            "report": report, "publication": publication}


def validate_event(event):
    """Consumer check: reject bad IDs, changed units/times, and malformed measurements."""
    if not isinstance(event, dict) or event.get("schema_version") != SCHEMA:
        raise ValueError("Unknown event schema")
    report, publication = event.get("report"), event.get("publication")
    if not isinstance(report, dict) or not isinstance(publication, dict):
        raise ValueError("Missing report or publication details")
    for field in ("report_id", "csv_sha256"):
        value = report.get(field)
        if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            raise ValueError("Invalid report hash")
    if report.get("source_time_convention") != "EST_UTC_MINUS_05" or not report.get("csv_blob_path"):
        raise ValueError("Missing report time convention or CSV path")
    parsed_version = datetime.fromisoformat(report["source_version_utc"].replace("Z", "+00:00"))
    if parsed_version.utcoffset() != timedelta(0):
        raise ValueError("Source version timestamp must be UTC")
    rebuilt = build(event["source"], event["delivery_date"], event["delivery_hour"], event["interval"],
                    event["pricing_location"], event["measurements"], event["flag"], report, publication)
    if any(event.get(k) != rebuilt[k] for k in (*CORE_FIELDS, "event_id", "report_event_id")):
        raise ValueError("Event fields, decimal formatting, or IDs do not match the schema")
    if event["source"] == "rt_totals":
        for field in ("xml_sha256", "prepared_csv_sha256"):
            value = report.get(field, "")
            if len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
                raise ValueError("Missing Totals source/prepared hash")
    return True
