import csv
import hashlib
import io
import json
import logging
import os
from datetime import datetime, timezone

import azure.functions as func
import requests
from azure.core.exceptions import ResourceExistsError
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient, ContentSettings
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

SOURCE_URL = (
    "https://reports-public.ieso.ca/public/RealtimeEnergyLMP/"
    "PUB_RealtimeEnergyLMP.csv"
)

EXPECTED_HEADER = [
    "Delivery Hour",
    "Interval",
    "Pricing Location",
    "LMP",
    "Energy Loss Price",
    "Energy Congestion Price",
]


def validate_report(content):
    rows = csv.reader(io.StringIO(content.decode("utf-8-sig")))
    preamble = next(rows, [])
    header = next(rows, [])
    first_row = next(rows, [])

    if (
        not preamble
        or not preamble[0].startswith("CREATED AT ")
        or header != EXPECTED_HEADER
        or len(first_row) != len(EXPECTED_HEADER)
    ):
        raise ValueError("Unexpected or empty IESO real-time LMP report")


def archive_report(content, headers, retrieved_at):
    account = os.environ["IESO_STORAGE_ACCOUNT"]
    container = os.environ["IESO_RAW_CONTAINER"]

    digest = hashlib.sha256(content).hexdigest()
    blob_name = (
        f"ieso/rt_lmp/sha256={digest}/PUB_RealtimeEnergyLMP.csv"
    )

    metadata = {
        "source": "rt_lmp",
        "source_url": SOURCE_URL,
        "sha256": digest,
        "retrieved_at_utc": retrieved_at,
    }

    for header, key in [
        ("ETag", "source_etag"),
        ("Last-Modified", "source_last_modified"),
    ]:
        if headers.get(header):
            metadata[key] = headers[header]

    with ManagedIdentityCredential() as credential:
        with BlobServiceClient(
            account_url=f"https://{account}.blob.core.windows.net",
            credential=credential,
            retry_total=2,
            connection_timeout=5,
            read_timeout=20,
        ) as storage:
            blob = storage.get_blob_client(
                container=container,
                blob=blob_name,
            )

            try:
                blob.upload_blob(
                    content,
                    overwrite=False,
                    metadata=metadata,
                    content_settings=ContentSettings(
                        content_type="text/csv"
                    ),
                )
                status = "archived"
            except ResourceExistsError:
                status = "already_archived"

    return {
        "status": status,
        "source": "rt_lmp",
        "container": container,
        "blob_name": blob_name,
        "sha256": digest,
        "bytes": len(content),
        "retrieved_at_utc": retrieved_at,
    }


@app.function_name(name="collect_ieso")
@app.route(route="collect_ieso", methods=["POST"])
def collect_ieso(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
    except ValueError:
        body = None

    if not isinstance(body, dict) or body.get("source") != "rt_lmp":
        return func.HttpResponse(
            json.dumps({
                "error": "Send a JSON object with source set to rt_lmp"
            }),
            status_code=400,
            mimetype="application/json",
        )

    try:
        retry = Retry(
            total=2,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=False,
        )

        with requests.Session() as session:
            session.mount(
                "https://",
                HTTPAdapter(max_retries=retry),
            )
            response = session.get(
                SOURCE_URL,
                timeout=(5, 20),
            )
            response.raise_for_status()
            content = response.content
            headers = response.headers

        validate_report(content)

    except (requests.RequestException, ValueError) as exc:
        logging.exception(
            "IESO source retrieval or format validation failed"
        )
        return func.HttpResponse(
            json.dumps({
                "error": str(exc),
                "stage": "source",
            }),
            status_code=502,
            mimetype="application/json",
        )

    try:
        result = archive_report(
            content,
            headers,
            datetime.now(timezone.utc).isoformat(),
        )

        logging.info(
            "IESO collection result: %s",
            json.dumps(result),
        )

        return func.HttpResponse(
            json.dumps(result),
            mimetype="application/json",
        )

    except Exception:
        logging.exception("IESO raw archive write failed")

        return func.HttpResponse(
            json.dumps({
                "error": (
                    "Archive write failed; inspect Application Insights"
                ),
                "stage": "storage",
            }),
            status_code=500,
            mimetype="application/json",
        )