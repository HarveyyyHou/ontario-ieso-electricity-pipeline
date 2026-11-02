import json
import logging
import os
import re
from contextlib import ExitStack

import azure.functions as func
from azure.data.tables import TableClient
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient

from collector import BlobArchive, Manifest, collect, request_options

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
logging.getLogger("azure").setLevel(logging.WARNING)


@app.function_name(name="collect_ieso")
@app.route(route="collect_ieso", methods=["POST"])
def collect_ieso(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        request_options(body)
    except (ValueError, TypeError) as exc:
        return func.HttpResponse(json.dumps({"status": "failed", "stage": "request", "error": str(exc)}),
                                 status_code=400, mimetype="application/json")
    try:
        account = os.environ["IESO_STORAGE_ACCOUNT"]
        container = os.environ["IESO_RAW_CONTAINER"]
        table_name = os.environ["IESO_MANIFEST_TABLE"]
        adf_run_id = req.headers.get("x-adf-run-id", "")
        if adf_run_id and not re.fullmatch(r"[A-Za-z0-9-]{1,80}", adf_run_id):
            raise ValueError("Invalid x-adf-run-id")
        with ExitStack() as stack:
            credential = stack.enter_context(ManagedIdentityCredential())
            blobs = stack.enter_context(BlobServiceClient(
                account_url=f"https://{account}.blob.core.windows.net", credential=credential,
                retry_total=1, initial_backoff=1, increment_base=1, random_jitter_range=0,
                connection_timeout=5, read_timeout=10))
            table = stack.enter_context(TableClient(
                endpoint=f"https://{account}.table.core.windows.net", table_name=table_name,
                credential=credential, retry_total=1, retry_backoff_factor=1,
                connection_timeout=5, read_timeout=10))
            result, code = collect(body, BlobArchive(blobs.get_container_client(container)),
                                   Manifest(table), adf_run_id=adf_run_id)
            result["container"] = container
        return func.HttpResponse(json.dumps(result), status_code=code, mimetype="application/json")
    except Exception:
        logging.exception("ieso_configuration_or_client_failure")
        return func.HttpResponse(json.dumps({"status": "failed", "stage": "configuration",
                                             "error": "Inspect Function settings, identity permissions, and Application Insights"}),
                                 status_code=500, mimetype="application/json")
