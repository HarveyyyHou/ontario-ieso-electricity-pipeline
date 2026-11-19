# Databricks notebook source
"""Versioned Serverless entry point for the ADF operations batch."""

import base64
import json
import sys
import tempfile
from pathlib import Path
from zipfile import ZipFile

for name in ("repo_root", "batch_id", "inventory_sha256", "source_revision", "adf_run_id", "job_run_id"):
    dbutils.widgets.text(name, "")
params = {name: dbutils.widgets.get(name) for name in
          ("repo_root", "batch_id", "inventory_sha256", "source_revision", "adf_run_id", "job_run_id")}
root = Path(params["repo_root"])
if not str(root).startswith("/Workspace/"):
    root = Path("/Workspace") / str(root).lstrip("/")
data = base64.b64decode((root / "runtime.b64").read_text().strip(), validate=True)
with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as handle:
    handle.write(data)
    runtime = handle.name
with ZipFile(runtime) as package:
    if package.testzip() is not None:
        raise RuntimeError("The packaged runtime is corrupt.")
sys.path.insert(0, runtime)
spark.addArtifacts(runtime, pyfile=True)

from milestone7 import batch

inventory = batch.read_inventory(spark, params["batch_id"], params["inventory_sha256"])
result = batch.refresh(spark, inventory, params["source_revision"], params["adf_run_id"], params["job_run_id"])
cleanup = {}
for source in ("rt_lmp", "rt_totals"):
    path = f"{batch.CHECKPOINTS}/{params['batch_id']}/staging/{source}"
    try:
        dbutils.fs.rm(path, True)
        cleanup[source] = "staging_removed_after_success"
    except Exception:
        cleanup[source] = "staging_cleanup_pending"
result["staging_cleanup"] = cleanup
output = json.dumps(result, default=str, indent=2)
print(output)
dbutils.notebook.exit(output)
