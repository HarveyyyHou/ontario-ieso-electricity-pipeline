"""Build the Serverless runtime from the current repository checkout."""

import base64
import io
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

root = Path(__file__).resolve().parents[1]
paths = {
    "functions/ieso_ingest/events.py": "functions/ieso_ingest/events.py",
    "milestone4/observations.py": "databricks/milestone4/observations.py",
    "milestone4/lakehouse.py": "databricks/milestone4/lakehouse.py",
    "milestone5/gold.py": "databricks/milestone5/gold.py",
    "milestone6/hourly_batch.py": "databricks/milestone6/hourly_batch.py",
    "milestone7/batch.py": "databricks/milestone7/batch.py",
}
out = io.BytesIO()
with ZipFile(out, "w", ZIP_DEFLATED) as package:
    for directory in ("functions", "functions/ieso_ingest", "milestone4", "milestone5", "milestone6", "milestone7"):
        package.writestr(directory + "/__init__.py", "")
    for target, source in paths.items():
        package.writestr(target, (root / source).read_bytes())
destination = root / "databricks/milestone7/runtime.b64"
destination.write_text(base64.b64encode(out.getvalue()).decode() + "\n", encoding="ascii")
print(f"Built {destination} from {len(paths)} source files.")
