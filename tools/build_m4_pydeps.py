
"""Package Milestone 4 Python modules for Spark workers."""

import base64
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "databricks/milestone4/ieso_m4_pydeps.zip"
ENCODED_OUTPUT = ROOT / "databricks/milestone4/ieso_m4_pydeps.b64"

SOURCES = {
    "milestone4/__init__.py":
        ROOT / "databricks/milestone4/__init__.py",
    "milestone4/lakehouse.py":
        ROOT / "databricks/milestone4/lakehouse.py",
    "milestone4/observations.py":
        ROOT / "databricks/milestone4/observations.py",
    "functions/ieso_ingest/events.py":
        ROOT / "functions/ieso_ingest/events.py",
}

for source in SOURCES.values():
    if not source.is_file():
        raise FileNotFoundError(f"Missing source file: {source}")

OUTPUT.parent.mkdir(parents=True, exist_ok=True)

with ZipFile(OUTPUT, "w", ZIP_DEFLATED) as archive:
    # Explicit package markers allow reliable ZIP imports.
    archive.writestr("functions/__init__.py", "")
    archive.writestr("functions/ieso_ingest/__init__.py", "")

    for archive_name, source_path in SOURCES.items():
        archive.write(source_path, archive_name)

with ZipFile(OUTPUT) as archive:
    if archive.testzip() is not None:
        raise RuntimeError("Python dependency ZIP failed verification")

    print(f"Created: {OUTPUT}")
    print("Included files:")
    for name in archive.namelist():
        print(f"  {name}")

# Encode the verified ZIP as text for reliable bundle synchronization.
encoded = base64.b64encode(OUTPUT.read_bytes()).decode("ascii")

ENCODED_OUTPUT.write_text(encoded + "\n", encoding="ascii")

print(f"Created bundle-safe dependency file: {ENCODED_OUTPUT}")