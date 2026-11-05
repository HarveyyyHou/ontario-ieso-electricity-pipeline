"""Write a concrete ADF linked service for the existing IESO Databricks workspace."""

import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NAME = "ls_ieso_databricks"
WORKSPACE_ID = re.compile(
    r"/subscriptions/[0-9a-f-]{36}/resourceGroups/ieso_development/providers/"
    r"Microsoft.Databricks/workspaces/dbw-ieso-dev-024ed7af", re.IGNORECASE,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace-url", required=True)
    parser.add_argument("--workspace-resource-id", required=True)
    args = parser.parse_args()
    host = args.workspace_url.rstrip("/")
    if not re.fullmatch(r"https://adb-[0-9]+\.[0-9]+\.azuredatabricks\.net", host):
        parser.error("Enter the workspace base URL, without the notebook path or query string")
    if not WORKSPACE_ID.fullmatch(args.workspace_resource_id):
        parser.error("Enter the Azure resource ID of dbw-ieso-dev-024ed7af in ieso_development")
    value = {"name": NAME, "properties": {
        "type": "AzureDatabricks",
        "typeProperties": {"domain": host, "authentication": "MSI",
                           "workspaceResourceId": args.workspace_resource_id},
        "annotations": [],
    }}
    destination = ROOT / "adf" / "linkedService" / f"{NAME}.json"
    destination.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    print(destination.relative_to(ROOT))


if __name__ == "__main__":
    main()
