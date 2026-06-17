"""
FASTA FinTech Lending DW — Microsoft Fabric Deployment Script
=============================================================
Deploys all pipelines and notebooks to a Fabric workspace via REST API.

Usage
-----
  python deploy_to_fabric.py --workspace-id <GUID>

Auth (pick one)
---------------
  1. Azure CLI (recommended):   az login  →  script auto-fetches token
  2. Environment variable:       set FABRIC_TOKEN=<bearer token>
  3. Service principal:          set AZURE_CLIENT_ID / AZURE_CLIENT_SECRET / AZURE_TENANT_ID

Requirements
------------
  pip install requests azure-identity

Fabric REST API docs
--------------------
  https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/definitions/
"""

import argparse
import base64
import json
import os
import sys
import time
import subprocess
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("Missing dependency: pip install requests")


# ── Config ────────────────────────────────────────────────────────────────────

FABRIC_API      = "https://api.fabric.microsoft.com/v1"
FABRIC_RESOURCE = "https://api.fabric.microsoft.com"

HERE = Path(__file__).parent

PIPELINES = [
    HERE / "fabric/data_factory/pipelines/pl_01_bronze_ingest.json",
    HERE / "fabric/data_factory/pipelines/pl_02_silver_cleanse.json",
    HERE / "fabric/data_factory/pipelines/pl_03_gold_marts.json",
    HERE / "fabric/data_factory/pipelines/pl_04_ml_scoring.json",
]

NOTEBOOKS = [
    HERE / "fabric/ml/notebooks/nb_fabric_ml_01_credit_risk.py",
    HERE / "fabric/ml/notebooks/nb_fabric_ml_02_affordability.py",
    HERE / "fabric/ml/notebooks/nb_fabric_ml_03_churn_collections.py",
    HERE / "fabric/ml/notebooks/nb_fabric_ml_04_channel_roi.py",
    HERE / "fabric/ml/notebooks/nb_fabric_ml_retrain_full.py",
]

TRIGGER_NOTE = """
TRIGGERS — manual step required
--------------------------------
Fabric REST API v1 does not yet support standalone trigger creation.
After this script completes, activate both triggers in the Fabric UI:

  1. Open workspace → Data Factory
  2. Triggers → New trigger → Import JSON:
       fabric/data_factory/triggers/tr_daily_bronze_ingest.json    (Mon-Fri 03:00 SAST)
       fabric/data_factory/triggers/tr_saturday_ml_scoring.json    (Sat 02:00 SAST)
  3. Click "Start" on each trigger
"""


# ── Auth ──────────────────────────────────────────────────────────────────────

def get_token() -> str:
    """Return a Bearer token. Priority: env var → Azure CLI → service principal → device code."""

    # 1. Explicit env var
    tok = os.environ.get("FABRIC_TOKEN", "")
    if tok:
        print("Auth: using FABRIC_TOKEN env var")
        return tok

    # 2. Azure CLI
    try:
        result = subprocess.run(
            ["az", "account", "get-access-token",
             "--resource", FABRIC_RESOURCE,
             "--query", "accessToken", "--output", "tsv"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0 and result.stdout.strip():
            print("Auth: Azure CLI token acquired")
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    # 3. Service principal via azure-identity
    client_id     = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")
    tenant_id     = os.environ.get("AZURE_TENANT_ID")

    if client_id and client_secret and tenant_id:
        try:
            from azure.identity import ClientSecretCredential
            cred  = ClientSecretCredential(tenant_id, client_id, client_secret)
            token = cred.get_token(f"{FABRIC_RESOURCE}/.default")
            print("Auth: service principal token acquired")
            return token.token
        except ImportError:
            pass

    # 4. Device code flow — same method that fabric_connect.py uses
    try:
        from azure.identity import DeviceCodeCredential
        print("\nAuth: launching device code sign-in...")
        cred  = DeviceCodeCredential(
            client_id="1b730954-1685-4b74-9bfd-dac224a7b894",  # Azure PowerShell public client
            tenant_id="common"
        )
        token = cred.get_token(f"{FABRIC_RESOURCE}/.default")
        print("Auth: device code sign-in successful")
        return token.token
    except ImportError:
        pass

    sys.exit(
        "No auth found. Run  az login  or set FABRIC_TOKEN / "
        "AZURE_CLIENT_ID + AZURE_CLIENT_SECRET + AZURE_TENANT_ID"
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def b64(data: str) -> str:
    return base64.b64encode(data.encode("utf-8")).decode("utf-8")


def py_to_ipynb(py_path: Path) -> str:
    """Convert a # COMMAND ---------- delimited .py file to .ipynb JSON string."""
    raw   = py_path.read_text(encoding="utf-8")
    cells = [c.strip() for c in raw.split("# COMMAND ----------") if c.strip()]

    nb_cells = []
    for cell in cells:
        if cell.startswith("# %md"):
            source = cell.replace("# %md", "", 1).strip().splitlines(keepends=True)
            nb_cells.append({
                "cell_type": "markdown",
                "metadata":  {},
                "source":    source
            })
        else:
            source = cell.splitlines(keepends=True)
            nb_cells.append({
                "cell_type": "code",
                "execution_count": None,
                "metadata":  {},
                "outputs":   [],
                "source":    source
            })

    nb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "PySpark",
                "language":     "python",
                "name":         "synapse_pyspark"
            },
            "language_info": {
                "name":    "python",
                "version": "3.11"
            },
            "a365ComputeOptions": None,
            "sessionKeepAliveTimeout": 30
        },
        "cells": nb_cells
    }
    return json.dumps(nb, indent=2)


def post_item(session: requests.Session, workspace_id: str,
              display_name: str, item_type: str,
              parts: list[dict], fmt: str | None = None) -> dict:
    """Create a Fabric item via the Items API."""
    url        = f"{FABRIC_API}/workspaces/{workspace_id}/items"
    definition = {"parts": parts}
    if fmt:
        definition["format"] = fmt
    payload = {
        "displayName": display_name,
        "type":        item_type,
        "definition":  definition
    }
    resp = session.post(url, json=payload, timeout=60)

    # 202 = long-running operation — poll until done
    if resp.status_code == 202:
        op_url = resp.headers.get("Location") or resp.headers.get("location")
        return poll_lro(session, op_url, display_name)

    resp.raise_for_status()
    return resp.json()


def poll_lro(session: requests.Session, op_url: str, name: str,
             max_wait: int = 120) -> dict:
    """Poll a Fabric long-running operation until terminal state."""
    elapsed = 0
    while elapsed < max_wait:
        r = session.get(op_url, timeout=30)
        r.raise_for_status()
        body   = r.json()
        status = body.get("status", "").upper()
        if status in ("SUCCEEDED", "COMPLETED"):
            return body
        if status in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"LRO failed for {name}: {body}")
        wait = int(r.headers.get("Retry-After", 5))
        print(f"  {name}: {status} — retrying in {wait}s …")
        time.sleep(wait)
        elapsed += wait
    raise TimeoutError(f"LRO timed out after {max_wait}s for {name}")


# ── Deploy functions ──────────────────────────────────────────────────────────

def deploy_pipeline(session: requests.Session, workspace_id: str,
                    json_path: Path) -> None:
    name     = json_path.stem
    raw      = json.loads(json_path.read_text(encoding="utf-8"))
    # Fabric Items API wants only the pipeline body (activities/parameters/etc),
    # not the outer wrapper fields ($schema, name, objectType, annotations).
    body     = raw.get("properties", raw)
    # Remove any non-serialisable meta keys Fabric rejects
    for drop in ("$schema", "name", "objectType", "annotations", "dependencyGraph"):
        body.pop(drop, None)
    parts    = [{
        "path":        "pipeline-content.json",
        "payload":     b64(json.dumps(body, indent=2)),
        "payloadType": "InlineBase64"
    }]
    print(f"  Deploying pipeline: {name} …")
    post_item(session, workspace_id, name, "DataPipeline", parts)
    print(f"  ✓  {name}")


def deploy_notebook(session: requests.Session, workspace_id: str,
                    py_path: Path) -> None:
    name  = py_path.stem
    ipynb = py_to_ipynb(py_path)
    # Fabric canonical notebook path is "artifact.content.ipynb", not "notebook-content.ipynb"
    parts = [{
        "path":        "artifact.content.ipynb",
        "payload":     b64(ipynb),
        "payloadType": "InlineBase64"
    }]
    print(f"  Deploying notebook: {name} …")
    post_item(session, workspace_id, name, "Notebook", parts, fmt="ipynb")
    print(f"  ✓  {name}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Deploy FASTA FinTech DW to Fabric")
    parser.add_argument("--workspace-id", required=True,
                        help="Fabric workspace GUID (Settings → About this workspace)")
    parser.add_argument("--skip-notebooks", action="store_true",
                        help="Deploy pipelines only")
    parser.add_argument("--skip-pipelines", action="store_true",
                        help="Deploy notebooks only")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be deployed without calling the API")
    args = parser.parse_args()

    workspace_id = args.workspace_id
    token        = get_token()

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json"
    })

    results = {"pipelines": [], "notebooks": [], "errors": []}

    # ── Pipelines ────────────────────────────────────────────────────────────
    if not args.skip_pipelines:
        print(f"\n{'='*60}")
        print("DEPLOYING PIPELINES")
        print(f"{'='*60}")
        for path in PIPELINES:
            if not path.exists():
                print(f"  SKIP (not found): {path.name}")
                continue
            if args.dry_run:
                print(f"  DRY-RUN: would deploy {path.name}")
                continue
            try:
                deploy_pipeline(session, workspace_id, path)
                results["pipelines"].append(path.name)
            except Exception as exc:
                msg = f"FAILED {path.name}: {exc}"
                print(f"  ✗  {msg}")
                results["errors"].append(msg)

    # ── Notebooks ────────────────────────────────────────────────────────────
    if not args.skip_notebooks:
        print(f"\n{'='*60}")
        print("DEPLOYING NOTEBOOKS")
        print(f"{'='*60}")
        for path in NOTEBOOKS:
            if not path.exists():
                print(f"  SKIP (not found): {path.name}")
                continue
            if args.dry_run:
                print(f"  DRY-RUN: would deploy {path.name}")
                continue
            try:
                deploy_notebook(session, workspace_id, path)
                results["notebooks"].append(path.name)
            except Exception as exc:
                msg = f"FAILED {path.name}: {exc}"
                print(f"  ✗  {msg}")
                results["errors"].append(msg)

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("DEPLOYMENT SUMMARY")
    print(f"{'='*60}")
    print(f"Pipelines deployed : {len(results['pipelines'])}/{len(PIPELINES)}")
    print(f"Notebooks deployed : {len(results['notebooks'])}/{len(NOTEBOOKS)}")
    if results["errors"]:
        print(f"\nErrors ({len(results['errors'])}):")
        for e in results["errors"]:
            print(f"  ✗  {e}")
    else:
        print("\nAll items deployed successfully.")

    print(TRIGGER_NOTE)

    # Write deploy log
    log_path = HERE / "fabric/ml/artifacts/deploy_log.json"
    log_path.write_text(json.dumps({
        "deployed_at":  time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "workspace_id": workspace_id,
        "dry_run":      args.dry_run,
        **results
    }, indent=2), encoding="utf-8")
    print(f"Deploy log: {log_path}")


if __name__ == "__main__":
    main()
