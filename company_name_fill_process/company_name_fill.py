"""
Step 3 — Fill (batch-update) the company name property on HubSpot company records.

Reads cleanup_files/company_name_updates.csv produced by Step 2 and batch-updates
the `name` property via the HubSpot batch update endpoint.

Safety:
  - DRY_RUN = True by default — logs intended changes without calling the API
  - Validates input columns before processing
  - Writes a results CSV to cleanup_files/company_name_fill_results.csv
"""

import os
import sys
import time
import logging

import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DRY_RUN = False  # Flip to False only after verifying dry-run output

HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
BASE_URL = "https://api.hubapi.com"

BASE_DIR = os.path.dirname(__file__)
INPUT_CSV = os.path.join(BASE_DIR, "cleanup_files", "company_name_updates.csv")
RESULTS_CSV = os.path.join(BASE_DIR, "cleanup_files", "company_name_fill_results.csv")

REQUIRED_COLUMNS = ["Company ID", "New Company Name"]

BATCH_SIZE = 100
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def ensure_token() -> None:
    if not HUBSPOT_ACCESS_TOKEN:
        log.error("Missing HUBSPOT_ACCESS_TOKEN in .env")
        sys.exit(1)


def headers() -> dict:
    return {
        "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


def request_with_backoff(session: requests.Session, method: str, url: str, **kwargs) -> requests.Response:
    """Make an HTTP request with retry + exponential backoff on 429 / 5xx."""
    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        resp = session.request(method, url, **kwargs)
        if resp.status_code == 429 or resp.status_code >= 500:
            retry_after = float(resp.headers.get("Retry-After", backoff))
            log.warning(
                "Attempt %d/%d — HTTP %d, retrying in %.1fs",
                attempt, MAX_RETRIES, resp.status_code, retry_after,
            )
            time.sleep(retry_after)
            backoff *= 2
            continue
        return resp
    return resp


def validate_columns(df: pd.DataFrame) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        log.error("Missing required columns in input CSV: %s", missing)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Batch update
# ---------------------------------------------------------------------------


def batch_update_companies(session: requests.Session, batch: list[dict]) -> list[dict]:
    """
    Send a batch update to HubSpot and return per-row result dicts.
    Each item in `batch` must have keys: company_id, new_name.
    """
    url = f"{BASE_URL}/crm/v3/objects/companies/batch/update"
    inputs = [
        {
            "id": item["company_id"],
            "properties": {"name": item["new_name"]},
        }
        for item in batch
    ]

    resp = request_with_backoff(
        session, "POST", url,
        headers=headers(),
        json={"inputs": inputs},
        timeout=60,
    )

    results = []
    if resp.ok:
        # HubSpot returns 200 on batch update success — if resp.ok, all inputs were accepted
        for item in batch:
            results.append({
                "Company ID": item["company_id"],
                "New Company Name": item["new_name"],
                "status": "updated",
                "error_message": "",
            })
    else:
        try:
            error_msg = resp.json().get("message", resp.text)
        except Exception:
            error_msg = resp.text
        for item in batch:
            results.append({
                "Company ID": item["company_id"],
                "New Company Name": item["new_name"],
                "status": "error",
                "error_message": str(error_msg),
            })

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    ensure_token()

    if not os.path.exists(INPUT_CSV):
        log.error("Input CSV not found: %s  — run prepare_company_name_updates.py first", INPUT_CSV)
        sys.exit(1)

    df = pd.read_csv(INPUT_CSV, dtype=str).fillna("")
    validate_columns(df)
    log.info("Loaded %d update rows from %s", len(df), INPUT_CSV)

    if df.empty:
        log.info("Nothing to update — exiting")
        return

    if DRY_RUN:
        log.info("DRY_RUN is enabled — no API calls will be made")

    session = requests.Session()
    all_results: list[dict] = []

    # Build batch items
    items = [
        {"company_id": str(row["Company ID"]).strip(), "new_name": str(row["New Company Name"]).strip()}
        for _, row in df.iterrows()
    ]

    for i in range(0, len(items), BATCH_SIZE):
        batch = items[i : i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(items) + BATCH_SIZE - 1) // BATCH_SIZE

        if DRY_RUN:
            log.info("[Batch %d/%d] DRY_RUN — would update %d companies", batch_num, total_batches, len(batch))
            for item in batch:
                all_results.append({
                    "Company ID": item["company_id"],
                    "New Company Name": item["new_name"],
                    "status": "dry_run",
                    "error_message": "",
                })
        else:
            log.info("[Batch %d/%d] Updating %d companies...", batch_num, total_batches, len(batch))
            batch_results = batch_update_companies(session, batch)
            all_results.extend(batch_results)

            updated = sum(1 for r in batch_results if r["status"] == "updated")
            errors = sum(1 for r in batch_results if r["status"] == "error")
            log.info("[Batch %d/%d] Results — updated: %d, errors: %d", batch_num, total_batches, updated, errors)

    # Write results log
    results_df = pd.DataFrame(all_results)
    results_df.to_csv(RESULTS_CSV, index=False)
    log.info("Results written to %s", RESULTS_CSV)

    # Summary
    for status in results_df["status"].unique():
        count = (results_df["status"] == status).sum()
        log.info("  %s: %d", status, count)


if __name__ == "__main__":
    main()
