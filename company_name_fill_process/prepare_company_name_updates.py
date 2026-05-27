"""
Step 1 — Prepare company name updates.

Reads data/csv/target_company_import_list.csv and builds a deduplicated
{Company ID: Company} dictionary. Writes cleanup_files/company_name_updates.csv
with one row per unique Company ID.

Input columns:  Company (new name), Company ID (hs_object_id)
Output columns: Company ID, New Company Name
"""

import os
import sys
import logging

import pandas as pd
from dotenv import load_dotenv

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(__file__)
INPUT_CSV = os.path.join(BASE_DIR, "..", "data", "csv", "target_company_import_list.csv")
OUTPUT_CSV = os.path.join(BASE_DIR, "cleanup_files", "company_name_updates.csv")

INPUT_REQUIRED_COLUMNS = ["Company", "Company ID"]


def validate_columns(df: pd.DataFrame, required: list[str], source: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        log.error("Missing required columns in %s: %s", source, missing)
        sys.exit(1)


def main() -> None:
    if not os.path.exists(INPUT_CSV):
        log.error("Input CSV not found: %s", INPUT_CSV)
        sys.exit(1)

    df = pd.read_csv(INPUT_CSV, dtype=str, encoding="utf-8-sig").fillna("")
    validate_columns(df, INPUT_REQUIRED_COLUMNS, INPUT_CSV)
    log.info("Loaded %d rows from %s", len(df), INPUT_CSV)

    # ------------------------------------------------------------------
    # Build deduplicated dict: {company_id: company_name}
    # First occurrence wins per Company ID
    # ------------------------------------------------------------------
    company_dict: dict[str, str] = {}
    skipped = 0

    for _, row in df.iterrows():
        company_id = str(row["Company ID"]).strip()
        company_name = str(row["Company"]).strip()

        if not company_id or not company_name:
            skipped += 1
            continue

        if company_id not in company_dict:
            company_dict[company_id] = company_name

    log.info("Unique companies: %d (from %d rows, %d skipped)", len(company_dict), len(df), skipped)

    # ------------------------------------------------------------------
    # Write output
    # ------------------------------------------------------------------
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    updates_df = pd.DataFrame([
        {"Company ID": cid, "New Company Name": name}
        for cid, name in company_dict.items()
    ])
    updates_df.to_csv(OUTPUT_CSV, index=False)
    log.info("Wrote %d updates to %s", len(updates_df), OUTPUT_CSV)


if __name__ == "__main__":
    main()
