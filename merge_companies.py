import os
import time
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

HUBSPOT_ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
INPUT_CSV_COMPANIES = "./data/dedupe_results_companies.csv"
OUTPUT_LOG_CSV_COMPANIES = "./data/merge_log_companies.csv"

TEST_MODE_COMPANIES = False
TEST_COMPANY_ID = "4856301124"   # can be primary or duplicate company ID

# Keep True for the first run
DRY_RUN_COMPANIES = False

BASE_URL = "https://api.hubapi.com"
HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_ACCESS_TOKEN}",
    "Content-Type": "application/json",
}


def ensure_token() -> None:
    if not HUBSPOT_ACCESS_TOKEN:
        raise ValueError("Missing HUBSPOT_ACCESS_TOKEN in .env")


def normalize_str(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def split_ids(value: str) -> list[str]:
    value = normalize_str(value)
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def build_merge_actions(df: pd.DataFrame) -> list[dict]:
    actions = []

    filtered_df = df[df["dedupe_status"].astype(str).str.strip().str.lower() != "unique"].copy()

    for _, row in filtered_df.iterrows():
        primary_id = normalize_str(row.get("primary_record_id", ""))
        duplicate_ids = split_ids(row.get("duplicate_record_ids", ""))

        if not primary_id:
            actions.append(
                {
                    "primary_record_id": "",
                    "duplicate_record_id": "",
                    "status": "skipped",
                    "message": "Missing primary_record_id",
                }
            )
            continue

        if not duplicate_ids:
            actions.append(
                {
                    "primary_record_id": primary_id,
                    "duplicate_record_id": "",
                    "status": "skipped",
                    "message": "No duplicate_record_ids found",
                }
            )
            continue

        for duplicate_id in duplicate_ids:
            if duplicate_id == primary_id:
                actions.append(
                    {
                        "primary_record_id": primary_id,
                        "duplicate_record_id": duplicate_id,
                        "status": "skipped",
                        "message": "Duplicate ID equals primary ID",
                    }
                )
                continue

            actions.append(
                {
                    "primary_record_id": primary_id,
                    "duplicate_record_id": duplicate_id,
                    "status": "pending",
                    "message": "",
                }
            )

    return actions


def merge_company(session: requests.Session, primary_id: str, duplicate_id: str) -> tuple[str, str]:
    url = f"{BASE_URL}/crm/v3/objects/companies/merge"
    payload = {
        "primaryObjectId": primary_id,
        "objectIdToMerge": duplicate_id,
    }

    response = session.post(url, headers=HEADERS, json=payload, timeout=45)

    if response.ok:
        return "merged", ""

    try:
        message = response.json()
    except Exception:
        message = response.text

    return "error", str(message)


def row_contains_company(row: pd.Series, company_id: str) -> bool:
    company_id = normalize_str(company_id)
    if not company_id:
        return False

    record_id = normalize_str(row.get("Record ID", ""))
    primary_id = normalize_str(row.get("primary_record_id", ""))
    duplicate_ids = split_ids(row.get("duplicate_record_ids", ""))

    return (
        record_id == company_id
        or primary_id == company_id
        or company_id in duplicate_ids
    )


def main() -> None:
    ensure_token()
    os.makedirs("./data", exist_ok=True)

    start_time = time.time()
    print("Starting company merge script...")

    df = pd.read_csv(INPUT_CSV_COMPANIES, dtype=str).fillna("")

    required_cols = [
        "dedupe_status",
        "primary_record_id",
        "duplicate_record_ids",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if TEST_MODE_COMPANIES:
        test_company_id = normalize_str(TEST_COMPANY_ID)

        test_df = df[df.apply(lambda row: row_contains_company(row, test_company_id), axis=1)].copy()

        if test_df.empty:
            raise ValueError(f"No rows found containing test company ID {test_company_id}")

        print("TEST MODE COMPANIES ENABLED")
        print(f"Test company ID: {test_company_id}")
        print(f"Rows found: {len(test_df)}")

        actions = build_merge_actions(test_df)
    else:
        actions = build_merge_actions(df)

    print(f"Merge actions built: {len(actions)}")

    session = requests.Session()
    log_rows = []

    for i, action in enumerate(actions, start=1):
        primary_id = action["primary_record_id"]
        duplicate_id = action["duplicate_record_id"]

        print(f"[{i}/{len(actions)}] primary={primary_id} duplicate={duplicate_id}")

        if action["status"] == "skipped":
            pass
        elif not primary_id or not duplicate_id:
            action["status"] = "skipped"
            action["message"] = "Invalid merge pair"
        elif DRY_RUN_COMPANIES:
            action["status"] = "dry_run"
            action["message"] = "No merge executed"
        else:
            status, message = merge_company(session, primary_id, duplicate_id)
            action["status"] = status
            action["message"] = message

        log_rows.append(action)
        print(action)

    log_df = pd.DataFrame(log_rows)
    log_df.to_csv(OUTPUT_LOG_CSV_COMPANIES, index=False)

    elapsed = time.time() - start_time
    print(f"Saved merge log to: {OUTPUT_LOG_CSV_COMPANIES}")
    print(f"Elapsed time: {elapsed:.2f} seconds")


if __name__ == "__main__":
    main()