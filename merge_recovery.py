import os
import time
import json
from datetime import datetime
from typing import List, Dict, Any, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------------------------------------------
# ENV
# -----------------------------------------------------------------------------
HUBSPOT_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN_TS")
if not HUBSPOT_TOKEN:
    raise ValueError("Missing HUBSPOT_ACCESS_TOKEN_TS in .env")

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
INPUT_CSV = "./data/csv/contacts/contacts_property_export_merged_record_ids.csv"

CONTACT_ID_COLUMN = "contact_id"
MERGED_IDS_COLUMN = "hs_merged_object_ids"

OBJECT_TYPE = "contacts"

ASSOCIATIONS = [
    "companies",
    "calls",
    "emails",
    "meetings",
    "notes",
    "tasks",
]

PROPERTIES = [
    "firstname",
    "lastname",
    "email",
]

PROPERTY_MODE = "value_and_history"

SLEEP_BETWEEN_CALLS = 0.15
MAX_RETRIES = 5
REQUEST_TIMEOUT = 120

TEST_MODE = False
TEST_PARENT_CONTACT_ID = None
TEST_MERGED_ID = None

OUTPUT_DIR = "./data/csv/merged_contacts"
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

OUTPUT_CSV = f"{OUTPUT_DIR}/merged_contact_recovery_with_associations_{TIMESTAMP}.csv"
OUTPUT_JSON = f"{OUTPUT_DIR}/merged_contact_recovery_with_associations_{TIMESTAMP}.jsonl"

LEGACY_BASE_URL = "https://api.hubapi.com/contacts/v1/contact/vid"
CRM_V3_BASE_URL = "https://api.hubapi.com/crm/v3/objects/contacts"

HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def hubspot_get(session: requests.Session, url: str, params: Optional[dict] = None) -> Optional[dict]:
    for attempt in range(MAX_RETRIES):
        r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)

        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 5))
            print(f"Rate limited. Sleeping {wait}s...")
            time.sleep(wait)
            continue

        if r.status_code >= 500 and attempt < MAX_RETRIES - 1:
            wait = 2 ** attempt
            print(f"{r.status_code} server error. Retrying in {wait}s...")
            time.sleep(wait)
            continue

        if r.status_code == 404:
            return None

        if not r.ok:
            print(f"ERROR {r.status_code}: {r.text}")
            return None

        return r.json()

    return None


def split_merged_ids(value: Any) -> List[str]:
    if value in (None, "", "nan"):
        return []
    return [x.strip() for x in str(value).split(";") if x.strip()]


def flatten_property_value(prop: Any) -> Optional[str]:
    if isinstance(prop, dict):
        return prop.get("value")
    return None


def flatten_property_versions(prop: Any) -> str:
    if not isinstance(prop, dict):
        return ""

    versions = prop.get("versions", []) or []
    cleaned = []

    for version in versions:
        cleaned.append({
            "value": version.get("value"),
            "timestamp": version.get("timestamp"),
            "source-type": version.get("source-type"),
            "source-id": version.get("source-id"),
            "selected": version.get("selected"),
        })

    return json.dumps(cleaned, ensure_ascii=False)


def extract_merge_audit_emails(payload: Dict[str, Any]) -> Dict[str, str]:
    audits = payload.get("merge-audits", []) or []

    merged_from_emails = []
    merged_to_emails = []

    for audit in audits:
        merged_from = audit.get("merged_from_email", {}) or {}
        merged_to = audit.get("merged_to_email", {}) or {}

        from_value = merged_from.get("value")
        to_value = merged_to.get("value")

        if from_value not in (None, ""):
            merged_from_emails.append(str(from_value))

        if to_value not in (None, ""):
            merged_to_emails.append(str(to_value))

    merged_from_emails = list(dict.fromkeys(merged_from_emails))
    merged_to_emails = list(dict.fromkeys(merged_to_emails))

    return {
        "merged_from_emails": ";".join(merged_from_emails),
        "merged_to_emails": ";".join(merged_to_emails),
    }


def extract_association_ids(payload: Dict[str, Any], association_name: str) -> str:
    association_block = payload.get("associations", {}).get(association_name, {}) or {}
    association_results = association_block.get("results", []) or []

    ids = []
    for item in association_results:
        assoc_id = item.get("id")
        if assoc_id not in (None, ""):
            ids.append(str(assoc_id))

    return ";".join(ids)


# -----------------------------------------------------------------------------
# STEP 1 - LEGACY MERGE RECOVERY
# -----------------------------------------------------------------------------
def fetch_legacy_merged_contact(session: requests.Session, merged_id: str) -> Optional[dict]:
    url = f"{LEGACY_BASE_URL}/{merged_id}/profile"

    params: Dict[str, Any] = {
        "propertyMode": PROPERTY_MODE,
        "formSubmissionMode": "all",
        "showListMemberships": "true",
    }

    if PROPERTIES:
        params["property"] = PROPERTIES

    return hubspot_get(session, url, params=params)


# -----------------------------------------------------------------------------
# STEP 2 - CRM V3 ASSOCIATION PULL
# -----------------------------------------------------------------------------
def fetch_contact_associations(session: requests.Session, recovered_contact_id: str) -> Optional[dict]:
    url = f"{CRM_V3_BASE_URL}/{recovered_contact_id}"

    params: Dict[str, Any] = {}

    if PROPERTIES:
        params["properties"] = ",".join(PROPERTIES)

    if ASSOCIATIONS:
        params["associations"] = ",".join(ASSOCIATIONS)

    return hubspot_get(session, url, params=params)


# -----------------------------------------------------------------------------
# ROW BUILDING
# -----------------------------------------------------------------------------
def extract_row(
    parent_contact_id: str,
    merged_id: str,
    legacy_payload: Optional[dict],
    association_payload: Optional[dict],
) -> Dict[str, Any]:
    if legacy_payload is None:
        row = {
            "parent_contact_id": parent_contact_id,
            "merged_contact_id": merged_id,
            "legacy_status": "not_found",
            "association_status": "not_attempted",
            "recovered_vid": "",
            "canonical_vid": "",
            "merged_vids": "",
            "merge_audits": "",
            "firstname": "",
            "lastname": "",
            "email": "",
            "email_versions": "",
            "merged_from_emails": "",
            "merged_to_emails": "",
        }
        for association_name in ASSOCIATIONS:
            row[f"associated_{association_name}_ids"] = ""
        return row

    props = legacy_payload.get("properties", {}) or {}
    recovered_vid = legacy_payload.get("vid")

    row = {
        "parent_contact_id": parent_contact_id,
        "merged_contact_id": merged_id,
        "legacy_status": "found",
        "association_status": "found" if association_payload is not None else "not_found",
        "recovered_vid": recovered_vid,
        "canonical_vid": legacy_payload.get("canonical-vid"),
        "merged_vids": json.dumps(legacy_payload.get("merged-vids", []), ensure_ascii=False),
        "merge_audits": json.dumps(legacy_payload.get("merge-audits", []), ensure_ascii=False),
    }

    for p in PROPERTIES:
        row[p] = flatten_property_value(props.get(p))

        if p == "email":
            row["email_versions"] = flatten_property_versions(props.get(p))

    audit_email_data = extract_merge_audit_emails(legacy_payload)
    row["merged_from_emails"] = audit_email_data["merged_from_emails"]
    row["merged_to_emails"] = audit_email_data["merged_to_emails"]

    for association_name in ASSOCIATIONS:
        if association_payload is None:
            row[f"associated_{association_name}_ids"] = ""
        else:
            row[f"associated_{association_name}_ids"] = extract_association_ids(
                association_payload,
                association_name,
            )

    return row


def build_raw_record(
    parent_contact_id: str,
    merged_id: str,
    legacy_payload: Optional[dict],
    association_payload: Optional[dict],
) -> Dict[str, Any]:
    return {
        "parent_contact_id": parent_contact_id,
        "merged_contact_id": merged_id,
        "legacy_payload": legacy_payload,
        "association_payload": association_payload,
    }


# -----------------------------------------------------------------------------
# INPUT
# -----------------------------------------------------------------------------
def get_input_rows() -> List[Dict[str, str]]:
    if TEST_MODE:
        if not TEST_MERGED_ID:
            raise ValueError("TEST_MERGED_ID is required when TEST_MODE = True")

        return [{
            CONTACT_ID_COLUMN: str(TEST_PARENT_CONTACT_ID or ""),
            MERGED_IDS_COLUMN: str(TEST_MERGED_ID),
        }]

    df = pd.read_csv(INPUT_CSV, dtype=str)

    missing_cols = [c for c in [CONTACT_ID_COLUMN, MERGED_IDS_COLUMN] if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    return df[[CONTACT_ID_COLUMN, MERGED_IDS_COLUMN]].fillna("").to_dict("records")


# -----------------------------------------------------------------------------
# CORE
# -----------------------------------------------------------------------------
def main() -> None:
    ensure_dir(OUTPUT_DIR)

    print("Starting merged contact recovery with associations")
    print(f"Input CSV: {INPUT_CSV}")
    print(f"Contact ID column: {CONTACT_ID_COLUMN}")
    print(f"Merged IDs column: {MERGED_IDS_COLUMN}")
    print(f"Properties: {PROPERTIES}")
    print(f"Associations: {ASSOCIATIONS}")
    print(f"Test mode: {TEST_MODE}")
    if TEST_MODE:
        print(f"Test parent contact ID: {TEST_PARENT_CONTACT_ID}")
        print(f"Test merged ID: {TEST_MERGED_ID}")
    print(f"Output CSV: {OUTPUT_CSV}")
    print(f"Output JSONL: {OUTPUT_JSON}")

    session = build_session()
    input_rows = get_input_rows()

    rows = []
    raw_records = []

    total_ids = 0
    legacy_found = 0
    associations_found = 0

    for source_row in input_rows:
        parent_id = str(source_row.get(CONTACT_ID_COLUMN, "")).strip()
        merged_ids = split_merged_ids(source_row.get(MERGED_IDS_COLUMN, ""))

        for merged_id in merged_ids:
            total_ids += 1

            legacy_payload = fetch_legacy_merged_contact(session, merged_id)

            if legacy_payload is not None:
                legacy_found += 1

            association_payload = None
            if legacy_payload is not None:
                recovered_vid = legacy_payload.get("vid")
                if recovered_vid not in (None, ""):
                    association_payload = fetch_contact_associations(session, str(recovered_vid))
                    if association_payload is not None:
                        associations_found += 1

            out_row = extract_row(
                parent_contact_id=parent_id,
                merged_id=merged_id,
                legacy_payload=legacy_payload,
                association_payload=association_payload,
            )
            rows.append(out_row)

            raw_records.append(
                build_raw_record(
                    parent_contact_id=parent_id,
                    merged_id=merged_id,
                    legacy_payload=legacy_payload,
                    association_payload=association_payload,
                )
            )

            if total_ids % 100 == 0:
                print(
                    f"Processed {total_ids} merged IDs | "
                    f"legacy found: {legacy_found} | "
                    f"associations found: {associations_found}"
                )

            time.sleep(SLEEP_BETWEEN_CALLS)

    pd.DataFrame(rows).to_csv(OUTPUT_CSV, index=False)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        for record in raw_records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    print("\nDone.")
    print(f"Total merged IDs processed: {total_ids}")
    print(f"Legacy found: {legacy_found}")
    print(f"Associations found: {associations_found}")
    print(f"CSV: {OUTPUT_CSV}")
    print(f"JSON: {OUTPUT_JSON}")


if __name__ == "__main__":
    main()