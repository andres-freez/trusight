import os
import time
import json
from datetime import datetime
from typing import Optional, List, Dict, Any

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

# Contact properties to recover from legacy VID endpoint
PROPERTIES = [
    "firstname",
    "lastname",
    "email",
    "hs_merged_object_ids",
]

PROPERTY_MODE = "value_and_history"

MAX_RETRIES = 5
REQUEST_TIMEOUT = 120
SLEEP_BETWEEN_CALLS = 0.15

TEST_MODE = False
TEST_CONTACT_ID = "211368243101"

OUTPUT_DIR = "./data/csv/contact_deletion_audit"
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_CSV = f"{OUTPUT_DIR}/contact_deletion_audit_{TIMESTAMP}.csv"
OUTPUT_JSON = f"{OUTPUT_DIR}/contact_deletion_audit_{TIMESTAMP}.jsonl"

CRM_V3_BASE_URL = "https://api.hubapi.com/crm/v3/objects/contacts"
LEGACY_VID_BASE_URL = "https://api.hubapi.com/contacts/v1/contact/vid"

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
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def hubspot_get(
    session: requests.Session,
    url: str,
    params: Optional[dict] = None,
) -> Optional[dict]:
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)

            if response.status_code == 429:
                wait = int(response.headers.get("Retry-After", 5))
                print(f"Rate limited. Sleeping {wait}s...")
                time.sleep(wait)
                continue

            if response.status_code >= 500 and attempt < MAX_RETRIES - 1:
                wait = 2 ** attempt
                print(f"{response.status_code} server error. Retrying in {wait}s...")
                time.sleep(wait)
                continue

            if response.status_code == 404:
                return None

            if not response.ok:
                print(f"ERROR {response.status_code}: {response.text}")
                return None

            return response.json()

        except requests.exceptions.Timeout as e:
            wait = 2 ** attempt
            print(f"Timeout on {url}. Retrying in {wait}s... ({e})")
            time.sleep(wait)
            continue

        except requests.exceptions.ConnectionError as e:
            wait = 2 ** attempt
            print(f"Connection error on {url}. Retrying in {wait}s... ({e})")
            time.sleep(wait)
            continue

        except requests.exceptions.RequestException as e:
            print(f"Request failed for {url}: {e}")
            return None

    print(f"GET failed after {MAX_RETRIES} retries: {url}")
    return None


def get_input_ids() -> List[str]:
    if TEST_MODE:
        return [str(TEST_CONTACT_ID)]

    df = pd.read_csv(INPUT_CSV, dtype=str)
    if CONTACT_ID_COLUMN not in df.columns:
        raise ValueError(f"Missing required column: {CONTACT_ID_COLUMN}")

    ids = (
        df[CONTACT_ID_COLUMN]
        .dropna()
        .astype(str)
        .str.strip()
    )

    return ids[ids != ""].drop_duplicates().tolist()


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


def extract_identity_emails(payload: Dict[str, Any]) -> Dict[str, str]:
    profiles = payload.get("identity-profiles", []) or []

    primary_email = None
    additional_emails = []

    for profile in profiles:
        identities = profile.get("identities", []) or []

        for identity in identities:
            if identity.get("type") != "EMAIL":
                continue

            email = identity.get("value")
            is_primary = identity.get("is-primary", False)

            if not email:
                continue

            if is_primary:
                primary_email = email
            else:
                additional_emails.append(email)

    additional_emails = list(dict.fromkeys(additional_emails))

    return {
        "primary_email_identity": primary_email or "",
        "additional_emails_identity": ";".join(additional_emails),
    }


# -----------------------------------------------------------------------------
# STEP 1 - CURRENT STATUS CHECK
# -----------------------------------------------------------------------------
def fetch_current_contact_status(
    session: requests.Session,
    contact_id: str,
) -> Dict[str, Any]:
    url = f"{CRM_V3_BASE_URL}/{contact_id}"
    params = {
        "archived": "true",
        "properties": ",".join(["email", "hs_merged_object_ids"]),
    }

    data = hubspot_get(session, url, params=params)

    if data is None:
        return {
            "existence_status": "not_found",
            "crm_contact_id": "",
            "crm_archived": "",
            "crm_archived_at": "",
            "crm_email": "",
            "crm_hs_merged_object_ids": "",
        }

    properties = data.get("properties", {}) or {}

    return {
        "existence_status": "archived" if data.get("archived", False) else "active",
        "crm_contact_id": data.get("id", ""),
        "crm_archived": data.get("archived", False),
        "crm_archived_at": data.get("archivedAt", ""),
        "crm_email": properties.get("email", ""),
        "crm_hs_merged_object_ids": properties.get("hs_merged_object_ids", ""),
    }


# -----------------------------------------------------------------------------
# STEP 2 - LEGACY VID HISTORY CHECK
# -----------------------------------------------------------------------------
def fetch_legacy_contact_history(
    session: requests.Session,
    contact_id: str,
) -> Optional[dict]:
    url = f"{LEGACY_VID_BASE_URL}/{contact_id}/profile"

    params: Dict[str, Any] = {
        "propertyMode": PROPERTY_MODE,
        "formSubmissionMode": "all",
        "showListMemberships": "true",
    }

    if PROPERTIES:
        params["property"] = PROPERTIES

    return hubspot_get(session, url, params=params)


# -----------------------------------------------------------------------------
# ROW BUILDING
# -----------------------------------------------------------------------------
def build_row(
    requested_contact_id: str,
    current_status: Dict[str, Any],
    legacy_payload: Optional[dict],
) -> Dict[str, Any]:
    row = {
        "requested_contact_id": requested_contact_id,
        "existence_status": current_status["existence_status"],
        "crm_contact_id": current_status["crm_contact_id"],
        "crm_archived": current_status["crm_archived"],
        "crm_archived_at": current_status["crm_archived_at"],
        "crm_email": current_status["crm_email"],
        "crm_hs_merged_object_ids": current_status["crm_hs_merged_object_ids"],
    }

    if legacy_payload is None:
        row.update({
            "legacy_found": "false",
            "legacy_vid": "",
            "legacy_canonical_vid": "",
            "legacy_merged_vids": "",
            "legacy_merge_audits": "",
            "firstname": "",
            "firstname_versions": "",
            "lastname": "",
            "lastname_versions": "",
            "email": "",
            "email_versions": "",
            "hs_merged_object_ids": "",
            "hs_merged_object_ids_versions": "",
            "primary_email_identity": "",
            "additional_emails_identity": "",
        })
        return row

    props = legacy_payload.get("properties", {}) or {}

    row.update({
        "legacy_found": "true",
        "legacy_vid": legacy_payload.get("vid", ""),
        "legacy_canonical_vid": legacy_payload.get("canonical-vid", ""),
        "legacy_merged_vids": json.dumps(legacy_payload.get("merged-vids", []), ensure_ascii=False),
        "legacy_merge_audits": json.dumps(legacy_payload.get("merge-audits", []), ensure_ascii=False),
    })

    for prop in PROPERTIES:
        row[prop] = flatten_property_value(props.get(prop))
        row[f"{prop}_versions"] = flatten_property_versions(props.get(prop))

    identity_data = extract_identity_emails(legacy_payload)
    row["primary_email_identity"] = identity_data["primary_email_identity"]
    row["additional_emails_identity"] = identity_data["additional_emails_identity"]

    return row


def build_raw_record(
    requested_contact_id: str,
    current_status: Dict[str, Any],
    legacy_payload: Optional[dict],
) -> Dict[str, Any]:
    return {
        "requested_contact_id": requested_contact_id,
        "current_status": current_status,
        "legacy_payload": legacy_payload,
    }


# -----------------------------------------------------------------------------
# CORE
# -----------------------------------------------------------------------------
def main() -> None:
    ensure_dir(OUTPUT_DIR)

    print("Starting contact deletion audit export")
    print(f"Input CSV: {INPUT_CSV}")
    print(f"Contact ID column: {CONTACT_ID_COLUMN}")
    print(f"Properties: {PROPERTIES}")
    print(f"Property mode: {PROPERTY_MODE}")
    print(f"Test mode: {TEST_MODE}")
    if TEST_MODE:
        print(f"Test contact ID: {TEST_CONTACT_ID}")
    print(f"Output CSV: {OUTPUT_CSV}")
    print(f"Output JSONL: {OUTPUT_JSON}")

    session = build_session()
    contact_ids = get_input_ids()

    rows = []
    raw_records = []

    total_contacts = 0
    active_count = 0
    archived_count = 0
    not_found_count = 0
    legacy_found_count = 0

    for contact_id in contact_ids:
        total_contacts += 1

        current_status = fetch_current_contact_status(session, contact_id)
        legacy_payload = fetch_legacy_contact_history(session, contact_id)

        if current_status["existence_status"] == "active":
            active_count += 1
        elif current_status["existence_status"] == "archived":
            archived_count += 1
        else:
            not_found_count += 1

        if legacy_payload is not None:
            legacy_found_count += 1

        rows.append(build_row(contact_id, current_status, legacy_payload))
        raw_records.append(build_raw_record(contact_id, current_status, legacy_payload))

        if total_contacts % 100 == 0 or total_contacts == len(contact_ids):
            print(
                f"Processed {total_contacts}/{len(contact_ids)} | "
                f"active: {active_count} | "
                f"archived: {archived_count} | "
                f"not_found: {not_found_count} | "
                f"legacy_found: {legacy_found_count}"
            )

        time.sleep(SLEEP_BETWEEN_CALLS)

    pd.DataFrame(rows).to_csv(OUTPUT_CSV, index=False)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        for record in raw_records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    print("\nDone.")
    print(f"Total contacts processed: {total_contacts}")
    print(f"Active: {active_count}")
    print(f"Archived: {archived_count}")
    print(f"Not found: {not_found_count}")
    print(f"Legacy found: {legacy_found_count}")
    print(f"CSV: {OUTPUT_CSV}")
    print(f"JSONL: {OUTPUT_JSON}")


if __name__ == "__main__":
    main()