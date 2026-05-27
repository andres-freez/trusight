import os
import time
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv(override=True)

# -----------------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# ENV
# -----------------------------------------------------------------------------
HUBSPOT_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN_TS")
if not HUBSPOT_TOKEN:
    raise ValueError("Missing HUBSPOT_ACCESS_TOKEN_TS in .env")

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
OBJECT_TYPE = "contacts"

ASSOCIATIONS = [
    "companies"
]

CURRENT_PROPERTIES = [
    "email",
    "hs_additional_emails",
    "hs_merged_object_ids"
]

# Properties you want with a _last_updated_date column derived from history
HISTORY_PROPERTIES = [
    
]

# "latest" -> most recent history timestamp | "earliest" -> first known
HISTORY_MODE = "latest"

PAGE_SIZE = 100
MAX_RETRIES = 5
REQUEST_TIMEOUT = 120

# V1 identity profile batch size (max 100 per HubSpot docs)
IDENTITY_BATCH_SIZE = 100

OUTPUT_BASENAME = f"{OBJECT_TYPE}_property_export"

# -----------------------------------------------------------------------------
# TEST MODE
# -----------------------------------------------------------------------------
TEST_MODE = False
TEST_RECORD_ID = "211353233772"

# -----------------------------------------------------------------------------
# CONSTANTS
# -----------------------------------------------------------------------------
BASE_URL = f"https://api.hubapi.com/crm/v3/objects/{OBJECT_TYPE}"
V1_BATCH_URL = "https://api.hubapi.com/contacts/v1/contact/vids/batch/"
V1_SINGLE_URL = "https://api.hubapi.com/contacts/v1/contact/vid"
HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_DIR = os.path.join("data", "csv", OBJECT_TYPE)
OUTPUT_CSV = os.path.join(OUTPUT_DIR, f"{OUTPUT_BASENAME}_merged_record_ids_{TIMESTAMP}.csv")


# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def ensure_output_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def dedupe_preserve_order(values: List[str]) -> List[str]:
    seen = set()
    output = []
    for value in values:
        if value not in seen:
            seen.add(value)
            output.append(value)
    return output


def parse_hubspot_timestamp(value: Any) -> str:
    if value in (None, "", "None"):
        return ""
    try:
        ts = int(value)
        if ts > 10_000_000_000:
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        else:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(value)


def get_history_choice(history_items: List[Dict[str, Any]], mode: str) -> Dict[str, Any]:
    if not history_items:
        return {}

    def ts_key(item: Dict[str, Any]) -> int:
        try:
            return int(item.get("timestamp"))
        except Exception:
            return -1

    ordered = sorted(history_items, key=ts_key)
    return ordered[0] if mode == "earliest" else ordered[-1]


def get_id_column_name(object_type: str) -> str:
    object_id_map = {
        "contacts": "contact_id",
        "companies": "company_id",
        "deals": "deal_id",
        "tickets": "ticket_id",
        "line_items": "line_item_id",
        "products": "product_id",
        "quotes": "quote_id",
        "calls": "call_id",
        "emails": "email_id",
        "meetings": "meeting_id",
        "notes": "note_id",
        "tasks": "task_id",
    }
    return object_id_map.get(object_type, "record_id")


def extract_association_ids(result: Dict[str, Any], association_name: str) -> str:
    association_block = result.get("associations", {}).get(association_name, {}) or {}
    association_results = association_block.get("results", []) or []
    ids = [str(item.get("id")) for item in association_results if item.get("id") not in (None, "")]
    return ";".join(ids)


def get_object_type_id(object_type: str) -> str:
    object_type_map = {
        "contacts": "0-1",
        "companies": "0-2",
        "deals": "0-3",
        "tickets": "0-5",
    }
    return object_type_map.get(object_type, object_type)


def print_error_response(response: requests.Response, params: Optional[dict]) -> None:
    log.error("=" * 80)
    log.error("HUBSPOT API ERROR")
    log.error("URL: %s", response.url)
    log.error("Params: %s", params)
    log.error("Status: %s", response.status_code)
    log.error("Response: %s", response.text)
    log.error("=" * 80)


def hubspot_get(
    session: requests.Session,
    url: str,
    params: Optional[dict] = None,
) -> dict:
    for attempt in range(MAX_RETRIES):
        response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", "5"))
            log.warning("Rate limited. Sleeping %ds...", retry_after)
            time.sleep(retry_after)
            continue

        if response.status_code >= 500 and attempt < MAX_RETRIES - 1:
            wait = 2 ** attempt
            log.warning("%d server error. Retrying in %ds...", response.status_code, wait)
            time.sleep(wait)
            continue

        if not response.ok:
            print_error_response(response, params)
            response.raise_for_status()

        return response.json()

    raise RuntimeError(f"GET failed after {MAX_RETRIES} retries: {url}")


# -----------------------------------------------------------------------------
# IDENTITY PROFILES — V1 API
# -----------------------------------------------------------------------------
def extract_identity_emails(payload: Dict[str, Any]) -> Dict[str, str]:
    """Parse identity-profiles from a v1 contact response to get the true
    primary email and all additional emails (as shown in HubSpot UI)."""
    profiles = payload.get("identity-profiles", []) or []

    primary_email = None
    additional_emails = []

    for profile in profiles:
        identities = profile.get("identities", []) or []
        for identity in identities:
            if identity.get("type") != "EMAIL":
                continue
            email = identity.get("value")
            if not email:
                continue
            if identity.get("is-primary", False):
                primary_email = email
            else:
                additional_emails.append(email)

    additional_emails = list(dict.fromkeys(additional_emails))

    return {
        "email": primary_email or "",
        "additional_emails": ";".join(additional_emails),
    }


def fetch_identity_emails_single(session: requests.Session, vid: str) -> Dict[str, str]:
    """Fetch identity profiles for a single contact via v1 API."""
    url = f"{V1_SINGLE_URL}/{vid}/profile"
    data = hubspot_get(session, url)
    emails = extract_identity_emails(data)
    log.info(
        "Identity for %s: primary=%s additional=%s",
        vid, emails["email"], emails["additional_emails"],
    )
    return emails


def fetch_identity_emails_batch(
    session: requests.Session,
    vids: List[str],
) -> Dict[str, Dict[str, str]]:
    """Fetch identity profiles for a batch of contact IDs via v1 batch API.
    Returns {vid: {email, additional_emails}} for each contact."""
    results = {}

    for i in range(0, len(vids), IDENTITY_BATCH_SIZE):
        batch = vids[i : i + IDENTITY_BATCH_SIZE]
        params = [("vid", vid) for vid in batch]

        log.info(
            "Fetching identity profiles batch %d-%d of %d",
            i + 1, min(i + IDENTITY_BATCH_SIZE, len(vids)), len(vids),
        )

        data = hubspot_get(session, V1_BATCH_URL, params=params)

        for vid_str, contact_data in data.items():
            results[str(vid_str)] = extract_identity_emails(contact_data)

    return results


# -----------------------------------------------------------------------------
# PROPERTY OPTION MAPS
# -----------------------------------------------------------------------------
def get_property_option_map(
    session: requests.Session,
    object_type: str,
    property_name: str,
) -> Dict[str, str]:
    object_type_id = get_object_type_id(object_type)
    url = f"https://api.hubapi.com/crm/v3/properties/{object_type_id}/{property_name}"
    data = hubspot_get(session, url)
    options = data.get("options", []) or []
    return {option.get("value", ""): option.get("label", "") for option in options}


def build_property_label_maps(
    session: requests.Session,
    object_type: str,
    properties: List[str],
) -> Dict[str, Dict[str, str]]:
    label_maps = {}
    for prop in properties:
        try:
            option_map = get_property_option_map(session, object_type, prop)
            if option_map:
                label_maps[prop] = option_map
                log.info("Loaded option map for %s: %s", prop, option_map)
        except Exception as e:
            log.debug("No option map for %s: %s", prop, e)
    return label_maps


def map_option_label(
    property_name: str,
    raw_value: Any,
    property_label_maps: Dict[str, Dict[str, str]],
) -> Any:
    if raw_value in (None, ""):
        return raw_value
    if property_name not in property_label_maps:
        return raw_value
    return property_label_maps[property_name].get(raw_value, raw_value)


# -----------------------------------------------------------------------------
# ROW BUILDER
# -----------------------------------------------------------------------------
def build_row_from_result(
    result: Dict[str, Any],
    current_props: List[str],
    history_props: List[str],
    property_label_maps: Dict[str, Dict[str, str]],
    association_types: List[str],
) -> Dict[str, Any]:
    id_column = get_id_column_name(OBJECT_TYPE)
    row = {id_column: result.get("id", "")}

    properties = result.get("properties", {}) or {}
    properties_with_history = result.get("propertiesWithHistory", {}) or {}

    for prop in current_props:
        raw_value = properties.get(prop, "")
        row[prop] = map_option_label(prop, raw_value, property_label_maps)

    for prop in history_props:
        history_items = properties_with_history.get(prop, []) or []
        selected_history = get_history_choice(history_items, HISTORY_MODE)

        selected_raw_value = selected_history.get("value", None)
        fallback_raw_value = row.get(prop, properties.get(prop, ""))
        raw_value = selected_raw_value if selected_raw_value not in (None, "") else fallback_raw_value
        row[prop] = map_option_label(prop, raw_value, property_label_maps)

        row[f"{prop}_last_updated_date"] = parse_hubspot_timestamp(
            selected_history.get("timestamp")
        )

    for association_name in association_types:
        row[f"associated_{association_name}_ids"] = extract_association_ids(
            result, association_name,
        )

    return row


# -----------------------------------------------------------------------------
# CORE FETCH
# -----------------------------------------------------------------------------
def fetch_test_record(
    session: requests.Session,
    property_label_maps: Dict[str, Dict[str, str]],
) -> List[Dict[str, Any]]:
    current_props = dedupe_preserve_order(CURRENT_PROPERTIES)
    history_props = dedupe_preserve_order(HISTORY_PROPERTIES)
    association_types = dedupe_preserve_order(ASSOCIATIONS)

    url = f"{BASE_URL}/{TEST_RECORD_ID}"
    params = {}
    if current_props:
        params["properties"] = ",".join(current_props)
    if history_props:
        params["propertiesWithHistory"] = ",".join(history_props)
    if association_types:
        params["associations"] = ",".join(association_types)

    log.info("Fetching TEST record %s", TEST_RECORD_ID)
    data = hubspot_get(session, url, params=params)

    row = build_row_from_result(
        data, current_props, history_props, property_label_maps, association_types,
    )

    # Add v1 identity profile emails alongside the v3 property values
    identity = fetch_identity_emails_single(session, TEST_RECORD_ID)
    row["identity_primary_email"] = identity["email"]
    row["identity_additional_emails"] = identity["additional_emails"]

    return [row]


def fetch_all_records(
    session: requests.Session,
    property_label_maps: Dict[str, Dict[str, str]],
) -> List[Dict[str, Any]]:
    current_props = dedupe_preserve_order(CURRENT_PROPERTIES)
    history_props = dedupe_preserve_order(HISTORY_PROPERTIES)
    association_types = dedupe_preserve_order(ASSOCIATIONS)

    records = []
    after = None
    page = 0

    while True:
        limit = 50 if history_props else PAGE_SIZE
        params = {"limit": limit}

        if current_props:
            params["properties"] = ",".join(current_props)
        if history_props:
            params["propertiesWithHistory"] = ",".join(history_props)
        if association_types:
            params["associations"] = ",".join(association_types)
        if after:
            params["after"] = after

        log.info("Fetching page %d ...", page + 1)
        data = hubspot_get(session, BASE_URL, params=params)
        results = data.get("results", [])

        page_rows = []
        for result in results:
            row = build_row_from_result(
                result, current_props, history_props, property_label_maps, association_types,
            )
            page_rows.append(row)

        # Batch-fetch identity profiles for this page's contacts
        vids = [row[get_id_column_name(OBJECT_TYPE)] for row in page_rows]
        identity_map = fetch_identity_emails_batch(session, vids)

        for row in page_rows:
            vid = row[get_id_column_name(OBJECT_TYPE)]
            identity = identity_map.get(str(vid), {})
            if identity:
                row["identity_primary_email"] = identity["email"]
                row["identity_additional_emails"] = identity["additional_emails"]

        records.extend(page_rows)
        page += 1
        log.info("Page %d fetched: %d rows | Total: %d", page, len(results), len(records))

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    return records


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main() -> None:
    ensure_output_dir(OUTPUT_DIR)

    log.info("Starting HubSpot export for object: %s", OBJECT_TYPE)
    log.info("Current properties: %s", CURRENT_PROPERTIES)
    log.info("History properties: %s", HISTORY_PROPERTIES)
    log.info("Associations: %s", ASSOCIATIONS)
    log.info("History mode: %s", HISTORY_MODE)
    log.info("Test mode: %s", TEST_MODE)
    if TEST_MODE:
        log.info("Test record ID: %s", TEST_RECORD_ID)
    log.info("Output file: %s", OUTPUT_CSV)

    session = build_session()

    properties_to_map = dedupe_preserve_order(CURRENT_PROPERTIES + HISTORY_PROPERTIES)
    property_label_maps = build_property_label_maps(session, OBJECT_TYPE, properties_to_map)

    if TEST_MODE:
        records = fetch_test_record(session, property_label_maps)
    else:
        records = fetch_all_records(session, property_label_maps)

    if not records:
        log.info("No records found.")
        return

    df = pd.DataFrame(records)
    df.to_csv(OUTPUT_CSV, index=False)

    log.info("Saved CSV: %s", OUTPUT_CSV)
    log.info("Sample rows:\n%s", df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
