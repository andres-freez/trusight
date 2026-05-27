import os
import time
import logging
from datetime import datetime
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
CONTACT_PROPERTIES = ["email", "hs_additional_emails"]
COMPANY_PROPERTIES = ["name", "domain"]

PAGE_SIZE = 100
MAX_RETRIES = 5
REQUEST_TIMEOUT = 120

COMPANY_BATCH_SIZE = 100
ASSOC_BATCH_SIZE = 100

# HubSpot-defined typeId for "contact -> primary company". Non-primary
# company associations use a different typeId (typically 279).
PRIMARY_COMPANY_TYPE_ID = 1

# -----------------------------------------------------------------------------
# TEST MODE
# -----------------------------------------------------------------------------
TEST_MODE = False
TEST_RECORD_ID = "211353233772"

# -----------------------------------------------------------------------------
# CONSTANTS
# -----------------------------------------------------------------------------
CONTACTS_BASE_URL = "https://api.hubapi.com/crm/v3/objects/contacts"
COMPANY_BATCH_READ_URL = "https://api.hubapi.com/crm/v3/objects/companies/batch/read"
V4_ASSOC_BATCH_READ_URL = (
    "https://api.hubapi.com/crm/v4/associations/contacts/companies/batch/read"
)

HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_DIR = os.path.join("data", "csv", "contacts")
OUTPUT_CSV = os.path.join(
    OUTPUT_DIR, f"contact_email_company_export_{TIMESTAMP}.csv"
)

OUTPUT_COLUMNS = [
    "contact_id",
    "email",
    "all_emails",
    "primary_company_name",
    "primary_company_domain",
    "all_company_names",
    "all_company_domains",
]


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
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def print_error_response(response: requests.Response, payload: Any) -> None:
    log.error("=" * 80)
    log.error("HUBSPOT API ERROR")
    log.error("URL: %s", response.url)
    log.error("Payload: %s", payload)
    log.error("Status: %s", response.status_code)
    log.error("Response: %s", response.text)
    log.error("=" * 80)


def hubspot_request(
    session: requests.Session,
    method: str,
    url: str,
    params: Optional[dict] = None,
    json_body: Optional[dict] = None,
) -> dict:
    for attempt in range(MAX_RETRIES):
        response = session.request(
            method,
            url,
            params=params,
            json=json_body,
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", "5"))
            log.warning("Rate limited. Sleeping %ds...", retry_after)
            time.sleep(retry_after)
            continue

        if response.status_code >= 500 and attempt < MAX_RETRIES - 1:
            wait = 2 ** attempt
            log.warning(
                "%d server error. Retrying in %ds...", response.status_code, wait
            )
            time.sleep(wait)
            continue

        if not response.ok:
            print_error_response(response, json_body or params)
            response.raise_for_status()

        if response.status_code == 204 or not response.content:
            return {}
        return response.json()

    raise RuntimeError(f"{method} failed after {MAX_RETRIES} retries: {url}")


def hubspot_get(session, url, params=None):
    return hubspot_request(session, "GET", url, params=params)


def hubspot_post(session, url, json_body=None):
    return hubspot_request(session, "POST", url, json_body=json_body)


# -----------------------------------------------------------------------------
# CONTACT FETCH
# -----------------------------------------------------------------------------
def fetch_contact_single(session: requests.Session, contact_id: str) -> Dict[str, Any]:
    params = {"properties": ",".join(CONTACT_PROPERTIES)}
    url = f"{CONTACTS_BASE_URL}/{contact_id}"
    log.info("Fetching contact %s", contact_id)
    return hubspot_get(session, url, params=params)


def fetch_contacts_page(
    session: requests.Session,
    after: Optional[str],
) -> Dict[str, Any]:
    params = {
        "limit": PAGE_SIZE,
        "properties": ",".join(CONTACT_PROPERTIES),
    }
    if after:
        params["after"] = after
    return hubspot_get(session, CONTACTS_BASE_URL, params=params)


# -----------------------------------------------------------------------------
# ASSOCIATIONS (v4) — primary vs additional
# -----------------------------------------------------------------------------
def fetch_contact_company_associations(
    session: requests.Session,
    contact_ids: List[str],
) -> Dict[str, Dict[str, Any]]:
    """Returns {contact_id: {"primary": company_id|None, "additional": [company_ids]}}."""
    results: Dict[str, Dict[str, Any]] = {}
    if not contact_ids:
        return results

    for i in range(0, len(contact_ids), ASSOC_BATCH_SIZE):
        batch = contact_ids[i : i + ASSOC_BATCH_SIZE]
        body = {"inputs": [{"id": str(cid)} for cid in batch]}

        log.info(
            "Fetching v4 associations batch %d-%d of %d",
            i + 1,
            min(i + ASSOC_BATCH_SIZE, len(contact_ids)),
            len(contact_ids),
        )

        data = hubspot_post(session, V4_ASSOC_BATCH_READ_URL, json_body=body)

        for result in data.get("results", []) or []:
            from_id = str(result.get("from", {}).get("id", ""))
            primary = None
            additional: List[str] = []

            for to_obj in result.get("to", []) or []:
                to_id = str(to_obj.get("toObjectId", ""))
                if not to_id:
                    continue

                is_primary = False
                for atype in to_obj.get("associationTypes", []) or []:
                    type_id = atype.get("typeId")
                    category = atype.get("category")
                    if (
                        type_id == PRIMARY_COMPANY_TYPE_ID
                        and category == "HUBSPOT_DEFINED"
                    ):
                        is_primary = True
                        break

                if is_primary:
                    primary = to_id
                else:
                    additional.append(to_id)

            results[from_id] = {"primary": primary, "additional": additional}

        # Ensure contacts with no associations still get a stub entry
        returned = {str(r.get("from", {}).get("id", "")) for r in data.get("results", []) or []}
        for cid in batch:
            if str(cid) not in returned:
                results[str(cid)] = {"primary": None, "additional": []}

    return results


# -----------------------------------------------------------------------------
# COMPANY DETAILS
# -----------------------------------------------------------------------------
def fetch_companies_details(
    session: requests.Session,
    company_ids: List[str],
) -> Dict[str, Dict[str, str]]:
    """Returns {company_id: {"name": ..., "domain": ...}}."""
    results: Dict[str, Dict[str, str]] = {}
    unique_ids = dedupe_preserve_order([cid for cid in company_ids if cid])
    if not unique_ids:
        return results

    for i in range(0, len(unique_ids), COMPANY_BATCH_SIZE):
        batch = unique_ids[i : i + COMPANY_BATCH_SIZE]
        body = {
            "properties": COMPANY_PROPERTIES,
            "inputs": [{"id": cid} for cid in batch],
        }

        log.info(
            "Fetching companies batch %d-%d of %d",
            i + 1,
            min(i + COMPANY_BATCH_SIZE, len(unique_ids)),
            len(unique_ids),
        )

        data = hubspot_post(session, COMPANY_BATCH_READ_URL, json_body=body)

        for result in data.get("results", []) or []:
            cid = str(result.get("id", ""))
            props = result.get("properties", {}) or {}
            results[cid] = {
                "name": props.get("name") or "",
                "domain": props.get("domain") or "",
            }

    return results


# -----------------------------------------------------------------------------
# ROW BUILDER
# -----------------------------------------------------------------------------
def build_all_emails(email: str, additional_emails: str) -> str:
    parts: List[str] = []
    if email:
        parts.append(email.strip())
    if additional_emails:
        for candidate in additional_emails.split(";"):
            cleaned = candidate.strip()
            if cleaned:
                parts.append(cleaned)
    return ";".join(dedupe_preserve_order(parts))


def build_all_company_field(primary_value: str, additional_values: List[str]) -> str:
    parts: List[str] = []
    if primary_value:
        parts.append(primary_value.strip())
    for candidate in additional_values:
        cleaned = (candidate or "").strip()
        if cleaned:
            parts.append(cleaned)
    return ";".join(dedupe_preserve_order(parts))


def build_contact_row(
    contact: Dict[str, Any],
    assoc_info: Dict[str, Any],
    company_map: Dict[str, Dict[str, str]],
) -> Dict[str, Any]:
    props = contact.get("properties", {}) or {}
    email = (props.get("email") or "").strip()
    additional_emails = props.get("hs_additional_emails") or ""

    primary_id = assoc_info.get("primary")
    additional_ids = assoc_info.get("additional", []) or []

    primary_company = company_map.get(primary_id, {}) if primary_id else {}
    primary_name = primary_company.get("name", "")
    primary_domain = primary_company.get("domain", "")

    additional_names = [company_map.get(cid, {}).get("name", "") for cid in additional_ids]
    additional_domains = [company_map.get(cid, {}).get("domain", "") for cid in additional_ids]

    return {
        "contact_id": contact.get("id", ""),
        "email": email,
        "all_emails": build_all_emails(email, additional_emails),
        "primary_company_name": primary_name,
        "primary_company_domain": primary_domain,
        "all_company_names": build_all_company_field(primary_name, additional_names),
        "all_company_domains": build_all_company_field(primary_domain, additional_domains),
    }


# -----------------------------------------------------------------------------
# DRIVERS
# -----------------------------------------------------------------------------
def enrich_contacts(
    session: requests.Session,
    contacts: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    contact_ids = [str(c.get("id")) for c in contacts if c.get("id")]

    assoc_map = fetch_contact_company_associations(session, contact_ids)

    all_company_ids: List[str] = []
    for info in assoc_map.values():
        if info.get("primary"):
            all_company_ids.append(info["primary"])
        all_company_ids.extend(info.get("additional", []))

    company_map = fetch_companies_details(session, all_company_ids)

    rows: List[Dict[str, Any]] = []
    for contact in contacts:
        cid = str(contact.get("id", ""))
        assoc_info = assoc_map.get(cid, {"primary": None, "additional": []})
        rows.append(build_contact_row(contact, assoc_info, company_map))
    return rows


def fetch_test_record(session: requests.Session) -> List[Dict[str, Any]]:
    contact = fetch_contact_single(session, TEST_RECORD_ID)
    return enrich_contacts(session, [contact])


def fetch_all_records(session: requests.Session) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    after: Optional[str] = None
    page = 0

    while True:
        log.info("Fetching contacts page %d ...", page + 1)
        data = fetch_contacts_page(session, after)
        page += 1

        results = data.get("results", []) or []
        if not results:
            break

        rows = enrich_contacts(session, results)
        all_rows.extend(rows)
        log.info(
            "Page %d fetched: %d contacts | Total: %d",
            page,
            len(results),
            len(all_rows),
        )

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

    return all_rows


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main() -> None:
    ensure_output_dir(OUTPUT_DIR)

    log.info("Starting contact + company email export")
    log.info("Contact properties: %s", CONTACT_PROPERTIES)
    log.info("Company properties: %s", COMPANY_PROPERTIES)
    log.info("Test mode: %s", TEST_MODE)
    if TEST_MODE:
        log.info("Test record ID: %s", TEST_RECORD_ID)
    log.info("Output file: %s", OUTPUT_CSV)

    session = build_session()

    if TEST_MODE:
        records = fetch_test_record(session)
    else:
        records = fetch_all_records(session)

    if not records:
        log.info("No records found.")
        return

    df = pd.DataFrame(records, columns=OUTPUT_COLUMNS)
    df.to_csv(OUTPUT_CSV, index=False)

    log.info("Saved CSV: %s", OUTPUT_CSV)
    log.info("Sample rows:\n%s", df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
