"""[PHASE 2] Unmerge a single merged HubSpot contact.

Fetches the survivor contact directly from HubSpot API, recovers each
historical (pre-merge) Record ID's identity from the V1 identity-profiles
endpoint, creates a new contact for each, and reattaches the survivor's
current associations to those new contacts.

Fails fast if the contact has no hs_merged_object_ids -- not a merged record.

Outputs (output/):
    unmerge_dryrun.csv   -- written in dry-run mode (default)
    unmerge_results.csv  -- written in apply mode (--apply)

Run:
    # Test mode (set TEST_MODE = True and TEST_CONTACT_ID below)
    python unmerge_process/imports/unmerge_contact.py

    # Single contact dry-run
    python unmerge_process/imports/unmerge_contact.py --contact-id 200514733095

    # Apply for real (writes to HubSpot)
    python unmerge_process/imports/unmerge_contact.py --contact-id 200514733095 --apply
"""
import argparse
import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# TEST MODE — set TEST_MODE = True and pick the contact to test against.
# When True the script ignores CLI args and uses TEST_CONTACT_ID directly.
# -----------------------------------------------------------------------------
TEST_MODE = True
TEST_CONTACT_IDS = ["200514733095", "200540767089"]
TEST_APPLY = True      # set False to dry-run in test mode
REQUIRE_EMAIL = False  # set True to skip contacts with no recoverable email

# -----------------------------------------------------------------------------
# ENV
# -----------------------------------------------------------------------------
HUBSPOT_TOKEN = os.getenv("HUBSPOT_KEY_TS_SANDBOX")
if not HUBSPOT_TOKEN:
    raise ValueError("Missing HUBSPOT_KEY_TS_SANDBOX in .env")

HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}
MAX_RETRIES = 5
REQUEST_TIMEOUT = 120
API_BASE = "https://api.hubapi.com"

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.normpath(os.path.join(THIS_DIR, "..", ".."))
SETTINGS_PATH = os.path.normpath(
    os.path.join(THIS_DIR, "..", "config", "settings.yaml")
)
OUTPUT_DIR = os.path.normpath(os.path.join(THIS_DIR, "..", "output"))

CRM_OBJECT_TYPES = ["companies", "deals", "tickets"]
ENGAGEMENT_TYPES = ["emails", "calls", "meetings"]
ALL_OBJECT_TYPES = CRM_OBJECT_TYPES + ENGAGEMENT_TYPES

OUTPUT_COLUMNS = [
    "survivor_id",
    "old_contact_id",
    "recovered_firstname",
    "recovered_lastname",
    "recovered_email",
    "status",
    "runtime",
    "survivor_url",
    "new_contact_url",
    "new_vid",
    "would_create",
    "skip_reason",
    "error_message",
    "associated_companies",
    "associated_deals",
    "associated_tickets",
    "associated_emails",
    "associated_calls",
    "associated_meetings",
]


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------

def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def hubspot_request(
    session: requests.Session, method: str, url: str, **kwargs
) -> requests.Response:
    for attempt in range(MAX_RETRIES):
        response = session.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", "5"))
            log.warning("Rate limited. Sleeping %ds.", retry_after)
            time.sleep(retry_after)
            continue
        if response.status_code >= 500 and attempt < MAX_RETRIES - 1:
            wait = 2 ** attempt
            log.warning("%d server error. Retrying in %ds.", response.status_code, wait)
            time.sleep(wait)
            continue
        return response
    raise RuntimeError(f"{method} {url} failed after {MAX_RETRIES} retries")


# ---------------------------------------------------------------------------
# Portal info
# ---------------------------------------------------------------------------

def get_portal_id(session: requests.Session) -> str:
    resp = hubspot_request(
        session, "GET", f"{API_BASE}/account-info/v3/details"
    )
    if not resp.ok:
        log.warning("Could not fetch portal ID: %s. URLs will be empty.", resp.status_code)
        return ""
    return str(resp.json().get("portalId", ""))


def contact_url(portal_id: str, contact_id: str) -> str:
    if not portal_id or not contact_id:
        return ""
    return f"https://app.hubspot.com/contacts/{portal_id}/record/0-1/{contact_id}"


# ---------------------------------------------------------------------------
# get_contact — API-first identity recovery
# ---------------------------------------------------------------------------

def get_contact(
    session: requests.Session, contact_id: str
) -> tuple[dict, list[dict]]:
    """Fetch the survivor from HubSpot and recover each historical identity.

    Returns (survivor_info, [historical_record, ...]).

    Raises ValueError if the contact does not exist or has no
    hs_merged_object_ids -- this is not a merged record and cannot be
    unmerged.

    Historical identities come from the V1 identity-profiles endpoint, which
    preserves a separate profile (with its original vid) for every contact
    that was ever merged into the survivor.
    """
    # --- Step 1: fetch current contact properties ---
    resp = hubspot_request(
        session, "GET",
        f"{API_BASE}/crm/v3/objects/contacts/{contact_id}",
        params={
            "properties": (
                "firstname,lastname,email,phone,"
                "hs_merged_object_ids,hs_additional_emails"
            )
        },
    )
    if not resp.ok:
        raise ValueError(
            f"Contact {contact_id} not found in HubSpot: "
            f"{resp.status_code} {resp.text[:200]}"
        )
    props = resp.json().get("properties") or {}

    merged_raw = (props.get("hs_merged_object_ids") or "").strip()
    if not merged_raw:
        raise ValueError(
            f"Contact {contact_id} has no hs_merged_object_ids. "
            "This is not a merged contact -- nothing to unmerge."
        )

    historical_vids = [
        v.strip() for v in merged_raw.split(";")
        if v.strip() and v.strip() != contact_id
    ]
    log.info(
        "Survivor %s | name: %s %s | merged vids: %s",
        contact_id,
        props.get("firstname", ""),
        props.get("lastname", ""),
        historical_vids,
    )

    additional_raw = (props.get("hs_additional_emails") or "").strip()
    additional_emails = [
        e.strip().lower() for e in additional_raw.split(",") if e.strip()
    ]

    survivor = {
        "current_vid": contact_id,
        "firstname": (props.get("firstname") or "").strip(),
        "lastname": (props.get("lastname") or "").strip(),
        "current_email": (props.get("email") or "").strip().lower(),
        "additional_emails": additional_emails,
    }

    # --- Step 2: recover historical emails from V1 identity-profiles ---
    vid_to_email: dict[str, str] = {}
    v1_resp = hubspot_request(
        session, "GET",
        f"{API_BASE}/contacts/v1/contact/vid/{contact_id}/profile",
    )
    if v1_resp.ok:
        for profile in (v1_resp.json().get("identity-profiles") or []):
            profile_vid = str(profile.get("vid", "")).strip()
            for identity in (profile.get("identities") or []):
                if identity.get("type") == "EMAIL":
                    email = (identity.get("value") or "").strip()
                    if email and profile_vid and profile_vid not in vid_to_email:
                        vid_to_email[profile_vid] = email
        log.info("V1 identity profiles recovered: %s", vid_to_email)
    else:
        log.warning(
            "V1 identity-profiles fetch failed for %s: %s",
            contact_id, v1_resp.status_code,
        )

    # --- Step 3: build historical records ---
    # Name comes from the survivor -- name-only merges share the same name.
    historical: list[dict] = []
    for vid in historical_vids:
        historical.append({
            "record_id": vid,
            "firstname": survivor["firstname"],
            "lastname": survivor["lastname"],
            "email": vid_to_email.get(vid, ""),
        })

    return survivor, historical


# ---------------------------------------------------------------------------
# Association fetch + attach
# ---------------------------------------------------------------------------

def fetch_current_associations(
    session: requests.Session, current_vid: str
) -> dict[str, list[str]]:
    associations: dict[str, list[str]] = {}
    for to_type in ALL_OBJECT_TYPES:
        url = f"{API_BASE}/crm/v4/associations/contacts/{to_type}/batch/read"
        resp = hubspot_request(session, "POST", url, json={"inputs": [{"id": current_vid}]})
        if not resp.ok:
            log.warning(
                "Read %s associations for %s failed: %s",
                to_type, current_vid, resp.status_code,
            )
            associations[to_type] = []
            continue
        ids: list[str] = []
        for result in resp.json().get("results", []) or []:
            for item in result.get("to", []) or []:
                obj_id = str(item.get("toObjectId", "")).strip()
                if obj_id:
                    ids.append(obj_id)
        associations[to_type] = ids
    return associations


_TYPE_ID_CACHE: dict[tuple[str, str], int] = {}


def get_default_association_type_id(
    session: requests.Session, from_type: str, to_type: str
) -> int:
    key = (from_type, to_type)
    if key in _TYPE_ID_CACHE:
        return _TYPE_ID_CACHE[key]
    resp = hubspot_request(
        session, "GET",
        f"{API_BASE}/crm/v4/associations/{from_type}/{to_type}/labels",
    )
    if not resp.ok:
        raise RuntimeError(
            f"Could not read association labels {from_type}->{to_type}: "
            f"{resp.status_code} {resp.text[:200]}"
        )
    for label in resp.json().get("results", []) or []:
        if label.get("category") == "HUBSPOT_DEFINED":
            type_id = int(label["typeId"])
            _TYPE_ID_CACHE[key] = type_id
            return type_id
    raise RuntimeError(
        f"No HUBSPOT_DEFINED association type for {from_type}->{to_type}"
    )


def attach_associations(
    session: requests.Session,
    new_vid: str,
    associations: dict[str, list[str]],
    dry_run: bool,
) -> str:
    """Attach all survivor associations to new_vid. Returns '' or error string."""
    if dry_run or not new_vid:
        return ""
    for to_type, ids in associations.items():
        if not ids:
            continue
        type_id = get_default_association_type_id(session, "contact", to_type)
        url = f"{API_BASE}/crm/v4/associations/contacts/{to_type}/batch/create"
        payload = {
            "inputs": [
                {
                    "from": {"id": new_vid},
                    "to": {"id": obj_id},
                    "types": [{
                        "associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": type_id,
                    }],
                }
                for obj_id in ids
            ]
        }
        resp = hubspot_request(session, "POST", url, json=payload)
        if not resp.ok:
            return f"attach {to_type} {resp.status_code}: {resp.text[:200]}"
    return ""


# ---------------------------------------------------------------------------
# Contact creation
# ---------------------------------------------------------------------------

def decide_skip(historical: dict, survivor: dict) -> str:
    email = (historical.get("email") or "").lower().strip()
    if not email:
        return "" if not REQUIRE_EMAIL else "no_email"
    if email == survivor.get("current_email", ""):
        return "email_matches_survivor_primary"
    if email in (survivor.get("additional_emails") or []):
        return "email_in_survivor_additional_emails"
    return ""


def create_contact(
    session: requests.Session, historical: dict, dry_run: bool
) -> tuple[str, str]:
    """Returns (new_vid, error_message). Empty strings on dry-run or failure."""
    if dry_run:
        return "", ""
    resp = hubspot_request(
        session, "POST",
        f"{API_BASE}/crm/v3/objects/contacts",
        json={
            "properties": {
                "firstname": historical["firstname"],
                "lastname": historical["lastname"],
                "email": historical["email"],
            }
        },
    )
    if not resp.ok:
        return "", f"create {resp.status_code}: {resp.text[:200]}"
    new_vid = str(resp.json().get("id", "")).strip()
    return new_vid, "" if new_vid else (
        "", f"create returned no id: {resp.text[:200]}"
    )


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def unmerge_contact_id(
    session: requests.Session,
    contact_id: str,
    portal_id: str,
    dry_run: bool,
) -> list[dict]:
    """Process one survivor. Returns one row dict per historical Record ID."""
    survivor, historical_records = get_contact(session, contact_id)

    if not historical_records:
        log.info("No historical vids found for %s.", contact_id)
        return []

    associations = fetch_current_associations(session, contact_id)
    log.info(
        "Survivor %s associations: %s",
        contact_id,
        ", ".join(f"{k}={len(v)}" for k, v in associations.items()),
    )

    runtime = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    survivor_url = contact_url(portal_id, contact_id)

    rows: list[dict] = []
    for historical in historical_records:
        record_id = historical["record_id"]
        skip_reason = decide_skip(historical, survivor)
        new_vid = ""
        error = ""
        status = ""

        if skip_reason:
            status = "DRY_SKIP" if dry_run else "SKIPPED"
        else:
            if dry_run:
                status = "DRY_CREATE"
            else:
                new_vid, error = create_contact(session, historical, dry_run)
                if error:
                    status = "FAILED_CREATE"
                else:
                    attach_error = attach_associations(
                        session, new_vid, associations, dry_run,
                    )
                    status = "CREATED" if not attach_error else "CREATED_PARTIAL_ATTACH"
                    error = attach_error
                    log.info("Created contact %s for historical %s", new_vid, record_id)

        rows.append({
            "survivor_id": contact_id,
            "old_contact_id": record_id,
            "recovered_firstname": historical["firstname"],
            "recovered_lastname": historical["lastname"],
            "recovered_email": historical["email"],
            "status": status,
            "runtime": runtime,
            "survivor_url": survivor_url,
            "new_contact_url": contact_url(portal_id, new_vid),
            "new_vid": new_vid,
            "would_create": "no" if skip_reason else "yes",
            "skip_reason": skip_reason,
            "error_message": error,
            "associated_companies": len(associations.get("companies", [])),
            "associated_deals": len(associations.get("deals", [])),
            "associated_tickets": len(associations.get("tickets", [])),
            "associated_emails": len(associations.get("emails", [])),
            "associated_calls": len(associations.get("calls", [])),
            "associated_meetings": len(associations.get("meetings", [])),
        })

    return rows


def write_excel(output_path: str, rows: list[dict]) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Unmerge Results")
        ws = writer.sheets["Unmerge Results"]
        for col in ws.iter_cols(1, ws.max_column):
            ws.column_dimensions[col[0].column_letter].width = 28


def main() -> None:
    if TEST_MODE:
        dry_run = not TEST_APPLY
        targets = TEST_CONTACT_IDS
        log.info(
            "TEST MODE -- contacts: %s | dry_run: %s", targets, dry_run,
        )
    else:
        parser = argparse.ArgumentParser(description="Unmerge a HubSpot contact.")
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument("--contact-id", help="Survivor contact ID to process.")
        parser.add_argument(
            "--apply", action="store_true",
            help="Write to HubSpot. Without this flag the script is dry-run only.",
        )
        args = parser.parse_args()
        dry_run = not args.apply
        targets = [str(args.contact_id).strip()]

    output_filename = "unmerge_dryrun.xlsx" if dry_run else f"unmerge_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    session = build_session()
    portal_id = get_portal_id(session)
    log.info("Portal ID: %s", portal_id)

    rows: list[dict] = []
    for vid in targets:
        rows.extend(unmerge_contact_id(session, vid, portal_id, dry_run))

    log.info("Writing %d rows to %s", len(rows), output_path)
    write_excel(output_path, rows)
    log.info("Done.")


if __name__ == "__main__":
    main()
