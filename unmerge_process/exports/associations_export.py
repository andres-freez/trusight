"""associations_export.py

Pulls associations currently attached to each HubSpot contact that absorbed a
name-only-matched secondary, then expands each association into one row per
historical contact ID (pre-merge) so downstream analysis can reason about
which secondary may have brought the association in.

Writes two CSVs so engagements stay separated from CRM-object associations:
    output/associations_export.csv             -- companies, deals, tickets
    output/engagement_associations_export.csv  -- emails, calls, meetings

Single responsibility: capture the association footprint of name-only-merged
contacts with both pre-merge and post-merge contact IDs preserved on every
row. Reuses the auth + retry + pagination patterns established in
hubspot_export.py.

Source of (current_vid, [historical_vids]) pairs:
    Latest ./data/name_only_matches_*.xlsx, sheet "Name Only Matches".
    Rows where match_status == "merged_into" contribute the pairs:
        current_vid     := current_contact_id   (post-merge survivor)
        historical_vid  := Record ID            (pre-merge contact)
    Each survivor is also emitted with historical_vid == current_vid so the
    survivor's own associations are visible alongside the secondaries'.

CSV columns (both output files share the same schema):
    historical_vid           -- pre-merge contact id (== current_vid for the
                                surviving primary itself, OR a secondary
                                Record ID that was merged into it)
    current_vid              -- post-merge surviving HubSpot contact id
    is_historical            -- "true" when historical_vid != current_vid
    associated_object_type   -- companies | deals | tickets in associations_export.csv
                                emails | calls | meetings in engagement_associations_export.csv
    associated_object_id
    association_timestamp    -- always empty here. HubSpot v4 batch read does
                                not return an association creation time on
                                these links; pull via timeline if a timestamp
                                is needed later.

Run:
    python unmerge_process/exports/associations_export.py

Idempotent: overwrites output/associations_export.csv on each run. Honors
config/settings.yaml::test_mode to limit the survivor list during smoke tests.
"""
import csv
import glob
import logging
import os
import time
from collections import defaultdict
from typing import Iterable

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

HUBSPOT_TOKEN = os.getenv("HUBSPOT_KEY_TS_SANDBOX")
if not HUBSPOT_TOKEN:
    raise ValueError("Missing HUBSPOT_KEY_TS_SANDBOX in .env")

HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}
MAX_RETRIES = 5
REQUEST_TIMEOUT = 120
V4_BATCH_READ_URL = (
    "https://api.hubapi.com/crm/v4/associations/contacts/{to_type}/batch/read"
)

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.normpath(os.path.join(THIS_DIR, "..", ".."))
SETTINGS_PATH = os.path.normpath(os.path.join(THIS_DIR, "..", "config", "settings.yaml"))
OUTPUT_DIR = os.path.normpath(os.path.join(THIS_DIR, "..", "output"))


def load_settings() -> dict:
    with open(SETTINGS_PATH) as fh:
        return yaml.safe_load(fh)


def find_latest_name_only_xlsx() -> str:
    pattern = os.path.join(PROJECT_ROOT, "data", "name_only_matches_*.xlsx")
    matches = glob.glob(pattern)
    if not matches:
        raise FileNotFoundError(
            f"No file matched {pattern}. "
            "Run queries/python/name_only_to_excel.py first."
        )
    return max(matches, key=os.path.getmtime)


def load_pairs_from_xlsx(xlsx_path: str) -> dict[str, set[str]]:
    """Returns {current_vid: {historical_vid, ...}} from rows where the dedupe
    Record ID was already merged_into the surviving HubSpot contact.

    The set always includes the current_vid itself so the survivor's own
    associations get emitted with is_historical=false.
    """
    df = pd.read_excel(
        xlsx_path, sheet_name="Name Only Matches", dtype=str
    ).fillna("")

    required = {"Record ID", "current_contact_id", "match_status"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"{xlsx_path} sheet 'Name Only Matches' is missing columns: {missing}"
        )

    merged = df[df["match_status"].str.strip().str.lower() == "merged_into"]

    pairs: dict[str, set[str]] = defaultdict(set)
    for _, row in merged.iterrows():
        current_vid = str(row["current_contact_id"]).strip()
        historical_vid = str(row["Record ID"]).strip()
        if not current_vid or not historical_vid:
            continue
        pairs[current_vid].add(current_vid)
        pairs[current_vid].add(historical_vid)

    return pairs


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def hubspot_post(session: requests.Session, url: str, payload: dict) -> dict:
    for attempt in range(MAX_RETRIES):
        response = session.post(url, json=payload, timeout=REQUEST_TIMEOUT)

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

        if not response.ok:
            log.error("HubSpot %s: %s", response.status_code, response.text)
            response.raise_for_status()

        return response.json()

    raise RuntimeError(f"POST failed after {MAX_RETRIES} retries: {url}")


def chunks(items: list, size: int) -> Iterable[list]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def fetch_associations(
    session: requests.Session,
    contact_vids: list[str],
    to_object_type: str,
    batch_size: int,
) -> dict[str, list[str]]:
    """Returns {contact_vid: [associated_object_id, ...]} for one to_object_type."""
    associations: dict[str, list[str]] = defaultdict(list)
    url = V4_BATCH_READ_URL.format(to_type=to_object_type)
    total_batches = (len(contact_vids) + batch_size - 1) // batch_size

    for batch_index, batch in enumerate(chunks(contact_vids, batch_size), start=1):
        log.info(
            "Fetching %s associations: batch %d/%d (%d contacts)",
            to_object_type, batch_index, total_batches, len(batch),
        )
        payload = {"inputs": [{"id": vid} for vid in batch]}
        data = hubspot_post(session, url, payload)

        for result in data.get("results", []) or []:
            from_id = str((result.get("from") or {}).get("id", "")).strip()
            if not from_id:
                continue
            for to_item in result.get("to", []) or []:
                associated_id = str(to_item.get("toObjectId", "")).strip()
                if associated_id:
                    associations[from_id].append(associated_id)

    return associations


def main() -> None:
    settings = load_settings()
    crm_types = settings.get(
        "association_types", ["companies", "deals", "tickets"]
    )
    engagement_types = settings.get(
        "engagement_association_types", ["emails", "calls", "meetings"]
    )
    batch_size = (settings.get("batch_sizes") or {}).get("associations_per_page", 100)
    outputs = settings.get("outputs") or {}
    crm_output_filename = outputs.get("associations", "associations_export.csv")
    engagement_output_filename = outputs.get(
        "engagement_associations", "engagement_associations_export.csv"
    )
    test_mode_enabled = (settings.get("test_mode") or {}).get("enabled", False)
    test_mode_limit = (settings.get("test_mode") or {}).get("max_primary_vids", 25)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    crm_output_csv = os.path.join(OUTPUT_DIR, crm_output_filename)
    engagement_output_csv = os.path.join(OUTPUT_DIR, engagement_output_filename)

    xlsx_path = find_latest_name_only_xlsx()
    log.info("Reading name-only matches: %s", xlsx_path)

    pairs = load_pairs_from_xlsx(xlsx_path)
    log.info(
        "Loaded %d surviving primaries with name-only-merged history.", len(pairs)
    )

    if test_mode_enabled:
        sliced_keys = sorted(pairs.keys())[:test_mode_limit]
        pairs = {k: pairs[k] for k in sliced_keys}
        log.info("TEST MODE -- limited to %d primaries.", len(pairs))

    if not pairs:
        log.info("Nothing to fetch. Writing empty CSVs.")
        _write_csv(crm_output_csv, [])
        _write_csv(engagement_output_csv, [])
        return

    current_vids = sorted(pairs.keys())
    session = build_session()

    crm_rows = _collect_rows(session, current_vids, pairs, crm_types, batch_size)
    log.info("Writing %d rows to %s", len(crm_rows), crm_output_csv)
    _write_csv(crm_output_csv, crm_rows)

    engagement_rows = _collect_rows(
        session, current_vids, pairs, engagement_types, batch_size,
    )
    log.info(
        "Writing %d rows to %s", len(engagement_rows), engagement_output_csv,
    )
    _write_csv(engagement_output_csv, engagement_rows)
    log.info("Done.")


def _collect_rows(
    session: requests.Session,
    current_vids: list[str],
    pairs: dict[str, set[str]],
    assoc_types: list[str],
    batch_size: int,
) -> list[dict]:
    rows: list[dict] = []
    for assoc_type in assoc_types:
        association_map = fetch_associations(
            session, current_vids, assoc_type, batch_size,
        )
        for current_vid in current_vids:
            associated_ids = association_map.get(current_vid, [])
            if not associated_ids:
                continue
            historical_vids = sorted(pairs[current_vid])
            for associated_id in associated_ids:
                for historical_vid in historical_vids:
                    rows.append({
                        "historical_vid": historical_vid,
                        "current_vid": current_vid,
                        "is_historical": (
                            "true" if historical_vid != current_vid else "false"
                        ),
                        "associated_object_type": assoc_type,
                        "associated_object_id": associated_id,
                        "association_timestamp": "",
                    })
    return rows


def _write_csv(output_csv: str, rows: list[dict]) -> None:
    fieldnames = [
        "historical_vid",
        "current_vid",
        "is_historical",
        "associated_object_type",
        "associated_object_id",
        "association_timestamp",
    ]
    with open(output_csv, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
