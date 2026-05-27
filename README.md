# TruSight — HubSpot CRM Data Management

Python scripts for HubSpot CRM hygiene: deduplication, merging, enrichment, and
unmerging of contacts, companies, and deals via the HubSpot API.

---

## Project Structure

```
trusight/
│
├── hubspot_export.py                    # General-purpose HubSpot object exporter
├── hubspot_no_vid_export.py             # Export contacts without a vid
├── contact_email_company_export.py      # Contact + company email export
├── merge_contacts.py                    # Merge duplicate contacts
├── merge_companies.py                   # Merge duplicate companies
├── dedupe_contacts.py                   # Identify duplicate contacts
├── dedupe_companies.py                  # Identify duplicate companies
├── merge_recovery.py                    # Recover data from bad merges
├── updated_by.py                        # Track last-updated-by on records
├── activtities.py                       # Contact activity export
├── contact_activites.py                 # Contact activity utilities
│
├── company_name_fill_process/           # Fill missing company name properties
│   ├── prepare_company_name_updates.py  # Step 1: identify missing names
│   └── company_name_fill.py             # Step 2: write names back via API
│
├── unmerge_process/                     # Reverse name-only-match merges
│   ├── config/settings.yaml             # Batch sizes, thresholds, output filenames
│   ├── exports/                         # Phase 1: pull data from HubSpot
│   │   ├── pull_merged_contacts.py      # Survivors + merged vids
│   │   ├── pull_engagements_for_merged.py
│   │   ├── pull_property_history.py
│   │   ├── associations_export.py       # CRM + engagement associations
│   │   └── pull_audit_log_merges.py     # Enterprise audit log
│   ├── analysis/                        # Phase 1: CSV-to-CSV transforms
│   │   ├── build_unmerge_candidates.py
│   │   └── classify_separability.py
│   ├── imports/                         # Phase 2: write back to HubSpot
│   │   └── unmerge_contact.py           # ← main unmerge script (see below)
│   └── output/                          # All CSVs and Excel results land here
│
├── queries/
│   ├── python/                          # DuckDB / pandas data queries
│   └── sql/                             # SQL for analysis and visualization
│
├── sql/                                 # Ad-hoc SQL queries
├── misc/                                # One-off and exploratory scripts
│
├── requirements.txt                     # Python dependencies
├── .env                                 # API keys — never committed
└── CLAUDE.md                            # AI coding conventions for this project
```

---

## Setup

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Copy `.env.example` to `.env` and fill in your HubSpot tokens:

```
HUBSPOT_ACCESS_TOKEN_TS=your_production_token
HUBSPOT_KEY_TS_SANDBOX=your_sandbox_token
```

---

## Unmerge Process

The main active workflow. Reverses HubSpot contacts that were incorrectly
merged on name-only match by recreating each historical Record ID as a new
contact and reattaching all associations.

### Quick start

```bash
# Test mode — edit TEST_CONTACT_IDS in the script, then:
python unmerge_process/imports/unmerge_contact.py

# Single contact dry-run (no writes)
python unmerge_process/imports/unmerge_contact.py --contact-id <survivor_id>

# Apply (writes to HubSpot)
python unmerge_process/imports/unmerge_contact.py --contact-id <survivor_id> --apply
```

### How it works

1. Fetches the survivor contact from HubSpot (`GET /crm/v3/objects/contacts/{id}`)
2. Validates `hs_merged_object_ids` is present — raises if not a merged contact
3. Recovers historical email identities from the V1 identity-profiles endpoint
4. Creates one new contact per historical Record ID (`POST /crm/v3/objects/contacts`)
5. Attaches all survivor associations to each new contact (`POST /crm/v4/associations/...`)

### Output

Results written to `unmerge_process/output/unmerge_results_<timestamp>.xlsx` with:

| Column | Description |
|---|---|
| `survivor_id` | The surviving (merged-into) contact ID |
| `old_contact_id` | The historical pre-merge Record ID |
| `status` | `CREATED` / `SKIPPED` / `FAILED_CREATE` |
| `runtime` | UTC timestamp of the run |
| `survivor_url` | HubSpot link to the survivor |
| `new_contact_url` | HubSpot link to the newly created contact |
| `associated_*` | Count of associations attached per object type |

### Key flags (top of `unmerge_contact.py`)

| Flag | Default | Description |
|---|---|---|
| `TEST_MODE` | `True` | Use `TEST_CONTACT_IDS` list instead of CLI args |
| `TEST_APPLY` | `True` | `False` = dry-run in test mode |
| `TEST_CONTACT_IDS` | `[...]` | Contacts to process in test mode |
| `REQUIRE_EMAIL` | `False` | `True` = skip contacts with no recoverable email |

---

## Merge / Dedupe Workflow

Standard three-step pattern for any object type:

```bash
# 1. Export
python <object>_export.py

# 2. Dedupe (read-only — produces candidate CSV)
python dedupe_<object>.py

# 3. Merge (requires DRY_RUN=False to write)
python merge_<object>.py
```

---

## Environment

| File | Purpose |
|---|---|
| `.env` | HubSpot tokens and secrets — never commit |
| `CLAUDE.md` | Coding conventions enforced by Claude Code |
| `unmerge_process/config/settings.yaml` | Runtime config for the unmerge process |
