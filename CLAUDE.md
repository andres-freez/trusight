# CLAUDE.md — HubSpot CRM Data Management Project

## Project Purpose

This project manages HubSpot CRM data hygiene: exporting objects, deduplicating records,
merging duplicates, creating associations, and filling/enriching property values via the
HubSpot API. It handles contacts, companies, deals, and custom objects.
The goal is a clean CRM with clean code — even when work is exploratory or one-off in nature.

---

## Canonical File Structure

```
project-root/
│
├── CLAUDE.md                            # This file
├── .env                                 # API keys and secrets — never commit
├── .nvenv                               # Environment/runtime config and parameters
│
├── queries/
│   ├── python/                          # Python-based data queries and exploration
│   └── sql/                             # SQL queries for visualization and analysis
│
├── data/
│   ├── csv/                             # Raw and processed CSV exports
│   ├── parquet/                         # Parquet format data files
│   └── json/                            # JSON format data files
│
├── merge_process/
│   └── <object_name>/                   # One subfolder per HubSpot object type
│       ├── <object_name>_export.py      # Step 1: Pull data from HubSpot
│       ├── dedupe_<object_name>.py      # Step 2: Identify duplicates
│       └── merge_<object_name>.py       # Step 3: Execute merges via API
│
├── <process_name>_process/              # Multi-step fill / enrichment workflows (see below)
│   ├── *.py                             # Scripts for each step of the process
│   └── cleanup_files/                   # Intermediate working CSVs (not in data/csv/)
│
└── misc/                                # One-off and exploratory scripts (see policy below)
```

### Real Example — Management Companies Object

```
merge_process/
└── management_companies/
    ├── management_companies_export.py
    ├── dedupe_management_companies.py
    └── merge_management_companies.py
```

### Real Example — Companies Object

```
merge_process/
└── companies/
    ├── companies_export.py
    ├── dedupe_companies.py
    └── merge_companies.py
```

### Real Example — Dual Return Process

```
dual_return_process/
├── deal_export.py                           # Step 1: Export all deals
├── dual_return.py                           # Step 2: Pair dual-return deals per contact
├── dual_return_deal_associations.py         # Step 3: Create labeled deal-to-deal associations
└── cleanup_files/
    ├── dual_return_pairs.csv                # Intermediate: paired deal IDs
    ├── dual_return_unpaired.csv             # Intermediate: exception log
    └── dual_return_association_results.csv  # Intermediate: API result log
```

### Real Example — New/Repeat Investor Fill

```
new_repeat_investor/
├── deal_export.py      # Step 1: Export deals with dual-return association IDs
└── investor_fill.py    # Step 2: Classify & fill new__repeat_investor property on deals
```

**Companies-specific notes:**
- Export saves to `data/csv/companies/companies.csv` (no date suffix — overwrite on re-run)
- Dedupe uses 4-tier UnionFind blocking: domain → name+phone → name+state → name_only
- Records with a domain also participate in name-based blocking (catches same-name pairs where one record has a domain and one does not)
- Dedupe output columns: `hs_object_id`, `name`, `domain`, `phone`, `state`, `dedupe_status`, `primary_record_id`, `duplicate_record_ids`, `match_rule`
- Winner selection: most populated fields (phone > state > domain), then lowest record ID
- `COMPANIES_OBJECT_TYPE_ID` defaults to `0-2` in the script if not set in `.env`
- Always test with `TEST_MODE = True` and a known record ID before full run
- Flip `DRY_RUN = False` only after test is verified in HubSpot

---

## File Naming Conventions

| Script Type              | Pattern                      | Example                                |
|--------------------------|------------------------------|----------------------------------------|
| Export                   | `<object_name>_export.py`    | `management_companies_export.py`       |
| Dedupe logic             | `dedupe_<object_name>.py`    | `dedupe_management_companies.py`       |
| Merge execution          | `merge_<object_name>.py`     | `merge_management_companies.py`        |
| Fill / enrichment script | descriptive name             | `investor_fill.py`, `dual_return.py`   |
| One-off scripts          | anything                     | goes in `misc/` — no exceptions        |

- Object names are **snake_case** and must match exactly across all three files in a group
- Never abbreviate object names inconsistently (e.g., don't mix `mgmt` and `management`)
- The three files for one object must share the identical `<object_name>` token
- Fill/enrichment scripts use descriptive action names, not object-name prefixes

---

## The Three-Step Merge Process

Every HubSpot object follows this exact workflow:

### Step 1 — Export (`<object_name>_export.py`)
- Pulls data from HubSpot via API or CSV export
- Saves output to `data/csv/<object_name>/` or `data/parquet/<object_name>/`
- Should be idempotent — safe to re-run without side effects
- Output files should be timestamped: `management_companies_2024_01_15.csv`
- Never overwrite a previous export — always use a date suffix

### Step 2 — Dedupe (`dedupe_<object_name>.py`)
- Reads from the exported data file produced in Step 1
- Matching criteria **will vary per object** — this is expected and intentional
- The script header must document what fields are used and what thresholds apply
- Outputs a dedupe candidate map to `data/csv/<object_name>/dedupe_candidates_<object_name>.csv`
- Does NOT touch HubSpot — this is a read-only analysis step

### Step 3 — Merge (`merge_<object_name>.py`)
- Reads the dedupe candidate map produced in Step 2
- Calls the HubSpot Merge API to combine duplicate records
- Must have a `DRY_RUN` mode (env var or CLI flag) that logs actions without calling the API
- Logs every merge action: winner record ID, loser record ID, timestamp
- Never deletes records — only merges

---

## Fill / Enrichment Processes

A fill process reads existing HubSpot records, derives or classifies a property value,
and writes it back via the API. This is distinct from the export/dedupe/merge workflow.

### When to use a `<process_name>_process/` folder

Use a dedicated top-level `<process_name>_process/` folder (not `merge_process/`) when:
- The workflow has 2+ sequential scripts that pass intermediate files to each other
- The process involves association creation, property fills, or enrichment — not record merging
- Intermediate working files exist that don't belong in `data/csv/` (use `cleanup_files/` instead)

### `cleanup_files/` subfolder convention

Place intermediate pipeline outputs (pairs CSVs, exception logs, association result logs)
inside `<process_name>_process/cleanup_files/`, **not** in `data/csv/`. These are working
artifacts, not canonical data exports. Final output CSVs that downstream scripts consume
from `data/csv/` still go in `data/csv/`.

### Fill process step pattern

Fill processes generally follow this sequence:
1. **Export** — pull records + any needed association data (`deal_export.py`)
2. **Prepare** — identify/pair/classify records (pure Python, no API writes)
3. **Fill** — batch-update the target property on HubSpot records

### Event-based classification

When deals must be classified per contact (e.g., New vs. Repeat Investor), use an
**event collapse** approach before chronological classification:
- Group deals by Contact ID
- Collapse dual-return pairs into single logical events using the association link
- Classify events chronologically (first closed event → "New"; subsequent → "Repeat")
- Map event classification back to individual deal IDs for the batch update

### `|`-delimited association IDs

When a deal has multiple associated deal IDs stored in a CSV column, encode them as a
pipe-delimited string (e.g., `"12345|67890"`). Parse with `str.split("|")` and strip whitespace.

---

## Enforced File Placement Rules

When writing or suggesting files, always follow this routing table:

| Content Type                          | Correct Location                        |
|---------------------------------------|-----------------------------------------|
| Data queries (Python)                 | `queries/python/`                       |
| Data queries (SQL)                    | `queries/sql/`                          |
| CSV data files (canonical exports)    | `data/csv/`                             |
| Parquet data files                    | `data/parquet/`                         |
| JSON data files                       | `data/json/`                            |
| Export + dedupe + merge scripts       | `merge_process/<object_name>/`          |
| Fill / enrichment multi-step process  | `<process_name>_process/`               |
| Intermediate pipeline working files   | `<process_name>_process/cleanup_files/` |
| Everything else                       | `misc/`                                 |

**Never place scripts in the project root.**
If a file doesn't clearly belong in `merge_process/`, `<process>_process/`, or `queries/`,
it goes in `misc/`.
**When the correct location is ambiguous, ask before creating the file.**

---

## Misc Folder Policy

`misc/` exists to contain one-off scripts without polluting the rest of the project.
Claude Code should:
- Route exploratory or one-off work here without complaint
- **Never** encourage expanding `misc/` when a proper structured home exists
- Periodically flag files in `misc/` that look like they've matured into real workflow
  scripts, and suggest promoting them to `merge_process/<object_name>/`

The existence of `misc/` is not an invitation to be messy — it's a quarantine zone.

---

## Environment and Secrets

- `.env` — HubSpot API keys, OAuth tokens, sensitive config.
  **Never read aloud, log, print, or hardcode these values under any circumstances.**
- `.nvenv` — Runtime parameters (batch sizes, object type flags, environment toggles).
  Safe to reference and suggest edits to.
- Always load `.env` using `python-dotenv` with `load_dotenv(override=True)` so the `.env` file
  takes precedence over stale shell environment variables — never `os.environ["KEY"] = "value"`
- Never suggest hardcoding credentials, even as placeholder comments like `# YOUR_KEY_HERE`

---

## HubSpot-Specific Coding Standards

### API Patterns
- Use the **HubSpot Python SDK** (`hubspot-api-client`) when available
- Always add **retry logic with exponential backoff** — HubSpot enforces per-second and
  per-day rate limits
- Use **batch endpoints** for bulk reads/writes — never loop single-record endpoints
- Always handle **pagination** for all list and search endpoints

### HubSpot v4 Associations API

Use the v4 associations endpoint for labeled (custom) deal-to-deal or object-to-object associations.

**Batch read existing associations:**
```
POST /crm/v4/associations/deals/deals/batch/read
{"inputs": [{"id": "<deal_id>"}, ...]}
```
- Response field names vary; check both `associationTypes` and `types`, both `typeId` and `associationTypeId`
- Filter by `typeId` + `category` to isolate a specific labeled association

**Batch create labeled associations:**
```
POST /crm/v4/associations/deals/deals/batch/create
{"inputs": [{"from": {"id": "..."}, "to": {"id": "..."}, "types": [{"associationCategory": "USER_DEFINED", "associationTypeId": 7}]}]}
```
- `associationTypeId: 7` is the Dual Return label for this portal — confirm in HubSpot before re-using
- Always pre-check for existing associations before creating to keep scripts idempotent

**Idempotency pre-check pattern:**
Before writing an association, fetch existing ones for both deals and skip if the link already exists.
Log skipped rows as `already_associated` in the results CSV.

### `COLUMN_MAP` Pattern

Export scripts should define a `COLUMN_MAP` dict that maps HubSpot internal property names
to human-readable CSV column names. This makes column renaming explicit and auditable:

```python
COLUMN_MAP = {
    "hs_object_id": "Deal ID",
    "createdate": "Create Date",
    "hs_is_closed_won": "Is Closed Won?",
    ...
}
```

Use `COLUMN_MAP` when writing CSVs so downstream scripts reference friendly column names,
not HubSpot internal names. Always list the fieldnames from `COLUMN_MAP.values()`.

### Merge Safety
- Every merge script must support a `DRY_RUN` flag before going live
- Log winner ID, loser ID, object type, and UTC timestamp for every merge operation
- Never delete records — the HubSpot Merge API preserves the winner; use it correctly

### Dedupe Standards
- Document matching logic in the script header: fields used, match type, score thresholds
- For fuzzy name matching, use `rapidfuzz` with explicit score thresholds (don't guess)
- Always produce a human-readable CSV of candidates before any merges run
- No black-box merges — a non-technical person should be able to audit the candidate file

### Data Files
- Parquet preferred over CSV for datasets larger than 50k rows
- Use `COLUMN_MAP` to give CSV columns human-readable names (see above) — not raw HubSpot internal names
- Never modify source export files — write new files for transformed/cleaned versions

---

## Rules Claude Code Must Always Follow

1. **Ask before creating files** if the correct folder is ambiguous
2. **Match naming conventions exactly** — if the object is `management_companies`,
   every file in the group uses that full name with no variation
3. **Keep `misc/` honest** — route loose scripts there, but flag anything that belongs
   in a structured folder
4. **Require dry-run mode** on every script that writes to HubSpot — including fill scripts
5. **Include logging** in all export, merge, and fill scripts
6. **Read runtime config from `.nvenv`** — don't hardcode parameters that belong there
7. **Check for existing associations** before creating new ones — all association scripts must be idempotent
8. **Log all API write results** — fill and association scripts must write a results CSV with status + error_message per row
9. **Validate input CSV columns** at the top of every script that reads a CSV — raise early with a clear message listing missing columns

## Rules Claude Code Must Never Break

1. Place scripts in the project root
2. Create a new top-level folder without asking first
3. Abbreviate or rename object names inconsistently within a script group
4. Suggest hardcoding API keys, tokens, or credentials in any form
5. Write a merge or fill script without a dry-run mode
6. Delete HubSpot records — merge only, always
7. Write intermediate pipeline files to `data/csv/` — those go in `cleanup_files/` inside the process folder
