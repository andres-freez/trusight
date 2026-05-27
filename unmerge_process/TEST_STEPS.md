# Unmerge Process — Test Steps

Phase 2 unmerge script for HubSpot contacts that were merged on name-only
match. The script reads `data/name_only_matches_*.xlsx`, recreates each
historical (pre-merge) Record ID as a new contact, and (when implemented)
reattaches attributable associations.

**The script is dry-run by default. Nothing writes to HubSpot without `--apply`.**

---

## Prerequisites

1. Pull latest from `main`.
2. `.env` at project root contains `HUBSPOT_ACCESS_TOKEN_TS` (production read token).
3. `data/name_only_matches_2026_04_30_094303.xlsx` (or newer) is present.
4. Python venv activated and dependencies installed:
   ```
   .venv/bin/pip install -r requirements.txt
   ```
   (`PyYAML`, `pandas`, `requests`, `openpyxl`, `python-dotenv` are required.)

---

## Test 1 — Dry-run on a known recoverable contact

Survivor `211371804173` (Jonathan Hines) absorbs two historical records, both
of which have emails not on the survivor — so both are flagged for creation.

```
.venv/bin/python unmerge_process/imports/unmerge_contact.py --contact-id 211371804173
```

**Expected log output:**

```
Survivor 211371804173 -- 2 historical record(s) to recover.
Survivor 211371804173 associations: companies=1, deals=0, tickets=0, emails=136, calls=3, meetings=8
Writing 2 rows to .../output/unmerge_dryrun.csv
Done.
```

**Expected CSV:** `unmerge_process/output/unmerge_dryrun.csv` — 2 rows, both with `would_create=yes`, `status=DRY_CREATE`. The first row carries the survivor's lost-association counts; the second row's lost columns are zeros (lost is per-survivor, attached to the first historical row to avoid double-counting).

---

## Test 2 — Dry-run on a contact whose historicals get skipped

Survivor `211357068713` (Jon Gattman). Both historicals have emails already on the survivor, so the script will skip both.

```
.venv/bin/python unmerge_process/imports/unmerge_contact.py --contact-id 211357068713
```

**Expected CSV rows:** `would_create=no` with `skip_reason` of either `email_matches_survivor_primary` or `email_in_survivor_additional_emails`. This is the typical post-merge state — to actually unmerge these, we will need to first remove the email from the survivor's `hs_additional_emails` before creating the new contact.

---

## What to verify in the CSV

| Column | Check |
|---|---|
| `current_vid` | Matches the survivor you targeted |
| `historical_record_id` | Pre-merge Record ID being recovered |
| `recovered_*` | Identity fields lifted from the xlsx |
| `would_create` / `skip_reason` | Logic gating creation |
| `attributed_*` | Per-historical association counts that would migrate to the new contact |
| `lost_*` | Survivor associations that would stay orphaned (only populated on the first historical row per survivor) |
| `lost_association_ids` | Pipe-delimited `type:id` list of every orphaned association |
| `status` | `DRY_CREATE` / `DRY_SKIP` in dry-run; `CREATED` / `SKIPPED` / `FAILED_CREATE` in `--apply` |

---

## Known limitations (Phase 2, current state)

1. **Attribution is email-only for engagements.** Calls and meetings always land in `lost_calls` / `lost_meetings` until phone-matching or audit-log attribution is added.
2. **CRM objects (companies / deals / tickets) all land in `lost_*`.** Audit-log attribution not yet implemented.
3. **Notes and tasks are not pulled.** The script only fetches `companies, deals, tickets, emails, calls, meetings`. If a survivor has notes/tasks, they won't appear in the dry-run.
4. **Survivor email collisions block creation.** Most historicals' emails got merged into the survivor's `hs_additional_emails`. Until we PATCH those off the survivor first, those rows will skip with `email_in_survivor_additional_emails`.
5. **No-email historicals skip with `no_email`.** Decision pending: create them anyway with just name + phone, or leave skipped.

---

## DO NOT run `--apply` yet

`--apply` writes to HubSpot:
- Creates new contacts via `POST /crm/v3/objects/contacts`
- Attaches associations via `POST /crm/v4/associations/contacts/{type}/batch/create`

Hold off until the limitations above are addressed and the dry-run output has been spot-checked across more survivors.
