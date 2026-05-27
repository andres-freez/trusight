# Unmerge Process

Read-only audit pipeline for HubSpot contacts that were merged on **name-only**
signal. Phase 1 (this README) **does not write back to HubSpot.** It produces
CSVs that classify each merged pair as safe to unmerge, needs review, or
unrecoverable. Phase 2 (writes) is scaffolded only and runs later, after the
Phase 1 candidate list has been reviewed.

## Layout

```
unmerge_process/
├── exports/    # Phase 1 -- HubSpot pulls (one CSV out per script)
├── analysis/   # Phase 1 -- CSV-to-CSV transforms (no API calls)
├── imports/    # Phase 2 -- SCAFFOLD ONLY; raises NotImplementedError
├── output/     # all CSVs land here
└── config/
    └── settings.yaml
```

## Phase 1 -- run order

Run from the project root so relative paths resolve.

```
# 1. Pulls (exports/ -- live HubSpot data)
python unmerge_process/exports/pull_merged_contacts.py
python unmerge_process/exports/pull_engagements_for_merged.py
python unmerge_process/exports/pull_property_history.py
python unmerge_process/exports/pull_associations.py
python unmerge_process/exports/pull_audit_log_merges.py     # Enterprise only; skips otherwise

# 2. Analysis (analysis/ -- CSV-to-CSV)
python unmerge_process/analysis/build_unmerge_candidates.py
python unmerge_process/analysis/classify_separability.py
```

Every script is idempotent. Re-running overwrites its CSV cleanly.

## CSV contract (output/)

| File                          | Source script                                | Grain                              |
|-------------------------------|----------------------------------------------|------------------------------------|
| merged_contacts.csv           | exports/pull_merged_contacts.py              | one row per primary with merge history |
| engagements_for_merged.csv    | exports/pull_engagements_for_merged.py       | one row per engagement on a primary |
| property_history.csv          | exports/pull_property_history.py             | one row per identity-property change |
| associations.csv              | exports/pull_associations.py                 | one row per association on a primary |
| audit_log_merges.csv          | exports/pull_audit_log_merges.py             | one row per merge event (Enterprise) |
| unmerge_candidates.csv        | analysis/build_unmerge_candidates.py         | one row per (primary, secondary) pair |
| unmerge_summary.csv           | analysis/classify_separability.py            | one row per separability_class |

Column lists live in each script's docstring -- those are the contract.

## Phase 2 -- imports/ (NOT YET IMPLEMENTED)

Phase 2 reverses the wrong merges by **rebuilding** each separable secondary
as a brand-new HubSpot contact populated from recovered historical data,
then re-attaching engagements and associations to the new contact.
HubSpot has no public unmerge API -- this is the workaround.

Every file in `imports/` is a stub that raises `NotImplementedError`. They
will be implemented only after the Phase 1 candidate list is human-reviewed.

| File                                       | Will do                                                              |
|--------------------------------------------|----------------------------------------------------------------------|
| imports/recreate_secondary_contacts.py     | create new HubSpot contacts for CONFIDENT secondaries                |
| imports/reassign_engagements.py            | move attributable engagements from primary to new contact            |
| imports/split_associations.py              | move companies / deals / tickets that should follow the new contact  |
| imports/verify_unmerge.py                  | post-run verification report                                         |

## Config

`config/settings.yaml` holds the merge cutoff date, batch sizes, output
filenames, and classification thresholds. Secrets stay in `.env`
(`HUBSPOT_ACCESS_TOKEN_TS`).

## Reuses

The `exports/` scripts reuse the auth + retry + pagination patterns already
established in `hubspot_export.py` at the project root. They do not reinvent
the HubSpot client layer.
