# Unmerge — Brief & Test Plan

## Dry-run results

We ran the unmerge dry-run on a known name-only-merged contact (Jonathan Hines, survivor `211371804173`). The script produced [output/unmerge_dryrun.csv](output/unmerge_dryrun.csv) showing:

- 2 historical (pre-merge) Record IDs to recover, both flagged `would_create=yes`
- Survivor's current associations on the row: `1 company, 136 emails, 3 calls, 8 meetings`

Every associated object would be reattached to the new contacts. No data is dropped; the survivor keeps its associations and the new contacts get a copy.

## How it works

For a given survivor `current_contact_id`:

1. **Read the audit xlsx** (`data/name_only_matches_*.xlsx`) and pull every row where `match_status = merged_into`. Each row gives us the pre-merge `Record ID`, `First Name`, `Last Name`, `Email`, `Phone Number`, `Create Date`.
2. **Create one new contact per historical Record ID** with those recovered fields.
3. **Read every current association on the survivor** (companies, deals, tickets, emails, calls, meetings).
4. **Attach all of those associations to each new contact.**

Result: the survivor still exists with its associations; in addition, each historical now lives as a real contact again, sharing the same association graph.

## Endpoints

Three HubSpot v3/v4 endpoints make this possible:

- **`POST /crm/v3/objects/contacts`** — creates the recovered contact with `firstname`, `lastname`, `email`, `phone`.
- **`POST /crm/v4/associations/contacts/{toObjectType}/batch/read`** — pulls every association currently on the survivor in one call per object type.
- **`POST /crm/v4/associations/contacts/{toObjectType}/batch/create`** — attaches those associations to the newly created contact using `HUBSPOT_DEFINED` default association types (typeIds discovered at runtime via `/crm/v4/associations/contact/{toType}/labels`, so no hardcoded numbers).

The reason this works without conflicts: HubSpot allows the same email, call, meeting, deal, etc. to be associated with multiple contacts. We're not *moving* associations off the survivor — we're *adding* the same associations to the new contacts.

## Test scenarios (sandbox first)

| # | Scenario | Steps | Verify |
|---|---|---|---|
| 1 | Plain merge → unmerge | In sandbox, manually merge two test contacts that have a few engagements/companies attached. Add the surviving contact ID + Record IDs to a temp xlsx in the same shape as `name_only_matches_*.xlsx`. Run `unmerge_contact.py --contact-id <survivor> --apply`. | Two new contacts created with the original identities; both have copies of every association the survivor had. |
| 2 | Name-only-match merge → unmerge | In sandbox, create two test contacts with the same first/last name and different emails, merge them, then run unmerge against the survivor. | Same as #1 — confirms the script works on the exact merge type our production data has. |
| 3 | Production go/no-go review | Walk through the sandbox results in a working session. Spot-check 5 contacts in HubSpot UI. If clean, schedule a small production batch (5–10 survivors) before any sweep. | New contacts visible in HubSpot UI with correct identity and full association history. |

## Status

- Dry-run is working today against production reads (`HUBSPOT_ACCESS_TOKEN_TS`).
- `--apply` mode is wired but **has not been run anywhere yet**. Sandbox testing per scenarios #1 and #2 is the next step before any write touches production.
- Run details and full CSV column reference: [TEST_STEPS.md](TEST_STEPS.md).
