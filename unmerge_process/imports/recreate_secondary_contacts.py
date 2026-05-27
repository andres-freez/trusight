"""[PHASE 2 -- SCAFFOLD ONLY] Recreate secondary contacts as new HubSpot records.

Single responsibility: read output/unmerge_candidates.csv filtered to
separability_class = CONFIDENT. For each row, create a new HubSpot contact
populated from:
    - secondary_email_recovered (becomes the new contact's email)
    - pre-merge values from output/property_history.csv for firstname,
      lastname, phone, company, jobtitle, etc.
Write a mapping CSV old secondary_vid -> new contact vid for downstream
import scripts to consume.

Reads:  output/unmerge_candidates.csv, output/property_history.csv
Writes: output/recreated_contacts.csv
        Columns: secondary_vid, primary_vid, new_vid, status, error_message

This file is a stub. Phase 2 work begins only after the candidate list has
been reviewed and CONFIDENT pairs have been audited by a human.
"""


def main() -> None:
    raise NotImplementedError("Phase 2 -- pending candidate review")


if __name__ == "__main__":
    main()
