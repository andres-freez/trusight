"""Holds the separability classification rule engine and, when run as a
script, produces output/unmerge_summary.csv (rollup by class).

Single responsibility: own the rules that map a candidate pair's evidence
(recovered email, attributable engagement count, ambiguous engagement count)
onto one of CONFIDENT / PROBABLE / NOT_SEPARABLE, plus a short reason
string. Imported by build_unmerge_candidates.py to label each pair as the
candidate table is assembled. Pure CSV-to-CSV; no HubSpot API calls.

Library surface (used by build_unmerge_candidates.py):
    classify_pair(evidence: dict, thresholds: dict) -> (class_label, reason)

Script behaviour: reads output/unmerge_candidates.csv and rolls counts up
into the summary CSV.

CSV columns (output/unmerge_summary.csv):
    separability_class       -- CONFIDENT | PROBABLE | NOT_SEPARABLE
    pair_count
    percent_of_total

Thresholds come from config/settings.yaml::classification.
Idempotent: overwrites the CSV on each run.
"""


def main() -> None:
    raise NotImplementedError("Scaffolding only. Awaiting approval before adding classification logic.")


if __name__ == "__main__":
    main()
