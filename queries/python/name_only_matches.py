"""
Reads the most recent dedupe_results_*.csv (produced by dedupe_contacts.py)
and isolates groups that were matched ONLY by the exact_name rule -- i.e.
no phone match and no compatible email-local part. Writes those rows to a
timestamped CSV for manual audit.

Requires dedupe_contacts.py to have been re-run after the match_rule column
was added.

Usage:
    python queries/python/name_only_matches.py

Env overrides:
    DEDUPE_RESULTS_CSV   input file (default: latest ./data/dedupe_results*.csv)
    NAME_ONLY_OUTPUT     output file (default: ./data/name_only_matches_<timestamp>.csv)
"""
import glob
import os
import time
from collections import Counter

import pandas as pd

REQUIRED_COLUMNS = ["dedupe_status", "primary_record_id", "match_rule"]


def find_latest_dedupe_results() -> str:
    matches = sorted(glob.glob("./data/dedupe_results*.csv"))
    if not matches:
        raise FileNotFoundError(
            "No ./data/dedupe_results*.csv found. Run dedupe_contacts.py first "
            "or set DEDUPE_RESULTS_CSV."
        )
    return matches[-1]


def main() -> None:
    input_csv = os.getenv("DEDUPE_RESULTS_CSV") or find_latest_dedupe_results()
    timestamp = time.strftime("%Y_%m_%d_%H%M%S")
    output_csv = os.getenv(
        "NAME_ONLY_OUTPUT", f"./data/name_only_matches_{timestamp}.csv"
    )

    print(f"Reading: {input_csv}")

    df = pd.read_csv(input_csv, dtype=str).fillna("")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns: {missing}. "
            "Re-run dedupe_contacts.py to regenerate dedupe_results.csv."
        )

    in_groups = df[df["dedupe_status"].str.strip().str.lower() != "unique"].copy()

    primary_rows = in_groups[in_groups["dedupe_status"].str.strip().str.lower() == "primary"]
    rule_counts = Counter(primary_rows["match_rule"])

    print("Groups by match_rule (counted at the primary row):")
    for rule, count in sorted(rule_counts.items(), key=lambda x: (-x[1], x[0])):
        label = rule if rule else "(empty)"
        print(f"  {label!r}: {count}")
    print(f"  total groups: {len(primary_rows)}")

    name_only = in_groups[in_groups["match_rule"] == "exact_name"].copy()

    output_dir = os.path.dirname(output_csv)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    name_only.to_csv(output_csv, index=False)

    print(f"\nName-only matches saved to: {output_csv}")
    print(f"  rows: {len(name_only)}")
    print(f"  groups: {name_only['primary_record_id'].nunique()}")


if __name__ == "__main__":
    main()
