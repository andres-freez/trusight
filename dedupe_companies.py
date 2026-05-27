import os
import re
from collections import defaultdict

import pandas as pd
from dotenv import load_dotenv


load_dotenv()

INPUT_CSV_COMPANIES = os.getenv("INPUT_CSV_COMPANIES", "companies.csv")
OUTPUT_CSV_COMPANIES = os.getenv("OUTPUT_CSV_COMPANIES", "dedupe_results_companies.csv")

REQUIRED_COLUMNS = [
    "Record ID",
    "Company Name",
    "Domain",
    "Create Date",
]


def normalize_text(value: str) -> str:
    if pd.isna(value):
        return ""
    value = str(value).strip().lower()
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_domain(value: str) -> str:
    """
    Normalize company domain for duplicate matching.
    Examples:
    https://www.hfireholdings.com -> hfireholdings.com
    www.hfireholdings.com -> hfireholdings.com
    HFireHoldings.com -> hfireholdings.com
    """
    value = normalize_text(value)

    if not value:
        return ""

    value = re.sub(r"^https?://", "", value)
    value = re.sub(r"^www\.", "", value)
    value = value.strip("/")
    value = value.split("/")[0]
    return value


def validate_columns(df: pd.DataFrame) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def safe_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def choose_primary_record(group_df: pd.DataFrame) -> pd.Series:
    """
    Newest Create Date wins.
    Tiebreaker: highest Record ID.
    """
    temp = group_df.copy()
    temp["create_date_parsed"] = safe_datetime(temp["Create Date"])

    def record_id_sort_key(val):
        try:
            return int(str(val))
        except Exception:
            return -1

    temp["record_id_sort"] = temp["Record ID"].apply(record_id_sort_key)

    temp = temp.sort_values(
        by=["create_date_parsed", "record_id_sort"],
        ascending=[False, False],
        na_position="last",
    )

    return temp.iloc[0]


def build_duplicate_groups(df: pd.DataFrame) -> list[list[int]]:
    """
    Build duplicate groups based only on normalized domain.
    """
    groups = []

    domain_blocks = defaultdict(list)

    for idx, row in df.iterrows():
        domain = row["domain_norm"]
        if domain:
            domain_blocks[domain].append(idx)

    for block in domain_blocks.values():
        if len(block) > 1:
            groups.append(sorted(block))

    return groups


def main() -> None:
    df = pd.read_csv(INPUT_CSV_COMPANIES, dtype=str).fillna("")
    validate_columns(df)

    df["domain_norm"] = df["Domain"].apply(normalize_domain)

    duplicate_groups = build_duplicate_groups(df)

    # Start with all original companies preserved
    output_df = df.copy()
    output_df["dedupe_status"] = "unique"
    output_df["primary_record_id"] = ""
    output_df["duplicate_record_ids"] = ""

    for group_indexes in duplicate_groups:
        group_df = df.loc[group_indexes].copy()
        primary = choose_primary_record(group_df)
        primary_id = str(primary["Record ID"])

        duplicate_ids = [
            str(record_id)
            for record_id in group_df["Record ID"].tolist()
            if str(record_id) != primary_id
        ]
        duplicate_ids_str = ",".join(duplicate_ids)

        for idx in group_indexes:
            current_record_id = str(output_df.at[idx, "Record ID"])
            output_df.at[idx, "primary_record_id"] = primary_id
            output_df.at[idx, "duplicate_record_ids"] = duplicate_ids_str

            if current_record_id == primary_id:
                output_df.at[idx, "dedupe_status"] = "primary"
            else:
                output_df.at[idx, "dedupe_status"] = "duplicate"

    output_df = output_df.drop(columns=["domain_norm"])

    output_dir = os.path.dirname(OUTPUT_CSV_COMPANIES)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    output_df.to_csv(OUTPUT_CSV_COMPANIES, index=False)

    print(f"Input file: {INPUT_CSV_COMPANIES}")
    print(f"Output file: {OUTPUT_CSV_COMPANIES}")
    print(f"Total input companies: {len(df)}")
    print(f"Duplicate groups found: {len(duplicate_groups)}")
    print(f"Primary companies: {(output_df['dedupe_status'] == 'primary').sum()}")
    print(f"Duplicate companies: {(output_df['dedupe_status'] == 'duplicate').sum()}")
    print(f"Unique companies: {(output_df['dedupe_status'] == 'unique').sum()}")


if __name__ == "__main__":
    main()