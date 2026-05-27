import os
import re
from collections import defaultdict
from difflib import SequenceMatcher

import pandas as pd
from dotenv import load_dotenv


load_dotenv()

INPUT_CSV = os.getenv("INPUT_CSV", "contacts.csv")
OUTPUT_CSV = os.getenv("OUTPUT_CSV", "dedupe_results.csv")

FIRST_NAME_THRESHOLD = float(os.getenv("FIRST_NAME_THRESHOLD", "0.78"))
LAST_NAME_THRESHOLD = float(os.getenv("LAST_NAME_THRESHOLD", "0.88"))
EMAIL_LOCAL_THRESHOLD = float(os.getenv("EMAIL_LOCAL_THRESHOLD", "0.88"))

REQUIRED_COLUMNS = [
    "Record ID",
    "First Name",
    "Last Name",
    "Email",
    "Phone Number",
    "Create Date",
]


def normalize_text(value: str) -> str:
    if pd.isna(value):
        return ""
    value = str(value).strip().lower()
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_name(value: str) -> str:
    value = normalize_text(value)
    value = re.sub(r"[^a-z\s'-]", "", value)
    return value.strip()


def normalize_phone(value: str) -> str:
    if pd.isna(value):
        return ""
    digits = re.sub(r"\D", "", str(value))

    # Normalize US country code
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]

    return digits


def normalize_email(value: str) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def extract_email_local_part(email: str) -> str:
    email = normalize_email(email)
    if "@" not in email:
        return ""
    return email.split("@", 1)[0]


def looks_like_email(value: str) -> bool:
    value = normalize_text(value)
    return "@" in value and "." in value


def similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def clean_name_tokens(name: str) -> list[str]:
    name = normalize_name(name)
    return [token for token in name.split() if token]


def parse_first_name(first_name: str) -> tuple[str, str]:
    """
    Example:
    'Editha Valenciano' -> ('editha', 'editha valenciano')
    """
    full_name = normalize_name(first_name)
    tokens = clean_name_tokens(full_name)
    base_name = tokens[0] if tokens else ""
    return base_name, full_name


def repair_row_fields(row: pd.Series) -> pd.Series:
    """
    Repairs obvious bad rows, such as Last Name containing an email.
    """
    row = row.copy()

    last_name = str(row.get("Last Name", "") or "")
    email = str(row.get("Email", "") or "")

    if looks_like_email(last_name) and not looks_like_email(email):
        row["Email"] = last_name
        row["Last Name"] = ""

    return row


def validate_columns(df: pd.DataFrame) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def safe_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def first_names_are_compatible(row1: pd.Series, row2: pd.Series) -> bool:
    fn1 = row1["first_name_base"]
    fn2 = row2["first_name_base"]
    return similarity(fn1, fn2) >= FIRST_NAME_THRESHOLD


def last_names_are_compatible(row1: pd.Series, row2: pd.Series) -> bool:
    ln1 = row1["last_name_norm"]
    ln2 = row2["last_name_norm"]

    if not ln1 or not ln2:
        return False

    if ln1 == ln2:
        return True

    return similarity(ln1, ln2) >= LAST_NAME_THRESHOLD


def full_names_are_compatible(row1: pd.Series, row2: pd.Series) -> bool:
    return first_names_are_compatible(row1, row2) and last_names_are_compatible(row1, row2)


def email_local_parts_are_compatible(email1: str, email2: str) -> bool:
    """
    Catches examples like:
    grayjimmy904@gmail.com
    grayjimmy9046@gmail.com
    """
    local1 = extract_email_local_part(email1)
    local2 = extract_email_local_part(email2)

    if not local1 or not local2:
        return False

    if local1 == local2:
        return True

    if local1 in local2 or local2 in local1:
        return True

    return similarity(local1, local2) >= EMAIL_LOCAL_THRESHOLD


def rows_match(row1: pd.Series, row2: pd.Series) -> tuple[bool, str]:
    phone1 = row1["phone_norm"]
    phone2 = row2["phone_norm"]

    email1 = row1["email_norm"]
    email2 = row2["email_norm"]

    first_ok = first_names_are_compatible(row1, row2)
    last_ok = last_names_are_compatible(row1, row2)
    full_name_ok = first_ok and last_ok

    # Strongest path: phone + usable first name
    if phone1 and phone2 and phone1 == phone2 and first_ok:
        return True, "phone+first_name"

    # Backup: phone + usable last name
    if phone1 and phone2 and phone1 == phone2 and last_ok:
        return True, "phone+last_name"

    # No reliable phone: use name + similar email local part
    if full_name_ok and email_local_parts_are_compatible(email1, email2):
        return True, "name+email"

    return False, ""


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


def build_candidate_pairs(df: pd.DataFrame) -> set[tuple[int, int]]:
    """
    Build likely candidate pairs using blocking.
    We block on:
    - same normalized phone
    - same first-name base
    - same last name
    """
    candidate_pairs = set()

    phone_blocks = defaultdict(list)
    first_name_blocks = defaultdict(list)
    last_name_blocks = defaultdict(list)

    for idx, row in df.iterrows():
        if row["phone_norm"]:
            phone_blocks[row["phone_norm"]].append(idx)

        if row["first_name_base"]:
            first_name_blocks[row["first_name_base"]].append(idx)

        if row["last_name_norm"]:
            last_name_blocks[row["last_name_norm"]].append(idx)

    for blocks in [phone_blocks, first_name_blocks, last_name_blocks]:
        for block in blocks.values():
            if len(block) > 1:
                for i in range(len(block)):
                    for j in range(i + 1, len(block)):
                        candidate_pairs.add(tuple(sorted((block[i], block[j]))))

    return candidate_pairs


def build_duplicate_groups(df: pd.DataFrame) -> list[list[int]]:
    adjacency = defaultdict(set)
    candidate_pairs = build_candidate_pairs(df)

    for idx1, idx2 in candidate_pairs:
        matched, _reason = rows_match(df.loc[idx1], df.loc[idx2])
        if matched:
            adjacency[idx1].add(idx2)
            adjacency[idx2].add(idx1)

    groups = []
    visited = set()

    for idx in df.index:
        if idx in visited or idx not in adjacency:
            continue

        stack = [idx]
        component = []

        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            component.append(current)
            stack.extend(adjacency[current] - visited)

        if len(component) > 1:
            groups.append(sorted(component))

    return groups


def main() -> None:
    df = pd.read_csv(INPUT_CSV, dtype=str).fillna("")
    validate_columns(df)

    # Repair obvious bad rows first
    df = df.apply(repair_row_fields, axis=1)

    # Normalized helper fields
    df["first_name_norm"] = df["First Name"].apply(normalize_name)
    df["last_name_norm"] = df["Last Name"].apply(normalize_name)
    df["email_norm"] = df["Email"].apply(normalize_email)
    df["phone_norm"] = df["Phone Number"].apply(normalize_phone)

    first_name_parsed = df["First Name"].apply(parse_first_name)
    df["first_name_base"] = first_name_parsed.apply(lambda x: x[0])
    df["first_name_full"] = first_name_parsed.apply(lambda x: x[1])

    duplicate_groups = build_duplicate_groups(df)

    # Start with all original contacts preserved
    output_df = df.copy()
    output_df["dedupe_status"] = "unique"
    output_df["primary_record_id"] = ""
    output_df["duplicate_record_ids"] = ""

    # Mark groups
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

    # Remove helper columns before export
    helper_cols = [
        "first_name_norm",
        "last_name_norm",
        "email_norm",
        "phone_norm",
        "first_name_base",
        "first_name_full",
    ]
    output_df = output_df.drop(columns=[col for col in helper_cols if col in output_df.columns])

    output_dir = os.path.dirname(OUTPUT_CSV)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    output_df.to_csv(OUTPUT_CSV, index=False)

    print(f"Input file: {INPUT_CSV}")
    print(f"Output file: {OUTPUT_CSV}")
    print(f"Total input contacts: {len(df)}")
    print(f"Duplicate groups found: {len(duplicate_groups)}")
    print(f"Primary contacts: {(output_df['dedupe_status'] == 'primary').sum()}")
    print(f"Duplicate contacts: {(output_df['dedupe_status'] == 'duplicate').sum()}")
    print(f"Unique contacts: {(output_df['dedupe_status'] == 'unique').sum()}")


if __name__ == "__main__":
    main()