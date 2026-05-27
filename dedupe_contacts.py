import os
import re
import time
from collections import defaultdict
from difflib import SequenceMatcher

import pandas as pd
from dotenv import load_dotenv


load_dotenv()

INPUT_CSV = os.getenv("INPUT_CSV", "./data/contacts.csv")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./data")
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "dedupe_results_namematches")

FIRST_NAME_THRESHOLD = float(os.getenv("FIRST_NAME_THRESHOLD", "0.78"))
LAST_NAME_THRESHOLD = float(os.getenv("LAST_NAME_THRESHOLD", "0.88"))
EMAIL_LOCAL_THRESHOLD = float(os.getenv("EMAIL_LOCAL_THRESHOLD", "0.88"))

TEST_MODE = False
TEST_RECORD_IDS = {"113993509929", "24134"}

REQUIRED_COLUMNS = [
    "Record ID",
    "First Name",
    "Last Name",
    "Email",
    "Phone Number",
    "Create Date",
]


def log(message: str, start_time: float) -> None:
    elapsed = time.time() - start_time
    print(f"[{elapsed:8.2f}s] {message}")


def normalize_text(value: str) -> str:
    if pd.isna(value):
        return ""
    value = str(value).strip().lower()
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_name(value: str) -> str:
    value = normalize_text(value)
    value = re.sub(r"[^a-z\s]", "", value)
    return value.strip()


def normalize_phone(value: str) -> str:
    if pd.isna(value):
        return ""
    digits = re.sub(r"\D", "", str(value))
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


def parse_first_name(first_name: str) -> str:
    full_name = normalize_name(first_name)
    tokens = [token for token in full_name.split() if token]
    return tokens[0] if tokens else ""


def repair_row_fields(row: pd.Series) -> pd.Series:
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
    return similarity(row1["first_name_base"], row2["first_name_base"]) >= FIRST_NAME_THRESHOLD


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


def exact_name_match(row1: pd.Series, row2: pd.Series) -> bool:
    return (
        row1["first_name_norm"] == row2["first_name_norm"]
        and row1["last_name_norm"] == row2["last_name_norm"]
        and row1["first_name_norm"] != ""
        and row1["last_name_norm"] != ""
    )


def email_local_parts_are_compatible(email1: str, email2: str) -> bool:
    local1 = extract_email_local_part(email1)
    local2 = extract_email_local_part(email2)

    if not local1 or not local2:
        return False

    if local1 == local2:
        return True

    if local1 in local2 or local2 in local1:
        return True

    return similarity(local1, local2) >= EMAIL_LOCAL_THRESHOLD


def has_contact_info(row: pd.Series) -> bool:
    return bool(row["email_norm"] or row["phone_norm"])


def rows_match(row1: pd.Series, row2: pd.Series) -> tuple[bool, str]:
    phone1 = row1["phone_norm"]
    phone2 = row2["phone_norm"]

    email1 = row1["email_norm"]
    email2 = row2["email_norm"]

    first_ok = first_names_are_compatible(row1, row2)
    last_ok = last_names_are_compatible(row1, row2)
    full_name_ok = first_ok and last_ok

    if phone1 and phone2 and phone1 == phone2 and first_ok:
        return True, "phone+first_name"

    if phone1 and phone2 and phone1 == phone2 and last_ok:
        return True, "phone+last_name"

    if exact_name_match(row1, row2):
        return True, "exact_name"

    if full_name_ok and email_local_parts_are_compatible(email1, email2):
        return True, "name+email"

    return False, ""


def choose_primary_record(group_df: pd.DataFrame) -> pd.Series:
    temp = group_df.copy()
    temp["create_date_parsed"] = safe_datetime(temp["Create Date"])
    temp["has_contact_info"] = (
        (temp["email_norm"].astype(str).str.strip() != "")
        | (temp["phone_norm"].astype(str).str.strip() != "")
    ).astype(int)

    def record_id_sort_key(val):
        try:
            return int(str(val))
        except Exception:
            return -1

    temp["record_id_sort"] = temp["Record ID"].apply(record_id_sort_key)

    temp = temp.sort_values(
        by=["has_contact_info", "create_date_parsed", "record_id_sort"],
        ascending=[False, False, False],
        na_position="last",
    )

    return temp.iloc[0]


class UnionFind:
    def __init__(self, items):
        self.parent = {item: item for item in items}
        self.rank = {item: 0 for item in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra = self.find(a)
        rb = self.find(b)

        if ra == rb:
            return

        if self.rank[ra] < self.rank[rb]:
            self.parent[ra] = rb
        elif self.rank[ra] > self.rank[rb]:
            self.parent[rb] = ra
        else:
            self.parent[rb] = ra
            self.rank[ra] += 1


def build_duplicate_groups(
    df: pd.DataFrame, start_time: float
) -> list[tuple[list[int], set[str]]]:
    uf = UnionFind(df.index.tolist())
    record_rules: dict[int, set[str]] = defaultdict(set)

    phone_blocks = defaultdict(list)
    exact_name_blocks = defaultdict(list)

    for idx, row in df.iterrows():
        if row["phone_norm"]:
            phone_blocks[row["phone_norm"]].append(idx)

        full_name_key = (row["first_name_norm"], row["last_name_norm"])
        if full_name_key[0] and full_name_key[1]:
            exact_name_blocks[full_name_key].append(idx)

    log(f"Phone blocks: {len(phone_blocks)}", start_time)
    log(f"Exact-name blocks: {len(exact_name_blocks)}", start_time)

    # Phone-based matching
    for block in phone_blocks.values():
        if len(block) < 2:
            continue
        for i in range(len(block)):
            for j in range(i + 1, len(block)):
                idx1, idx2 = block[i], block[j]
                matched, rule = rows_match(df.loc[idx1], df.loc[idx2])
                if matched:
                    uf.union(idx1, idx2)
                    record_rules[idx1].add(rule)
                    record_rules[idx2].add(rule)

    # Exact-name matching: this is the new fast rule that catches Misha and R.B.
    for block in exact_name_blocks.values():
        if len(block) < 2:
            continue
        anchor = block[0]
        for idx in block[1:]:
            uf.union(anchor, idx)
            record_rules[anchor].add("exact_name")
            record_rules[idx].add("exact_name")

    # Email similarity within exact name groups -- record name+email if it would fire
    for block in exact_name_blocks.values():
        if len(block) < 2:
            continue
        for i in range(len(block)):
            for j in range(i + 1, len(block)):
                idx1, idx2 = block[i], block[j]
                matched, rule = rows_match(df.loc[idx1], df.loc[idx2])
                if matched:
                    record_rules[idx1].add(rule)
                    record_rules[idx2].add(rule)

    groups_map = defaultdict(list)
    for idx in df.index:
        groups_map[uf.find(idx)].append(idx)

    result: list[tuple[list[int], set[str]]] = []
    for group in groups_map.values():
        if len(group) < 2:
            continue
        sorted_group = sorted(group)
        merged_rules: set[str] = set()
        for idx in sorted_group:
            merged_rules |= record_rules.get(idx, set())
        result.append((sorted_group, merged_rules))

    return result


def main() -> None:
    start_time = time.time()
    log("Starting dedupe script", start_time)

    df = pd.read_csv(INPUT_CSV, dtype=str).fillna("")
    validate_columns(df)
    log(f"Loaded rows: {len(df)}", start_time)

    if TEST_MODE:
        test_ids = {str(x).strip() for x in TEST_RECORD_IDS if str(x).strip()}
        df = df[df["Record ID"].astype(str).str.strip().isin(test_ids)].copy()
        log(f"TEST MODE enabled. Filtered rows: {len(df)}", start_time)

        if df.empty:
            raise ValueError(f"No rows found for TEST_RECORD_IDS={sorted(test_ids)}")

    df = df.apply(repair_row_fields, axis=1)

    df["first_name_norm"] = df["First Name"].apply(normalize_name)
    df["last_name_norm"] = df["Last Name"].apply(normalize_name)
    df["email_norm"] = df["Email"].apply(normalize_email)
    df["phone_norm"] = df["Phone Number"].apply(normalize_phone)
    df["first_name_base"] = df["First Name"].apply(parse_first_name)

    if TEST_MODE:
        log("Normalized test rows:", start_time)
        print(
            df[
                [
                    "Record ID",
                    "First Name",
                    "Last Name",
                    "Email",
                    "Phone Number",
                    "first_name_norm",
                    "last_name_norm",
                    "email_norm",
                    "phone_norm",
                ]
            ].to_string(index=False)
        )

    duplicate_groups = build_duplicate_groups(df, start_time)
    log(f"Duplicate groups found: {len(duplicate_groups)}", start_time)

    output_df = df.copy()
    output_df["dedupe_status"] = "unique"
    output_df["primary_record_id"] = ""
    output_df["duplicate_record_ids"] = ""
    output_df["match_rule"] = ""

    for group_indexes, group_rule_set in duplicate_groups:
        group_df = df.loc[group_indexes].copy()
        primary = choose_primary_record(group_df)
        primary_id = str(primary["Record ID"])

        duplicate_ids = [
            str(record_id)
            for record_id in group_df["Record ID"].tolist()
            if str(record_id) != primary_id
        ]
        duplicate_ids_str = ",".join(duplicate_ids)
        rule_str = ",".join(sorted(group_rule_set))

        if TEST_MODE:
            log(f"Group: {[str(x) for x in group_df['Record ID'].tolist()]}", start_time)
            log(f"Chosen primary: {primary_id}", start_time)
            log(f"Match rules: {rule_str}", start_time)

        for idx in group_indexes:
            current_record_id = str(output_df.at[idx, "Record ID"])
            output_df.at[idx, "primary_record_id"] = primary_id
            output_df.at[idx, "duplicate_record_ids"] = duplicate_ids_str
            output_df.at[idx, "match_rule"] = rule_str

            if current_record_id == primary_id:
                output_df.at[idx, "dedupe_status"] = "primary"
            else:
                output_df.at[idx, "dedupe_status"] = "duplicate"

    helper_cols = [
        "first_name_norm",
        "last_name_norm",
        "email_norm",
        "phone_norm",
        "first_name_base",
    ]
    output_df = output_df.drop(columns=[col for col in helper_cols if col in output_df.columns])

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = time.strftime("%Y_%m_%d_%H%M%S")
    output_csv = os.path.join(OUTPUT_DIR, f"{OUTPUT_PREFIX}_{timestamp}.csv")

    output_df.to_csv(output_csv, index=False)

    log(f"Saved output: {output_csv}", start_time)
    log(f"Primary count: {(output_df['dedupe_status'] == 'primary').sum()}", start_time)
    log(f"Duplicate count: {(output_df['dedupe_status'] == 'duplicate').sum()}", start_time)
    log(f"Unique count: {(output_df['dedupe_status'] == 'unique').sum()}", start_time)


if __name__ == "__main__":
    main()