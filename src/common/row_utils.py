from decimal import Decimal, InvalidOperation
import json
import hashlib
from typing import Any


def normalize_key(value: str | None) -> str:
    if value is None:
        return ""

    return "".join(
        char.lower()
        for char in str(value)
        if char.isalnum()
    )


def clean_csv_row(row: dict) -> dict:
    clean_row = {}

    for key, value in row.items():
        if key is None:
            clean_row["_extra_fields"] = value
            continue

        clean_row[str(key)] = value

    return clean_row


def get_by_possible_keys(
    row: dict,
    possible_keys: list[str],
) -> str | None:
    normalized_lookup = {}

    for key, value in row.items():
        normalized_key = normalize_key(key)

        if normalized_key:
            normalized_lookup[normalized_key] = value

    for key in possible_keys:
        value = normalized_lookup.get(normalize_key(key))

        if value is not None and str(value).strip() != "":
            return value

    return None


def calculate_row_hash(row: dict) -> str:
    clean_row = clean_csv_row(row)

    normalized = json.dumps(
        clean_row,
        sort_keys=True,
        ensure_ascii=False,
    )

    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def row_to_json_string(row: dict) -> str:
    clean_row = clean_csv_row(row)

    return json.dumps(
        clean_row,
        ensure_ascii=False,
    )

def parse_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None

    text = str(value).strip()

    if not text:
        return None

    try:
        return Decimal(text)
    except InvalidOperation:
        return None