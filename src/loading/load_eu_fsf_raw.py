import os
import csv
import json
import hashlib
from io import StringIO
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from psycopg2.extras import Json

from common.minio_client import MinioClient
from common.postgres_client import PostgresClient
from common.manifest_utils import (
    find_latest_successful_manifest,
    evaluate_snapshot_freshness,
    handle_stale_snapshot,
)


# -------------------------------------------------------------------
# Environment configuration
# -------------------------------------------------------------------

MINIO_BUCKET = os.getenv("MINIO_BUCKET")
APP_ENV = os.getenv("APP_ENV", "dev")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

LOAD_DT = datetime.now(timezone.utc)
LOAD_TIMESTAMP_UTC = LOAD_DT.isoformat()

# If set: load exactly this snapshot.
# If not set: load latest successful EU FSF snapshot from MinIO.
EU_FSF_LOAD_DATE = os.getenv("EU_FSF_LOAD_DATE")

EU_FSF_MAX_SNAPSHOT_AGE_DAYS = int(
    os.getenv("EU_FSF_MAX_SNAPSHOT_AGE_DAYS", "7")
)

# Supported values: warn, fail, allow
EU_FSF_STALE_SNAPSHOT_POLICY = os.getenv(
    "EU_FSF_STALE_SNAPSHOT_POLICY",
    "warn",
).lower()


# -------------------------------------------------------------------
# Validation
# -------------------------------------------------------------------

def validate_environment() -> None:
    required_values = {
        "MINIO_BUCKET": MINIO_BUCKET,
        "POSTGRES_HOST": POSTGRES_HOST,
        "POSTGRES_PORT": POSTGRES_PORT,
        "POSTGRES_DB": POSTGRES_DB,
        "POSTGRES_USER": POSTGRES_USER,
        "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
    }

    missing = [key for key, value in required_values.items() if not value]

    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )

    if EU_FSF_STALE_SNAPSHOT_POLICY not in {"warn", "fail", "allow"}:
        raise RuntimeError(
            "EU_FSF_STALE_SNAPSHOT_POLICY must be one of: warn, fail, allow"
        )


# -------------------------------------------------------------------
# PostgreSQL setup
# -------------------------------------------------------------------

def ensure_raw_eu_fsf_table(postgres: PostgresClient) -> None:
    postgres.ensure_schemas()

    postgres.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.eu_fsf_full_csv (
            raw_id BIGSERIAL PRIMARY KEY,

            app_env TEXT,
            source TEXT,
            source_name TEXT,
            dataset_group TEXT,
            snapshot_type TEXT,

            row_number INTEGER,
            row_hash TEXT,

            entity_logical_id TEXT,
            eu_reference_number TEXT,
            un_reference_number TEXT,
            subject_type TEXT,

            name_alias_whole_name TEXT,
            programme TEXT,
            regulation_type TEXT,
            regulation_number_title TEXT,

            designation_date_raw TEXT,
            publication_date_raw TEXT,

            raw_row JSONB,

            source_url TEXT,
            source_object_key TEXT,
            metadata_object_key TEXT,

            source_load_date DATE,
            loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_eu_fsf_full_csv_row_hash
            ON raw.eu_fsf_full_csv (row_hash);

        CREATE INDEX IF NOT EXISTS idx_eu_fsf_full_csv_eu_ref
            ON raw.eu_fsf_full_csv (eu_reference_number);

        CREATE INDEX IF NOT EXISTS idx_eu_fsf_full_csv_name
            ON raw.eu_fsf_full_csv (name_alias_whole_name);

        CREATE INDEX IF NOT EXISTS idx_eu_fsf_full_csv_load_date
            ON raw.eu_fsf_full_csv (source_load_date);

        CREATE INDEX IF NOT EXISTS idx_eu_fsf_full_csv_source_object
            ON raw.eu_fsf_full_csv (source_object_key);
        """
    )

# -------------------------------------------------------------------
# Manifest / freshness helpers
# -------------------------------------------------------------------

def resolve_eu_fsf_manifest(
    minio: MinioClient,
) -> tuple[str, str, dict, dict]:
    if EU_FSF_LOAD_DATE:
        manifest_key = (
            f"eu_fsf/_manifests/load_date={EU_FSF_LOAD_DATE}/download_eu_fsf_manifest.json"
        )

        if not minio.object_exists(manifest_key):
            raise RuntimeError(
                f"EU FSF manifest not found in MinIO: "
                f"s3://{MINIO_BUCKET}/{manifest_key}. "
                "Run download_eu_fsf_csv.py first or set EU_FSF_LOAD_DATE correctly."
            )

        manifest = minio.get_json_object(manifest_key)
        effective_load_date = EU_FSF_LOAD_DATE

    else:
        print("EU_FSF_LOAD_DATE not set. Searching latest successful EU FSF manifest in MinIO.")

        manifest_key, effective_load_date, manifest = find_latest_successful_manifest(
            minio=minio,
            manifest_prefix="eu_fsf/_manifests/",
            manifest_filename="download_eu_fsf_manifest.json",
        )

    freshness = evaluate_snapshot_freshness(
        effective_load_date=effective_load_date,
        max_age_days=EU_FSF_MAX_SNAPSHOT_AGE_DAYS,
        policy=EU_FSF_STALE_SNAPSHOT_POLICY,
    )

    handle_stale_snapshot(
        freshness=freshness,
        source_name="EU FSF",
    )

    return manifest_key, effective_load_date, manifest, freshness


# -------------------------------------------------------------------
# CSV / row helpers
# -------------------------------------------------------------------

def read_csv_rows(csv_text: str) -> list[dict[str, str]]:
    reader = csv.DictReader(
        StringIO(csv_text),
        delimiter=";",
        quotechar='"',
    )

    return list(reader)

def normalize_key(value: str | None) -> str:
    if value is None:
        return ""

    return "".join(
        char.lower()
        for char in str(value)
        if char.isalnum()
    )


def get_by_possible_keys(
    row: dict[str, str],
    possible_keys: list[str],
) -> str | None:
    normalized_lookup = {}

    for key, value in row.items():
        if key is None:
            continue
        normalized_key = normalize_key(key)

        if normalized_key:
            normalized_lookup[normalized_key] = value

    for key in possible_keys:
        value = normalized_lookup.get(normalize_key(key))

        if value is not None and str(value).strip() != "":
            return value

    return None

def clean_csv_row(row: dict) -> dict:
    clean_row = {}

    for key, value in row.items():
        if key is None:
            clean_row["_extra_fields"] = value
            continue

        clean_row[str(key)] = value

    return clean_row

def calculate_row_hash(row: dict[str, str]) -> str:

    clean_row = clean_csv_row(row)

    normalized = json.dumps(
        clean_row,
        sort_keys=True,
        ensure_ascii=False,
    )

    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

# -------------------------------------------------------------------
# Database helpers
# -------------------------------------------------------------------

def delete_existing_rows_for_object(
    postgres: PostgresClient,
    source_object_key: str,
) -> None:
    postgres.execute(
        """
        DELETE FROM raw.eu_fsf_full_csv
        WHERE source_object_key = %s
        """,
        (source_object_key,),
    )


def insert_eu_fsf_rows(
    postgres: PostgresClient,
    rows: list[tuple],
) -> int:
    if not rows:
        return 0

    insert_sql = """
        INSERT INTO raw.eu_fsf_full_csv (
            app_env,
            source,
            source_name,
            dataset_group,
            snapshot_type,
            row_number,
            row_hash,
            entity_logical_id,
            eu_reference_number,
            un_reference_number,
            subject_type,
            name_alias_whole_name,
            programme,
            regulation_type,
            regulation_number_title,
            designation_date_raw,
            publication_date_raw,
            raw_row,
            source_url,
            source_object_key,
            metadata_object_key,
            source_load_date
        )
        VALUES %s
    """

    return postgres.execute_values(
        insert_sql,
        rows,
        page_size=1000,
    )


# -------------------------------------------------------------------
# Main job
# -------------------------------------------------------------------

def main() -> None:
    validate_environment()

    print("------------------------------------------------------------")
    print("Loading EU FSF raw full CSV")
    print(f"Requested EU_FSF_LOAD_DATE: {EU_FSF_LOAD_DATE or 'not set'}")
    print(f"Stale snapshot policy: {EU_FSF_STALE_SNAPSHOT_POLICY}")
    print(f"Max snapshot age days: {EU_FSF_MAX_SNAPSHOT_AGE_DAYS}")

    minio = MinioClient.from_env()
    postgres = PostgresClient.from_env()

    try:
        ensure_raw_eu_fsf_table(postgres)

        manifest_key, effective_load_date, manifest, freshness = resolve_eu_fsf_manifest(
            minio=minio,
        )

        print("------------------------------------------------------------")
        print(f"Using EU FSF manifest: s3://{MINIO_BUCKET}/{manifest_key}")
        print(f"Effective EU FSF load date: {effective_load_date}")
        print(f"Freshness status: {freshness['freshness_status']}")
        print(f"Snapshot age days: {freshness['snapshot_age_days']}")

        success_files = [
            file
            for file in manifest.get("files", [])
            if file.get("status") == "success"
        ]

        if not success_files:
            raise RuntimeError(
                f"No successful EU FSF files found in manifest for load_date={effective_load_date}."
            )

        total_inserted = 0

        for file_entry in success_files:
            source_name = file_entry.get("source_name")
            dataset_group = file_entry.get("dataset_group")
            snapshot_type = file_entry.get("snapshot_type")
            source_url = file_entry.get("source_url")
            data_object_key = file_entry.get("data_object_key")
            metadata_object_key = file_entry.get("metadata_object_key")

            if not data_object_key:
                print("Skipping manifest entry without data_object_key.")
                continue

            print("------------------------------------------------------------")
            print(f"Source name: {source_name}")
            print(f"Object: s3://{MINIO_BUCKET}/{data_object_key}")

            csv_text = minio.get_text_object(data_object_key)
            csv_rows = read_csv_rows(csv_text)

            if not csv_rows:
                print(f"WARNING: CSV file contains no data rows: {data_object_key}")
                continue

            prepared_rows = []

            for row_number, row in enumerate(csv_rows, start=1):
                clean_row = clean_csv_row(row)

                entity_logical_id = get_by_possible_keys(
                    clean_row,
                    [
                        "Entity_LogicalId",
                        "entity_logical_id",
                        "Entity Logical Id",
                        "logical_id",
                    ],
                )

                eu_reference_number = get_by_possible_keys(
                    clean_row,
                    [
                        "Entity_EU_ReferenceNumber",
                        "eu_reference_number",
                        "EU Reference Number",
                        "EUReferenceNumber",
                    ],
                )

                un_reference_number = get_by_possible_keys(
                    clean_row,
                    [
                        "Entity_UnitedNationId",
                        "un_reference_number",
                        "United Nation Id",
                        "UN Reference Number",
                    ],
                )

                subject_type = get_by_possible_keys(
                    clean_row,
                    [
                        "Entity_SubjectType_ClassificationCode",
                        "Entity_SubjectType",
                        "SubjectType_ClassificationCode",
                        "subject_type",
                        "Subject Type",
                        "classification_code",
                    ],
                )

                name_alias_whole_name = get_by_possible_keys(
                    clean_row,
                    [
                        "NameAlias_WholeName",
                        "name_alias_whole_name",
                        "WholeName",
                        "whole_name",
                        "name",
                    ],
                )

                programme = get_by_possible_keys(
                    clean_row,
                    [
                        "Entity_Regulation_Programme",
                        "NameAlias_Regulation_Programme",
                        "Regulation_Programme",
                        "programme",
                        "Programme",
                        "sanctions_programme",
                    ],
                )

                regulation_type = get_by_possible_keys(
                    clean_row,
                    [
                        "Entity_Regulation_Type",
                        "NameAlias_Regulation_Type",
                        "Regulation_Type",
                        "regulation_type",
                        "Regulation Type",
                    ],
                )

                regulation_number_title = get_by_possible_keys(
                    clean_row,
                    [
                        "Entity_Regulation_NumberTitle",
                        "NameAlias_Regulation_NumberTitle",
                        "Regulation_NumberTitle",
                        "regulation_number_title",
                        "Regulation Number Title",
                        "legal_basis",
                    ],
                )

                designation_date_raw = get_by_possible_keys(
                    clean_row,
                    [
                        "Entity_DesignationDate",
                        "designation_date",
                        "Designation Date",
                    ],
                )

                publication_date_raw = get_by_possible_keys(
                    clean_row,
                    [
                        "Entity_Regulation_PublicationDate",
                        "NameAlias_Regulation_PublicationDate",
                        "Regulation_PublicationDate",
                        "publication_date",
                        "Publication Date",
                    ],
                )

                prepared_rows.append(
                    (
                        APP_ENV,
                        "EU Financial Sanctions Files",
                        source_name,
                        dataset_group,
                        snapshot_type,
                        row_number,
                        calculate_row_hash(clean_row),
                        entity_logical_id,
                        eu_reference_number,
                        un_reference_number,
                        subject_type,
                        name_alias_whole_name,
                        programme,
                        regulation_type,
                        regulation_number_title,
                        designation_date_raw,
                        publication_date_raw,
                        Json(clean_row),
                        source_url,
                        data_object_key,
                        metadata_object_key,
                        effective_load_date,
                    )
                )

            delete_existing_rows_for_object(
                postgres=postgres,
                source_object_key=data_object_key,
            )

            inserted_count = insert_eu_fsf_rows(
                postgres=postgres,
                rows=prepared_rows,
            )

            total_inserted += inserted_count

            print(f"Inserted rows: {inserted_count}")

        print("------------------------------------------------------------")
        print("EU FSF raw load finished successfully.")
        print(f"Effective EU FSF load date: {effective_load_date}")
        print(f"Freshness status: {freshness['freshness_status']}")
        print(f"Total inserted rows: {total_inserted}")

    finally:
        postgres.close()


if __name__ == "__main__":
    main()