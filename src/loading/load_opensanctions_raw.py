import os
import json
import hashlib
from datetime import datetime, timezone
from typing import Iterator

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from common.minio_client import MinioClient
from common.postgres_client import PostgresClient
from common.manifest_utils import (
    find_latest_successful_manifest,
    evaluate_snapshot_freshness,
    handle_stale_snapshot,
)
from common.row_utils import (
    clean_csv_row,
    get_by_possible_keys,
    calculate_row_hash,
    row_to_json_string,
)
from common.stream_utils import iter_csv_rows_from_text_stream

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
# If not set: load latest successful OpenSanctions snapshot from MinIO.
OPENSANCTIONS_LOAD_DATE = os.getenv("OPENSANCTIONS_LOAD_DATE")

OPENSANCTIONS_MAX_SNAPSHOT_AGE_DAYS = int(
    os.getenv("OPENSANCTIONS_MAX_SNAPSHOT_AGE_DAYS", "3")
)

# Supported values: warn, fail, allow
OPENSANCTIONS_STALE_SNAPSHOT_POLICY = os.getenv(
    "OPENSANCTIONS_STALE_SNAPSHOT_POLICY",
    "warn",
).lower()

OPENSANCTIONS_LOAD_BATCH_SIZE = int(
    os.getenv("OPENSANCTIONS_LOAD_BATCH_SIZE", "50000")
)

# 0 = unlimited
OPENSANCTIONS_MAX_ROWS_PER_FILE = int(
    os.getenv("OPENSANCTIONS_MAX_ROWS_PER_FILE", "0")
)

OPENSANCTIONS_REBUILD_INDEXES = (
    os.getenv("OPENSANCTIONS_REBUILD_INDEXES", "true").lower() == "true"
)

OPENSANCTIONS_STORE_RAW_ROW_JSON = (
    os.getenv("OPENSANCTIONS_STORE_RAW_ROW_JSON", "false").lower() == "true"
)


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

    if OPENSANCTIONS_STALE_SNAPSHOT_POLICY not in {"warn", "fail", "allow"}:
        raise RuntimeError(
            "OPENSANCTIONS_STALE_SNAPSHOT_POLICY must be one of: warn, fail, allow"
        )


# -------------------------------------------------------------------
# PostgreSQL setup
# -------------------------------------------------------------------

def ensure_raw_opensanctions_table(postgres: PostgresClient) -> None:
    postgres.ensure_schemas()

    postgres.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.opensanctions_targets_simple (
            raw_id BIGSERIAL PRIMARY KEY,

            app_env TEXT,
            source TEXT,
            source_name TEXT,
            dataset_group TEXT,
            snapshot_type TEXT,

            row_number INTEGER,
            row_hash TEXT,

            opensanctions_id TEXT,
            schema_name TEXT,
            caption TEXT,
            datasets TEXT,
            countries TEXT,
            first_seen_raw TEXT,
            last_seen_raw TEXT,
            last_change_raw TEXT,

            raw_row JSONB,

            source_url TEXT,
            source_object_key TEXT,
            metadata_object_key TEXT,

            source_load_date DATE,
            loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


def drop_raw_opensanctions_indexes(postgres: PostgresClient) -> None:
    postgres.execute(
        """
        DROP INDEX IF EXISTS raw.idx_opensanctions_targets_id;
        DROP INDEX IF EXISTS raw.idx_opensanctions_targets_schema;
        DROP INDEX IF EXISTS raw.idx_opensanctions_targets_caption;
        DROP INDEX IF EXISTS raw.idx_opensanctions_targets_load_date;
        DROP INDEX IF EXISTS raw.idx_opensanctions_targets_source_object;
        """
    )


def create_raw_opensanctions_indexes(postgres: PostgresClient) -> None:
    postgres.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_opensanctions_targets_id
            ON raw.opensanctions_targets_simple (opensanctions_id);

        CREATE INDEX IF NOT EXISTS idx_opensanctions_targets_schema
            ON raw.opensanctions_targets_simple (schema_name);

        CREATE INDEX IF NOT EXISTS idx_opensanctions_targets_caption
            ON raw.opensanctions_targets_simple (caption);

        CREATE INDEX IF NOT EXISTS idx_opensanctions_targets_load_date
            ON raw.opensanctions_targets_simple (source_load_date);

        CREATE INDEX IF NOT EXISTS idx_opensanctions_targets_source_object
            ON raw.opensanctions_targets_simple (source_object_key);
        """
    )


# -------------------------------------------------------------------
# Manifest / freshness helpers
# -------------------------------------------------------------------

def resolve_opensanctions_manifest(
    minio: MinioClient,
) -> tuple[str, str, dict, dict]:
    if OPENSANCTIONS_LOAD_DATE:
        manifest_key = (
            "opensanctions/_manifests/"
            f"load_date={OPENSANCTIONS_LOAD_DATE}/download_opensanctions_manifest.json"
        )

        if not minio.object_exists(manifest_key):
            raise RuntimeError(
                f"OpenSanctions manifest not found in MinIO: "
                f"s3://{MINIO_BUCKET}/{manifest_key}. "
                "Run download_opensanctions.py first or set OPENSANCTIONS_LOAD_DATE correctly."
            )

        manifest = minio.get_json_object(manifest_key)
        effective_load_date = OPENSANCTIONS_LOAD_DATE

    else:
        print(
            "OPENSANCTIONS_LOAD_DATE not set. "
            "Searching latest successful OpenSanctions manifest in MinIO."
        )

        manifest_key, effective_load_date, manifest = find_latest_successful_manifest(
            minio=minio,
            manifest_prefix="opensanctions/_manifests/",
            manifest_filename="download_opensanctions_manifest.json",
        )

    freshness = evaluate_snapshot_freshness(
        effective_load_date=effective_load_date,
        max_age_days=OPENSANCTIONS_MAX_SNAPSHOT_AGE_DAYS,
        policy=OPENSANCTIONS_STALE_SNAPSHOT_POLICY,
    )

    handle_stale_snapshot(
        freshness=freshness,
        source_name="OpenSanctions",
    )

    return manifest_key, effective_load_date, manifest, freshness

# -------------------------------------------------------------------
# Database helpers
# -------------------------------------------------------------------

def delete_existing_rows_for_object(
    postgres: PostgresClient,
    source_object_key: str,
) -> None:
    postgres.execute(
        """
        DELETE FROM raw.opensanctions_targets_simple
        WHERE source_object_key = %s
        """,
        (source_object_key,),
    )


def insert_opensanctions_rows(
    postgres: PostgresClient,
    rows: list[tuple],
) -> int:
    return postgres.copy_rows(
        table_name="raw.opensanctions_targets_simple",
        columns=[
            "app_env",
            "source",
            "source_name",
            "dataset_group",
            "snapshot_type",
            "row_number",
            "row_hash",
            "opensanctions_id",
            "schema_name",
            "caption",
            "datasets",
            "countries",
            "first_seen_raw",
            "last_seen_raw",
            "last_change_raw",
            "raw_row",
            "source_url",
            "source_object_key",
            "metadata_object_key",
            "source_load_date",
        ],
        rows=rows,
    )


# -------------------------------------------------------------------
# Row mapping
# -------------------------------------------------------------------

def map_opensanctions_row(
    row: dict,
    row_number: int,
    file_entry: dict,
    data_object_key: str,
    effective_load_date: str,
) -> tuple:
    clean_row = clean_csv_row(row)

    opensanctions_id = get_by_possible_keys(
        clean_row,
        ["id", "entity_id", "opensanctions_id"],
    )

    schema_name = get_by_possible_keys(
        clean_row,
        ["schema", "schema_name"],
    )

    caption = get_by_possible_keys(
        clean_row,
        ["caption", "name", "label"],
    )

    datasets = get_by_possible_keys(
        clean_row,
        ["datasets", "dataset"],
    )

    countries = get_by_possible_keys(
        clean_row,
        ["countries", "country"],
    )

    first_seen_raw = get_by_possible_keys(
        clean_row,
        ["first_seen", "first_seen_at"],
    )

    last_seen_raw = get_by_possible_keys(
        clean_row,
        ["last_seen", "last_seen_at"],
    )

    last_change_raw = get_by_possible_keys(
        clean_row,
        ["last_change", "last_changed_at"],
    )

    return (
        APP_ENV,
        "OpenSanctions",
        file_entry.get("source_name"),
        file_entry.get("dataset_group"),
        file_entry.get("snapshot_type"),
        row_number,
        calculate_row_hash(clean_row),
        opensanctions_id,
        schema_name,
        caption,
        datasets,
        countries,
        first_seen_raw,
        last_seen_raw,
        last_change_raw,
        row_to_json_string(clean_row) if OPENSANCTIONS_STORE_RAW_ROW_JSON else None,
        file_entry.get("source_url"),
        data_object_key,
        file_entry.get("metadata_object_key"),
        effective_load_date,
    )


# -------------------------------------------------------------------
# Processing
# -------------------------------------------------------------------

def process_opensanctions_file(
    postgres: PostgresClient,
    row_iterator: Iterator[dict],
    file_entry: dict,
    data_object_key: str,
    effective_load_date: str,
) -> int:
    delete_existing_rows_for_object(
        postgres=postgres,
        source_object_key=data_object_key,
    )

    total_inserted = 0
    prepared_rows = []

    for row_number, row in enumerate(row_iterator, start=1):
        if (
            OPENSANCTIONS_MAX_ROWS_PER_FILE > 0
            and row_number > OPENSANCTIONS_MAX_ROWS_PER_FILE
        ):
            print(
                f"Reached OPENSANCTIONS_MAX_ROWS_PER_FILE="
                f"{OPENSANCTIONS_MAX_ROWS_PER_FILE}. "
                "Stopping OpenSanctions file processing."
            )

            if hasattr(row_iterator, "close"):
                row_iterator.close()

            break

        prepared_rows.append(
            map_opensanctions_row(
                row=row,
                row_number=row_number,
                file_entry=file_entry,
                data_object_key=data_object_key,
                effective_load_date=effective_load_date,
            )
        )

        if len(prepared_rows) >= OPENSANCTIONS_LOAD_BATCH_SIZE:
            inserted_count = insert_opensanctions_rows(
                postgres=postgres,
                rows=prepared_rows,
            )
            total_inserted += inserted_count
            prepared_rows = []

            print(f"Inserted OpenSanctions rows: {total_inserted}")

    if prepared_rows:
        inserted_count = insert_opensanctions_rows(
            postgres=postgres,
            rows=prepared_rows,
        )
        total_inserted += inserted_count

    return total_inserted


# -------------------------------------------------------------------
# Main job
# -------------------------------------------------------------------

def main() -> None:
    validate_environment()

    print("------------------------------------------------------------")
    print("Loading OpenSanctions raw targets.simple CSV")
    print("Target table: raw.opensanctions_targets_simple")
    print("Insert mode: PostgreSQL COPY")
    print(f"Requested OPENSANCTIONS_LOAD_DATE: {OPENSANCTIONS_LOAD_DATE or 'not set'}")
    print(f"Stale snapshot policy: {OPENSANCTIONS_STALE_SNAPSHOT_POLICY}")
    print(f"Max snapshot age days: {OPENSANCTIONS_MAX_SNAPSHOT_AGE_DAYS}")
    print(f"Batch size: {OPENSANCTIONS_LOAD_BATCH_SIZE}")
    print(f"Max rows per file: {OPENSANCTIONS_MAX_ROWS_PER_FILE or 'unlimited'}")
    print(f"Rebuild indexes: {OPENSANCTIONS_REBUILD_INDEXES}")
    print(f"Store raw_row JSON: {OPENSANCTIONS_STORE_RAW_ROW_JSON}")

    minio = MinioClient.from_env()
    postgres = PostgresClient.from_env()

    try:
        ensure_raw_opensanctions_table(postgres)

        if OPENSANCTIONS_REBUILD_INDEXES:
            print("Dropping OpenSanctions indexes before load.")
            drop_raw_opensanctions_indexes(postgres)

        manifest_key, effective_load_date, manifest, freshness = (
            resolve_opensanctions_manifest(minio=minio)
        )

        print("------------------------------------------------------------")
        print(f"Using OpenSanctions manifest: s3://{MINIO_BUCKET}/{manifest_key}")
        print(f"Effective OpenSanctions load date: {effective_load_date}")
        print(f"Freshness status: {freshness['freshness_status']}")
        print(f"Snapshot age days: {freshness['snapshot_age_days']}")

        success_files = [
            file
            for file in manifest.get("files", [])
            if file.get("status") == "success"
        ]

        if not success_files:
            raise RuntimeError(
                "No successful OpenSanctions files found in manifest "
                f"for load_date={effective_load_date}."
            )

        total_inserted = 0

        for file_entry in success_files:
            source_name = file_entry.get("source_name")
            data_object_key = file_entry.get("data_object_key")

            if not data_object_key:
                print("Skipping manifest entry without data_object_key.")
                continue

            print("------------------------------------------------------------")
            print(f"Source name: {source_name}")
            print(f"Object: s3://{MINIO_BUCKET}/{data_object_key}")
            print("Writing to table: raw.opensanctions_targets_simple")

            with minio.get_object_stream(data_object_key) as stream:
                row_iterator = iter_csv_rows_from_text_stream(stream)

                inserted_count = process_opensanctions_file(
                    postgres=postgres,
                    row_iterator=row_iterator,
                    file_entry=file_entry,
                    data_object_key=data_object_key,
                    effective_load_date=effective_load_date,
                )

                total_inserted += inserted_count

                print(f"Inserted OpenSanctions rows for file: {inserted_count}")

        if OPENSANCTIONS_REBUILD_INDEXES:
            print("Creating OpenSanctions indexes after load.")
            create_raw_opensanctions_indexes(postgres)

        print("------------------------------------------------------------")
        print("OpenSanctions raw load finished successfully.")
        print(f"Effective OpenSanctions load date: {effective_load_date}")
        print(f"Freshness status: {freshness['freshness_status']}")
        print(f"Total inserted rows: {total_inserted}")

    finally:
        postgres.close()


if __name__ == "__main__":
    main()