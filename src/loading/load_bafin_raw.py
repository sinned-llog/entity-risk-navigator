import os
import json
import hashlib
from datetime import datetime, timezone

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
# If not set: load latest successful BaFin snapshot from MinIO.
BAFIN_LOAD_DATE = os.getenv("BAFIN_LOAD_DATE")

BAFIN_MAX_SNAPSHOT_AGE_DAYS = int(
    os.getenv("BAFIN_MAX_SNAPSHOT_AGE_DAYS", "30")
)

# Supported values: warn, fail, allow
BAFIN_STALE_SNAPSHOT_POLICY = os.getenv(
    "BAFIN_STALE_SNAPSHOT_POLICY",
    "warn",
).lower()

BAFIN_LOAD_BATCH_SIZE = int(
    os.getenv("BAFIN_LOAD_BATCH_SIZE", "1000")
)

# 0 = unlimited
BAFIN_MAX_FILES = int(
    os.getenv("BAFIN_MAX_FILES", "0")
)

BAFIN_REBUILD_INDEXES = (
    os.getenv("BAFIN_REBUILD_INDEXES", "false").lower() == "true"
)

# For BaFin HTML pages this can stay true in dev.
# If pages become large, set to false and rely on MinIO + content_hash.
BAFIN_STORE_RAW_CONTENT = (
    os.getenv("BAFIN_STORE_RAW_CONTENT", "true").lower() == "true"
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

    missing = [
        key
        for key, value in required_values.items()
        if not value
    ]

    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )

    if BAFIN_STALE_SNAPSHOT_POLICY not in {"warn", "fail", "allow"}:
        raise RuntimeError(
            "BAFIN_STALE_SNAPSHOT_POLICY must be one of: warn, fail, allow"
        )


# -------------------------------------------------------------------
# PostgreSQL setup
# -------------------------------------------------------------------

def ensure_raw_bafin_table(postgres: PostgresClient) -> None:
    postgres.ensure_schemas()

    postgres.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.bafin_pages (
            raw_id BIGSERIAL PRIMARY KEY,

            app_env TEXT,
            source TEXT,
            source_name TEXT,
            dataset_group TEXT,
            snapshot_type TEXT,

            file_number INTEGER,

            candidate_id TEXT,
            lei TEXT,
            bafin_institut_id TEXT,
            legal_name TEXT,
            search_name TEXT,
            jurisdiction TEXT,
            country TEXT,
            source_reason TEXT,
            priority TEXT,

            http_status INTEGER,
            downloaded_bytes BIGINT,
            content_hash TEXT,

            raw_content TEXT,
            raw_content_type TEXT,
            metadata_json JSONB,

            source_url TEXT,
            source_object_key TEXT,
            metadata_object_key TEXT,

            source_load_date DATE,
            loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


def drop_raw_bafin_indexes(postgres: PostgresClient) -> None:
    postgres.execute(
        """
        DROP INDEX IF EXISTS raw.idx_bafin_pages_bafin_id;
        DROP INDEX IF EXISTS raw.idx_bafin_pages_company_name;
        DROP INDEX IF EXISTS raw.idx_bafin_pages_content_hash;
        DROP INDEX IF EXISTS raw.idx_bafin_pages_load_date;
        DROP INDEX IF EXISTS raw.idx_bafin_pages_source_object;
        """
    )


def create_raw_bafin_indexes(postgres: PostgresClient) -> None:
    postgres.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_bafin_pages_bafin_id
            ON raw.bafin_pages (bafin_id);

        CREATE INDEX IF NOT EXISTS idx_bafin_pages_company_name
            ON raw.bafin_pages (company_name);

        CREATE INDEX IF NOT EXISTS idx_bafin_pages_content_hash
            ON raw.bafin_pages (content_hash);

        CREATE INDEX IF NOT EXISTS idx_bafin_pages_load_date
            ON raw.bafin_pages (source_load_date);

        CREATE INDEX IF NOT EXISTS idx_bafin_pages_source_object
            ON raw.bafin_pages (source_object_key);
        """
    )

# -------------------------------------------------------------------
# Manifest / freshness helpers
# -------------------------------------------------------------------

def resolve_bafin_manifest(
    minio: MinioClient,
) -> tuple[str, str, dict, dict]:
    if BAFIN_LOAD_DATE:
        manifest_key = (
            f"bafin/_manifests/load_date={BAFIN_LOAD_DATE}/download_bafin_manifest.json"
        )

        if not minio.object_exists(manifest_key):
            raise RuntimeError(
                f"BaFin manifest not found in MinIO: "
                f"s3://{MINIO_BUCKET}/{manifest_key}. "
                "Run download_bafin.py first or set BAFIN_LOAD_DATE correctly."
            )

        manifest = minio.get_json_object(manifest_key)
        effective_load_date = BAFIN_LOAD_DATE

    else:
        print("BAFIN_LOAD_DATE not set. Searching latest successful BaFin manifest in MinIO.")

        manifest_key, effective_load_date, manifest = find_latest_successful_manifest(
            minio=minio,
            manifest_prefix="bafin/_manifests/",
            manifest_filename="download_bafin_manifest.json",
        )

    freshness = evaluate_snapshot_freshness(
        effective_load_date=effective_load_date,
        max_age_days=BAFIN_MAX_SNAPSHOT_AGE_DAYS,
        policy=BAFIN_STALE_SNAPSHOT_POLICY,
    )

    handle_stale_snapshot(
        freshness=freshness,
        source_name="BaFin",
    )

    return manifest_key, effective_load_date, manifest, freshness


# -------------------------------------------------------------------
# Stream helpers
# -------------------------------------------------------------------

def read_text_and_hash_from_stream(
    stream,
    encoding: str = "utf-8-sig",
    store_content: bool = True,
    chunk_size: int = 1024 * 1024,
) -> tuple[str | None, str]:
    """
    Reads an object stream in chunks.

    If store_content is False, this does not accumulate the full object
    in memory and only calculates the SHA256 hash.
    """

    hasher = hashlib.sha256()
    chunks = []

    while True:
        chunk = stream.read(chunk_size)

        if not chunk:
            break

        hasher.update(chunk)

        if store_content:
            chunks.append(chunk)

    content_hash = hasher.hexdigest()

    if not store_content:
        return None, content_hash

    raw_content = b"".join(chunks).decode(
        encoding,
        errors="replace",
    )

    return raw_content, content_hash


def load_metadata_json(
    minio: MinioClient,
    metadata_object_key: str | None,
) -> str | None:
    if not metadata_object_key:
        return None

    try:
        metadata = minio.get_json_object(metadata_object_key)
    except Exception as exc:
        print(
            f"WARNING: Could not load metadata object "
            f"{metadata_object_key}: {exc}"
        )
        return None

    return json.dumps(
        metadata,
        ensure_ascii=False,
    )


# -------------------------------------------------------------------
# Database helpers
# -------------------------------------------------------------------

def delete_existing_rows_for_object(
    postgres: PostgresClient,
    source_object_key: str,
) -> None:
    postgres.execute(
        """
        DELETE FROM raw.bafin_pages
        WHERE source_object_key = %s
        """,
        (source_object_key,),
    )


def insert_bafin_rows(
    postgres: PostgresClient,
    rows: list[tuple],
) -> int:
    return postgres.copy_rows(
        table_name="raw.bafin_pages",
        columns=[
            "app_env",
            "source",
            "source_name",
            "dataset_group",
            "snapshot_type",
            "file_number",
            "candidate_id",
            "lei",
            "bafin_institut_id",
            "legal_name",
            "search_name",
            "jurisdiction",
            "country",
            "source_reason",
            "priority",
            "http_status",
            "downloaded_bytes",
            "content_hash",
            "raw_content",
            "raw_content_type",
            "metadata_json",
            "source_url",
            "source_object_key",
            "metadata_object_key",
            "source_load_date",
        ],
        rows=rows,
    )

# -------------------------------------------------------------------
# Mapping
# -------------------------------------------------------------------

def map_bafin_file_entry(
    minio: MinioClient,
    file_entry: dict,
    file_number: int,
    effective_load_date: str,
) -> tuple:
    data_object_key = file_entry.get("data_object_key")
    metadata_object_key = file_entry.get("metadata_object_key")

    if not data_object_key:
        raise RuntimeError("BaFin manifest entry is missing data_object_key.")

    if metadata_object_key:
        try:
            metadata = minio.get_json_object(metadata_object_key)
        except Exception as exc:
            print(
                f"WARNING: Could not load metadata object "
                f"{metadata_object_key}: {exc}"
            )
            metadata = {}

    with minio.get_object_stream(data_object_key) as stream:
        raw_content, content_hash = read_text_and_hash_from_stream(
            stream=stream,
            store_content=BAFIN_STORE_RAW_CONTENT,
        )

    # Prefer metadata/file_entry hash if present, but keep calculated hash as fallback.
    source_hash = (
        metadata.get("sha256")
        or file_entry.get("sha256")
        or content_hash
    )

    return (
        APP_ENV,
        "BaFin Unternehmensdatenbank",
        metadata.get("source_name") or "bafin_company_search",
        "company_detail",
        "detail_page",
        file_number,
        file_entry.get("candidate_id") or metadata.get("candidate_id"),
        file_entry.get("lei") or metadata.get("lei"),
        file_entry.get("bafin_institut_id") or metadata.get("bafin_institut_id"),
        file_entry.get("legal_name") or metadata.get("legal_name"),
        file_entry.get("search_name") or metadata.get("search_name"),
        metadata.get("jurisdiction"),
        metadata.get("country"),
        metadata.get("source_reason"),
        metadata.get("priority"),
        metadata.get("http_status"),
        metadata.get("downloaded_bytes") or file_entry.get("downloaded_bytes"),
        source_hash,
        raw_content,
        metadata.get("content_type"),
        json.dumps(metadata, ensure_ascii=False) if metadata else None,
        file_entry.get("source_url") or metadata.get("source_url"),
        data_object_key,
        metadata_object_key,
        effective_load_date,
    )


# -------------------------------------------------------------------
# Processing
# -------------------------------------------------------------------

def process_bafin_files(
    minio: MinioClient,
    postgres: PostgresClient,
    success_files: list[dict],
    effective_load_date: str,
) -> int:
    total_inserted = 0
    prepared_rows = []

    for file_number, file_entry in enumerate(success_files, start=1):
        if BAFIN_MAX_FILES > 0 and file_number > BAFIN_MAX_FILES:
            print(
                f"Reached BAFIN_MAX_FILES={BAFIN_MAX_FILES}. "
                "Stopping BaFin file processing."
            )
            break

        data_object_key = file_entry.get("data_object_key")

        if not data_object_key:
            print("Skipping BaFin manifest entry without data_object_key.")
            continue

        print("------------------------------------------------------------")
        print(f"Source name: {file_entry.get('source_name')}")
        print(f"Object: s3://{MINIO_BUCKET}/{data_object_key}")
        print("Writing to table: raw.bafin_pages")

        delete_existing_rows_for_object(
            postgres=postgres,
            source_object_key=data_object_key,
        )

        prepared_rows.append(
            map_bafin_file_entry(
                minio=minio,
                file_entry=file_entry,
                file_number=file_number,
                effective_load_date=effective_load_date,
            )
        )

        if len(prepared_rows) >= BAFIN_LOAD_BATCH_SIZE:
            inserted_count = insert_bafin_rows(
                postgres=postgres,
                rows=prepared_rows,
            )

            total_inserted += inserted_count
            prepared_rows = []

            print(f"Inserted BaFin rows so far: {total_inserted}")

    if prepared_rows:
        inserted_count = insert_bafin_rows(
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
    print("Loading BaFin raw pages")
    print("Target table: raw.bafin_pages")
    print("Insert mode: PostgreSQL COPY")
    print(f"Requested BAFIN_LOAD_DATE: {BAFIN_LOAD_DATE or 'not set'}")
    print(f"Stale snapshot policy: {BAFIN_STALE_SNAPSHOT_POLICY}")
    print(f"Max snapshot age days: {BAFIN_MAX_SNAPSHOT_AGE_DAYS}")
    print(f"Batch size: {BAFIN_LOAD_BATCH_SIZE}")
    print(f"Max files: {BAFIN_MAX_FILES or 'unlimited'}")
    print(f"Rebuild indexes: {BAFIN_REBUILD_INDEXES}")
    print(f"Store raw content: {BAFIN_STORE_RAW_CONTENT}")

    minio = MinioClient.from_env()
    postgres = PostgresClient.from_env()

    try:
        ensure_raw_bafin_table(postgres)

        if BAFIN_REBUILD_INDEXES:
            print("Dropping BaFin indexes before load.")
            drop_raw_bafin_indexes(postgres)

        manifest_key, effective_load_date, manifest, freshness = resolve_bafin_manifest(
            minio=minio,
        )

        print("------------------------------------------------------------")
        print(f"Using BaFin manifest: s3://{MINIO_BUCKET}/{manifest_key}")
        print(f"Effective BaFin load date: {effective_load_date}")
        print(f"Freshness status: {freshness['freshness_status']}")
        print(f"Snapshot age days: {freshness['snapshot_age_days']}")

        success_files = [
            file
            for file in manifest.get("files", [])
            if file.get("status") == "success"
        ]

        if not success_files:
            raise RuntimeError(
                f"No successful BaFin files found in manifest for load_date={effective_load_date}."
            )

        total_inserted = process_bafin_files(
            minio=minio,
            postgres=postgres,
            success_files=success_files,
            effective_load_date=effective_load_date,
        )

        if BAFIN_REBUILD_INDEXES:
            print("Creating BaFin indexes after load.")
            create_raw_bafin_indexes(postgres)

        print("------------------------------------------------------------")
        print("BaFin raw load finished successfully.")
        print(f"Effective BaFin load date: {effective_load_date}")
        print(f"Freshness status: {freshness['freshness_status']}")
        print(f"Total inserted rows: {total_inserted}")

    finally:
        postgres.close()


if __name__ == "__main__":
    main()