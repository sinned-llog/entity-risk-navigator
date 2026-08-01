import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from common.audit_logger import (
    finish_job_run_failure,
    finish_job_run_success,
    start_job_run,
)
from common.manifest_utils import (
    evaluate_snapshot_freshness,
    find_latest_successful_manifest,
    handle_stale_snapshot,
)
from common.minio_client import MinioClient
from common.postgres_client import PostgresClient

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

# If set: load explicitly this snapshot.
# If not set: search for the latest successful BaFin manifest in MinIO.
BAFIN_LOAD_DATE = os.getenv("BAFIN_LOAD_DATE")

BAFIN_MAX_SNAPSHOT_AGE_DAYS = int(
    os.getenv("BAFIN_MAX_SNAPSHOT_AGE_DAYS", "30")
)

# Supported values: warn, fail, allow
BAFIN_STALE_SNAPSHOT_POLICY = os.getenv(
    "BAFIN_STALE_SNAPSHOT_POLICY",
    "warn",
).lower()

BAFIN_LOAD_BATCH_SIZE = int(os.getenv("BAFIN_LOAD_BATCH_SIZE", "100"))

# 0 = unlimited
BAFIN_MAX_FILES = int(os.getenv("BAFIN_MAX_FILES", "0"))

BAFIN_REBUILD_INDEXES = (
    os.getenv("BAFIN_REBUILD_INDEXES", "false").lower() == "true"
)

# Store raw HTML/JSON string in Postgres. If false, only sha256 hash is computed.
BAFIN_STORE_RAW_CONTENT = (
    os.getenv("BAFIN_STORE_RAW_CONTENT", "true").lower() == "true"
)


# -------------------------------------------------------------------
# Environment Validation
# -------------------------------------------------------------------

def validate_environment() -> None:
    """Validate that required environment variables are set and properly configured."""
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

    if BAFIN_STALE_SNAPSHOT_POLICY not in {"warn", "fail", "allow"}:
        raise RuntimeError(
            "BAFIN_STALE_SNAPSHOT_POLICY must be one of: warn, fail, allow"
        )


# -------------------------------------------------------------------
# Database Table & Index Management
# -------------------------------------------------------------------

def ensure_raw_bafin_table(postgres: PostgresClient) -> None:
    """Ensures raw schema and the target raw.bafin_pages table exist."""
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
    """Drops secondary indexes for faster bulk COPY performance."""
    postgres.execute(
        """
        DROP INDEX IF EXISTS raw.idx_bafin_pages_bafin_institut_id;
        DROP INDEX IF EXISTS raw.idx_bafin_pages_legal_name;
        DROP INDEX IF EXISTS raw.idx_bafin_pages_search_name;
        DROP INDEX IF EXISTS raw.idx_bafin_pages_content_hash;
        DROP INDEX IF EXISTS raw.idx_bafin_pages_load_date;
        DROP INDEX IF EXISTS raw.idx_bafin_pages_source_object;
        """
    )


def create_raw_bafin_indexes(postgres: PostgresClient) -> None:
    """Re-creates secondary indexes post COPY ingestion."""
    postgres.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_bafin_pages_bafin_institut_id
            ON raw.bafin_pages (bafin_institut_id);

        CREATE INDEX IF NOT EXISTS idx_bafin_pages_legal_name
            ON raw.bafin_pages (legal_name);

        CREATE INDEX IF NOT EXISTS idx_bafin_pages_search_name
            ON raw.bafin_pages (search_name);

        CREATE INDEX IF NOT EXISTS idx_bafin_pages_content_hash
            ON raw.bafin_pages (content_hash);

        CREATE INDEX IF NOT EXISTS idx_bafin_pages_load_date
            ON raw.bafin_pages (source_load_date);

        CREATE INDEX IF NOT EXISTS idx_bafin_pages_source_object
            ON raw.bafin_pages (source_object_key);
        """
    )


# -------------------------------------------------------------------
# Manifest & Freshness Resolution
# -------------------------------------------------------------------

def resolve_bafin_manifest(
    minio: MinioClient,
) -> Tuple[str, str, dict, dict]:
    """Locates the target manifest in MinIO and verifies snapshot freshness policy."""
    if BAFIN_LOAD_DATE:
        manifest_key = (
            f"bafin/_manifests/load_date={BAFIN_LOAD_DATE}/download_bafin_manifest.json"
        )

        if not minio.object_exists(manifest_key):
            raise RuntimeError(
                f"BaFin manifest not found in MinIO: s3://{MINIO_BUCKET}/{manifest_key}. "
                "Run download_bafin.py first or verify BAFIN_LOAD_DATE."
            )

        manifest = minio.get_json_object(manifest_key)
        effective_load_date = BAFIN_LOAD_DATE

    else:
        logger.info(
            "BAFIN_LOAD_DATE not set. Searching for latest successful BaFin manifest in MinIO."
        )
        manifest_key, effective_load_date, manifest = (
            find_latest_successful_manifest(
                minio=minio,
                manifest_prefix="bafin/_manifests/",
                manifest_filename="download_bafin_manifest.json",
            )
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
# Stream & Hashing Helpers
# -------------------------------------------------------------------

def read_text_and_hash_from_stream(
    stream: Any,
    encoding: str = "utf-8-sig",
    store_content: bool = True,
    chunk_size: int = 1024 * 1024,
) -> Tuple[Optional[str], str]:
    """Reads object content in chunks to calculate SHA256 hash and extract string body."""
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


# -------------------------------------------------------------------
# Database Operations
# -------------------------------------------------------------------

def delete_existing_rows_for_object(
    postgres: PostgresClient,
    source_object_key: str,
) -> None:
    """Removes previously loaded records for the given object key to support idempotency."""
    postgres.execute(
        """
        DELETE FROM raw.bafin_pages
        WHERE source_object_key = %s
        """,
        (source_object_key,),
    )


def insert_bafin_rows(
    postgres: PostgresClient,
    rows: List[Tuple[Any, ...]],
) -> int:
    """Executes high-performance COPY insertion into raw.bafin_pages within an active transaction."""
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
# Record Mapping
# -------------------------------------------------------------------

def map_bafin_file_entry(
    minio: MinioClient,
    file_entry: Dict[str, Any],
    file_number: int,
    effective_load_date: str,
) -> Tuple[Any, ...]:
    """Retrieves file & metadata from MinIO and returns a tuple structured for database COPY."""
    data_object_key = file_entry.get("data_object_key")
    metadata_object_key = file_entry.get("metadata_object_key")

    if not data_object_key:
        raise RuntimeError("BaFin manifest entry is missing data_object_key.")

    metadata: Dict[str, Any] = {}

    if metadata_object_key:
        try:
            metadata = minio.get_json_object(metadata_object_key)
        except Exception as exc:
            logger.warning(
                f"Could not load metadata object {metadata_object_key}: {exc}"
            )

    with minio.get_object_stream(data_object_key) as stream:
        raw_content, content_hash = read_text_and_hash_from_stream(
            stream=stream,
            store_content=BAFIN_STORE_RAW_CONTENT,
        )

    # Prefer metadata/file_entry SHA256 if available, fallback to computed stream hash
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
# File Batch Processing
# -------------------------------------------------------------------

def process_bafin_files(
    minio: MinioClient,
    postgres: PostgresClient,
    success_files: List[Dict[str, Any]],
    effective_load_date: str,
) -> int:
    """Iterates through manifest entries, reads content from MinIO, and batch-inserts into Postgres."""
    total_inserted = 0
    prepared_rows: List[Tuple[Any, ...]] = []

    for file_number, file_entry in enumerate(success_files, start=1):
        if BAFIN_MAX_FILES > 0 and file_number > BAFIN_MAX_FILES:
            logger.info(
                f"Reached BAFIN_MAX_FILES={BAFIN_MAX_FILES}. Stopping file processing."
            )
            break

        data_object_key = file_entry.get("data_object_key")
        if not data_object_key:
            logger.warning("Skipping BaFin manifest entry missing data_object_key.")
            continue

        logger.info(
            f"[{file_number}/{len(success_files)}] Processing candidate "
            f"'{file_entry.get('candidate_id')}' -> s3://{MINIO_BUCKET}/{data_object_key}"
        )

        # Wrap object cleanup and COPY insertion inside a transaction block
        with postgres.transaction():
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
                logger.info(f"Inserted BaFin rows so far: {total_inserted}")

    # Insert remaining buffer
    if prepared_rows:
        with postgres.transaction():
            inserted_count = insert_bafin_rows(
                postgres=postgres,
                rows=prepared_rows,
            )
            total_inserted += inserted_count

    return total_inserted


# -------------------------------------------------------------------
# Main Ingestion Runner
# -------------------------------------------------------------------

def main() -> None:
    """Main execution entrypoint for loading raw BaFin data from MinIO into Postgres."""
    validate_environment()

    logger.info("------------------------------------------------------------")
    logger.info("Starting BaFin Raw Load from MinIO")
    logger.info("Target table: raw.bafin_pages")
    logger.info(f"Requested BAFIN_LOAD_DATE: {BAFIN_LOAD_DATE or 'not set'}")
    logger.info(f"Batch size: {BAFIN_LOAD_BATCH_SIZE}")

    minio = MinioClient.from_env()
    postgres = PostgresClient.from_env()

    job_run_id = None
    manifest_key = None
    effective_load_date = None
    manifest = None
    freshness = None
    total_inserted = 0
    processed_files_count = 0

    try:
        # Start audit job tracking
        job_run_id = start_job_run(
            postgres=postgres,
            job_name="load_bafin_raw",
            job_type="raw_load",
            source="BaFin Unternehmensdatenbank",
            target_system="postgres",
            target_table="raw.bafin_pages",
            app_env=APP_ENV,
            metadata_json={
                "requested_load_date": BAFIN_LOAD_DATE,
                "max_snapshot_age_days": BAFIN_MAX_SNAPSHOT_AGE_DAYS,
                "stale_snapshot_policy": BAFIN_STALE_SNAPSHOT_POLICY,
                "batch_size": BAFIN_LOAD_BATCH_SIZE,
                "max_files": BAFIN_MAX_FILES,
                "rebuild_indexes": BAFIN_REBUILD_INDEXES,
                "store_raw_content": BAFIN_STORE_RAW_CONTENT,
            },
        )

        ensure_raw_bafin_table(postgres)

        if BAFIN_REBUILD_INDEXES:
            logger.info("Dropping BaFin indexes before bulk COPY load.")
            drop_raw_bafin_indexes(postgres)

        manifest_key, effective_load_date, manifest, freshness = (
            resolve_bafin_manifest(minio=minio)
        )

        logger.info(f"Using manifest: s3://{MINIO_BUCKET}/{manifest_key}")
        logger.info(f"Effective load date: {effective_load_date}")
        logger.info(f"Freshness status: {freshness['freshness_status']}")

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
            logger.info("Re-creating BaFin indexes after bulk COPY load.")
            create_raw_bafin_indexes(postgres)

        audit_status = "success"
        if (
            manifest.get("status") == "success_with_warnings"
            or int(manifest.get("warning_count") or 0) > 0
        ):
            audit_status = "success_with_warnings"

        processed_files_count = (
            min(len(success_files), BAFIN_MAX_FILES)
            if BAFIN_MAX_FILES > 0
            else len(success_files)
        )

        # Log successful completion to audit database
        finish_job_run_success(
            postgres=postgres,
            job_run_id=job_run_id,
            status=audit_status,
            manifest_key=manifest_key,
            effective_load_date=effective_load_date,
            freshness=freshness,
            files_discovered=len(manifest.get("files", [])),
            files_processed=processed_files_count,
            files_success=processed_files_count,
            files_failed=manifest.get("failed_count"),
            rows_inserted=total_inserted,
            warning_count=manifest.get("warning_count"),
            error_count=manifest.get("error_count"),
            metadata_json={
                "manifest_status": manifest.get("status"),
                "manifest_success_count": manifest.get("success_count"),
                "manifest_failed_count": manifest.get("failed_count"),
                "store_raw_content": BAFIN_STORE_RAW_CONTENT,
                "batch_size": BAFIN_LOAD_BATCH_SIZE,
                "max_files": BAFIN_MAX_FILES,
            },
        )

        logger.info("------------------------------------------------------------")
        logger.info("BaFin raw load finished successfully.")
        logger.info(f"Total inserted rows: {total_inserted}")

    except Exception as exc:
        logger.error(f"BaFin raw load failed: {str(exc)}", exc_info=True)
        if job_run_id:
            finish_job_run_failure(
                postgres=postgres,
                job_run_id=job_run_id,
                error_message=str(exc),
                manifest_key=manifest_key,
                effective_load_date=effective_load_date,
                freshness=freshness,
                files_discovered=len(manifest.get("files", [])) if manifest else None,
                files_processed=processed_files_count,
                files_success=processed_files_count,
                rows_inserted=total_inserted,
            )
        raise

    finally:
        postgres.close()


if __name__ == "__main__":
    main()