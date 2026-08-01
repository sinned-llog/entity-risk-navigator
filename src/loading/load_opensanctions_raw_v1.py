import os
import json
from datetime import datetime, timezone
from typing import Iterator

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from common.minio_client import MinioClient
from common.postgres_client import PostgresClient
from common.manifest_utils import find_latest_successful_manifest, evaluate_snapshot_freshness, handle_stale_snapshot
from common.stream_utils import iter_csv_rows_from_text_stream
from common.audit_logger import start_job_run, finish_job_run_success, finish_job_run_failure

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

OPENSANCTIONS_LOAD_DATE = os.getenv("OPENSANCTIONS_LOAD_DATE")

OPENSANCTIONS_MAX_SNAPSHOT_AGE_DAYS = int(
    os.getenv("OPENSANCTIONS_MAX_SNAPSHOT_AGE_DAYS", "3")
)

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
# PostgreSQL setup (Raw Schema)
# -------------------------------------------------------------------

def ensure_raw_opensanctions_table(postgres: PostgresClient) -> None:
    postgres.ensure_schemas()

    postgres.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.opensanctions_targets (
            raw_id BIGSERIAL PRIMARY KEY,

            app_env TEXT,
            source TEXT,
            source_name TEXT,
            dataset_group TEXT,
            snapshot_type TEXT,
            file_row_number INTEGER,

            id TEXT,
            schema TEXT,
            caption TEXT,
            aliases TEXT,
            birth_date TEXT,
            countries TEXT,
            addresses TEXT,
            identifiers TEXT,
            sanctions TEXT,
            phones TEXT,
            emails TEXT,
            program_ids TEXT,
            datasets TEXT,
            first_seen TEXT,
            last_seen TEXT,
            last_change TEXT,

            source_url TEXT,
            source_object_key TEXT,
            metadata_object_key TEXT,
            source_load_date DATE,
            loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
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
                f"s3://{MINIO_BUCKET}/{manifest_key}."
            )

        manifest = minio.get_json_object(manifest_key)
        effective_load_date = OPENSANCTIONS_LOAD_DATE

    else:
        print("Searching latest successful OpenSanctions manifest in MinIO.")
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
        DELETE FROM raw.opensanctions_targets
        WHERE source_object_key = %s
        """,
        (source_object_key,),
        commit=False,
    )


def insert_opensanctions_rows(
    postgres: PostgresClient,
    rows: list[tuple],
) -> int:
    return postgres.copy_rows(
        table_name="raw.opensanctions_targets",
        columns=[
            "app_env",
            "source",
            "source_name",
            "dataset_group",
            "snapshot_type",
            "file_row_number",
            "id",
            "schema",
            "caption",
            "aliases",
            "birth_date",
            "countries",
            "addresses",
            "identifiers",
            "sanctions",
            "phones",
            "emails",
            "program_ids",
            "datasets",
            "first_seen",
            "last_seen",
            "last_change",
            "source_url",
            "source_object_key",
            "metadata_object_key",
            "source_load_date",
        ],
        rows=rows,
        commit=False,
    )


# -------------------------------------------------------------------
# Processing (minimal EL-Mapping)
# -------------------------------------------------------------------

def map_opensanctions_raw_row(
    row: dict,
    row_number: int,
    file_entry: dict,
    data_object_key: str,
    effective_load_date: str,
) -> tuple:
   
    return (
        APP_ENV,
        "OpenSanctions",
        file_entry.get("source_name"),
        file_entry.get("dataset_group"),
        file_entry.get("snapshot_type"),
        row_number,
        row.get("id"),
        row.get("schema"),
        row.get("name") or row.get("caption"),
        row.get("aliases"),
        row.get("birth_date"),
        row.get("countries"),
        row.get("addresses"),
        row.get("identifiers"),
        row.get("sanctions"),
        row.get("phones"),
        row.get("emails"),
        row.get("program_ids"),
        row.get("dataset") or row.get("datasets"),
        row.get("first_seen"),
        row.get("last_seen"),
        row.get("last_change"),
        file_entry.get("source_url"),
        data_object_key,
        file_entry.get("metadata_object_key"),
        effective_load_date,
    )


def process_opensanctions_file(
    postgres: PostgresClient,
    row_iterator: Iterator[dict],
    file_entry: dict,
    data_object_key: str,
    effective_load_date: str,
) -> tuple[int, int]:
    total_inserted = 0
    total_read = 0
    prepared_rows = []

    with postgres.transaction():
        delete_existing_rows_for_object(
            postgres=postgres,
            source_object_key=data_object_key,
        )

        for row_number, row in enumerate(row_iterator, start=1):
            if (
                OPENSANCTIONS_MAX_ROWS_PER_FILE > 0
                and row_number > OPENSANCTIONS_MAX_ROWS_PER_FILE
            ):
                print(f"Reached OPENSANCTIONS_MAX_ROWS_PER_FILE={OPENSANCTIONS_MAX_ROWS_PER_FILE}.")
                if hasattr(row_iterator, "close"):
                    row_iterator.close()
                break

            total_read += 1
            prepared_rows.append(
                map_opensanctions_raw_row(
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

        if prepared_rows:
            inserted_count = insert_opensanctions_rows(
                postgres=postgres,
                rows=prepared_rows,
            )
            total_inserted += inserted_count
            prepared_rows = []

    return total_read, total_inserted


# -------------------------------------------------------------------
# Main job
# -------------------------------------------------------------------

def main() -> None:
    validate_environment()

    print("------------------------------------------------------------")
    print("Loading OpenSanctions raw targets CSV")
    print("Target table: raw.opensanctions_targets")
    print("Insert mode: PostgreSQL COPY (Raw EL)")

    minio = MinioClient.from_env()
    postgres = PostgresClient.from_env()

    job_run_id = None
    manifest_key = None
    effective_load_date = None
    manifest = None
    freshness = None

    total_inserted = 0
    total_rows_read = 0
    processed_file_count = 0

    try:
        job_run_id = start_job_run(
            postgres=postgres,
            job_name="load_opensanctions_raw",
            job_type="raw_load",
            source="OpenSanctions",
            target_system="postgres",
            target_table="raw.opensanctions_targets",
            app_env=APP_ENV,
            metadata_json={
                "requested_load_date": OPENSANCTIONS_LOAD_DATE,
                "max_snapshot_age_days": OPENSANCTIONS_MAX_SNAPSHOT_AGE_DAYS,
                "stale_snapshot_policy": OPENSANCTIONS_STALE_SNAPSHOT_POLICY,
                "batch_size": OPENSANCTIONS_LOAD_BATCH_SIZE,
                "max_rows_per_file": OPENSANCTIONS_MAX_ROWS_PER_FILE,
            },
        )

        ensure_raw_opensanctions_table(postgres)

        manifest_key, effective_load_date, manifest, freshness = resolve_opensanctions_manifest(
            minio=minio,
        )

        success_files = [
            file
            for file in manifest.get("files", [])
            if file.get("status") == "success"
        ]

        if not success_files:
            raise RuntimeError(
                f"No successful OpenSanctions files found in manifest for load_date={effective_load_date}."
            )

        for file_entry in success_files:
            data_object_key = file_entry.get("data_object_key")
            if not data_object_key:
                continue

            processed_file_count += 1
            print(f"Processing object: s3://{MINIO_BUCKET}/{data_object_key}")

            with minio.get_object_stream(data_object_key) as stream:
                row_iterator = iter_csv_rows_from_text_stream(stream)

                file_read, file_inserted = process_opensanctions_file(
                    postgres=postgres,
                    row_iterator=row_iterator,
                    file_entry=file_entry,
                    data_object_key=data_object_key,
                    effective_load_date=effective_load_date,
                )

                total_rows_read += file_read
                total_inserted += file_inserted

        finish_job_run_success(
            postgres=postgres,
            job_run_id=job_run_id,
            status="success",
            manifest_key=manifest_key,
            effective_load_date=effective_load_date,
            freshness=freshness,
            files_discovered=len(manifest.get("files", [])),
            files_processed=processed_file_count,
            files_success=processed_file_count,
            files_failed=manifest.get("failed_count"),
            rows_read=total_rows_read,
            rows_inserted=total_inserted,
        )

        print("------------------------------------------------------------")
        print("OpenSanctions raw load finished successfully.")

    except Exception as exc:
        if job_run_id:
            finish_job_run_failure(
                postgres=postgres,
                job_run_id=job_run_id,
                error_message=str(exc),
                manifest_key=manifest_key,
                effective_load_date=effective_load_date,
                freshness=freshness,
            )
        raise

    finally:
        postgres.close()

if __name__ == "__main__":
    main()