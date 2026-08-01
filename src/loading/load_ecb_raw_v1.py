import os
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
from common.stream_utils import (
    iter_csv_rows_from_text_stream,
    iter_csv_rows_from_zip_stream_v1,
)
from common.row_utils import row_to_json_string
from common.audit_logger import (
    start_job_run,
    finish_job_run_success,
    finish_job_run_failure,
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

ECB_LOAD_DATE = os.getenv("ECB_LOAD_DATE")

ECB_MAX_SNAPSHOT_AGE_DAYS = int(
    os.getenv("ECB_MAX_SNAPSHOT_AGE_DAYS", "3")
)

ECB_STALE_SNAPSHOT_POLICY = os.getenv(
    "ECB_STALE_SNAPSHOT_POLICY",
    "warn",
).lower()

ECB_LOAD_BATCH_SIZE = int(
    os.getenv("ECB_LOAD_BATCH_SIZE", "5000")
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

    if ECB_STALE_SNAPSHOT_POLICY not in {"warn", "fail", "allow"}:
        raise RuntimeError(
            "ECB_STALE_SNAPSHOT_POLICY must be one of: warn, fail, allow"
        )


# -------------------------------------------------------------------
# PostgreSQL Landing Table Setup
# -------------------------------------------------------------------

def ensure_raw_ecb_table(postgres: PostgresClient) -> None:
    """Creates the raw ecb_observations_full landing table."""
    postgres.ensure_schemas()

    postgres.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.ecb_observations_full (
            raw_id BIGSERIAL PRIMARY KEY,

            app_env TEXT,
            dataset_code TEXT,
            series_key TEXT,
            indicator_name TEXT,
            frequency TEXT,
            unit TEXT,
            
            raw_row JSONB,

            source_url TEXT,
            source_object_key TEXT,
            metadata_object_key TEXT,
            source_load_date DATE,
            loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_ecb_full_source_object
            ON raw.ecb_observations_full (source_object_key);

        CREATE INDEX IF NOT EXISTS idx_ecb_full_load_date
            ON raw.ecb_observations_full (source_load_date);
        """
    )


# -------------------------------------------------------------------
# Manifest / Freshness Helpers
# -------------------------------------------------------------------

def resolve_ecb_manifest(
    minio: MinioClient,
) -> tuple[str, str, dict, dict]:
    if ECB_LOAD_DATE:
        manifest_key = (
            f"ecb/_manifests/load_date={ECB_LOAD_DATE}/download_ecb_manifest.json"
        )

        if not minio.object_exists(manifest_key):
            raise RuntimeError(
                f"ECB manifest not found in MinIO: "
                f"s3://{MINIO_BUCKET}/{manifest_key}."
            )

        manifest = minio.get_json_object(manifest_key)
        effective_load_date = ECB_LOAD_DATE

    else:
        print("ECB_LOAD_DATE not set. Searching latest successful ECB manifest in MinIO.")

        manifest_key, effective_load_date, manifest = find_latest_successful_manifest(
            minio=minio,
            manifest_prefix="ecb/_manifests/",
            manifest_filename="download_ecb_manifest.json",
        )

    freshness = evaluate_snapshot_freshness(
        effective_load_date=effective_load_date,
        max_age_days=ECB_MAX_SNAPSHOT_AGE_DAYS,
        policy=ECB_STALE_SNAPSHOT_POLICY,
    )

    handle_stale_snapshot(
        freshness=freshness,
        source_name="ECB",
    )

    return manifest_key, effective_load_date, manifest, freshness


# -------------------------------------------------------------------
# Database Persistence Helpers
# -------------------------------------------------------------------

def delete_existing_rows_for_object(
    postgres: PostgresClient,
    source_object_key: str,
) -> None:
    """Deletes existing rows inside the active transaction."""
    postgres.execute(
        """
        DELETE FROM raw.ecb_observations_full
        WHERE source_object_key = %s
        """,
        (source_object_key,),
    )


def insert_ecb_raw_rows(
    postgres: PostgresClient,
    rows: list[tuple],
) -> int:
    """Bulk inserts raw JSON and metadata using PostgreSQL COPY."""
    return postgres.copy_rows(
        table_name="raw.ecb_observations_full",
        columns=[
            "app_env",
            "dataset_code",
            "series_key",
            "indicator_name",
            "frequency",
            "unit",
            "raw_row",
            "source_url",
            "source_object_key",
            "metadata_object_key",
            "source_load_date",
        ],
        rows=rows,
    )


# -------------------------------------------------------------------
# Main Load Execution
# -------------------------------------------------------------------

def main() -> None:
    validate_environment()

    print("------------------------------------------------------------")
    print("Extract & Load: ECB Raw Observations Full")
    print("Target table: raw.ecb_observations_full")
    print("Ingestion mode: Minimal EL (Transformations offloaded to dbt)")
    print(f"Batch size: {ECB_LOAD_BATCH_SIZE}")

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
            job_name="load_ecb_raw_full",
            job_type="raw_load",
            source="ECB Data Portal",
            target_system="postgres",
            target_table="raw.ecb_observations_full",
            app_env=APP_ENV,
            metadata_json={
                "requested_load_date": ECB_LOAD_DATE,
                "batch_size": ECB_LOAD_BATCH_SIZE,
            },
        )

        ensure_raw_ecb_table(postgres)

        manifest_key, effective_load_date, manifest, freshness = resolve_ecb_manifest(
            minio=minio,
        )

        success_files = [
            file
            for file in manifest.get("files", [])
            if file.get("status") == "success"
        ]

        if not success_files:
            raise RuntimeError(
                f"No successful ECB files found in manifest for load_date={effective_load_date}."
            )

        for file_entry in success_files:
            dataset_code = file_entry.get("dataset_code")
            series_key = file_entry.get("series_key")
            indicator_name = file_entry.get("indicator_name")
            frequency = file_entry.get("frequency")
            unit = file_entry.get("unit")
            source_url = file_entry.get("source_url")
            data_object_key = file_entry.get("data_object_key")
            metadata_object_key = file_entry.get("metadata_object_key")

            if not data_object_key:
                continue

            processed_file_count += 1
            file_row_count = 0
            file_inserted_count = 0

            # Atomic transaction per file
            with postgres.transaction():
                delete_existing_rows_for_object(
                    postgres=postgres,
                    source_object_key=data_object_key,
                )

                prepared_rows = []

                with minio.get_object_stream(data_object_key) as stream:
                    if data_object_key.lower().endswith(".zip"):
                        row_iterator = iter_csv_rows_from_zip_stream_v1(stream)
                    else:
                        row_iterator = iter_csv_rows_from_text_stream(stream)

                    for row in row_iterator:
                        file_row_count += 1

                        # Directly payload raw dictionary to JSON without casting
                        prepared_rows.append(
                            (
                                APP_ENV,
                                dataset_code,
                                series_key,
                                indicator_name,
                                frequency,
                                unit,
                                row_to_json_string(row),
                                source_url,
                                data_object_key,
                                metadata_object_key,
                                effective_load_date,
                            )
                        )

                        if len(prepared_rows) >= ECB_LOAD_BATCH_SIZE:
                            inserted_count = insert_ecb_raw_rows(
                                postgres=postgres,
                                rows=prepared_rows,
                            )
                            file_inserted_count += inserted_count
                            prepared_rows = []

                    if prepared_rows:
                        inserted_count = insert_ecb_raw_rows(
                            postgres=postgres,
                            rows=prepared_rows,
                        )
                        file_inserted_count += inserted_count
                        prepared_rows = []

            total_rows_read += file_row_count
            total_inserted += file_inserted_count

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
            rows_read=total_rows_read,
            rows_inserted=total_inserted,
        )

        print("ECB raw load to ecb_observations_full finished successfully.")

    except Exception as exc:
        if job_run_id:
            finish_job_run_failure(
                postgres=postgres,
                job_run_id=job_run_id,
                error_message=str(exc),
                manifest_key=manifest_key,
                effective_load_date=effective_load_date,
            )
        raise

    finally:
        postgres.close()


if __name__ == "__main__":
    main()