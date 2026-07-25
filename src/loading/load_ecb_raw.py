import os
import csv
from io import TextIOWrapper
from typing import Iterator
from datetime import datetime, date, timezone

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
from common.stream_utils import iter_csv_rows_from_text_stream
from common.row_utils import parse_decimal, row_to_json_string
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
# If not set: load latest successful ECB snapshot from MinIO.
ECB_LOAD_DATE = os.getenv("ECB_LOAD_DATE")

ECB_MAX_SNAPSHOT_AGE_DAYS = int(
    os.getenv("ECB_MAX_SNAPSHOT_AGE_DAYS", "3")
)

# Supported values: warn, fail, allow
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
# PostgreSQL setup
# -------------------------------------------------------------------

def ensure_raw_ecb_table(postgres: PostgresClient) -> None:
    postgres.ensure_schemas()

    postgres.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.ecb_observations (
            raw_id BIGSERIAL PRIMARY KEY,

            app_env TEXT,
            source TEXT,

            dataset_code TEXT,
            series_key TEXT,
            indicator_name TEXT,
            frequency TEXT,
            unit TEXT,

            dataflow TEXT,
            freq TEXT,
            ref_area TEXT,

            time_period_raw TEXT,
            obs_date DATE,

            obs_value_raw TEXT,
            obs_value NUMERIC,
            obs_status TEXT,

            raw_row JSONB,

            source_url TEXT,
            source_object_key TEXT,
            metadata_object_key TEXT,

            source_load_date DATE,
            loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_ecb_observations_series_date
            ON raw.ecb_observations (series_key, obs_date);

        CREATE INDEX IF NOT EXISTS idx_ecb_observations_load_date
            ON raw.ecb_observations (source_load_date);

        CREATE INDEX IF NOT EXISTS idx_ecb_observations_source_object
            ON raw.ecb_observations (source_object_key);
        """
    )


# -------------------------------------------------------------------
# Manifest / freshness helpers
# ------------------------------------------------------------------
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
# Parsing helpers
# -------------------------------------------------------------------

def parse_obs_date(
    time_period_raw: str | None,
    frequency: str | None,
) -> date | None:
    if not time_period_raw:
        return None

    value = time_period_raw.strip()

    if not value:
        return None

    try:
        if frequency == "M" and len(value) == 7:
            return datetime.strptime(value + "-01", "%Y-%m-%d").date()

        if frequency == "B" and len(value) == 10:
            return datetime.strptime(value, "%Y-%m-%d").date()

        if len(value) == 10:
            return datetime.strptime(value, "%Y-%m-%d").date()

        if len(value) == 7:
            return datetime.strptime(value + "-01", "%Y-%m-%d").date()

    except ValueError:
        return None

    return None

# -------------------------------------------------------------------
# Database helpers
# -------------------------------------------------------------------

def delete_existing_rows_for_object(
    postgres: PostgresClient,
    source_object_key: str,
) -> None:
    postgres.execute(
        """
        DELETE FROM raw.ecb_observations
        WHERE source_object_key = %s
        """,
        (source_object_key,),
    )


def insert_ecb_rows(
    postgres: PostgresClient,
    rows: list[tuple],
) -> int:
    return postgres.copy_rows(
        table_name="raw.ecb_observations",
        columns=[
            "app_env",
            "source",
            "dataset_code",
            "series_key",
            "indicator_name",
            "frequency",
            "unit",
            "dataflow",
            "freq",
            "ref_area",
            "time_period_raw",
            "obs_date",
            "obs_value_raw",
            "obs_value",
            "obs_status",
            "raw_row",
            "source_url",
            "source_object_key",
            "metadata_object_key",
            "source_load_date",
        ],
        rows=rows,
    )

# -------------------------------------------------------------------
# Main job
# -------------------------------------------------------------------

def main() -> None:
    validate_environment()

    print("------------------------------------------------------------")
    print("Loading ECB raw observations")
    print("Target table: raw.ecb_observations")
    print("Insert mode: PostgreSQL COPY")
    print(f"Batch size: {ECB_LOAD_BATCH_SIZE}")
    print(f"Requested ECB_LOAD_DATE: {ECB_LOAD_DATE or 'not set'}")
    print(f"Stale snapshot policy: {ECB_STALE_SNAPSHOT_POLICY}")
    print(f"Max snapshot age days: {ECB_MAX_SNAPSHOT_AGE_DAYS}")

    minio = MinioClient.from_env()
    postgres = PostgresClient.from_env()

    try:
        ensure_raw_ecb_table(postgres)

        manifest_key, effective_load_date, manifest, freshness = resolve_ecb_manifest(
            minio=minio,
        )

        print("------------------------------------------------------------")
        print(f"Using ECB manifest: s3://{MINIO_BUCKET}/{manifest_key}")
        print(f"Effective ECB load date: {effective_load_date}")
        print(f"Freshness status: {freshness['freshness_status']}")
        print(f"Snapshot age days: {freshness['snapshot_age_days']}")

        success_files = [
            file
            for file in manifest.get("files", [])
            if file.get("status") == "success"
        ]

        if not success_files:
            raise RuntimeError(
                f"No successful ECB files found in manifest for load_date={effective_load_date}."
            )

        total_inserted = 0

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
                print("Skipping manifest entry without data_object_key.")
                continue

            print("------------------------------------------------------------")
            print(f"Dataset: {dataset_code}")
            print(f"Series key: {series_key}")
            print(f"Indicator: {indicator_name}")
            print(f"Object: s3://{MINIO_BUCKET}/{data_object_key}")
            print("Writing to table: raw.ecb_observations")

            delete_existing_rows_for_object(
                postgres=postgres,
                source_object_key=data_object_key,
            )

            prepared_rows = []
            file_row_count = 0
            file_inserted_count = 0

            with minio.get_object_stream(data_object_key) as stream:
                row_iterator = iter_csv_rows_from_text_stream(stream)

                for row in row_iterator:
                    file_row_count += 1

                    time_period_raw = row.get("TIME_PERIOD")
                    obs_value_raw = row.get("OBS_VALUE")
                    obs_status = row.get("OBS_STATUS")

                    obs_date = parse_obs_date(time_period_raw, frequency)
                    obs_value = parse_decimal(obs_value_raw)

                    prepared_rows.append(
                        (
                            APP_ENV,
                            "ECB Data Portal",
                            dataset_code,
                            series_key,
                            indicator_name,
                            frequency,
                            unit,
                            row.get("DATAFLOW") or row.get("KEY"),
                            row.get("FREQ"),
                            row.get("REF_AREA"),
                            time_period_raw,
                            obs_date,
                            obs_value_raw,
                            obs_value,
                            obs_status,
                            row_to_json_string(row),
                            source_url,
                            data_object_key,
                            metadata_object_key,
                            effective_load_date,
                        )
                    )

                    if len(prepared_rows) >= ECB_LOAD_BATCH_SIZE:
                        inserted_count = insert_ecb_rows(
                            postgres=postgres,
                            rows=prepared_rows,
                        )

                        file_inserted_count += inserted_count
                        total_inserted += inserted_count
                        prepared_rows = []

            if prepared_rows:
                inserted_count = insert_ecb_rows(
                    postgres=postgres,
                    rows=prepared_rows,
                )

                file_inserted_count += inserted_count
                total_inserted += inserted_count

            if file_row_count == 0:
                print(f"WARNING: CSV file contains no data rows: {data_object_key}")
                continue

            print(f"Inserted rows: {file_inserted_count}")

        print("------------------------------------------------------------")
        print("ECB raw load finished successfully.")
        print(f"Effective ECB load date: {effective_load_date}")
        print(f"Freshness status: {freshness['freshness_status']}")
        print(f"Total inserted rows: {total_inserted}")

    finally:
        postgres.close()


if __name__ == "__main__":
    main()