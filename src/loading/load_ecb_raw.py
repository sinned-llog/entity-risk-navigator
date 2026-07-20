import os
import csv
import json
from io import StringIO
from datetime import datetime, date, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import psycopg2
from psycopg2.extras import execute_values, Json
from common.minio_client import MinioClient


# -------------------------------------------------------------------
# Environment configuration
# -------------------------------------------------------------------

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

APP_ENV = os.getenv("APP_ENV", "dev")

LOAD_DT = datetime.now(timezone.utc)
DEFAULT_LOAD_DATE = LOAD_DT.strftime("%Y-%m-%d")

ECB_LOAD_DATE = os.getenv("ECB_LOAD_DATE", DEFAULT_LOAD_DATE)


# -------------------------------------------------------------------
# Validation
# -------------------------------------------------------------------

def validate_environment() -> None:
    required_values = {
        "MINIO_ENDPOINT": MINIO_ENDPOINT,
        "MINIO_ROOT_USER": MINIO_ROOT_USER,
        "MINIO_ROOT_PASSWORD": MINIO_ROOT_PASSWORD,
        "MINIO_BUCKET": MINIO_BUCKET,
        "POSTGRES_HOST": POSTGRES_HOST,
        "POSTGRES_DB": POSTGRES_DB,
        "POSTGRES_USER": POSTGRES_USER,
        "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
    }

    missing = [key for key, value in required_values.items() if not value]

    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )


# -------------------------------------------------------------------
# Clients
# -------------------------------------------------------------------

def create_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


# -------------------------------------------------------------------
# PostgreSQL setup
# -------------------------------------------------------------------

def ensure_raw_schema_and_table(conn) -> None:
    ddl = """
    CREATE SCHEMA IF NOT EXISTS raw;

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

    with conn.cursor() as cursor:
        cursor.execute(ddl)

    conn.commit()


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def get_text_object(s3_client, object_key: str) -> str:
    response = s3_client.get_object(
        Bucket=MINIO_BUCKET,
        Key=object_key,
    )

    content = response["Body"].read()
    return content.decode("utf-8-sig", errors="replace")


def load_manifest(s3_client, load_date: str) -> dict[str, Any]:
    manifest_key = (
        f"ecb/_manifests/load_date={load_date}/download_ecb_manifest.json"
    )

    text = get_text_object(s3_client, manifest_key)
    manifest = json.loads(text)

    return manifest


def parse_obs_date(time_period_raw: str | None, frequency: str | None) -> date | None:
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


def parse_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None

    text = value.strip()

    if not text:
        return None

    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def read_ecb_csv_rows(csv_text: str) -> list[dict[str, str]]:
    reader = csv.DictReader(StringIO(csv_text))
    return list(reader)


def delete_existing_rows_for_object(conn, source_object_key: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            DELETE FROM raw.ecb_observations
            WHERE source_object_key = %s
            """,
            (source_object_key,),
        )

    conn.commit()


def insert_ecb_rows(conn, rows: list[tuple]) -> int:
    if not rows:
        return 0

    sql = """
        INSERT INTO raw.ecb_observations (
            app_env,
            source,
            dataset_code,
            series_key,
            indicator_name,
            frequency,
            unit,
            dataflow,
            freq,
            ref_area,
            time_period_raw,
            obs_date,
            obs_value_raw,
            obs_value,
            obs_status,
            raw_row,
            source_url,
            source_object_key,
            metadata_object_key,
            source_load_date
        )
        VALUES %s
    """

    with conn.cursor() as cursor:
        execute_values(cursor, sql, rows, page_size=1000)

    conn.commit()
    return len(rows)


# -------------------------------------------------------------------
# Main job
# -------------------------------------------------------------------

def main() -> None:
    validate_environment()

    print("------------------------------------------------------------")
    print("Loading ECB raw observations")
    print(f"ECB load date: {ECB_LOAD_DATE}")

    s3_client = create_s3_client()
    conn = create_postgres_connection()

    try:
        ensure_raw_schema_and_table(conn)

        manifest = load_manifest(s3_client, ECB_LOAD_DATE)

        success_files = [
            file
            for file in manifest.get("files", [])
            if file.get("status") == "success"
        ]

        if not success_files:
            raise RuntimeError(
                f"No successful ECB files found in manifest for load_date={ECB_LOAD_DATE}."
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
            print(f"Object: s3://{MINIO_BUCKET}/{data_object_key}")

            csv_text = get_text_object(s3_client, data_object_key)
            csv_rows = read_ecb_csv_rows(csv_text)

            prepared_rows = []

            for row in csv_rows:
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
                        row.get("DATAFLOW"),
                        row.get("FREQ"),
                        row.get("REF_AREA"),
                        time_period_raw,
                        obs_date,
                        obs_value_raw,
                        obs_value,
                        obs_status,
                        json.dumps(row),
                        source_url,
                        data_object_key,
                        metadata_object_key,
                        ECB_LOAD_DATE,
                    )
                )

            delete_existing_rows_for_object(conn, data_object_key)
            inserted_count = insert_ecb_rows(conn, prepared_rows)
            total_inserted += inserted_count

            print(f"Inserted rows: {inserted_count}")

        print("------------------------------------------------------------")
        print("ECB raw load finished successfully.")
        print(f"Total inserted rows: {total_inserted}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()