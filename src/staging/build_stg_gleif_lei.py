import os
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from common.postgres_client import PostgresClient
from common.audit_logger import (
    start_job_run,
    finish_job_run_success,
    finish_job_run_failure,
)


# -------------------------------------------------------------------
# Environment configuration
# -------------------------------------------------------------------

APP_ENV = os.getenv("APP_ENV", "dev")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Optional: build staging for an exact raw source_load_date.
GLEIF_STAGING_LOAD_DATE = os.getenv("GLEIF_STAGING_LOAD_DATE")

# Load Mode Configuration
is_full_load = os.getenv("GLEIF_DOWNLOAD_LEI_FULL", "false").lower() in ("true", "1", "yes")

LOAD_DT = datetime.now(timezone.utc)
LOAD_TIMESTAMP_UTC = LOAD_DT.isoformat()


# -------------------------------------------------------------------
# Validation
# -------------------------------------------------------------------

def validate_environment() -> None:
    required_values = {
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


# -------------------------------------------------------------------
# PostgreSQL setup & schema management
# -------------------------------------------------------------------

def ensure_staging_gleif_lei_table(postgres: PostgresClient) -> None:
    """
    Ensures that the final staging schema and target table exist.
    """
    postgres.execute(
        """
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.stg_gleif_lei (
            lei TEXT PRIMARY KEY,

            legal_name TEXT,
            legal_name_normalized TEXT,

            entity_status TEXT,
            registration_status TEXT,

            legal_jurisdiction TEXT,
            legal_address_country TEXT,
            headquarters_address_country TEXT,

            next_renewal_date DATE,
            last_update_date DATE,

            source_load_date DATE,
            source_object_key TEXT,

            raw_id BIGINT,
            loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


# -------------------------------------------------------------------
# Resolve source load date
# -------------------------------------------------------------------

def resolve_effective_load_date(postgres: PostgresClient):
    if GLEIF_STAGING_LOAD_DATE:
        return GLEIF_STAGING_LOAD_DATE

    latest_load_date = postgres.fetch_scalar(
        """
        SELECT MAX(source_load_date)
        FROM raw.gleif_lei
        WHERE source_load_date IS NOT NULL;
        """
    )

    if not latest_load_date:
        raise RuntimeError(
            "No source_load_date found in raw.gleif_lei. "
            "Run load_gleif_raw.py first."
        )

    return latest_load_date


def count_raw_rows(
    postgres: PostgresClient,
    effective_load_date,
) -> int:
    return postgres.fetch_scalar(
        """
        SELECT COUNT(*)
        FROM raw.gleif_lei
        WHERE source_load_date = %s;
        """,
        (effective_load_date,),
    ) or 0


# -------------------------------------------------------------------
# Transformation Query SQL Generator
# -------------------------------------------------------------------

def build_transformation_sql(target_table_name: str) -> str:
    """
    Generates the SQL for Deduplication + Transformation and direct INSERT.
    """
    return f"""
    WITH selected AS (
        SELECT DISTINCT ON (lei)
            raw_id,
            lei,
            legal_name,

            NULLIF(
                regexp_replace(
                    lower(trim(COALESCE(legal_name, ''))),
                    '\\s+',
                    ' ',
                    'g'
                ),
                ''
            ) AS legal_name_normalized,

            entity_status,
            registration_status,

            legal_jurisdiction,
            legal_address_country,
            headquarters_address_country,

            CASE
                WHEN next_renewal_date_raw ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}'
                    THEN substring(next_renewal_date_raw from 1 for 10)::date
                ELSE NULL
            END AS next_renewal_date,

            CASE
                WHEN last_update_date_raw ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}'
                    THEN substring(last_update_date_raw from 1 for 10)::date
                ELSE NULL
            END AS last_update_date,

            source_load_date,
            source_object_key

        FROM raw.gleif_lei
        WHERE source_load_date = %s
          AND lei IS NOT NULL
          AND trim(lei) <> ''

        ORDER BY
            lei,
            source_load_date DESC,
            raw_id DESC
    ),
    inserted AS (
        INSERT INTO staging.{target_table_name} (
            raw_id,
            lei,
            legal_name,
            legal_name_normalized,
            entity_status,
            registration_status,
            legal_jurisdiction,
            legal_address_country,
            headquarters_address_country,
            next_renewal_date,
            last_update_date,
            source_load_date,
            source_object_key
        )
        SELECT
            raw_id,
            lei,
            legal_name,
            legal_name_normalized,
            entity_status,
            registration_status,
            legal_jurisdiction,
            legal_address_country,
            headquarters_address_country,
            next_renewal_date,
            last_update_date,
            source_load_date,
            source_object_key
        FROM selected
        RETURNING 1
    )
    SELECT COUNT(*) FROM inserted;
    """


# -------------------------------------------------------------------
# Strategy 1: Full Load (Build-and-Swap)
# -------------------------------------------------------------------

def execute_full_load_build_and_swap(
    postgres: PostgresClient,
    build_table_name: str,
    job_run_id: int,
    effective_load_date,
) -> int:
    # 1. Erstelle Unlogged Table ohne Indizes (schnellster Schreibvorgang)
    postgres.execute(
        f"""
        DROP TABLE IF EXISTS staging.{build_table_name};
        CREATE UNLOGGED TABLE staging.{build_table_name} (
            lei TEXT,
            legal_name TEXT,
            legal_name_normalized TEXT,
            entity_status TEXT,
            registration_status TEXT,
            legal_jurisdiction TEXT,
            legal_address_country TEXT,
            headquarters_address_country TEXT,
            next_renewal_date DATE,
            last_update_date DATE,
            source_load_date DATE,
            source_object_key TEXT,
            raw_id BIGINT,
            loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # 2. Daten transformieren & einfügen
    transform_query = build_transformation_sql(build_table_name)
    inserted_count = postgres.fetch_scalar(transform_query, (effective_load_date,)) or 0

    if inserted_count == 0:
        raise RuntimeError(f"Build table staging.{build_table_name} contains no rows.")

    # 3. Indizes NACH dem Laden aufbauen (viel schneller)
    postgres.execute(
        f"""
        ALTER TABLE staging.{build_table_name}
            ADD CONSTRAINT {build_table_name}_pk PRIMARY KEY (lei);

        CREATE INDEX ix_glei_b_{job_run_id}_name
            ON staging.{build_table_name} (legal_name_normalized);

        CREATE INDEX ix_glei_b_{job_run_id}_entity_status
            ON staging.{build_table_name} (entity_status);

        CREATE INDEX ix_glei_b_{job_run_id}_registration_status
            ON staging.{build_table_name} (registration_status);

        CREATE INDEX ix_glei_b_{job_run_id}_jurisdiction
            ON staging.{build_table_name} (legal_jurisdiction);

        CREATE INDEX ix_glei_b_{job_run_id}_load_date
            ON staging.{build_table_name} (source_load_date);

        ANALYZE staging.{build_table_name};
        """
    )

    # 4. Atomarer Table-Swap
    postgres.execute(
        f"""
        BEGIN;

        DROP TABLE IF EXISTS staging.stg_gleif_lei_old;

        ALTER TABLE staging.stg_gleif_lei
            RENAME TO stg_gleif_lei_old;

        ALTER TABLE staging.{build_table_name}
            RENAME TO stg_gleif_lei;

        ALTER TABLE staging.stg_gleif_lei SET LOGGED;

        DROP TABLE staging.stg_gleif_lei_old;

        COMMIT;
        """
    )

    return inserted_count


def cleanup_build_table(
    postgres: PostgresClient,
    build_table_name: str | None,
) -> None:
    if not build_table_name:
        return
    try:
        postgres.execute(f"DROP TABLE IF EXISTS staging.{build_table_name};")
    except Exception as cleanup_exc:
        print(f"WARNING: Could not clean up build table staging.{build_table_name}: {cleanup_exc}")


# -------------------------------------------------------------------
# Strategy 2: Delta Load (In-Place Truncate & Insert)
# -------------------------------------------------------------------

def execute_delta_load_truncate_insert(
    postgres: PostgresClient,
    effective_load_date,
) -> int:
    try:
        # Transaktion manuell verwalten, damit fetch_scalar nicht durcheinander kommt
        postgres.execute("BEGIN;")
        postgres.execute("TRUNCATE TABLE staging.stg_gleif_lei;")

        transform_query = build_transformation_sql("stg_gleif_lei")
        inserted_count = postgres.fetch_scalar(transform_query, (effective_load_date,)) or 0

        postgres.execute("COMMIT;")
        postgres.execute("ANALYZE staging.stg_gleif_lei;")

        return inserted_count
    except Exception:
        postgres.execute("ROLLBACK;")
        raise


# -------------------------------------------------------------------
# Main job
# -------------------------------------------------------------------

def main() -> None:
    validate_environment()

    mode_label = "FULL LOAD (Build-and-Swap)" if is_full_load else "DELTA LOAD (In-Place Truncate & Insert)"

    print("------------------------------------------------------------")
    print("Populating staging.stg_gleif_lei")
    print(f"Execution Strategy: {mode_label}")
    print("Source table: raw.gleif_lei")
    print(f"Requested GLEIF_STAGING_LOAD_DATE: {GLEIF_STAGING_LOAD_DATE or 'not set'}")

    postgres = PostgresClient.from_env()

    job_run_id = None
    effective_load_date = None
    rows_read = 0
    rows_inserted = 0
    build_table_name = None

    try:
        job_run_id = start_job_run(
            postgres=postgres,
            job_name="build_stg_gleif_lei",
            job_type="staging",
            source="GLEIF Golden Copy public downloads",
            target_system="postgres",
            target_table="staging.stg_gleif_lei",
            app_env=APP_ENV,
            metadata_json={
                "requested_load_date": GLEIF_STAGING_LOAD_DATE,
                "is_full_load": is_full_load,
                "source_table": "raw.gleif_lei",
                "target_table": "staging.stg_gleif_lei",
                "build_strategy": "build_table_swap" if is_full_load else "in_place_truncate_insert",
            },
        )

        ensure_staging_gleif_lei_table(postgres)

        effective_load_date = resolve_effective_load_date(postgres)

        print(f"Effective GLEIF staging load date: {effective_load_date}")

        rows_read = count_raw_rows(
            postgres=postgres,
            effective_load_date=effective_load_date,
        )

        if rows_read == 0:
            raise RuntimeError(
                f"No raw.gleif_lei rows found for source_load_date={effective_load_date}."
            )

        # Weichenstellung: Full vs. Delta
        if is_full_load:
            build_table_name = f"stg_gleif_lei_b_{job_run_id}"
            print(f"Creating build table: staging.{build_table_name}")

            rows_inserted = execute_full_load_build_and_swap(
                postgres=postgres,
                build_table_name=build_table_name,
                job_run_id=job_run_id,
                effective_load_date=effective_load_date,
            )
            # Nach erfolgreichem Swap wurde die build table umbenannt
            build_table_name = None
        else:
            rows_inserted = execute_delta_load_truncate_insert(
                postgres=postgres,
                effective_load_date=effective_load_date,
            )

        if rows_inserted == 0:
            raise RuntimeError("Staging load completed but inserted 0 rows.")

        finish_job_run_success(
            postgres=postgres,
            job_run_id=job_run_id,
            status="success",
            effective_load_date=effective_load_date,
            rows_read=rows_read,
            rows_inserted=rows_inserted,
            metadata_json={
                "source_table": "raw.gleif_lei",
                "target_table": "staging.stg_gleif_lei",
                "effective_load_date": str(effective_load_date),
                "is_full_load": is_full_load,
                "rows_read": rows_read,
                "rows_inserted": rows_inserted,
                "build_strategy": "build_table_swap" if is_full_load else "in_place_truncate_insert",
            },
        )

        print("------------------------------------------------------------")
        print("staging.stg_gleif_lei updated successfully.")
        print(f"Effective GLEIF staging load date: {effective_load_date}")
        print(f"Rows read: {rows_read}")
        print(f"Rows inserted: {rows_inserted}")

    except Exception as exc:
        if is_full_load and build_table_name:
            cleanup_build_table(postgres, build_table_name)

        if job_run_id:
            finish_job_run_failure(
                postgres=postgres,
                job_run_id=job_run_id,
                error_message=str(exc),
                effective_load_date=effective_load_date,
                rows_read=rows_read,
                rows_inserted=rows_inserted,
                metadata_json={
                    "source_table": "raw.gleif_lei",
                    "target_table": "staging.stg_gleif_lei",
                    "effective_load_date": str(effective_load_date)
                    if effective_load_date
                    else None,
                    "is_full_load": is_full_load,
                    "build_strategy": "build_table_swap" if is_full_load else "in_place_truncate_insert",
                },
            )

        raise

    finally:
        postgres.close()


if __name__ == "__main__":
    main()