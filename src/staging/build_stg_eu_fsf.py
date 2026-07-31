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
EU_FSF_STAGING_LOAD_DATE = os.getenv("EU_FSF_STAGING_LOAD_DATE")

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
# PostgreSQL setup
# -------------------------------------------------------------------

def ensure_staging_eu_fsf_table(postgres: PostgresClient) -> None:
    postgres.execute(
        """
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.stg_eu_fsf_full_csv (
            eu_fsf_entry_key TEXT PRIMARY KEY,

            row_hash TEXT,
            row_number INTEGER,

            entity_logical_id TEXT,
            eu_reference_number TEXT,
            un_reference_number TEXT,
            subject_type TEXT,

            name_alias_whole_name TEXT,
            name_alias_whole_name_normalized TEXT,

            programme TEXT,
            regulation_type TEXT,
            regulation_number_title TEXT,

            designation_date DATE,
            publication_date DATE,

            source_load_date DATE,
            source_object_key TEXT,
            metadata_object_key TEXT,
            source_url TEXT,

            raw_id BIGINT,
            loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


# -------------------------------------------------------------------
# Resolve source load date
# -------------------------------------------------------------------

def resolve_effective_load_date(postgres: PostgresClient):
    if EU_FSF_STAGING_LOAD_DATE:
        return EU_FSF_STAGING_LOAD_DATE

    latest_load_date = postgres.fetch_scalar(
        """
        SELECT MAX(source_load_date)
        FROM raw.eu_fsf_full_csv
        WHERE source_load_date IS NOT NULL;
        """
    )

    if not latest_load_date:
        raise RuntimeError(
            "No source_load_date found in raw.eu_fsf_full_csv. "
            "Run load_eu_fsf_raw.py first."
        )

    return latest_load_date


def count_raw_rows(
    postgres: PostgresClient,
    effective_load_date,
) -> int:
    return postgres.fetch_scalar(
        """
        SELECT COUNT(*)
        FROM raw.eu_fsf_full_csv
        WHERE source_load_date = %s;
        """,
        (effective_load_date,),
    ) or 0


# -------------------------------------------------------------------
# Build table helpers (Build-and-Swap)
# -------------------------------------------------------------------

def create_staging_eu_fsf_build_table(
    postgres: PostgresClient,
    build_table_name: str,
) -> None:
    postgres.execute(
        f"""
        DROP TABLE IF EXISTS staging.{build_table_name};

        CREATE UNLOGGED TABLE staging.{build_table_name} (
            eu_fsf_entry_key TEXT,

            row_hash TEXT,
            row_number INTEGER,

            entity_logical_id TEXT,
            eu_reference_number TEXT,
            un_reference_number TEXT,
            subject_type TEXT,

            name_alias_whole_name TEXT,
            name_alias_whole_name_normalized TEXT,

            programme TEXT,
            regulation_type TEXT,
            regulation_number_title TEXT,

            designation_date DATE,
            publication_date DATE,

            source_load_date DATE,
            source_object_key TEXT,
            metadata_object_key TEXT,
            source_url TEXT,

            raw_id BIGINT,
            loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


def build_staging_eu_fsf_build_table(
    postgres: PostgresClient,
    build_table_name: str,
    effective_load_date,
) -> int:
    inserted_count = postgres.fetch_scalar(
        f"""
        WITH source_rows AS (
            SELECT
                raw_id,
                row_hash,
                row_number,

                entity_logical_id,
                eu_reference_number,
                un_reference_number,
                subject_type,

                name_alias_whole_name,

                NULLIF(
                    regexp_replace(
                        lower(trim(COALESCE(name_alias_whole_name, ''))),
                        '\\s+',
                        ' ',
                        'g'
                    ),
                    ''
                ) AS name_alias_whole_name_normalized,

                programme,
                regulation_type,
                regulation_number_title,

                designation_date_raw,
                publication_date_raw,

                source_load_date,
                source_object_key,
                metadata_object_key,
                source_url,

                COALESCE(
                    row_hash,
                    md5(
                        concat_ws(
                            '|',
                            COALESCE(entity_logical_id, ''),
                            COALESCE(eu_reference_number, ''),
                            COALESCE(un_reference_number, ''),
                            COALESCE(subject_type, ''),
                            COALESCE(name_alias_whole_name, ''),
                            COALESCE(programme, ''),
                            COALESCE(regulation_type, ''),
                            COALESCE(regulation_number_title, ''),
                            COALESCE(designation_date_raw, ''),
                            COALESCE(publication_date_raw, '')
                        )
                    )
                ) AS eu_fsf_entry_key

            FROM raw.eu_fsf_full_csv
            WHERE source_load_date = %s
        ),
        selected AS (
            SELECT DISTINCT ON (eu_fsf_entry_key)
                eu_fsf_entry_key,

                raw_id,
                row_hash,
                row_number,

                entity_logical_id,
                eu_reference_number,
                un_reference_number,
                subject_type,

                name_alias_whole_name,
                name_alias_whole_name_normalized,

                programme,
                regulation_type,
                regulation_number_title,

                CASE
                    WHEN designation_date_raw ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}'
                        THEN substring(designation_date_raw from 1 for 10)::date
                    ELSE NULL
                END AS designation_date,

                CASE
                    WHEN publication_date_raw ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}'
                        THEN substring(publication_date_raw from 1 for 10)::date
                    ELSE NULL
                END AS publication_date,

                source_load_date,
                source_object_key,
                metadata_object_key,
                source_url

            FROM source_rows
            WHERE eu_fsf_entry_key IS NOT NULL
              AND trim(eu_fsf_entry_key) <> ''

            ORDER BY
                eu_fsf_entry_key,
                source_load_date DESC,
                raw_id DESC
        ),
        inserted AS (
            INSERT INTO staging.{build_table_name} (
                eu_fsf_entry_key,

                raw_id,
                row_hash,
                row_number,

                entity_logical_id,
                eu_reference_number,
                un_reference_number,
                subject_type,

                name_alias_whole_name,
                name_alias_whole_name_normalized,

                programme,
                regulation_type,
                regulation_number_title,

                designation_date,
                publication_date,

                source_load_date,
                source_object_key,
                metadata_object_key,
                source_url
            )
            SELECT
                eu_fsf_entry_key,

                raw_id,
                row_hash,
                row_number,

                entity_logical_id,
                eu_reference_number,
                un_reference_number,
                subject_type,

                name_alias_whole_name,
                name_alias_whole_name_normalized,

                programme,
                regulation_type,
                regulation_number_title,

                designation_date,
                publication_date,

                source_load_date,
                source_object_key,
                metadata_object_key,
                source_url
            FROM selected
            RETURNING 1
        )
        SELECT COUNT(*)
        FROM inserted;
        """,
        (effective_load_date,),
    )

    return inserted_count or 0


def create_staging_eu_fsf_build_indexes(
    postgres: PostgresClient,
    build_table_name: str,
    job_run_id: int,
) -> None:
    postgres.execute(
        f"""
        ALTER TABLE staging.{build_table_name}
            ADD CONSTRAINT {build_table_name}_pk PRIMARY KEY (eu_fsf_entry_key);

        CREATE INDEX ix_efsfb_{job_run_id}_row_hash
            ON staging.{build_table_name} (row_hash);

        CREATE INDEX ix_efsfb_{job_run_id}_entity_logical_id
            ON staging.{build_table_name} (entity_logical_id);

        CREATE INDEX ix_efsfb_{job_run_id}_eu_reference_number
            ON staging.{build_table_name} (eu_reference_number);

        CREATE INDEX ix_efsfb_{job_run_id}_un_reference_number
            ON staging.{build_table_name} (un_reference_number);

        CREATE INDEX ix_efsfb_{job_run_id}_subject_type
            ON staging.{build_table_name} (subject_type);

        CREATE INDEX ix_efsfb_{job_run_id}_name_normalized
            ON staging.{build_table_name} (name_alias_whole_name_normalized);

        CREATE INDEX ix_efsfb_{job_run_id}_programme
            ON staging.{build_table_name} (programme);

        CREATE INDEX ix_efsfb_{job_run_id}_load_date
            ON staging.{build_table_name} (source_load_date);

        ANALYZE staging.{build_table_name};
        """
    )


# -------------------------------------------------------------------
# Swap / cleanup helpers
# -------------------------------------------------------------------

def swap_staging_eu_fsf_table(
    postgres: PostgresClient,
    build_table_name: str,
) -> None:
    postgres.execute(
        f"""
        BEGIN;

        DROP TABLE IF EXISTS staging.stg_eu_fsf_full_csv_old;

        ALTER TABLE staging.stg_eu_fsf_full_csv
            RENAME TO stg_eu_fsf_full_csv_old;

        ALTER TABLE staging.{build_table_name}
            RENAME TO stg_eu_fsf_full_csv;

        ALTER TABLE staging.stg_eu_fsf_full_csv SET LOGGED;

        DROP TABLE staging.stg_eu_fsf_full_csv_old;

        COMMIT;
        """
    )


def cleanup_staging_eu_fsf_build_table(
    postgres: PostgresClient,
    build_table_name: str | None,
) -> None:
    if not build_table_name:
        return

    try:
        postgres.execute(
            f"""
            DROP TABLE IF EXISTS staging.{build_table_name};
            """
        )
    except Exception as cleanup_exc:
        print(
            f"WARNING: Could not clean up build table "
            f"staging.{build_table_name}: {cleanup_exc}"
        )


# -------------------------------------------------------------------
# Main job
# -------------------------------------------------------------------

def main() -> None:
    validate_environment()

    print("------------------------------------------------------------")
    print("Building staging.stg_eu_fsf_full_csv with build-and-swap")
    print("Source table: raw.eu_fsf_full_csv")
    print(f"Requested EU_FSF_STAGING_LOAD_DATE: {EU_FSF_STAGING_LOAD_DATE or 'not set'}")

    postgres = PostgresClient.from_env()

    job_run_id = None
    effective_load_date = None
    rows_read = 0
    rows_inserted = 0
    build_table_name = None

    try:
        job_run_id = start_job_run(
            postgres=postgres,
            job_name="build_stg_eu_fsf_full_csv",
            job_type="staging",
            source="EU Financial Sanctions Files",
            target_system="postgres",
            target_table="staging.stg_eu_fsf_full_csv",
            app_env=APP_ENV,
            metadata_json={
                "requested_load_date": EU_FSF_STAGING_LOAD_DATE,
                "source_table": "raw.eu_fsf_full_csv",
                "target_table": "staging.stg_eu_fsf_full_csv",
                "build_strategy": "build_table_swap",
            },
        )

        build_table_name = f"stg_eu_fsf_full_csv_b_{job_run_id}"

        ensure_staging_eu_fsf_table(postgres)

        effective_load_date = resolve_effective_load_date(postgres)

        print(f"Effective EU FSF staging load date: {effective_load_date}")
        print(f"Build table: staging.{build_table_name}")

        rows_read = count_raw_rows(
            postgres=postgres,
            effective_load_date=effective_load_date,
        )

        if rows_read == 0:
            raise RuntimeError(
                f"No raw.eu_fsf_full_csv rows found for source_load_date={effective_load_date}."
            )

        create_staging_eu_fsf_build_table(
            postgres=postgres,
            build_table_name=build_table_name,
        )

        rows_inserted = build_staging_eu_fsf_build_table(
            postgres=postgres,
            build_table_name=build_table_name,
            effective_load_date=effective_load_date,
        )

        if rows_inserted == 0:
            raise RuntimeError(
                f"Build table staging.{build_table_name} contains no rows."
            )

        create_staging_eu_fsf_build_indexes(
            postgres=postgres,
            build_table_name=build_table_name,
            job_run_id=job_run_id,
        )

        swap_staging_eu_fsf_table(
            postgres=postgres,
            build_table_name=build_table_name,
        )

        build_table_name = None

        finish_job_run_success(
            postgres=postgres,
            job_run_id=job_run_id,
            status="success",
            effective_load_date=effective_load_date,
            rows_read=rows_read,
            rows_inserted=rows_inserted,
            metadata_json={
                "source_table": "raw.eu_fsf_full_csv",
                "target_table": "staging.stg_eu_fsf_full_csv",
                "effective_load_date": str(effective_load_date),
                "rows_read": rows_read,
                "rows_inserted": rows_inserted,
                "build_strategy": "build_table_swap",
            },
        )

        print("------------------------------------------------------------")
        print("staging.stg_eu_fsf_full_csv built successfully.")
        print(f"Effective EU FSF staging load date: {effective_load_date}")
        print(f"Rows read: {rows_read}")
        print(f"Rows inserted: {rows_inserted}")

    except Exception as exc:
        cleanup_staging_eu_fsf_build_table(
            postgres=postgres,
            build_table_name=build_table_name,
        )

        if job_run_id:
            finish_job_run_failure(
                postgres=postgres,
                job_run_id=job_run_id,
                error_message=str(exc),
                effective_load_date=effective_load_date,
                rows_read=rows_read,
                rows_inserted=rows_inserted,
                metadata_json={
                    "source_table": "raw.eu_fsf_full_csv",
                    "target_table": "staging.stg_eu_fsf_full_csv",
                    "effective_load_date": str(effective_load_date)
                    if effective_load_date
                    else None,
                    "build_table_name": build_table_name,
                    "build_strategy": "build_table_swap",
                },
            )

        raise

    finally:
        postgres.close()


if __name__ == "__main__":
    main()