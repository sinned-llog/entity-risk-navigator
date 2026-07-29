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

# Full load flag: 'true' / '1' / 'yes' triggers build-and-swap, otherwise incremental upsert
IS_FULL_LOAD = os.getenv("GLEIF_DOWNLOAD_RR_FULL", "false").lower() in ("true", "1", "yes")

# Optional: build staging for an exact raw source_load_date.
GLEIF_STAGING_LOAD_DATE = os.getenv("GLEIF_STAGING_LOAD_DATE")

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

def ensure_staging_gleif_relationships_table(postgres: PostgresClient) -> None:
    postgres.execute(
        """
        CREATE SCHEMA IF NOT EXISTS staging;

        CREATE TABLE IF NOT EXISTS staging.stg_gleif_relationships (
            relationship_key TEXT PRIMARY KEY,

            start_node_id TEXT,
            start_node_id_type TEXT,
            end_node_id TEXT,
            end_node_id_type TEXT,

            relationship_type TEXT,
            relationship_status TEXT,

            relationship_period_start DATE,
            relationship_period_end DATE,
            relationship_period_type TEXT,

            registration_status TEXT,
            initial_registration_date DATE,
            last_update_date DATE,
            next_renewal_date DATE,

            managing_lou TEXT,
            validation_sources TEXT,
            validation_documents TEXT,
            validation_reference TEXT,

            source_load_date DATE,
            source_object_key TEXT,

            raw_id BIGINT,
            loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS ix_stg_gleif_rel_start_node ON staging.stg_gleif_relationships (start_node_id);
        CREATE INDEX IF NOT EXISTS ix_stg_gleif_rel_end_node ON staging.stg_gleif_relationships (end_node_id);
        CREATE INDEX IF NOT EXISTS ix_stg_gleif_rel_type ON staging.stg_gleif_relationships (relationship_type);
        CREATE INDEX IF NOT EXISTS ix_stg_gleif_rel_status ON staging.stg_gleif_relationships (relationship_status);
        CREATE INDEX IF NOT EXISTS ix_stg_gleif_rel_load_date ON staging.stg_gleif_relationships (source_load_date);
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
        FROM raw.gleif_rr
        WHERE source_load_date IS NOT NULL;
        """
    )

    if not latest_load_date:
        raise RuntimeError(
            "No source_load_date found in raw.gleif_rr. "
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
        FROM raw.gleif_rr
        WHERE source_load_date = %s;
        """,
        (effective_load_date,),
    ) or 0


# -------------------------------------------------------------------
# Full Load (Build-and-Swap) Helpers
# -------------------------------------------------------------------

def create_staging_gleif_relationships_build_table(
    postgres: PostgresClient,
    build_table_name: str,
) -> None:
    postgres.execute(
        f"""
        DROP TABLE IF EXISTS staging.{build_table_name};

        CREATE UNLOGGED TABLE staging.{build_table_name} (
            relationship_key TEXT,

            start_node_id TEXT,
            start_node_id_type TEXT,
            end_node_id TEXT,
            end_node_id_type TEXT,

            relationship_type TEXT,
            relationship_status TEXT,

            relationship_period_start DATE,
            relationship_period_end DATE,
            relationship_period_type TEXT,

            registration_status TEXT,
            initial_registration_date DATE,
            last_update_date DATE,
            next_renewal_date DATE,

            managing_lou TEXT,
            validation_sources TEXT,
            validation_documents TEXT,
            validation_reference TEXT,

            source_load_date DATE,
            source_object_key TEXT,

            raw_id BIGINT,
            loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


def build_staging_gleif_relationships_build_table(
    postgres: PostgresClient,
    build_table_name: str,
    effective_load_date,
) -> int:
    inserted_count = postgres.fetch_scalar(
        f"""
        WITH source_rows AS (
            SELECT
                raw_id,

                start_node_id,
                start_node_id_type,
                end_node_id,
                end_node_id_type,

                relationship_type,
                relationship_status,

                relationship_period_start_raw,
                relationship_period_end_raw,
                relationship_period_type,

                registration_status,
                initial_registration_date_raw,
                last_update_date_raw,
                next_renewal_date_raw,

                managing_lou,
                validation_sources,
                validation_documents,
                validation_reference,

                source_load_date,
                source_object_key,

                md5(
                    concat_ws(
                        '|',
                        COALESCE(start_node_id, ''),
                        COALESCE(start_node_id_type, ''),
                        COALESCE(end_node_id, ''),
                        COALESCE(end_node_id_type, ''),
                        COALESCE(relationship_type, ''),
                        COALESCE(relationship_period_start_raw, ''),
                        COALESCE(relationship_period_end_raw, ''),
                        COALESCE(relationship_period_type, '')
                    )
                ) AS relationship_key

            FROM raw.gleif_rr
            WHERE source_load_date = %s
              AND start_node_id IS NOT NULL
              AND trim(start_node_id) <> ''
              AND end_node_id IS NOT NULL
              AND trim(end_node_id) <> ''
              AND relationship_type IS NOT NULL
              AND trim(relationship_type) <> ''
        ),
        selected AS (
            SELECT DISTINCT ON (relationship_key)
                relationship_key,

                raw_id,

                start_node_id,
                start_node_id_type,
                end_node_id,
                end_node_id_type,

                relationship_type,
                relationship_status,

                CASE
                    WHEN relationship_period_start_raw ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}'
                        THEN substring(relationship_period_start_raw from 1 for 10)::date
                    ELSE NULL
                END AS relationship_period_start,

                CASE
                    WHEN relationship_period_end_raw ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}'
                        THEN substring(relationship_period_end_raw from 1 for 10)::date
                    ELSE NULL
                END AS relationship_period_end,

                relationship_period_type,

                registration_status,

                CASE
                    WHEN initial_registration_date_raw ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}'
                        THEN substring(initial_registration_date_raw from 1 for 10)::date
                    ELSE NULL
                END AS initial_registration_date,

                CASE
                    WHEN last_update_date_raw ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}'
                        THEN substring(last_update_date_raw from 1 for 10)::date
                    ELSE NULL
                END AS last_update_date,

                CASE
                    WHEN next_renewal_date_raw ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}'
                        THEN substring(next_renewal_date_raw from 1 for 10)::date
                    ELSE NULL
                END AS next_renewal_date,

                managing_lou,
                validation_sources,
                validation_documents,
                validation_reference,

                source_load_date,
                source_object_key

            FROM source_rows

            ORDER BY
                relationship_key,
                source_load_date DESC,
                raw_id DESC
        ),
        inserted AS (
            INSERT INTO staging.{build_table_name} (
                relationship_key,
                raw_id,

                start_node_id,
                start_node_id_type,
                end_node_id,
                end_node_id_type,

                relationship_type,
                relationship_status,

                relationship_period_start,
                relationship_period_end,
                relationship_period_type,

                registration_status,
                initial_registration_date,
                last_update_date,
                next_renewal_date,

                managing_lou,
                validation_sources,
                validation_documents,
                validation_reference,

                source_load_date,
                source_object_key
            )
            SELECT
                relationship_key,
                raw_id,

                start_node_id,
                start_node_id_type,
                end_node_id,
                end_node_id_type,

                relationship_type,
                relationship_status,

                relationship_period_start,
                relationship_period_end,
                relationship_period_type,

                registration_status,
                initial_registration_date,
                last_update_date,
                next_renewal_date,

                managing_lou,
                validation_sources,
                validation_documents,
                validation_reference,

                source_load_date,
                source_object_key
            FROM selected
            RETURNING 1
        )
        SELECT COUNT(*)
        FROM inserted;
        """,
        (effective_load_date,),
    )

    return inserted_count or 0


def create_staging_gleif_relationships_build_indexes(
    postgres: PostgresClient,
    build_table_name: str,
    job_run_id: int,
) -> None:
    postgres.execute(
        f"""
        ALTER TABLE staging.{build_table_name}
            ADD CONSTRAINT {build_table_name}_pk PRIMARY KEY (relationship_key);

        CREATE INDEX ix_grr_b_{job_run_id}_start_node
            ON staging.{build_table_name} (start_node_id);

        CREATE INDEX ix_grr_b_{job_run_id}_end_node
            ON staging.{build_table_name} (end_node_id);

        CREATE INDEX ix_grr_b_{job_run_id}_relationship_type
            ON staging.{build_table_name} (relationship_type);

        CREATE INDEX ix_grr_b_{job_run_id}_relationship_status
            ON staging.{build_table_name} (relationship_status);

        CREATE INDEX ix_grr_b_{job_run_id}_registration_status
            ON staging.{build_table_name} (registration_status);

        CREATE INDEX ix_grr_b_{job_run_id}_load_date
            ON staging.{build_table_name} (source_load_date);

        ANALYZE staging.{build_table_name};
        """
    )


def swap_staging_gleif_relationships_table(
    postgres: PostgresClient,
    build_table_name: str,
) -> None:
    postgres.execute(
        f"""
        BEGIN;

        DROP TABLE IF EXISTS staging.stg_gleif_relationships_old;

        ALTER TABLE staging.stg_gleif_relationships
            RENAME TO stg_gleif_relationships_old;

        ALTER TABLE staging.{build_table_name}
            RENAME TO stg_gleif_relationships;

        ALTER TABLE staging.stg_gleif_relationships SET LOGGED;

        DROP TABLE staging.stg_gleif_relationships_old;

        COMMIT;
        """
    )


def cleanup_staging_gleif_relationships_build_table(
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
# Delta / Incremental Upsert Helpers
# -------------------------------------------------------------------

def execute_incremental_upsert(
    postgres: PostgresClient,
    build_table_name: str,
    effective_load_date,
) -> int:
    """
    Alters delta data in an unlogged table and performs an UPSERT
    on staging.stg_gleif_relationships.
    """
    create_staging_gleif_relationships_build_table(postgres, build_table_name)
    build_staging_gleif_relationships_build_table(postgres, build_table_name, effective_load_date)

    upserted_count = postgres.fetch_scalar(
        f"""
        WITH upserted AS (
            INSERT INTO staging.stg_gleif_relationships (
                relationship_key,
                raw_id,

                start_node_id,
                start_node_id_type,
                end_node_id,
                end_node_id_type,

                relationship_type,
                relationship_status,

                relationship_period_start,
                relationship_period_end,
                relationship_period_type,

                registration_status,
                initial_registration_date,
                last_update_date,
                next_renewal_date,

                managing_lou,
                validation_sources,
                validation_documents,
                validation_reference,

                source_load_date,
                source_object_key,
                loaded_at
            )
            SELECT
                relationship_key,
                raw_id,

                start_node_id,
                start_node_id_type,
                end_node_id,
                end_node_id_type,

                relationship_type,
                relationship_status,

                relationship_period_start,
                relationship_period_end,
                relationship_period_type,

                registration_status,
                initial_registration_date,
                last_update_date,
                next_renewal_date,

                managing_lou,
                validation_sources,
                validation_documents,
                validation_reference,

                source_load_date,
                source_object_key,
                CURRENT_TIMESTAMP
            FROM staging.{build_table_name}
            ON CONFLICT (relationship_key) DO UPDATE SET
                raw_id                      = EXCLUDED.raw_id,
                start_node_id               = EXCLUDED.start_node_id,
                start_node_id_type          = EXCLUDED.start_node_id_type,
                end_node_id                 = EXCLUDED.end_node_id,
                end_node_id_type            = EXCLUDED.end_node_id_type,
                relationship_type           = EXCLUDED.relationship_type,
                relationship_status         = EXCLUDED.relationship_status,
                relationship_period_start   = EXCLUDED.relationship_period_start,
                relationship_period_end     = EXCLUDED.relationship_period_end,
                relationship_period_type    = EXCLUDED.relationship_period_type,
                registration_status         = EXCLUDED.registration_status,
                initial_registration_date   = EXCLUDED.initial_registration_date,
                last_update_date            = EXCLUDED.last_update_date,
                next_renewal_date           = EXCLUDED.next_renewal_date,
                managing_lou                = EXCLUDED.managing_lou,
                validation_sources          = EXCLUDED.validation_sources,
                validation_documents        = EXCLUDED.validation_documents,
                validation_reference        = EXCLUDED.validation_reference,
                source_load_date            = EXCLUDED.source_load_date,
                source_object_key          = EXCLUDED.source_object_key,
                loaded_at                   = EXCLUDED.loaded_at
            RETURNING 1
        )
        SELECT COUNT(*) FROM upserted;
        """
    )

    cleanup_staging_gleif_relationships_build_table(postgres, build_table_name)
    postgres.execute("ANALYZE staging.stg_gleif_relationships;")

    return upserted_count or 0


# -------------------------------------------------------------------
# Main job
# -------------------------------------------------------------------

def main() -> None:
    validate_environment()

    strategy_name = "full_reload" if IS_FULL_LOAD else "incremental_upsert"

    print("------------------------------------------------------------")
    print("Building staging.stg_gleif_relationships")
    print(f"Mode: {strategy_name.upper()} (GLEIF_DOWNLOAD_RR_FULL={IS_FULL_LOAD})")
    print("Source table: raw.gleif_rr")
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
            job_name="build_stg_gleif_relationships",
            job_type="staging",
            source="GLEIF Golden Copy public downloads",
            target_system="postgres",
            target_table="staging.stg_gleif_relationships",
            app_env=APP_ENV,
            metadata_json={
                "requested_load_date": GLEIF_STAGING_LOAD_DATE,
                "source_table": "raw.gleif_rr",
                "target_table": "staging.stg_gleif_relationships",
                "build_strategy": strategy_name,
                "is_full_load": IS_FULL_LOAD,
            },
        )

        build_table_name = f"stg_gleif_relationships_b_{job_run_id}"

        ensure_staging_gleif_relationships_table(postgres)
        effective_load_date = resolve_effective_load_date(postgres)

        print(f"Effective GLEIF relationships staging load date: {effective_load_date}")

        rows_read = count_raw_rows(
            postgres=postgres,
            effective_load_date=effective_load_date,
        )

        if rows_read == 0:
            raise RuntimeError(
                f"No raw.gleif_rr rows found for source_load_date={effective_load_date}."
            )

        if IS_FULL_LOAD:
            # -------------------------------------------------------
            # PFAD A: Full Load via Build-and-Swap
            # -------------------------------------------------------
            print(f"Executing Full Load via build table: staging.{build_table_name}")

            create_staging_gleif_relationships_build_table(
                postgres=postgres,
                build_table_name=build_table_name,
            )

            rows_inserted = build_staging_gleif_relationships_build_table(
                postgres=postgres,
                build_table_name=build_table_name,
                effective_load_date=effective_load_date,
            )

            if rows_inserted == 0:
                raise RuntimeError(
                    f"Build table staging.{build_table_name} contains no rows."
                )

            create_staging_gleif_relationships_build_indexes(
                postgres=postgres,
                build_table_name=build_table_name,
                job_run_id=job_run_id,
            )

            swap_staging_gleif_relationships_table(
                postgres=postgres,
                build_table_name=build_table_name,
            )

            build_table_name = None  # Vermeidet unnötiges Cleanup

        else:
            # -------------------------------------------------------
            # PFAD B: Incremental Load via UPSERT
            # -------------------------------------------------------
            print("Executing Incremental Load via UPSERT...")

            rows_inserted = execute_incremental_upsert(
                postgres=postgres,
                build_table_name=build_table_name,
                effective_load_date=effective_load_date,
            )

            build_table_name = None

        # -----------------------------------------------------------
        # Job Erfolgreich Beenden
        # -----------------------------------------------------------
        finish_job_run_success(
            postgres=postgres,
            job_run_id=job_run_id,
            status="success",
            effective_load_date=effective_load_date,
            rows_read=rows_read,
            rows_inserted=rows_inserted,
            metadata_json={
                "source_table": "raw.gleif_rr",
                "target_table": "staging.stg_gleif_relationships",
                "effective_load_date": str(effective_load_date),
                "rows_read": rows_read,
                "rows_inserted": rows_inserted,
                "build_strategy": strategy_name,
                "is_full_load": IS_FULL_LOAD,
            },
        )

        print("------------------------------------------------------------")
        print(f"staging.stg_gleif_relationships process completed ({strategy_name}).")
        print(f"Effective load date: {effective_load_date}")
        print(f"Rows read: {rows_read}")
        print(f"Rows affected/inserted: {rows_inserted}")

    except Exception as exc:
        cleanup_staging_gleif_relationships_build_table(
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
                    "source_table": "raw.gleif_rr",
                    "target_table": "staging.stg_gleif_relationships",
                    "effective_load_date": str(effective_load_date)
                    if effective_load_date
                    else None,
                    "build_table_name": build_table_name,
                    "build_strategy": strategy_name,
                    "is_full_load": IS_FULL_LOAD,
                },
            )

        raise

    finally:
        postgres.close()


if __name__ == "__main__":
    main()