import os
import tempfile
from datetime import datetime, timezone
from typing import Callable, Iterator

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
from common.stream_utils import (
    iter_csv_rows_from_text_stream,
    iter_csv_rows_from_zip_stream_v1,
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

GLEIF_LOAD_DATE = os.getenv("GLEIF_LOAD_DATE")
GLEIF_MAX_SNAPSHOT_AGE_DAYS = int(
    os.getenv("GLEIF_MAX_SNAPSHOT_AGE_DAYS", "3")
)
GLEIF_STALE_SNAPSHOT_POLICY = os.getenv(
    "GLEIF_STALE_SNAPSHOT_POLICY", "warn"
).lower()

GLEIF_LOAD_BATCH_SIZE = int(os.getenv("GLEIF_LOAD_BATCH_SIZE", "5000"))
GLEIF_MAX_ROWS_PER_FILE = int(os.getenv("GLEIF_MAX_ROWS_PER_FILE", "0"))

# Helper for Boolean Env-Vars
def get_bool_env(var_name: str, default: bool = False) -> bool:
    val = os.getenv(var_name)
    if val is None:
        return default
    return val.strip().lower() in ("true", "1", "yes", "y", "t")

# Controling the load modes (Full vs. Delta)
GLEIF_DOWNLOAD_LEI_FULL = get_bool_env("GLEIF_DOWNLOAD_LEI_FULL", True)
GLEIF_DOWNLOAD_LEI_DELTA_LASTDAY = get_bool_env("GLEIF_DOWNLOAD_LEI_DELTA_LASTDAY", False)
GLEIF_DOWNLOAD_RR_FULL = get_bool_env("GLEIF_DOWNLOAD_RR_FULL", True)
GLEIF_DOWNLOAD_RR_DELTA_LASTDAY = get_bool_env("GLEIF_DOWNLOAD_RR_DELTA_LASTDAY", False)

ENABLED_DATASET_GROUPS = set()
if GLEIF_DOWNLOAD_LEI_FULL or GLEIF_DOWNLOAD_LEI_DELTA_LASTDAY:
    ENABLED_DATASET_GROUPS.add("lei")
if GLEIF_DOWNLOAD_RR_FULL or GLEIF_DOWNLOAD_RR_DELTA_LASTDAY:
    ENABLED_DATASET_GROUPS.add("rr")

# -------------------------------------------------------------------
# Pre-defined Keys (Performance Optimization for Millions of Rows)
# -------------------------------------------------------------------

# LEI Field Lookups
LEI_KEYS = ("LEI", "lei")
LEGAL_NAME_KEYS = (
    "Entity.LegalName",
    "Entity.LegalName.Name",
    "Entity_LegalName",
    "LegalName",
    "legal_name",
)
ENTITY_STATUS_KEYS = (
    "Entity.EntityStatus",
    "Entity_EntityStatus",
    "EntityStatus",
    "entity_status",
)
LEI_REGISTRATION_STATUS_KEYS = (
    "Registration.RegistrationStatus",
    "Registration_RegistrationStatus",
    "RegistrationStatus",
    "registration_status",
)
LEGAL_JURISDICTION_KEYS = (
    "Entity.LegalJurisdiction",
    "Entity_LegalJurisdiction",
    "LegalJurisdiction",
    "legal_jurisdiction",
)
LEGAL_ADDRESS_COUNTRY_KEYS = (
    "Entity.LegalAddress.Country",
    "LegalAddress.Country",
    "legal_address_country",
)
HQ_ADDRESS_COUNTRY_KEYS = (
    "Entity.HeadquartersAddress.Country",
    "HeadquartersAddress.Country",
    "headquarters_address_country",
)
NEXT_RENEWAL_DATE_KEYS = (
    "Registration.NextRenewalDate",
    "NextRenewalDate",
    "next_renewal_date",
)
LAST_UPDATE_DATE_KEYS = (
    "Registration.LastUpdateDate",
    "LastUpdateDate",
    "last_update_date",
)

# RR Field Lookups
START_NODE_ID_KEYS = (
    "Relationship.StartNode.NodeID",
    "StartNode.NodeID",
    "start_node_id",
)
START_NODE_TYPE_KEYS = (
    "Relationship.StartNode.NodeIDType",
    "StartNode.NodeIDType",
    "start_node_id_type",
)
END_NODE_ID_KEYS = (
    "Relationship.EndNode.NodeID",
    "EndNode.NodeID",
    "end_node_id",
)
END_NODE_TYPE_KEYS = (
    "Relationship.EndNode.NodeIDType",
    "EndNode.NodeIDType",
    "end_node_id_type",
)
RELATIONSHIP_TYPE_KEYS = (
    "Relationship.RelationshipType",
    "RelationshipType",
    "relationship_type",
)
RELATIONSHIP_STATUS_KEYS = (
    "Relationship.RelationshipStatus",
    "RelationshipStatus",
    "relationship_status",
)
PERIOD_START_KEYS = (
    "Relationship.Period.1.startDate",
    "Relationship.Period.1.StartDate",
    "relationship_period_start",
)
PERIOD_END_KEYS = (
    "Relationship.Period.1.endDate",
    "Relationship.Period.1.EndDate",
    "relationship_period_end",
)
PERIOD_TYPE_KEYS = (
    "Relationship.Period.1.periodType",
    "Relationship.Period.1.PeriodType",
    "relationship_period_type",
)
RR_REGISTRATION_STATUS_KEYS = (
    "Registration.RegistrationStatus",
    "RegistrationStatus",
    "registration_status",
)
INITIAL_REGISTRATION_DATE_KEYS = (
    "Registration.InitialRegistrationDate",
    "InitialRegistrationDate",
    "initial_registration_date",
)
MANAGING_LOU_KEYS = (
    "Registration.ManagingLOU",
    "ManagingLOU",
    "managing_lou",
)
VALIDATION_SOURCES_KEYS = (
    "Registration.ValidationSources",
    "ValidationSources",
    "validation_sources",
)
VALIDATION_DOCS_KEYS = (
    "Registration.ValidationDocuments",
    "ValidationDocuments",
    "validation_documents",
)
VALIDATION_REF_KEYS = (
    "Registration.ValidationReference",
    "ValidationReference",
    "validation_reference",
)


# -------------------------------------------------------------------
# Fast Row Helper
# -------------------------------------------------------------------

def get_first(row: dict, keys: tuple[str, ...]) -> str | None:
    for key in keys:
        val = row.get(key)
        if val is not None:
            return val
    return None


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

    if GLEIF_STALE_SNAPSHOT_POLICY not in {"warn", "fail", "allow"}:
        raise RuntimeError(
            "GLEIF_STALE_SNAPSHOT_POLICY must be one of: warn, fail, allow"
        )

    if not ENABLED_DATASET_GROUPS:
        raise RuntimeError(
            "No GLEIF download flags are set to True! At least one GLEIF_DOWNLOAD_* variable must be true."
        )

def is_file_enabled(file_entry: dict) -> bool:
    group = file_entry.get("dataset_group")
    snapshot_type = file_entry.get("snapshot_type")  # e.g. "full" or "delta" / "lastday"

    if group == "lei":
        if snapshot_type == "full" and GLEIF_DOWNLOAD_LEI_FULL:
            return True
        if snapshot_type in ("delta", "lastday") and GLEIF_DOWNLOAD_LEI_DELTA_LASTDAY:
            return True

    elif group == "rr":
        if snapshot_type == "full" and GLEIF_DOWNLOAD_RR_FULL:
            return True
        if snapshot_type in ("delta", "lastday") and GLEIF_DOWNLOAD_RR_DELTA_LASTDAY:
            return True
        
    return False

# -------------------------------------------------------------------
# PostgreSQL setup
# -------------------------------------------------------------------

def ensure_raw_gleif_test_tables(postgres: PostgresClient) -> None:
    postgres.ensure_schemas()
    postgres.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.gleif_lei_full (
            raw_id BIGSERIAL PRIMARY KEY,
            app_env TEXT,
            source TEXT,
            source_name TEXT,
            dataset_group TEXT,
            snapshot_type TEXT,
            row_number INTEGER,
            row_hash TEXT,
            lei TEXT,
            legal_name TEXT,
            entity_status TEXT,
            registration_status TEXT,
            legal_jurisdiction TEXT,
            legal_address_country TEXT,
            headquarters_address_country TEXT,
            next_renewal_date_raw TEXT,
            last_update_date_raw TEXT,
            raw_row JSONB,
            source_url TEXT,
            source_object_key TEXT,
            metadata_object_key TEXT,
            source_load_date DATE,
            loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS raw.gleif_rr_full (
            raw_id BIGSERIAL PRIMARY KEY,
            app_env TEXT,
            source TEXT,
            source_name TEXT,
            dataset_group TEXT,
            snapshot_type TEXT,
            row_number INTEGER,
            row_hash TEXT,
            start_node_id TEXT,
            start_node_id_type TEXT,
            end_node_id TEXT,
            end_node_id_type TEXT,
            relationship_type TEXT,
            relationship_status TEXT,
            relationship_period_start_raw TEXT,
            relationship_period_end_raw TEXT,
            relationship_period_type TEXT,
            registration_status TEXT,
            initial_registration_date_raw TEXT,
            last_update_date_raw TEXT,
            next_renewal_date_raw TEXT,
            managing_lou TEXT,
            validation_sources TEXT,
            validation_documents TEXT,
            validation_reference TEXT,
            raw_row JSONB,
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

def resolve_gleif_manifest(
    minio: MinioClient,
) -> tuple[str, str, dict, dict]:
    if GLEIF_LOAD_DATE:
        manifest_key = (
            f"gleif/_manifests/load_date={GLEIF_LOAD_DATE}/download_gleif_manifest.json"
        )
        if not minio.object_exists(manifest_key):
            raise RuntimeError(
                f"GLEIF manifest not found in MinIO: s3://{MINIO_BUCKET}/{manifest_key}."
            )
        manifest = minio.get_json_object(manifest_key)
        effective_load_date = GLEIF_LOAD_DATE
    else:
        print("GLEIF_LOAD_DATE not set. Searching latest successful GLEIF manifest in MinIO.")
        manifest_key, effective_load_date, manifest = find_latest_successful_manifest(
            minio=minio,
            manifest_prefix="gleif/_manifests/",
            manifest_filename="download_gleif_manifest.json",
        )

    freshness = evaluate_snapshot_freshness(
        effective_load_date=effective_load_date,
        max_age_days=GLEIF_MAX_SNAPSHOT_AGE_DAYS,
        policy=GLEIF_STALE_SNAPSHOT_POLICY,
    )

    handle_stale_snapshot(freshness=freshness, source_name="GLEIF")

    return manifest_key, effective_load_date, manifest, freshness


# -------------------------------------------------------------------
# Database Operations
# -------------------------------------------------------------------

LEI_COLUMNS = [
    "app_env",
    "source",
    "source_name",
    "dataset_group",
    "snapshot_type",
    "row_number",
    "row_hash",
    "lei",
    "legal_name",
    "entity_status",
    "registration_status",
    "legal_jurisdiction",
    "legal_address_country",
    "headquarters_address_country",
    "next_renewal_date_raw",
    "last_update_date_raw",
    "raw_row",
    "source_url",
    "source_object_key",
    "metadata_object_key",
    "source_load_date",
]

RR_COLUMNS = [
    "app_env",
    "source",
    "source_name",
    "dataset_group",
    "snapshot_type",
    "row_number",
    "row_hash",
    "start_node_id",
    "start_node_id_type",
    "end_node_id",
    "end_node_id_type",
    "relationship_type",
    "relationship_status",
    "relationship_period_start_raw",
    "relationship_period_end_raw",
    "relationship_period_type",
    "registration_status",
    "initial_registration_date_raw",
    "last_update_date_raw",
    "next_renewal_date_raw",
    "managing_lou",
    "validation_sources",
    "validation_documents",
    "validation_reference",
    "raw_row",
    "source_url",
    "source_object_key",
    "metadata_object_key",
    "source_load_date",
]


def map_lei_row_minimal(
    row: dict,
    row_number: int,
    file_entry: dict,
    data_object_key: str,
    effective_load_date: str,
) -> tuple:
    return (
        APP_ENV,
        "GLEIF Golden Copy public downloads",
        file_entry.get("source_name"),
        file_entry.get("dataset_group"),
        file_entry.get("snapshot_type"),
        row_number,
        None,
        get_first(row, LEI_KEYS),
        get_first(row, LEGAL_NAME_KEYS),
        get_first(row, ENTITY_STATUS_KEYS),
        get_first(row, LEI_REGISTRATION_STATUS_KEYS),
        get_first(row, LEGAL_JURISDICTION_KEYS),
        get_first(row, LEGAL_ADDRESS_COUNTRY_KEYS),
        get_first(row, HQ_ADDRESS_COUNTRY_KEYS),
        get_first(row, NEXT_RENEWAL_DATE_KEYS),
        get_first(row, LAST_UPDATE_DATE_KEYS),
        None,
        file_entry.get("source_url"),
        data_object_key,
        file_entry.get("metadata_object_key"),
        effective_load_date,
    )


def map_rr_row_minimal(
    row: dict,
    row_number: int,
    file_entry: dict,
    data_object_key: str,
    effective_load_date: str,
) -> tuple:
    return (
        APP_ENV,
        "GLEIF Golden Copy public downloads",
        file_entry.get("source_name"),
        file_entry.get("dataset_group"),
        file_entry.get("snapshot_type"),
        row_number,
        None,
        get_first(row, START_NODE_ID_KEYS),
        get_first(row, START_NODE_TYPE_KEYS),
        get_first(row, END_NODE_ID_KEYS),
        get_first(row, END_NODE_TYPE_KEYS),
        get_first(row, RELATIONSHIP_TYPE_KEYS),
        get_first(row, RELATIONSHIP_STATUS_KEYS),
        get_first(row, PERIOD_START_KEYS),
        get_first(row, PERIOD_END_KEYS),
        get_first(row, PERIOD_TYPE_KEYS),
        get_first(row, RR_REGISTRATION_STATUS_KEYS),
        get_first(row, INITIAL_REGISTRATION_DATE_KEYS),
        get_first(row, LAST_UPDATE_DATE_KEYS),
        get_first(row, NEXT_RENEWAL_DATE_KEYS),
        get_first(row, MANAGING_LOU_KEYS),
        get_first(row, VALIDATION_SOURCES_KEYS),
        get_first(row, VALIDATION_DOCS_KEYS),
        get_first(row, VALIDATION_REF_KEYS),
        None,
        file_entry.get("source_url"),
        data_object_key,
        file_entry.get("metadata_object_key"),
        effective_load_date,
    )


# -------------------------------------------------------------------
# Generic Atomic File Processor
# -------------------------------------------------------------------

def process_dataset_file(
    postgres: PostgresClient,
    row_iterator: Iterator[dict],
    file_entry: dict,
    data_object_key: str,
    effective_load_date: str,
    table_name: str,
    columns: list[str],
    map_fn: Callable,
) -> tuple[int, int]:
    rows_read = 0
    total_inserted = 0
    prepared_rows = []

    # Transaction Safety: Delete + Copy are isolated in one transaction per file
    with postgres.transaction():
        postgres.execute(
            f"DELETE FROM {table_name} WHERE source_object_key = %s",
            (data_object_key,),
            commit=False,
        )

        for row_number, row in enumerate(row_iterator, start=1):
            if 0 < GLEIF_MAX_ROWS_PER_FILE < row_number:
                print(f"Reached GLEIF_MAX_ROWS_PER_FILE={GLEIF_MAX_ROWS_PER_FILE}.")
                break

            rows_read += 1
            prepared_rows.append(
                map_fn(
                    row=row,
                    row_number=row_number,
                    file_entry=file_entry,
                    data_object_key=data_object_key,
                    effective_load_date=effective_load_date,
                )
            )

            if len(prepared_rows) >= GLEIF_LOAD_BATCH_SIZE:
                inserted_count = postgres.copy_rows(
                    table_name=table_name,
                    columns=columns,
                    rows=prepared_rows,
                    commit=False,
                )
                total_inserted += inserted_count
                prepared_rows.clear()

        if prepared_rows:
            inserted_count = postgres.copy_rows(
                table_name=table_name,
                columns=columns,
                    rows=prepared_rows,
                    commit=False,
            )
            total_inserted += inserted_count
            prepared_rows.clear()

    return rows_read, total_inserted


def process_gleif_file_rows(
    postgres: PostgresClient,
    row_iterator: Iterator[dict],
    file_entry: dict,
    data_object_key: str,
    effective_load_date: str,
) -> tuple[int, int, int, int]:
    dataset_group = file_entry.get("dataset_group")

    if dataset_group == "lei":
        read, inserted = process_dataset_file(
            postgres=postgres,
            row_iterator=row_iterator,
            file_entry=file_entry,
            data_object_key=data_object_key,
            effective_load_date=effective_load_date,
            table_name="raw.gleif_lei_full",
            columns=LEI_COLUMNS,
            map_fn=map_lei_row_minimal,
        )
        print(f"Read LEI rows: {read} | Inserted LEI rows: {inserted}")
        return read, inserted, 0, 0

    if dataset_group == "rr":
        read, inserted = process_dataset_file(
            postgres=postgres,
            row_iterator=row_iterator,
            file_entry=file_entry,
            data_object_key=data_object_key,
            effective_load_date=effective_load_date,
            table_name="raw.gleif_rr_full",
            columns=RR_COLUMNS,
            map_fn=map_rr_row_minimal,
        )
        print(f"Read RR rows: {read} | Inserted RR rows: {inserted}")
        return 0, 0, read, inserted

    print(f"WARNING: unsupported GLEIF dataset_group: {dataset_group}")
    return 0, 0, 0, 0


# -------------------------------------------------------------------
# Main job
# -------------------------------------------------------------------

def main() -> None:
    validate_environment()

    print("------------------------------------------------------------")
    print("Loading GLEIF raw data OPTIMIZED")
    print("Target tables: raw.gleif_lei_full, raw.gleif_rr_full")
    print("Insert mode: PostgreSQL COPY (Transaction Isolated)")
    print(f"Batch size: {GLEIF_LOAD_BATCH_SIZE}")

    minio = MinioClient.from_env()
    postgres = PostgresClient.from_env()

    job_run_id = None
    manifest_key = None
    effective_load_date = None
    manifest = None
    freshness = None

    total_lei_rows_read = 0
    total_rr_rows_read = 0
    total_lei_inserted = 0
    total_rr_inserted = 0
    processed_file_count = 0

    try:
        job_run_id = start_job_run(
            postgres=postgres,
            job_name="load_gleif_raw_full",
            job_type="raw_load",
            source="GLEIF Golden Copy public downloads",
            target_system="postgres",
            target_table="raw.gleif_lei_full, raw.gleif_rr_full",
            app_env=APP_ENV,
            metadata_json={
                "requested_load_date": GLEIF_LOAD_DATE,
                "max_snapshot_age_days": GLEIF_MAX_SNAPSHOT_AGE_DAYS,
                "stale_snapshot_policy": GLEIF_STALE_SNAPSHOT_POLICY,
                "batch_size": GLEIF_LOAD_BATCH_SIZE,
                "max_rows_per_file": GLEIF_MAX_ROWS_PER_FILE,
                "dataset_groups_to_load": sorted(ENABLED_DATASET_GROUPS),
                "python_transformations": "minimal_optimized",
            },
        )

        ensure_raw_gleif_test_tables(postgres)

        manifest_key, effective_load_date, manifest, freshness = resolve_gleif_manifest(
            minio=minio,
        )

        success_files = [
            f for f in manifest.get("files", [])
            if f.get("status") == "success" and is_file_enabled(f)
        ]

        if not success_files:
            raise RuntimeError(
                f"No successful GLEIF files found for dataset groups "
                f"{sorted(ENABLED_DATASET_GROUPS)} and load_date={effective_load_date}."
            )

        for file_entry in success_files:
            data_object_key = file_entry.get("data_object_key")
            if not data_object_key:
                continue

            processed_file_count += 1

            if data_object_key.lower().endswith(".zip"):
                with tempfile.TemporaryDirectory() as tmp_dir:
                    local_zip_path = os.path.join(
                        tmp_dir, os.path.basename(data_object_key)
                    )
                    minio.download_object_to_file(
                        object_key=data_object_key,
                        local_file_path=local_zip_path,
                    )
                    with open(local_zip_path, "rb") as local_zip_file:
                        row_iterator = iter_csv_rows_from_zip_stream_v1(local_zip_file)
                        lei_r, lei_i, rr_r, rr_i = process_gleif_file_rows(
                            postgres=postgres,
                            row_iterator=row_iterator,
                            file_entry=file_entry,
                            data_object_key=data_object_key,
                            effective_load_date=effective_load_date,
                        )
            else:
                with minio.get_object_stream(data_object_key) as stream:
                    row_iterator = iter_csv_rows_from_text_stream(stream)
                    lei_r, lei_i, rr_r, rr_i = process_gleif_file_rows(
                        postgres=postgres,
                        row_iterator=row_iterator,
                        file_entry=file_entry,
                        data_object_key=data_object_key,
                        effective_load_date=effective_load_date,
                    )

            total_lei_rows_read += lei_r
            total_lei_inserted += lei_i
            total_rr_rows_read += rr_r
            total_rr_inserted += rr_i

        total_rows_read = total_lei_rows_read + total_rr_rows_read
        total_inserted = total_lei_inserted + total_rr_inserted

        audit_status = (
            "success_with_warnings"
            if manifest.get("status") == "success_with_warnings" or int(manifest.get("warning_count") or 0) > 0
            else "success"
        )

        finish_job_run_success(
            postgres=postgres,
            job_run_id=job_run_id,
            status=audit_status,
            manifest_key=manifest_key,
            effective_load_date=effective_load_date,
            freshness=freshness,
            files_discovered=len(manifest.get("files", [])),
            files_processed=processed_file_count,
            files_success=processed_file_count,
            files_failed=manifest.get("failed_count"),
            rows_read=total_rows_read,
            rows_inserted=total_inserted,
            warning_count=manifest.get("warning_count"),
            error_count=manifest.get("error_count"),
            metadata_json={
                "lei_rows_read": total_lei_rows_read,
                "lei_inserted_rows": total_lei_inserted,
                "rr_rows_read": total_rr_rows_read,
                "rr_inserted_rows": total_rr_inserted,
            },
        )

        print("------------------------------------------------------------")
        print("GLEIF load finished successfully.")
        print(f"Total rows read: {total_rows_read} | Total rows inserted: {total_inserted}")

    except Exception as exc:
        if job_run_id:
            total_rows_read = total_lei_rows_read + total_rr_rows_read
            total_inserted = total_lei_inserted + total_rr_inserted

            finish_job_run_failure(
                postgres=postgres,
                job_run_id=job_run_id,
                error_message=str(exc),
                manifest_key=manifest_key,
                effective_load_date=effective_load_date,
                freshness=freshness,
                files_discovered=len(manifest.get("files", [])) if manifest else None,
                files_processed=processed_file_count,
                files_success=processed_file_count,
                files_failed=manifest.get("failed_count") if manifest else None,
                rows_read=total_rows_read,
                rows_inserted=total_inserted,
                warning_count=manifest.get("warning_count") if manifest else None,
                error_count=manifest.get("error_count") if manifest else None,
                metadata_json={
                    "manifest_status": manifest.get("status") if manifest else None,
                    "lei_rows_read": total_lei_rows_read,
                    "lei_inserted_rows": total_lei_inserted,
                    "rr_rows_read": total_rr_rows_read,
                    "rr_inserted_rows": total_rr_inserted,
                },
            )
        raise exc


if __name__ == "__main__":
    main()