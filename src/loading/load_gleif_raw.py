import os
import json
import tempfile
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
from common.row_utils import (
    clean_csv_row,
    get_by_possible_keys,
    calculate_row_hash,
    row_to_json_string,
)
from common.stream_utils import (
    iter_csv_rows_from_zip_stream,
    iter_csv_rows_from_text_stream,
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
# If not set: load latest successful GLEIF snapshot from MinIO.
GLEIF_LOAD_DATE = os.getenv("GLEIF_LOAD_DATE")

GLEIF_MAX_SNAPSHOT_AGE_DAYS = int(os.getenv("GLEIF_MAX_SNAPSHOT_AGE_DAYS", "3"))

# Supported values: warn, fail, allow
GLEIF_STALE_SNAPSHOT_POLICY = os.getenv(
    "GLEIF_STALE_SNAPSHOT_POLICY",
    "warn",
).lower()

# Erhöht auf 50.000 für optimale COPY-Performance
GLEIF_LOAD_BATCH_SIZE = int(os.getenv("GLEIF_LOAD_BATCH_SIZE", "50000"))
GLEIF_MAX_ROWS_PER_FILE = int(
    os.getenv("GLEIF_MAX_ROWS_PER_FILE", "0")
)

GLEIF_REBUILD_INDEXES = (
    os.getenv("GLEIF_REBUILD_INDEXES", "true").lower() == "true"
)

GLEIF_STORE_RAW_ROW_JSON = (
    os.getenv("GLEIF_STORE_RAW_ROW_JSON", "false").lower() == "true"
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

    if GLEIF_STALE_SNAPSHOT_POLICY not in {"warn", "fail", "allow"}:
        raise RuntimeError(
            "GLEIF_STALE_SNAPSHOT_POLICY must be one of: warn, fail, allow"
        )


# -------------------------------------------------------------------
# PostgreSQL setup
# -------------------------------------------------------------------

def ensure_raw_gleif_tables(postgres: PostgresClient) -> None:
    postgres.ensure_schemas()

    postgres.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.gleif_lei (
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

        CREATE TABLE IF NOT EXISTS raw.gleif_rr (
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
                f"GLEIF manifest not found in MinIO: "
                f"s3://{MINIO_BUCKET}/{manifest_key}. "
                "Run download_gleif.py first or set GLEIF_LOAD_DATE correctly."
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

    handle_stale_snapshot(
        freshness=freshness,
        source_name="GLEIF",
    )

    return manifest_key, effective_load_date, manifest, freshness

# -------------------------------------------------------------------
# Database helpers
# -------------------------------------------------------------------

def delete_existing_lei_rows_for_object(
    postgres: PostgresClient,
    source_object_key: str,
) -> None:
    postgres.execute(
        """
        DELETE FROM raw.gleif_lei
        WHERE source_object_key = %s
        """,
        (source_object_key,),
    )


def delete_existing_rr_rows_for_object(
    postgres: PostgresClient,
    source_object_key: str,
) -> None:
    postgres.execute(
        """
        DELETE FROM raw.gleif_rr
        WHERE source_object_key = %s
        """,
        (source_object_key,),
    )


def insert_gleif_lei_rows(
    postgres: PostgresClient,
    rows: list[tuple],
) -> int:
    return postgres.copy_rows(
        table_name="raw.gleif_lei",
        columns=[
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
        ],
        rows=rows,
    )


def insert_gleif_rr_rows(
    postgres: PostgresClient,
    rows: list[tuple],
) -> int:
    return postgres.copy_rows(
        table_name="raw.gleif_rr",
        columns=[
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
        ],
        rows=rows,
    )


# -------------------------------------------------------------------
# Row mapping: LEI
# -------------------------------------------------------------------

def map_lei_row(
    row: dict,
    row_number: int,
    file_entry: dict,
    data_object_key: str,
    effective_load_date: str,
) -> tuple:
    clean_row = clean_csv_row(row)

    lei = get_by_possible_keys(clean_row, ["LEI", "lei"])
    legal_name = get_by_possible_keys(
        clean_row,
        [
            "Entity.LegalName",
            "Entity.LegalName.Name",
            "Entity_LegalName",
            "LegalName",
            "legal_name",
        ],
    )
    entity_status = get_by_possible_keys(
        clean_row,
        [
            "Entity.EntityStatus",
            "Entity_EntityStatus",
            "EntityStatus",
            "entity_status",
        ],
    )
    registration_status = get_by_possible_keys(
        clean_row,
        [
            "Registration.RegistrationStatus",
            "Registration_RegistrationStatus",
            "RegistrationStatus",
            "registration_status",
        ],
    )
    legal_jurisdiction = get_by_possible_keys(
        clean_row,
        [
            "Entity.LegalJurisdiction",
            "Entity_LegalJurisdiction",
            "LegalJurisdiction",
            "legal_jurisdiction",
        ],
    )
    legal_address_country = get_by_possible_keys(
        clean_row,
        [
            "Entity.LegalAddress.Country",
            "LegalAddress.Country",
            "legal_address_country",
        ],
    )
    headquarters_address_country = get_by_possible_keys(
        clean_row,
        [
            "Entity.HeadquartersAddress.Country",
            "HeadquartersAddress.Country",
            "headquarters_address_country",
        ],
    )
    next_renewal_date_raw = get_by_possible_keys(
        clean_row,
        [
            "Registration.NextRenewalDate",
            "NextRenewalDate",
            "next_renewal_date",
        ],
    )
    last_update_date_raw = get_by_possible_keys(
        clean_row,
        [
            "Registration.LastUpdateDate",
            "LastUpdateDate",
            "last_update_date",
        ],
    )

    return (
        APP_ENV,
        "GLEIF Golden Copy public downloads",
        file_entry.get("source_name"),
        file_entry.get("dataset_group"),
        file_entry.get("snapshot_type"),
        row_number,
        calculate_row_hash(clean_row),
        lei,
        legal_name,
        entity_status,
        registration_status,
        legal_jurisdiction,
        legal_address_country,
        headquarters_address_country,
        next_renewal_date_raw,
        last_update_date_raw,
        row_to_json_string(clean_row) if GLEIF_STORE_RAW_ROW_JSON else None,
        file_entry.get("source_url"),
        data_object_key,
        file_entry.get("metadata_object_key"),
        effective_load_date,
    )


# -------------------------------------------------------------------
# Row mapping: RR
# -------------------------------------------------------------------

def map_rr_row(
    row: dict,
    row_number: int,
    file_entry: dict,
    data_object_key: str,
    effective_load_date: str,
) -> tuple:
    clean_row = clean_csv_row(row)

    start_node_id = get_by_possible_keys(
        clean_row,
        [
            "Relationship.StartNode.NodeID",
            "StartNode.NodeID",
            "start_node_id",
        ],
    )
    start_node_id_type = get_by_possible_keys(
        clean_row,
        [
            "Relationship.StartNode.NodeIDType",
            "StartNode.NodeIDType",
            "start_node_id_type",
        ],
    )
    end_node_id = get_by_possible_keys(
        clean_row,
        [
            "Relationship.EndNode.NodeID",
            "EndNode.NodeID",
            "end_node_id",
        ],
    )
    end_node_id_type = get_by_possible_keys(
        clean_row,
        [
            "Relationship.EndNode.NodeIDType",
            "EndNode.NodeIDType",
            "end_node_id_type",
        ],
    )
    relationship_type = get_by_possible_keys(
        clean_row,
        [
            "Relationship.RelationshipType",
            "RelationshipType",
            "relationship_type",
        ],
    )
    relationship_status = get_by_possible_keys(
        clean_row,
        [
            "Relationship.RelationshipStatus",
            "RelationshipStatus",
            "relationship_status",
        ],
    )
    relationship_period_start_raw = get_by_possible_keys(
        clean_row,
        [
            "Relationship.Period.1.startDate",
            "Relationship.Period.1.StartDate",
            "relationship_period_start",
        ],
    )
    relationship_period_end_raw = get_by_possible_keys(
        clean_row,
        [
            "Relationship.Period.1.endDate",
            "Relationship.Period.1.EndDate",
            "relationship_period_end",
        ],
    )
    relationship_period_type = get_by_possible_keys(
        clean_row,
        [
            "Relationship.Period.1.periodType",
            "Relationship.Period.1.PeriodType",
            "relationship_period_type",
        ],
    )
    registration_status = get_by_possible_keys(
        clean_row,
        [
            "Registration.RegistrationStatus",
            "RegistrationStatus",
            "registration_status",
        ],
    )
    initial_registration_date_raw = get_by_possible_keys(
        clean_row,
        [
            "Registration.InitialRegistrationDate",
            "InitialRegistrationDate",
            "initial_registration_date",
        ],
    )
    last_update_date_raw = get_by_possible_keys(
        clean_row,
        [
            "Registration.LastUpdateDate",
            "LastUpdateDate",
            "last_update_date",
        ],
    )
    next_renewal_date_raw = get_by_possible_keys(
        clean_row,
        [
            "Registration.NextRenewalDate",
            "NextRenewalDate",
            "next_renewal_date",
        ],
    )
    managing_lou = get_by_possible_keys(
        clean_row,
        [
            "Registration.ManagingLOU",
            "ManagingLOU",
            "managing_lou",
        ],
    )
    validation_sources = get_by_possible_keys(
        clean_row,
        [
            "Registration.ValidationSources",
            "ValidationSources",
            "validation_sources",
        ],
    )
    validation_documents = get_by_possible_keys(
        clean_row,
        [
            "Registration.ValidationDocuments",
            "ValidationDocuments",
            "validation_documents",
        ],
    )
    validation_reference = get_by_possible_keys(
        clean_row,
        [
            "Registration.ValidationReference",
            "ValidationReference",
            "validation_reference",
        ],
    )

    return (
        APP_ENV,
        "GLEIF Golden Copy public downloads",
        file_entry.get("source_name"),
        file_entry.get("dataset_group"),
        file_entry.get("snapshot_type"),
        row_number,
        calculate_row_hash(clean_row),
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
        row_to_json_string(clean_row) if GLEIF_STORE_RAW_ROW_JSON else None,
        file_entry.get("source_url"),
        data_object_key,
        file_entry.get("metadata_object_key"),
        effective_load_date,
    )


# -------------------------------------------------------------------
# Processing functions
# -------------------------------------------------------------------

def process_lei_file(
    postgres: PostgresClient,
    row_iterator: Iterator[dict],
    file_entry: dict,
    data_object_key: str,
    effective_load_date: str,
) -> int:
    delete_existing_lei_rows_for_object(
        postgres=postgres,
        source_object_key=data_object_key,
    )

    total_inserted = 0
    prepared_rows = []

    for row_number, row in enumerate(row_iterator, start=1):
        if GLEIF_MAX_ROWS_PER_FILE > 0 and row_number > GLEIF_MAX_ROWS_PER_FILE:
            print(f"Reached GLEIF_MAX_ROWS_PER_FILE={GLEIF_MAX_ROWS_PER_FILE}. Stopping LEI file processing.")
            break

        prepared_rows.append(
            map_lei_row(
                row=row,
                row_number=row_number,
                file_entry=file_entry,
                data_object_key=data_object_key,
                effective_load_date=effective_load_date,
            )
        )

        if len(prepared_rows) >= GLEIF_LOAD_BATCH_SIZE:
            inserted_count = insert_gleif_lei_rows(
                postgres=postgres,
                rows=prepared_rows,
            )
            total_inserted += inserted_count
            prepared_rows = []

            print(f"Inserted LEI rows: {total_inserted}")

    if prepared_rows:
        inserted_count = insert_gleif_lei_rows(
            postgres=postgres,
            rows=prepared_rows,
        )
        total_inserted += inserted_count

    return total_inserted


def process_rr_file(
    postgres: PostgresClient,
    row_iterator: Iterator[dict],
    file_entry: dict,
    data_object_key: str,
    effective_load_date: str,
) -> int:
    delete_existing_rr_rows_for_object(
        postgres=postgres,
        source_object_key=data_object_key,
    )

    total_inserted = 0
    prepared_rows = []

    for row_number, row in enumerate(row_iterator, start=1):
        if GLEIF_MAX_ROWS_PER_FILE > 0 and row_number > GLEIF_MAX_ROWS_PER_FILE:
            print(f"Reached GLEIF_MAX_ROWS_PER_FILE={GLEIF_MAX_ROWS_PER_FILE}. Stopping RR file processing.")
            break

        prepared_rows.append(
            map_rr_row(
                row=row,
                row_number=row_number,
                file_entry=file_entry,
                data_object_key=data_object_key,
                effective_load_date=effective_load_date,
            )
        )

        if len(prepared_rows) >= GLEIF_LOAD_BATCH_SIZE:
            inserted_count = insert_gleif_rr_rows(
                postgres=postgres,
                rows=prepared_rows,
            )
            total_inserted += inserted_count
            prepared_rows = []

            print(f"Inserted RR rows: {total_inserted}")

    if prepared_rows:
        inserted_count = insert_gleif_rr_rows(
            postgres=postgres,
            rows=prepared_rows,
        )
        total_inserted += inserted_count

    return total_inserted


def drop_raw_gleif_indexes(postgres: PostgresClient) -> None:
    postgres.execute(
        """
        DROP INDEX IF EXISTS raw.idx_gleif_lei_lei;
        DROP INDEX IF EXISTS raw.idx_gleif_lei_legal_name;
        DROP INDEX IF EXISTS raw.idx_gleif_lei_status;
        DROP INDEX IF EXISTS raw.idx_gleif_lei_load_date;
        DROP INDEX IF EXISTS raw.idx_gleif_lei_source_object;

        DROP INDEX IF EXISTS raw.idx_gleif_rr_start_node;
        DROP INDEX IF EXISTS raw.idx_gleif_rr_end_node;
        DROP INDEX IF EXISTS raw.idx_gleif_rr_relationship_type;
        DROP INDEX IF EXISTS raw.idx_gleif_rr_status;
        DROP INDEX IF EXISTS raw.idx_gleif_rr_load_date;
        DROP INDEX IF EXISTS raw.idx_gleif_rr_source_object;
        """
    )


def create_raw_gleif_indexes(postgres: PostgresClient) -> None:
    postgres.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gleif_lei_lei
            ON raw.gleif_lei (lei);

        CREATE INDEX IF NOT EXISTS idx_gleif_lei_legal_name
            ON raw.gleif_lei (legal_name);

        CREATE INDEX IF NOT EXISTS idx_gleif_lei_status
            ON raw.gleif_lei (entity_status, registration_status);

        CREATE INDEX IF NOT EXISTS idx_gleif_lei_load_date
            ON raw.gleif_lei (source_load_date);

        CREATE INDEX IF NOT EXISTS idx_gleif_lei_source_object
            ON raw.gleif_lei (source_object_key);

        CREATE INDEX IF NOT EXISTS idx_gleif_rr_start_node
            ON raw.gleif_rr (start_node_id);

        CREATE INDEX IF NOT EXISTS idx_gleif_rr_end_node
            ON raw.gleif_rr (end_node_id);

        CREATE INDEX IF NOT EXISTS idx_gleif_rr_relationship_type
            ON raw.gleif_rr (relationship_type);

        CREATE INDEX IF NOT EXISTS idx_gleif_rr_status
            ON raw.gleif_rr (relationship_status, registration_status);

        CREATE INDEX IF NOT EXISTS idx_gleif_rr_load_date
            ON raw.gleif_rr (source_load_date);

        CREATE INDEX IF NOT EXISTS idx_gleif_rr_source_object
            ON raw.gleif_rr (source_object_key);
        """
    )

def process_gleif_file_rows(
    postgres: PostgresClient,
    row_iterator: Iterator[dict],
    file_entry: dict,
    data_object_key: str,
    effective_load_date: str,
) -> tuple[int, int]:
    dataset_group = file_entry.get("dataset_group")

    if dataset_group == "lei":
        inserted_count = process_lei_file(
            postgres=postgres,
            row_iterator=row_iterator,
            file_entry=file_entry,
            data_object_key=data_object_key,
            effective_load_date=effective_load_date,
        )

        print(f"Inserted LEI rows for file: {inserted_count}")
        return inserted_count, 0

    if dataset_group == "rr":
        inserted_count = process_rr_file(
            postgres=postgres,
            row_iterator=row_iterator,
            file_entry=file_entry,
            data_object_key=data_object_key,
            effective_load_date=effective_load_date,
        )

        print(f"Inserted RR rows for file: {inserted_count}")
        return 0, inserted_count

    print(f"WARNING: unsupported GLEIF dataset_group: {dataset_group}")
    return 0, 0
# -------------------------------------------------------------------
# Main job
# -------------------------------------------------------------------

def main() -> None:
    validate_environment()

    print("------------------------------------------------------------")
    print("Loading GLEIF raw data")
    print("Target tables: raw.gleif_lei, raw.gleif_rr")
    print("Insert mode: PostgreSQL COPY")
    print(f"Requested GLEIF_LOAD_DATE: {GLEIF_LOAD_DATE or 'not set'}")
    print(f"Stale snapshot policy: {GLEIF_STALE_SNAPSHOT_POLICY}")
    print(f"Max snapshot age days: {GLEIF_MAX_SNAPSHOT_AGE_DAYS}")
    print(f"Batch size: {GLEIF_LOAD_BATCH_SIZE}")
    print(f"Max rows per file: {GLEIF_MAX_ROWS_PER_FILE or 'unlimited'}")
    print(f"Rebuild indexes: {GLEIF_REBUILD_INDEXES}")
    print(f"Store raw_row JSON: {GLEIF_STORE_RAW_ROW_JSON}")

    minio = MinioClient.from_env()
    postgres = PostgresClient.from_env()

    try:
        ensure_raw_gleif_tables(postgres)

        if GLEIF_REBUILD_INDEXES:
            print("Dropping GLEIF indexes before load.")
            drop_raw_gleif_indexes(postgres)

        manifest_key, effective_load_date, manifest, freshness = resolve_gleif_manifest(
            minio=minio,
        )

        print("------------------------------------------------------------")
        print(f"Using GLEIF manifest: s3://{MINIO_BUCKET}/{manifest_key}")
        print(f"Effective GLEIF load date: {effective_load_date}")
        print(f"Freshness status: {freshness['freshness_status']}")
        print(f"Snapshot age days: {freshness['snapshot_age_days']}")

        success_files = [
            file
            for file in manifest.get("files", [])
            if file.get("status") == "success"
        ]

        if not success_files:
            raise RuntimeError(
                f"No successful GLEIF files found in manifest for load_date={effective_load_date}."
            )

        total_lei_inserted = 0
        total_rr_inserted = 0

        for file_entry in success_files:
            dataset_group = file_entry.get("dataset_group")
            source_name = file_entry.get("source_name")
            data_object_key = file_entry.get("data_object_key")

            if not data_object_key:
                print("Skipping manifest entry without data_object_key.")
                continue

            print("------------------------------------------------------------")
            print(f"Source name: {source_name}")
            print(f"Dataset group: {dataset_group}")
            print(f"Object: s3://{MINIO_BUCKET}/{data_object_key}")
            if dataset_group == "lei":
                print("Writing to table: raw.gleif_lei")
            elif dataset_group == "rr":
                print("Writing to table: raw.gleif_rr")

            if data_object_key.lower().endswith(".zip"):
                with tempfile.TemporaryDirectory() as tmp_dir:
                    local_zip_path = os.path.join(
                        tmp_dir,
                        os.path.basename(data_object_key),
                    )

                    print(f"Downloading ZIP object to temporary file: {local_zip_path}")

                    minio.download_object_to_file(
                        object_key=data_object_key,
                        local_file_path=local_zip_path,
                    )

                    with open(local_zip_path, "rb") as local_zip_file:
                        row_iterator = iter_csv_rows_from_zip_stream(local_zip_file)

                        lei_count, rr_count = process_gleif_file_rows(
                            postgres=postgres,
                            row_iterator=row_iterator,
                            file_entry=file_entry,
                            data_object_key=data_object_key,
                            effective_load_date=effective_load_date,
                        )

                        total_lei_inserted += lei_count
                        total_rr_inserted += rr_count

            else:
                with minio.get_object_stream(data_object_key) as stream:

                    row_iterator = iter_csv_rows_from_text_stream(stream)

                    lei_count, rr_count = process_gleif_file_rows(
                        postgres=postgres,
                        row_iterator=row_iterator,
                        file_entry=file_entry,
                        data_object_key=data_object_key,
                        effective_load_date=effective_load_date,
                    )

                    total_lei_inserted += lei_count
                    total_rr_inserted += rr_count

        if GLEIF_REBUILD_INDEXES:
            print("Creating GLEIF indexes after load.")
            create_raw_gleif_indexes(postgres)

        print("------------------------------------------------------------")
        print("GLEIF raw load finished successfully.")
        print(f"Effective GLEIF load date: {effective_load_date}")
        print(f"Freshness status: {freshness['freshness_status']}")
        print(f"Total LEI inserted rows: {total_lei_inserted}")
        print(f"Total RR inserted rows: {total_rr_inserted}")

    finally:
        postgres.close()


if __name__ == "__main__":
    main()