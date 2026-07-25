import json
from typing import Any

from common.postgres_client import PostgresClient


# -------------------------------------------------------------------
# Audit schema setup
# -------------------------------------------------------------------

def ensure_audit_schema(postgres: PostgresClient) -> None:
    """
    Creates the audit schema and audit.job_runs table if they do not exist.

    This table is intentionally generic and can be used for:
    - download jobs: source/API/web -> MinIO
    - raw load jobs: MinIO -> PostgreSQL raw
    - staging jobs: raw -> staging
    - mart jobs: staging/cross-source -> mart
    """

    postgres.execute(
        """
        CREATE SCHEMA IF NOT EXISTS audit;

        CREATE TABLE IF NOT EXISTS audit.job_runs (
            job_run_id BIGSERIAL PRIMARY KEY,

            job_name TEXT NOT NULL,
            job_type TEXT NOT NULL,
            source TEXT,
            target_system TEXT,
            target_table TEXT,

            app_env TEXT,

            manifest_key TEXT,
            source_url TEXT,
            source_object_key TEXT,
            metadata_object_key TEXT,
            effective_load_date DATE,

            started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMPTZ,

            status TEXT NOT NULL,

            freshness_status TEXT,
            snapshot_age_days INTEGER,

            files_discovered BIGINT,
            files_processed BIGINT,
            files_success BIGINT,
            files_failed BIGINT,

            rows_read BIGINT,
            rows_inserted BIGINT,
            rows_updated BIGINT,
            rows_deleted BIGINT,
            rows_failed BIGINT,

            downloaded_bytes BIGINT,
            content_hash TEXT,

            warning_count INTEGER DEFAULT 0,
            error_count INTEGER DEFAULT 0,

            error_message TEXT,
            metadata_json JSONB
        );
        """
    )

    postgres.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_job_runs_job_name
            ON audit.job_runs (job_name);

        CREATE INDEX IF NOT EXISTS idx_job_runs_job_type
            ON audit.job_runs (job_type);

        CREATE INDEX IF NOT EXISTS idx_job_runs_source
            ON audit.job_runs (source);

        CREATE INDEX IF NOT EXISTS idx_job_runs_status
            ON audit.job_runs (status);

        CREATE INDEX IF NOT EXISTS idx_job_runs_started_at
            ON audit.job_runs (started_at);

        CREATE INDEX IF NOT EXISTS idx_job_runs_effective_load_date
            ON audit.job_runs (effective_load_date);

        CREATE INDEX IF NOT EXISTS idx_job_runs_manifest_key
            ON audit.job_runs (manifest_key);
        """
    )


# -------------------------------------------------------------------
# Internal helpers
# -------------------------------------------------------------------

def _json_dumps_or_none(value: dict[str, Any] | None) -> str | None:
    if value is None:
        return None

    return json.dumps(value, ensure_ascii=False)


def _freshness_value(
    freshness: dict[str, Any] | None,
    key: str,
) -> Any:
    if not freshness:
        return None

    return freshness.get(key)


# -------------------------------------------------------------------
# Public audit API
# -------------------------------------------------------------------

def start_job_run(
    postgres: PostgresClient,
    job_name: str,
    job_type: str,
    source: str | None = None,
    target_system: str | None = None,
    target_table: str | None = None,
    app_env: str | None = None,
    manifest_key: str | None = None,
    source_url: str | None = None,
    source_object_key: str | None = None,
    metadata_object_key: str | None = None,
    effective_load_date: str | None = None,
    metadata_json: dict[str, Any] | None = None,
) -> int:
    """
    Starts a structured audit record and returns job_run_id.

    status is set to 'running'. The caller should update the row using one of:
    - finish_job_run_success
    - finish_job_run_failure
    """

    ensure_audit_schema(postgres)

    return postgres.fetch_scalar(
        """
        INSERT INTO audit.job_runs (
            job_name,
            job_type,
            source,
            target_system,
            target_table,
            app_env,
            manifest_key,
            source_url,
            source_object_key,
            metadata_object_key,
            effective_load_date,
            status,
            metadata_json
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, 'running', %s::jsonb
        )
        RETURNING job_run_id;
        """,
        (
            job_name,
            job_type,
            source,
            target_system,
            target_table,
            app_env,
            manifest_key,
            source_url,
            source_object_key,
            metadata_object_key,
            effective_load_date,
            _json_dumps_or_none(metadata_json),
        ),
    )


def finish_job_run_success(
    postgres: PostgresClient,
    job_run_id: int,
    status: str = "success",
    manifest_key: str | None = None,
    source_url: str | None = None,
    source_object_key: str | None = None,
    metadata_object_key: str | None = None,
    effective_load_date: str | None = None,
    freshness: dict[str, Any] | None = None,
    files_discovered: int | None = None,
    files_processed: int | None = None,
    files_success: int | None = None,
    files_failed: int | None = None,
    rows_read: int | None = None,
    rows_inserted: int | None = None,
    rows_updated: int | None = None,
    rows_deleted: int | None = None,
    rows_failed: int | None = None,
    downloaded_bytes: int | None = None,
    content_hash: str | None = None,
    warning_count: int | None = None,
    error_count: int | None = None,
    metadata_json: dict[str, Any] | None = None,
) -> None:
    """
    Finishes a job run successfully.

    status may be 'success' or 'success_with_warnings'.
    """

    if status not in {"success", "success_with_warnings"}:
        raise ValueError("Successful audit status must be success or success_with_warnings.")

    postgres.execute(
        """
        UPDATE audit.job_runs
        SET
            finished_at = CURRENT_TIMESTAMP,
            status = %s,
            manifest_key = COALESCE(%s, manifest_key),
            source_url = COALESCE(%s, source_url),
            source_object_key = COALESCE(%s, source_object_key),
            metadata_object_key = COALESCE(%s, metadata_object_key),
            effective_load_date = COALESCE(%s, effective_load_date),
            freshness_status = COALESCE(%s, freshness_status),
            snapshot_age_days = COALESCE(%s, snapshot_age_days),
            files_discovered = COALESCE(%s, files_discovered),
            files_processed = COALESCE(%s, files_processed),
            files_success = COALESCE(%s, files_success),
            files_failed = COALESCE(%s, files_failed),
            rows_read = COALESCE(%s, rows_read),
            rows_inserted = COALESCE(%s, rows_inserted),
            rows_updated = COALESCE(%s, rows_updated),
            rows_deleted = COALESCE(%s, rows_deleted),
            rows_failed = COALESCE(%s, rows_failed),
            downloaded_bytes = COALESCE(%s, downloaded_bytes),
            content_hash = COALESCE(%s, content_hash),
            warning_count = COALESCE(%s, warning_count),
            error_count = COALESCE(%s, error_count),
            metadata_json = COALESCE(%s::jsonb, metadata_json)
        WHERE job_run_id = %s;
        """,
        (
            status,
            manifest_key,
            source_url,
            source_object_key,
            metadata_object_key,
            effective_load_date,
            _freshness_value(freshness, "freshness_status"),
            _freshness_value(freshness, "snapshot_age_days"),
            files_discovered,
            files_processed,
            files_success,
            files_failed,
            rows_read,
            rows_inserted,
            rows_updated,
            rows_deleted,
            rows_failed,
            downloaded_bytes,
            content_hash,
            warning_count,
            error_count,
            _json_dumps_or_none(metadata_json),
            job_run_id,
        ),
    )


def finish_job_run_failure(
    postgres: PostgresClient,
    job_run_id: int,
    error_message: str,
    manifest_key: str | None = None,
    source_url: str | None = None,
    source_object_key: str | None = None,
    metadata_object_key: str | None = None,
    effective_load_date: str | None = None,
    freshness: dict[str, Any] | None = None,
    files_discovered: int | None = None,
    files_processed: int | None = None,
    files_success: int | None = None,
    files_failed: int | None = None,
    rows_read: int | None = None,
    rows_inserted: int | None = None,
    rows_updated: int | None = None,
    rows_deleted: int | None = None,
    rows_failed: int | None = None,
    downloaded_bytes: int | None = None,
    content_hash: str | None = None,
    warning_count: int | None = None,
    error_count: int | None = None,
    metadata_json: dict[str, Any] | None = None,
) -> None:
    """
    Finishes a job run as failed and stores the error message.
    """

    postgres.execute(
        """
        UPDATE audit.job_runs
        SET
            finished_at = CURRENT_TIMESTAMP,
            status = 'failed',
            manifest_key = COALESCE(%s, manifest_key),
            source_url = COALESCE(%s, source_url),
            source_object_key = COALESCE(%s, source_object_key),
            metadata_object_key = COALESCE(%s, metadata_object_key),
            effective_load_date = COALESCE(%s, effective_load_date),
            freshness_status = COALESCE(%s, freshness_status),
            snapshot_age_days = COALESCE(%s, snapshot_age_days),
            files_discovered = COALESCE(%s, files_discovered),
            files_processed = COALESCE(%s, files_processed),
            files_success = COALESCE(%s, files_success),
            files_failed = COALESCE(%s, files_failed),
            rows_read = COALESCE(%s, rows_read),
            rows_inserted = COALESCE(%s, rows_inserted),
            rows_updated = COALESCE(%s, rows_updated),
            rows_deleted = COALESCE(%s, rows_deleted),
            rows_failed = COALESCE(%s, rows_failed),
            downloaded_bytes = COALESCE(%s, downloaded_bytes),
            content_hash = COALESCE(%s, content_hash),
            warning_count = COALESCE(%s, warning_count),
            error_count = COALESCE(%s, error_count),
            error_message = %s,
            metadata_json = COALESCE(%s::jsonb, metadata_json)
        WHERE job_run_id = %s;
        """,
        (
            manifest_key,
            source_url,
            source_object_key,
            metadata_object_key,
            effective_load_date,
            _freshness_value(freshness, "freshness_status"),
            _freshness_value(freshness, "snapshot_age_days"),
            files_discovered,
            files_processed,
            files_success,
            files_failed,
            rows_read,
            rows_inserted,
            rows_updated,
            rows_deleted,
            rows_failed,
            downloaded_bytes,
            content_hash,
            warning_count,
            error_count,
            error_message[:8000] if error_message else None,
            _json_dumps_or_none(metadata_json),
            job_run_id,
        ),
    )
