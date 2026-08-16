import argparse
import os
import sys
import subprocess
from typing import Any

from common.postgres_client import PostgresClient
from common.audit_logger import (
    start_job_run,
    finish_job_run_success,
    finish_job_run_failure,
)


APP_ENV = os.getenv("APP_ENV", "dev")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/app/dbt")


def build_dbt_command(model_name: str, dbt_vars: dict[str, Any] | None = None) -> list:
    """
    Builds a dbt run command for a single model.
    """
    cmd = [
        "dbt",
        "run",
        "--select",
        model_name,
        "--project-dir",
        DBT_PROJECT_DIR,
        "--profiles-dir",
        DBT_PROJECT_DIR,
    ]

    if dbt_vars:
        vars_parts = []

        for key, value in dbt_vars.items():
            if value is not None:
                vars_parts.append(f"{key}: '{value}'")

        if vars_parts:
            vars_string = "{" + ", ".join(vars_parts) + "}"
            cmd.extend(["--vars", vars_string])

    return cmd

def parse_source_tables(source: str | None) -> list:
    """
    Parses comma-separated upstream source tables from model config.

    Example:
    "raw.gleif_lei_full, raw.gleif_rr_full"
    -> ["raw.gleif_lei_full", "raw.gleif_rr_full"]
    """
    if not source:
        return []

    return [
        item.strip()
        for item in source.split(",")
        if item.strip()
    ]

def get_inherited_freshness(
    postgres: PostgresClient,
    source: str | None,
) -> dict[str, Any]:
    """
    Inherits freshness information from latest successful upstream audit runs.

    The inheritance is based on the model config's source field.

    Rules:
    - If any upstream source is stale -> stale
    - If any upstream source is unknown or missing -> unknown
    - If all upstream sources are fresh -> fresh
    - snapshot_age_days is the max age across matched upstream sources

    Important:
    Some audit rows contain multiple target tables in one field, e.g.
    "raw.gleif_lei_full, raw.gleif_rr_full".
    This query expands comma-separated target_table values in SQL.
    """
    source_tables = parse_source_tables(source)

    if not source_tables:
        return {
            "freshness_status": "unknown",
            "snapshot_age_days": None,
        }

    values_sql = ", ".join(["(%s)"] * len(source_tables))

    query = f"""
        with requested_sources(source_table) as (
            values {values_sql}
        ),

        expanded_audit as (
            select
                jr.job_run_id,
                jr.target_table,
                trim(target_table_part) as expanded_target_table,
                jr.status,
                jr.freshness_status,
                jr.snapshot_age_days,
                jr.started_at,
                jr.finished_at
            from audit.job_runs jr
            cross join lateral regexp_split_to_table(
                coalesce(jr.target_table, ''),
                '\\s*,\\s*'
            ) as target_table_part
            where jr.status in ('success', 'success_with_warnings')
              and jr.target_table is not null
              and trim(target_table_part) <> ''
        ),

        latest_per_source as (
            select
                rs.source_table,
                ea.freshness_status,
                ea.snapshot_age_days,
                row_number() over (
                    partition by rs.source_table
                    order by
                        ea.finished_at desc nulls last,
                        ea.started_at desc nulls last,
                        ea.job_run_id desc
                ) as row_num
            from requested_sources rs
            left join expanded_audit ea
                on rs.source_table = ea.expanded_target_table
        ),

        latest_only as (
            select
                source_table,
                freshness_status,
                snapshot_age_days
            from latest_per_source
            where row_num = 1
               or row_num is null
        )

        select
            case
                when count(*) filter (
                    where freshness_status = 'stale'
                ) > 0
                    then 'stale'

                when count(*) filter (
                    where freshness_status is null
                       or freshness_status = 'unknown'
                ) > 0
                    then 'unknown'

                when count(*) filter (
                    where freshness_status = 'fresh'
                ) = count(*)
                    then 'fresh'

                else 'unknown'
            end as inherited_freshness_status,

            max(snapshot_age_days) as inherited_snapshot_age_days
        from latest_only
    """

    row = postgres.fetch_one(query, tuple(source_tables))

    if row is None:
        return {
            "freshness_status": "unknown",
            "snapshot_age_days": None,
        }

    return {
        "freshness_status": row[0] or "unknown",
        "snapshot_age_days": row[1],
    }


def run_dbt_model(model_config: dict[str, Any]) -> None:
    """
    Executes a single dbt model and records the execution in the audit log.
    """
    model_name = model_config["model_name"]
    target_table = model_config["target_table"]
    source = model_config.get("source", "Unknown")
    is_view = model_config.get("is_view", False)
    job_type = model_config.get("job_type", "intermediate")
    dbt_vars = model_config.get("dbt_vars")

    postgres = PostgresClient.from_env()
    job_run_id = None

    try:
        job_run_id = start_job_run(
            postgres=postgres,
            job_name=f"dbt_build_{model_name}",
            job_type=job_type,
            source=source,
            target_system="postgres",
            target_table=target_table,
            app_env=APP_ENV,
            metadata_json={
                "dbt_model": model_name,
                "materialization": "view" if is_view else "table",
                "dbt_vars": dbt_vars or {},
            },
        )

        cmd = build_dbt_command(
            model_name=model_name,
            dbt_vars=dbt_vars,
        )

        print("\n==================================================")
        print(f"Executing dbt model: {model_name}")
        print(f"Command: {' '.join(cmd)}")
        print("==================================================")

        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )

        print(result.stdout)

        if is_view:
            rows_read = postgres.fetch_scalar(f"SELECT COUNT(*) FROM {target_table};") or 0
            rows_inserted = 0
        else:
            rows_inserted = postgres.fetch_scalar(f"SELECT COUNT(*) FROM {target_table};") or 0
            rows_read = rows_inserted

        inherited_freshness = get_inherited_freshness(
            postgres=postgres,
            source=source,
        )

        finish_job_run_success(
            postgres=postgres,
            job_run_id=job_run_id,
            status="success",
            effective_load_date=(dbt_vars or {}).get("gleif_staging_load_date"),
            freshness=inherited_freshness,
            rows_read=rows_read,
            rows_inserted=rows_inserted,
            metadata_json={
                "dbt_model": model_name,
                "materialization": "view" if is_view else "table",
                "dbt_vars": dbt_vars or {},
                "inherited_freshness": inherited_freshness,
                "freshness_source": source,
            },
        )

        print(
            f"Successfully processed {model_name} "
            f"(Rows read: {rows_read}, Rows inserted: {rows_inserted})."
        )

    except subprocess.CalledProcessError as err:
        error_msg = err.stderr or err.stdout
        print(f"dbt Execution Failed for {model_name}: {error_msg}")

        if job_run_id:
            finish_job_run_failure(
                postgres=postgres,
                job_run_id=job_run_id,
                error_message=error_msg,
            )

        raise

    finally:
        postgres.close()


def select_models(
        available_models: list[dict[str, Any]],
        selected_model_names: list[str] | None,
    ) -> list[dict[str, Any]]:
    """
    Filters model config list based on CLI --select.
    Fails explicitly if unknown model names are requested.
    """
    if not selected_model_names:
        return available_models

    known_model_names = {model["model_name"] for model in available_models}
    requested_model_names = set(selected_model_names)

    unknown_models = requested_model_names - known_model_names

    if unknown_models:
        print(
            "Error: Selected model(s) not found in model config: "
            + ", ".join(sorted(unknown_models))
        )
        print(
            "Available models: "
            + ", ".join(model["model_name"] for model in available_models)
        )
        sys.exit(1)

    return [
        model
        for model in available_models
        if model["model_name"] in requested_model_names
    ]


def run_model_pipeline(
    models: list[dict[str, Any]],
    description: str,
) -> None:
    """
    Generic CLI entrypoint for dbt model pipeline execution.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--select",
        nargs="+",
        help="Specific model name(s) to execute.",
    )

    args = parser.parse_args()

    models_to_run = select_models(
        available_models=models,
        selected_model_names=args.select,
    )

    print(f"Starting Pipeline execution ({len(models_to_run)} model(s))...")

    for model_config in models_to_run:
        run_dbt_model(model_config)

    print("\nPipeline execution finished successfully.")
