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

        finish_job_run_success(
            postgres=postgres,
            job_run_id=job_run_id,
            status="success",
            effective_load_date=(dbt_vars or {}).get("gleif_staging_load_date"),
            rows_read=rows_read,
            rows_inserted=rows_inserted,
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