import os
import subprocess
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
ECB_STAGING_LOAD_DATE = os.getenv("ECB_STAGING_LOAD_DATE")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/app/dbt")


def run_dbt_model(model_name: str, target_table: str) -> None:
    """Executes a dbt staging model and tracks execution state in audit log."""
    postgres = PostgresClient.from_env()
    job_run_id = None

    try:
        # Start audit job logging
        job_run_id = start_job_run(
            postgres=postgres,
            job_name=f"dbt_build_{model_name}",
            job_type="staging",
            source="ECB Data Portal Raw Schema",
            target_system="postgres",
            target_table=target_table,
            app_env=APP_ENV,
            metadata_json={"dbt_model": model_name},
        )

        # Build dbt CLI command (including optional variable load dates)
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

        if ECB_STAGING_LOAD_DATE:
            cmd.extend(
                [
                    "--vars",
                    f"{{ecb_staging_load_date: '{ECB_STAGING_LOAD_DATE}'}}",
                ]
            )

        print(f"Executing: {' '.join(cmd)}")
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout)

        # Query total count of rows produced in the target staging table
        rows_inserted = (
            postgres.fetch_scalar(f"SELECT COUNT(*) FROM {target_table};") or 0
        )

        # Mark audit execution as successful
        finish_job_run_success(
            postgres=postgres,
            job_run_id=job_run_id,
            status="success",
            effective_load_date=ECB_STAGING_LOAD_DATE,
            rows_read=rows_inserted,
            rows_inserted=rows_inserted,
        )

        print(f"Successfully processed {model_name}. Total rows: {rows_inserted}")

    except subprocess.CalledProcessError as err:
        error_msg = err.stderr or err.stdout
        print(f"dbt Execution Failed: {error_msg}")
        if job_run_id:
            finish_job_run_failure(
                postgres=postgres,
                job_run_id=job_run_id,
                error_message=error_msg,
            )
        raise
    finally:
        postgres.close()


if __name__ == "__main__":
    # Execute for ECB observations staging model
    run_dbt_model("stg_ecb_observations", "staging.stg_ecb_observations_full")