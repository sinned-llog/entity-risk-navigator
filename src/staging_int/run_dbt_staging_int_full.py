
import argparse
import os
import sys  
import subprocess
from common.postgres_client import PostgresClient
from common.audit_logger import (
    start_job_run,
    finish_job_run_success,
    finish_job_run_failure,
)

# Mandatory environment variables with fallback defaults
APP_ENV = os.getenv("APP_ENV", "dev")
GLEIF_STAGING_LOAD_DATE = os.getenv("GLEIF_STAGING_LOAD_DATE")
DBT_PROJECT_DIR = "/app/dbt"

# List of intermediate models to execute and track in sequence
INTERMEDIATE_MODELS = [
    {
        "model_name": "stg_int_entity_candidates", 
        "target_table": "staging.stg_int_entity_candidates",
        "is_view": True,  # Materialized as view -> 0 inserted rows
        "source": "staging.stg_gleif_lei_full"
    },
    {
        "model_name": "stg_int_gleif_parent_summary", 
        "target_table": "staging.stg_int_gleif_parent_summary",
        "is_view": False,  # Materialized as table -> actual row count inserted
        "source": "staging.stg_gleif_lei_full, staging.stg_gleif_rr_full"
    },
    {
        "model_name": "stg_int_eu_fsf_subjects", 
        "target_table": "staging.stg_int_eu_fsf_subjects",
        "is_view": False,  # Materialized as table -> actual row count inserted
        "source": "staging.stg_eu_fsf_full"
        },
    {
        "model_name": "stg_int_eu_fsf_names", 
        "target_table": "staging.stg_int_eu_fsf_names",
        "is_view": False,  # Physical table -> exact inserted rows logged
        "source": "staging.stg_eu_fsf_full, staging.stg_int_eu_fsf_subjects"
    },
    {
        "model_name": "stg_int_opensanctions_targets", 
        "target_table": "staging.stg_int_opensanctions_targets",
        "is_view": False,  # Physical table -> exact inserted rows logged
        "source": "staging.stg_opensanctions"
        },
    {
        "model_name": "stg_int_opensanctions_names", 
        "target_table": "staging.stg_int_opensanctions_names",
        "is_view": False,  # Physical table -> exact inserted rows logged
        "source": "staging.stg_opensanctions"
    },
]


def run_dbt_model(model_config: dict):
    """
    Executes a single dbt intermediate model and tracks its status in the audit logging system.
    """
    model_name = model_config["model_name"]
    target_table = model_config["target_table"]
    source = model_config.get("source", "Staging Layer")
    is_view = model_config.get("is_view", True)
    
    postgres = PostgresClient.from_env()
    job_run_id = None

    try:
        # 1. Initialize audit log entry
        job_run_id = start_job_run(
            postgres=postgres,
            job_name=f"dbt_build_{model_name}",
            job_type="intermediate",
            source=source,
            target_system="postgres",
            target_table=target_table,
            app_env=APP_ENV,
            metadata_json={
                "dbt_model": model_name, 
                "materialization": "view" if is_view else "table"
            }
        )

        # 2. Build dbt CLI command
        cmd = [
            "dbt", "run", 
            "--select", model_name,
            "--project-dir", DBT_PROJECT_DIR,
            "--profiles-dir", DBT_PROJECT_DIR
        ]
        if GLEIF_STAGING_LOAD_DATE:
            cmd.extend(["--vars", f"{{gleif_staging_load_date: '{GLEIF_STAGING_LOAD_DATE}'}}"])

        print(f"\n==================================================")
        print(f"Executing dbt model: {model_name}")
        print(f"Command: {' '.join(cmd)}")
        print(f"==================================================")
        
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout)

        # 3. Handle audit metrics based on materialization type
        if is_view:
            # Views do not physically insert rows into storage
            rows_read = postgres.fetch_scalar(f"SELECT COUNT(*) FROM {target_table};") or 0
            rows_inserted = 0
        else:
            # Physical tables write actual rows to disk
            rows_inserted = postgres.fetch_scalar(f"SELECT COUNT(*) FROM {target_table};") or 0
            rows_read = rows_inserted

        # 4. Finalize audit log entry on success
        finish_job_run_success(
            postgres=postgres,
            job_run_id=job_run_id,
            status="success",
            effective_load_date=GLEIF_STAGING_LOAD_DATE,
            rows_read=rows_read,
            rows_inserted=rows_inserted,
        )
        print(f"Successfully processed {model_name} (Rows read: {rows_read}, Rows inserted: {rows_inserted}).")

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


if __name__ == "__main__":
    # CLI Argument-Parser für flexible Einzel-Ausführungen
    parser = argparse.ArgumentParser(description="Run Intermediate dbt Pipeline Models.")
    parser.add_argument(
        "--select", 
        nargs="+", 
        help="Specific model name(s) to execute. Example: --select stg_int_opensanctions_targets"
    )
    args = parser.parse_args()

    # Filtern, falls spezifische Modelle angefordert wurden
    models_to_run = INTERMEDIATE_MODELS
    if args.select:
        models_to_run = [m for m in INTERMEDIATE_MODELS if m["model_name"] in args.select]
        if not models_to_run:
            print(f"Error: Selected model(s) {args.select} not found in INTERMEDIATE_MODELS config.")
            sys.exit(1)

    print(f"Starting Pipeline execution ({len(models_to_run)} model(s))...")
    
    for item in models_to_run:
        run_dbt_model(item)
        
    print("\nPipeline execution finished successfully.")