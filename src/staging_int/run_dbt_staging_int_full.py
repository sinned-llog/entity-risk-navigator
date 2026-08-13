
import os
from common.dbt_load_runner import run_model_pipeline

GLEIF_STAGING_LOAD_DATE = os.getenv("GLEIF_STAGING_LOAD_DATE")

def gleif_vars() -> dict:
    return {
        "gleif_staging_load_date": GLEIF_STAGING_LOAD_DATE,
    }

# List of intermediate models to execute and track in sequence
INTERMEDIATE_MODELS = [
    {
        "model_name": "stg_int_entity_candidates", 
        "target_table": "staging.stg_int_entity_candidates",
        "is_view": True,  # Materialized as view -> 0 inserted rows
        "source": "staging.stg_gleif_lei_full",
        "dbt_vars": gleif_vars(),
    },
    {
        "model_name": "stg_int_gleif_parent_summary", 
        "target_table": "staging.stg_int_gleif_parent_summary",
        "is_view": False,  # Materialized as table -> actual row count inserted
        "source": "staging.stg_gleif_lei_full, staging.stg_gleif_rr_full",
        "dbt_vars": gleif_vars(),
    },
    {
        "model_name": "stg_int_eu_fsf_subjects", 
        "target_table": "staging.stg_int_eu_fsf_subjects",
        "is_view": False,  # Materialized as table -> actual row count inserted
        "source": "staging.stg_eu_fsf_full",
    },
    {
        "model_name": "stg_int_eu_fsf_names", 
        "target_table": "staging.stg_int_eu_fsf_names",
        "is_view": False,  # Physical table -> exact inserted rows logged
        "source": "staging.stg_eu_fsf_full, staging.stg_int_eu_fsf_subjects",
    },
    {
        "model_name": "stg_int_opensanctions_targets", 
        "target_table": "staging.stg_int_opensanctions_targets",
        "is_view": False,  # Physical table -> exact inserted rows logged
        "source": "staging.stg_opensanctions",
    },
    {
        "model_name": "stg_int_opensanctions_names", 
        "target_table": "staging.stg_int_opensanctions_names",
        "is_view": False,  # Physical table -> exact inserted rows logged
        "source": "staging.stg_opensanctions",
    },
    {
        "model_name": "stg_int_sanctions_subjects_unified", 
        "target_table": "staging.stg_int_sanctions_subjects_unified",
        "is_view": False,  # Physical table -> exact inserted rows logged
        "source": "staging.stg_int_eu_fsf_subjects, staging.stg_int_opensanctions_targets",
    },
    {
        "model_name": "stg_int_sanctions_names_unified", 
        "target_table": "staging.stg_int_sanctions_names_unified",
        "is_view": False,  # Physical table -> exact inserted rows logged
        "source": "staging.stg_int_opensanctions_names, staging.stg_int_eu_fsf_names, staging.stg_int_sanctions_subjects_unified",
    },
    {
        "model_name": "stg_int_sanctions_matches", 
        "target_table": "staging.stg_int_entity_sanctions_matches",
        "is_view": False,  # Physical table -> exact inserted rows logged
        "source": "staging.stg_int_entity_candidates, staging.stg_int_sanctions_names_unified, staging.stg_int_sanctions_subjects_unified",
    },
    {
        "model_name": "mart_entity_sanctions_screening",
        "target_table": "marts.mart_entity_sanctions_screening",
        "is_view": False,
        "source": "staging.stg_int_entity_sanctions_matches",
    },  
]


if __name__ == "__main__":
    run_model_pipeline(
        models=INTERMEDIATE_MODELS,
        description="Run intermediate dbt pipeline models.",
    )
