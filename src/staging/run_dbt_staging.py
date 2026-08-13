from common.dbt_load_runner import run_model_pipeline


STAGING_MODELS = [
    {
        "model_name": "stg_ecb_observations",
        "target_table": "staging.stg_ecb_observations_full",
        "is_view": False,
        "source": "raw.ecb_observations_full",
        "job_type": "staging",
    },
    {
        "model_name": "stg_eu_fsf_full",
        "target_table": "staging.stg_eu_fsf_full",
        "is_view": False,
        "source": "raw.eu_fsf_full",
        "job_type": "staging",
    },
    {
        "model_name": "stg_gleif_lei_full",
        "target_table": "staging.stg_gleif_lei_full",
        "is_view": False,
        "source": "raw.gleif_lei_full",
        "job_type": "staging",
    },
    {
        "model_name": "stg_gleif_rr_full",
        "target_table": "staging.stg_gleif_rr_full",
        "is_view": False,
        "source": "raw.gleif_rr_full",
        "job_type": "staging",
    },
    {
        "model_name": "stg_opensanctions",
        "target_table": "staging.stg_opensanctions",
        "is_view": False,
        "source": "raw.opensanctions_targets",
        "job_type": "staging",
    },
]
 
 
if __name__ == "__main__":
    run_model_pipeline(
        models=STAGING_MODELS,
        description="Run staging dbt pipeline models.",
    )