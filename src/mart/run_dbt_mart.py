from common.dbt_load_runner import run_model_pipeline


MART_MODELS = [
    {
        "model_name": "mart_entity_sanctions_screening",
        "target_table": "marts.mart_entity_sanctions_screening",
        "is_view": False,
        "source": "staging.stg_int_entity_sanctions_matches",
        "job_type": "mart",
    },
    {
        "model_name": "mart_entity_master",
        "target_table": "marts.mart_entity_master",
        "is_view": False,
        "source": "staging.stg_int_entity_candidates, staging.stg_int_gleif_parent_summary",
        "job_type": "mart",
    },
    {
        "model_name": "mart_entity_risk_score",
        "target_table": "marts.mart_entity_risk_score",
        "is_view": False,
        "source": "marts.mart_entity_master, marts.mart_entity_sanctions_screening",
        "job_type": "mart",
    },
    {
            "model_name": "mart_pipeline_audit_status",
            "target_table": "marts.mart_pipeline_audit_status",
            "is_view": False,
            "source": "audit.job_runs",
            "job_type": "mart",
    },
    {
        "model_name": "mart_ecb_macro_context",
        "target_table": "marts.mart_ecb_macro_context",
        "is_view": False,
        "source": "staging.stg_ecb_observations_full",
        "job_type": "mart",
    },
    {
        "model_name": "mart_entity_relationship_edges",
        "target_table": "marts.mart_entity_relationship_edges",
        "is_view": False,
        "source": "marts.mart_entity_master",
        "job_type": "mart",
    },
    {
        "model_name": "mart_entity_parent_paths",
        "target_table": "marts.mart_entity_parent_paths",
        "is_view": False,
        "source": "marts.mart_entity_master",
        "job_type": "mart",
    },
    {
        "model_name": "mart_entity_relationship_context",
        "target_table": "marts.mart_entity_relationship_context",
        "is_view": False,
        "source": "marts.mart_entity_master, marts.mart_entity_relationship_edges, marts.mart_entity_parent_paths",
        "job_type": "mart",
    },
]


if __name__ == "__main__":
    run_model_pipeline(
        models=MART_MODELS,
        description="Run mart dbt pipeline models.",
    )
