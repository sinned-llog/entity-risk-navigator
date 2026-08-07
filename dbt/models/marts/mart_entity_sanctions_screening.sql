{{ config(
    materialized='table',
    schema='marts',
    alias='mart_entity_sanctions_screening',
    post_hook=[
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_mart_entity_sanctions_screening_id ON {{ this }} (entity_sanctions_match_id);",
      "CREATE INDEX IF NOT EXISTS idx_mart_entity_sanctions_screening_lei ON {{ this }} (lei);",
      "CREATE INDEX IF NOT EXISTS idx_mart_entity_sanctions_screening_entity ON {{ this }} (entity_candidate_id);",
      "CREATE INDEX IF NOT EXISTS idx_mart_entity_sanctions_screening_quality ON {{ this }} (match_quality_tier);",
      "CREATE INDEX IF NOT EXISTS idx_mart_entity_sanctions_screening_source ON {{ this }} (sanctions_source);",
      "CREATE INDEX IF NOT EXISTS idx_mart_entity_sanctions_screening_country ON {{ this }} (country);",
      "CREATE INDEX IF NOT EXISTS idx_mart_entity_sanctions_screening_load_date ON {{ this }} (source_load_date);"
    ]
) }}

with source_matches as (

    select
        entity_sanctions_match_id,
        entity_candidate_id,

        lei,
        legal_name,
        legal_name_normalized,
        country,
        entity_status,
        registration_status,

        sanctions_source,
        sanctions_entity_type,
        sanction_subject_id,
        source_subject_id,
        source_entity_key,
        sanctions_name_id,

        sanctions_name,
        sanctions_subject_primary_name,

        source_name_type,
        is_primary_name,

        match_type,
        match_score,
        match_quality_score,
        match_quality_tier,
        match_quality_reasons,

        is_short_match_key,
        is_generic_match_key,
        has_country_overlap,
        country_overlap_status,
        has_identifier_overlap,

        source_sanction_context,
        source_programs,
        source_lists,
        source_countries,
        source_reference_url,

        source_load_date,
        intermediate_loaded_at

    from {{ ref('stg_int_sanctions_matches') }}

),

final as (

    select
        entity_sanctions_match_id,
        entity_candidate_id,

        lei,
        legal_name,
        legal_name_normalized,
        country,
        entity_status,
        registration_status,

        sanctions_source,
        sanctions_entity_type,
        sanction_subject_id,
        source_subject_id,
        source_entity_key,
        sanctions_name_id,

        sanctions_name,
        nullif(trim(sanctions_subject_primary_name), '') as sanctions_subject_primary_name,

        source_name_type,
        is_primary_name,

        match_type,
        match_score,
        match_quality_score,
        match_quality_tier,
        match_quality_reasons,

        case
            when match_quality_tier = 'high_confidence' then 1
            when match_quality_tier = 'medium_confidence' then 2
            when match_quality_tier = 'review_required' then 3
            else 9
        end as match_quality_rank,

        case
            when match_quality_tier in ('high_confidence', 'medium_confidence') then true
            else false
        end as is_potential_risk_match,

        case
            when match_quality_tier = 'high_confidence' then true
            else false
        end as is_high_confidence_match,

        case
            when match_quality_tier = 'medium_confidence' then true
            else false
        end as is_medium_confidence_match,

        case
            when match_quality_tier = 'review_required' then true
            else false
        end as is_review_required_match,

        is_short_match_key,
        is_generic_match_key,
        has_country_overlap,
        country_overlap_status,
        has_identifier_overlap,

        nullif(trim(left(source_sanction_context, 1500)), '') as sanction_context_summary,
        nullif(trim(source_programs), '') as source_programs,
        nullif(trim(left(source_lists, 1000)), '') as source_lists,
        nullif(trim(source_countries), '') as source_countries,
        nullif(trim(source_reference_url), '') as source_reference_url,

        source_load_date,
        intermediate_loaded_at as staging_loaded_at,
        current_timestamp as mart_loaded_at

    from source_matches

)

select *
from final
