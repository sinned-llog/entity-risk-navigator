{{ config(
    materialized='table',
    schema='marts',
    alias='mart_entity_risk_score',
    post_hook=[
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_mart_entity_risk_score_entity_id ON {{ this }} (entity_candidate_id);",
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_mart_entity_risk_score_lei ON {{ this }} (lei);",
      "CREATE INDEX IF NOT EXISTS idx_mart_entity_risk_score_tier ON {{ this }} (risk_tier);",
      "CREATE INDEX IF NOT EXISTS idx_mart_entity_risk_score_score ON {{ this }} (risk_score);",
      "CREATE INDEX IF NOT EXISTS idx_mart_entity_risk_score_country ON {{ this }} (country);",
      "CREATE INDEX IF NOT EXISTS idx_mart_entity_risk_score_load_date ON {{ this }} (source_load_date);"
    ]
) }}

with entity_master as (

    select
        entity_candidate_id,
        candidate_source,
        candidate_source_id,

        lei,
        legal_name,
        legal_name_normalized,

        entity_status,
        registration_status,

        legal_jurisdiction,
        country,
        legal_address_country,
        headquarters_address_country,

        direct_parent_lei,
        direct_parent_name,
        ultimate_parent_lei,
        ultimate_parent_name,

        has_direct_parent,
        has_ultimate_parent,
        has_any_parent,

        source_load_date as entity_source_load_date

    from {{ ref('mart_entity_master') }}

),

sanctions_summary as (

    select
        entity_candidate_id,

        count(*) as total_sanctions_match_count,

        count(*) filter (
            where match_quality_tier = 'high_confidence'
        ) as high_confidence_match_count,

        count(*) filter (
            where match_quality_tier = 'medium_confidence'
        ) as medium_confidence_match_count,

        count(*) filter (
            where match_quality_tier = 'review_required'
        ) as review_required_match_count,

        count(distinct sanctions_source) as sanctions_source_count,
        count(distinct sanction_subject_id) as distinct_sanction_subject_count,

        string_agg(
            distinct sanctions_source,
            ';' order by sanctions_source
        ) as sanctions_sources,

        string_agg(
            distinct match_quality_tier,
            ';' order by match_quality_tier
        ) as match_quality_tiers,

        string_agg(
            distinct source_programs,
            ';' order by source_programs
        ) filter (
            where source_programs is not null
              and trim(source_programs) <> ''
        ) as sanctions_programs,

        string_agg(
            distinct source_lists,
            ';' order by source_lists
        ) filter (
            where source_lists is not null
              and trim(source_lists) <> ''
        ) as sanctions_lists,

        string_agg(
            distinct source_entity_key,
            ';' order by source_entity_key
        ) filter (
            where source_entity_key is not null
            and trim(source_entity_key) <> ''
        ) as source_entity_keys,

        max(match_quality_score) as max_match_quality_score,

        min(match_quality_rank) as best_match_quality_rank,

        max(source_load_date) as sanctions_source_load_date

    from {{ ref('mart_entity_sanctions_screening') }}

    group by
        entity_candidate_id

),

risk_scoring as (

    select
        e.entity_candidate_id,
        e.candidate_source,
        e.candidate_source_id,


        e.lei,
        e.legal_name,
        e.legal_name_normalized,

        e.entity_status,
        e.registration_status,

        e.legal_jurisdiction,
        e.country,
        e.legal_address_country,
        e.headquarters_address_country,

        e.direct_parent_lei,
        e.direct_parent_name,
        e.ultimate_parent_lei,
        e.ultimate_parent_name,

        e.has_direct_parent,
        e.has_ultimate_parent,
        e.has_any_parent,

        coalesce(s.total_sanctions_match_count, 0) as total_sanctions_match_count,
        coalesce(s.high_confidence_match_count, 0) as high_confidence_match_count,
        coalesce(s.medium_confidence_match_count, 0) as medium_confidence_match_count,
        coalesce(s.review_required_match_count, 0) as review_required_match_count,

        coalesce(s.sanctions_source_count, 0) as sanctions_source_count,
        coalesce(s.distinct_sanction_subject_count, 0) as distinct_sanction_subject_count,

        s.sanctions_sources,
        s.match_quality_tiers,
        s.sanctions_programs,
        s.sanctions_lists,

        s.max_match_quality_score,
        s.best_match_quality_rank,

        s.source_entity_keys,

        case
            when coalesce(s.total_sanctions_match_count, 0) > 0 then true
            else false
        end as has_sanctions_match,

        case
            when coalesce(s.high_confidence_match_count, 0) > 0 then true
            else false
        end as has_high_confidence_match,

        case
            when coalesce(s.medium_confidence_match_count, 0) > 0 then true
            else false
        end as has_medium_confidence_match,

        case
            when coalesce(s.review_required_match_count, 0) > 0 then true
            else false
        end as has_review_required_match,

        case
            when coalesce(s.high_confidence_match_count, 0) > 0
                then 100
            when coalesce(s.medium_confidence_match_count, 0) > 0
                then 60
            when coalesce(s.review_required_match_count, 0) > 0
                then 20
            else 0
        end as risk_score,

        case
            when coalesce(s.high_confidence_match_count, 0) > 0
                then 'high'
            when coalesce(s.medium_confidence_match_count, 0) > 0
                then 'medium'
            when coalesce(s.review_required_match_count, 0) > 0
                then 'review'
            else 'low_or_no_known_match'
        end as risk_tier,

        case
            when coalesce(s.high_confidence_match_count, 0) > 0
                then 'highest_risk_reason=high_confidence_sanctions_match'

            when coalesce(s.medium_confidence_match_count, 0) > 0
                then 'highest_risk_reason=medium_confidence_sanctions_match'

            when coalesce(s.review_required_match_count, 0) > 0
                then 'highest_risk_reason=review_required_sanctions_match'

            else 'highest_risk_reason=no_known_sanctions_match'
        end as risk_reasons,

        greatest(
            e.entity_source_load_date,
            coalesce(s.sanctions_source_load_date, e.entity_source_load_date)
        ) as source_load_date,

        concat_ws(
            '; ',
            'total_matches=' || coalesce(s.total_sanctions_match_count, 0)::text,
            'high=' || coalesce(s.high_confidence_match_count, 0)::text,
            'medium=' || coalesce(s.medium_confidence_match_count, 0)::text,
            'review=' || coalesce(s.review_required_match_count, 0)::text
        ) as match_tier_summary

    from entity_master e

    left join sanctions_summary s
        on e.entity_candidate_id = s.entity_candidate_id

),

final as (

    select
        entity_candidate_id,
        candidate_source,
        candidate_source_id,

        lei,
        legal_name,
        legal_name_normalized,

        entity_status,
        registration_status,

        legal_jurisdiction,
        country,
        legal_address_country,
        headquarters_address_country,

        direct_parent_lei,
        direct_parent_name,
        ultimate_parent_lei,
        ultimate_parent_name,

        has_direct_parent,
        has_ultimate_parent,
        has_any_parent,

        total_sanctions_match_count,
        high_confidence_match_count,
        medium_confidence_match_count,
        review_required_match_count,
        source_entity_keys,

        sanctions_source_count,
        distinct_sanction_subject_count,

        sanctions_sources,
        match_quality_tiers,
        sanctions_programs,
        sanctions_lists,

        max_match_quality_score,
        best_match_quality_rank,

        has_sanctions_match,
        has_high_confidence_match,
        has_medium_confidence_match,
        has_review_required_match,

        risk_score,
        risk_tier,
        risk_reasons,
        match_tier_summary,

        source_load_date,
        current_timestamp as mart_loaded_at

    from risk_scoring

)

select *
from final