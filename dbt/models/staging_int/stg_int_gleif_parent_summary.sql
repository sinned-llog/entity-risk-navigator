{{ config(
    materialized='table',
    schema='staging',
    alias='stg_int_gleif_parent_summary',
    post_hook=[
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_int_gleif_parent_summary_lei ON {{ this }} (lei);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_gleif_parent_summary_direct_parent ON {{ this }} (direct_parent_lei);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_gleif_parent_summary_ultimate_parent ON {{ this }} (ultimate_parent_lei);"
    ]
) }}

with entities as (

    select
        lei,
        legal_name,
        legal_name_normalized,
        entity_status,
        registration_status,
        legal_jurisdiction,
        legal_address_country,
        headquarters_address_country,
        source_load_date
    from {{ ref('stg_gleif_lei_full') }}
    where lei is not null
      and trim(lei) <> ''

),

direct_parent_candidates as (

    select
        relationship_key,
        start_node_id,
        end_node_id,
        relationship_status,
        registration_status,
        relationship_period_type,
        relationship_period_start,
        relationship_period_end,
        last_update_date,
        validation_sources,
        validation_documents,
        source_load_date
    from {{ ref('stg_gleif_rr_full') }}
    where relationship_type = 'IS_DIRECTLY_CONSOLIDATED_BY'
      and start_node_id is not null
      and trim(start_node_id) <> ''
      and end_node_id is not null
      and trim(end_node_id) <> ''
      and (
            relationship_status is null
            or relationship_status in ('active', 'unknown')
      )
      and (
            registration_status is null
            or registration_status not in ('annulled', 'rejected', 'retired')
      )

),

ultimate_parent_candidates as (

    select
        relationship_key,
        start_node_id,
        end_node_id,
        relationship_status,
        registration_status,
        relationship_period_type,
        relationship_period_start,
        relationship_period_end,
        last_update_date,
        validation_sources,
        validation_documents,
        source_load_date
    from {{ ref('stg_gleif_rr_full') }}
    where relationship_type = 'IS_ULTIMATELY_CONSOLIDATED_BY'
      and start_node_id is not null
      and trim(start_node_id) <> ''
      and end_node_id is not null
      and trim(end_node_id) <> ''
      and (
            relationship_status is null
            or relationship_status in ('active', 'unknown')
      )
      and (
            registration_status is null
            or registration_status not in ('annulled', 'rejected', 'retired')
      )

),

direct_parent as (

    select distinct on (start_node_id)
        start_node_id as lei,
        end_node_id as direct_parent_lei,
        relationship_key as direct_parent_relationship_key,
        relationship_status as direct_parent_relationship_status,
        registration_status as direct_parent_registration_status,
        relationship_period_type as direct_parent_period_type,
        relationship_period_start as direct_parent_period_start,
        relationship_period_end as direct_parent_period_end,
        last_update_date as direct_parent_last_update_date,
        validation_sources as direct_parent_validation_sources,
        validation_documents as direct_parent_validation_documents,
        source_load_date as direct_parent_source_load_date
    from direct_parent_candidates
    order by
        start_node_id,
        source_load_date desc,
        relationship_period_start desc nulls last,
        last_update_date desc nulls last,
        relationship_key desc

),

ultimate_parent as (

    select distinct on (start_node_id)
        start_node_id as lei,
        end_node_id as ultimate_parent_lei,
        relationship_key as ultimate_parent_relationship_key,
        relationship_status as ultimate_parent_relationship_status,
        registration_status as ultimate_parent_registration_status,
        relationship_period_type as ultimate_parent_period_type,
        relationship_period_start as ultimate_parent_period_start,
        relationship_period_end as ultimate_parent_period_end,
        last_update_date as ultimate_parent_last_update_date,
        validation_sources as ultimate_parent_validation_sources,
        validation_documents as ultimate_parent_validation_documents,
        source_load_date as ultimate_parent_source_load_date
    from ultimate_parent_candidates
    order by
        start_node_id,
        source_load_date desc,
        relationship_period_start desc nulls last,
        last_update_date desc nulls last,
        relationship_key desc

),

final as (

    select
        e.lei,
        e.legal_name,
        e.legal_name_normalized,
        e.entity_status,
        e.registration_status,

        e.legal_jurisdiction,
        e.legal_address_country,
        e.headquarters_address_country,

        dp.direct_parent_lei,
        dp_entity.legal_name as direct_parent_name,
        dp_entity.legal_name_normalized as direct_parent_name_normalized,
        dp.direct_parent_relationship_key,
        dp.direct_parent_relationship_status,
        dp.direct_parent_registration_status,
        dp.direct_parent_period_type,
        dp.direct_parent_period_start,
        dp.direct_parent_period_end,
        dp.direct_parent_last_update_date,
        dp.direct_parent_validation_sources,
        dp.direct_parent_validation_documents,
        dp.direct_parent_source_load_date,

        up.ultimate_parent_lei,
        up_entity.legal_name as ultimate_parent_name,
        up_entity.legal_name_normalized as ultimate_parent_name_normalized,
        up.ultimate_parent_relationship_key,
        up.ultimate_parent_relationship_status,
        up.ultimate_parent_registration_status,
        up.ultimate_parent_period_type,
        up.ultimate_parent_period_start,
        up.ultimate_parent_period_end,
        up.ultimate_parent_last_update_date,
        up.ultimate_parent_validation_sources,
        up.ultimate_parent_validation_documents,
        up.ultimate_parent_source_load_date,

        case
            when dp.direct_parent_lei is not null then true
            else false
        end as has_direct_parent,

        case
            when up.ultimate_parent_lei is not null then true
            else false
        end as has_ultimate_parent,

        case
            when dp.direct_parent_lei is not null
              or up.ultimate_parent_lei is not null
                then true
            else false
        end as has_any_parent,

        e.source_load_date,

        current_timestamp as intermediate_loaded_at

    from entities e

    left join direct_parent dp
        on e.lei = dp.lei

    left join entities dp_entity
        on dp.direct_parent_lei = dp_entity.lei

    left join ultimate_parent up
        on e.lei = up.lei

    left join entities up_entity
        on up.ultimate_parent_lei = up_entity.lei

)

select *
from final