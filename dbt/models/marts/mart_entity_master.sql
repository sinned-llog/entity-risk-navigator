{{ config(
    materialized='table',
    schema='marts',
    alias='mart_entity_master',
    post_hook=[
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_mart_entity_master_entity_id ON {{ this }} (entity_candidate_id);",
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_mart_entity_master_lei ON {{ this }} (lei);",
      "CREATE INDEX IF NOT EXISTS idx_mart_entity_master_legal_name_norm ON {{ this }} (legal_name_normalized);",
      "CREATE INDEX IF NOT EXISTS idx_mart_entity_master_country ON {{ this }} (country);",
      "CREATE INDEX IF NOT EXISTS idx_mart_entity_master_entity_status ON {{ this }} (entity_status);",
      "CREATE INDEX IF NOT EXISTS idx_mart_entity_master_parent ON {{ this }} (direct_parent_lei, ultimate_parent_lei);",
      "CREATE INDEX IF NOT EXISTS idx_mart_entity_master_load_date ON {{ this }} (source_load_date);"
    ]
) }}

with entity_candidates as (

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
        legal_address_country,
        headquarters_address_country,
        country,

        next_renewal_date,
        last_update_date,

        source_load_date

    from {{ ref('stg_int_entity_candidates') }}

    where entity_candidate_id is not null
      and lei is not null
      and trim(lei) <> ''

),

parent_summary as (

    select
        lei,

        direct_parent_lei,
        direct_parent_name,
        direct_parent_name_normalized,

        ultimate_parent_lei,
        ultimate_parent_name,
        ultimate_parent_name_normalized,

        has_direct_parent,
        has_ultimate_parent,
        has_any_parent

    from {{ ref('stg_int_gleif_parent_summary') }}

    where lei is not null
      and trim(lei) <> ''

),

final as (

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

        p.direct_parent_lei,
        p.direct_parent_name,
        p.direct_parent_name_normalized,

        p.ultimate_parent_lei,
        p.ultimate_parent_name,
        p.ultimate_parent_name_normalized,

        coalesce(p.has_direct_parent, false) as has_direct_parent,
        coalesce(p.has_ultimate_parent, false) as has_ultimate_parent,
        coalesce(p.has_any_parent, false) as has_any_parent,

        e.next_renewal_date,
        e.last_update_date,

        e.source_load_date,

        current_timestamp as mart_loaded_at

    from entity_candidates e

    left join parent_summary p
        on e.lei = p.lei

)

select *
from final