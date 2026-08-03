{{ config(
    schema='staging',
    materialized='view',
    alias='stg_int_entity_candidates'
) }}

with filtered_gleif_source as (

    select
        raw_id,
        lei,
        legal_name,
        legal_name_normalized,
        entity_status,
        registration_status,
        legal_jurisdiction,
        legal_address_country,
        headquarters_address_country,
        next_renewal_date,
        last_update_date,
        source_load_date,
        source_object_key,
        row_hash,
        raw_row,
        staging_loaded_at
    from {{ ref('stg_gleif_lei_full') }}
    where lei is not null
      and trim(lei) <> ''
      and legal_name_normalized is not null
      and trim(legal_name_normalized) <> ''

),

final as (

    select
        -- Safe MD5 candidate surrogate key (lei is guaranteed non-null by CTE filter)
        md5(concat_ws('|', 'gleif', lei)) as entity_candidate_id,

        'gleif' as candidate_source,
        lei as candidate_source_id,

        lei,
        legal_name,
        legal_name_normalized,

        entity_status,
        registration_status,

        legal_jurisdiction,
        legal_address_country,
        headquarters_address_country,

        -- Robust country fallback chain (Address -> HQ -> Jurisdiction Code prefix)
        coalesce(
            legal_address_country,
            headquarters_address_country,
            nullif(split_part(legal_jurisdiction, '-', 1), '')
        ) as country,

        next_renewal_date,
        last_update_date,

        source_load_date,
        source_object_key,
        raw_id,
        row_hash,
        raw_row,

        staging_loaded_at as source_staging_loaded_at,
        current_timestamp as intermediate_loaded_at

    from filtered_gleif_source

)

select *
from final