{{ config(
    materialized='table',
    schema='staging',
    alias='stg_int_sanctions_names_unified',
    post_hook=[
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_int_sanctions_names_unified_id ON {{ this }} (sanctions_name_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_sanctions_names_unified_subject_id ON {{ this }} (sanction_subject_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_sanctions_names_unified_source ON {{ this }} (sanctions_source);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_sanctions_names_unified_source_subject ON {{ this }} (source_subject_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_sanctions_names_unified_source_entity ON {{ this }} (source_entity_key);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_sanctions_names_unified_source_name ON {{ this }} (source_name_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_sanctions_names_unified_name_norm ON {{ this }} (name_normalized);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_sanctions_names_unified_match_key ON {{ this }} (name_match_key);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_sanctions_names_unified_entity_type ON {{ this }} (entity_type);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_sanctions_names_unified_load_date ON {{ this }} (source_load_date);"
    ]
) }}

with eu_fsf_names as (

    select
        'eu_fsf' as sanctions_source,

        eu_fsf_name_id as source_name_id,
        eu_fsf_subject_id as source_subject_id,
        eu_fsf_subject_key as source_entity_key,

        entity_type,
        entity_subject_type,

        name_type as source_name_type,

        false as is_primary_name,

        name,
        name_normalized,

        case
            when name ~ '[^ -~]' then true
            else false
        end as has_non_ascii,

        source_load_date,
        source_object_key,
        metadata_object_key,
        raw_id,
        row_hash

    from {{ ref('stg_int_eu_fsf_names') }}

    where coalesce(trim(name), '') <> ''
      and coalesce(trim(name_normalized), '') <> ''

),

opensanctions_names as (

    select
        'opensanctions' as sanctions_source,

        opensanctions_name_id as source_name_id,
        opensanctions_target_id as source_subject_id,
        opensanctions_id as source_entity_key,

        schema_name as entity_type,
        null::text as entity_subject_type,

        name_type as source_name_type,

        case
            when name_type = 'primary_name' then true
            else false
        end as is_primary_name,

        name,
        name_normalized,

        has_non_ascii,

        source_load_date,
        source_object_key,
        metadata_object_key,
        raw_id,
        row_hash

    from {{ ref('stg_int_opensanctions_names') }}

    where coalesce(trim(name), '') <> ''
      and coalesce(trim(name_normalized), '') <> ''

),

all_names as (

    select
        sanctions_source,
        source_name_id,
        source_subject_id,
        source_entity_key,
        entity_type,
        entity_subject_type,
        source_name_type,
        is_primary_name,
        name,
        name_normalized,
        has_non_ascii,
        source_load_date,
        source_object_key,
        metadata_object_key,
        raw_id,
        row_hash
    from eu_fsf_names

    union all

    select
        sanctions_source,
        source_name_id,
        source_subject_id,
        source_entity_key,
        entity_type,
        entity_subject_type,
        source_name_type,
        is_primary_name,
        name,
        name_normalized,
        has_non_ascii,
        source_load_date,
        source_object_key,
        metadata_object_key,
        raw_id,
        row_hash
    from opensanctions_names

),

cleaned_names as (

    select
        *,

        nullif(
            trim(
                regexp_replace(
                    regexp_replace(
                        lower(trim(coalesce(name_normalized, ''))),
                        '[[:punct:]]+',
                        ' ',
                        'g'
                    ),
                    '[[:space:]]+',
                    ' ',
                    'g'
                )
            ),
            ''
        ) as name_match_key

    from all_names

),

final as (

    select
        md5(
            concat_ws(
                '|',
                cn.sanctions_source,
                cn.source_name_id
            )
        ) as sanctions_name_id,

        su.sanction_subject_id,

        cn.sanctions_source,

        cn.source_name_id,
        cn.source_subject_id,
        cn.source_entity_key,

        cn.entity_type,
        cn.entity_subject_type,

        cn.source_name_type,
        cn.is_primary_name,

        cn.name,
        cn.name_normalized,
        cn.name_match_key,

        cn.has_non_ascii,

        cn.source_load_date,
        cn.source_object_key,
        cn.metadata_object_key,
        cn.raw_id,
        cn.row_hash,

        current_timestamp as intermediate_loaded_at

    from cleaned_names cn

    left join {{ ref('stg_int_sanctions_subjects_unified') }} su
        on cn.sanctions_source = su.sanctions_source
       and cn.source_subject_id = su.source_subject_id

    where cn.name_match_key is not null
      and trim(cn.name_match_key) <> ''

)

select *
from final
