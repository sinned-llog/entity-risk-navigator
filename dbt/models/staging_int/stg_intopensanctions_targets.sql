{{ config(
    materialized='table',
    schema='staging',
    alias='stg_int_opensanctions_targets',
    post_hook=[
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_int_opensanctions_targets_id ON {{ this }} (opensanctions_target_id);",
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_int_opensanctions_targets_os_id ON {{ this }} (opensanctions_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_opensanctions_targets_name_norm ON {{ this }} (caption_normalized);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_opensanctions_targets_schema ON {{ this }} (schema_name);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_opensanctions_targets_load_date ON {{ this }} (source_load_date);"
    ]
) }}

with cleaned_source as (

    select
        raw_id,

        nullif(trim(opensanctions_id), '') as opensanctions_id,
        lower(nullif(trim(schema_name), '')) as schema_name,
        nullif(trim(caption), '') as caption,

        nullif(
            regexp_replace(
                lower(trim(coalesce(caption, ''))),
                '[[:space:]]+',
                ' ',
                'g'
            ),
            ''
        ) as caption_normalized,

        nullif(trim(aliases), '') as aliases,

        nullif(
            regexp_replace(
                lower(trim(coalesce(aliases, ''))),
                '[[:space:]]+',
                ' ',
                'g'
            ),
            ''
        ) as aliases_normalized,

        nullif(trim(birth_date), '') as birth_date_raw,
        nullif(upper(trim(countries)), '') as countries,
        nullif(trim(addresses), '') as addresses,
        nullif(trim(identifiers), '') as identifiers,
        nullif(trim(sanctions), '') as sanctions,
        nullif(trim(phones), '') as phones,
        nullif(trim(emails), '') as emails,
        nullif(trim(program_ids), '') as program_ids,
        nullif(trim(datasets), '') as datasets,

        nullif(trim(first_seen_raw), '') as first_seen_raw,
        nullif(trim(last_seen_raw), '') as last_seen_raw,
        nullif(trim(last_change_raw), '') as last_change_raw,

        source_load_date::date as source_load_date,
        source_object_key,
        metadata_object_key,
        row_hash

    from {{ ref('stg_opensanctions') }}

),

base as (

    select
        *,

        -- Sicheres Parseausdrücke für Standard ISO-Timestamps (z.B. YYYY-MM-DDTHH:MI:SS)
        case 
            when first_seen_raw ~ '^\d{4}-\d{2}-\d{2}' then first_seen_raw::timestamp
            else null
        end as first_seen_at,

        case 
            when last_seen_raw ~ '^\d{4}-\d{2}-\d{2}' then last_seen_raw::timestamp
            else null
        end as last_seen_at,

        case 
            when last_change_raw ~ '^\d{4}-\d{2}-\d{2}' then last_change_raw::timestamp
            else null
        end as last_change_at

    from cleaned_source

    where opensanctions_id is not null
      and caption is not null

),

canonical_targets as (

    select
        raw_id,
        opensanctions_id,
        schema_name,
        caption,
        caption_normalized,
        aliases,
        aliases_normalized,
        birth_date_raw,
        countries,
        addresses,
        identifiers,
        sanctions,
        phones,
        emails,
        program_ids,
        datasets,
        first_seen_raw,
        last_seen_raw,
        last_change_raw,
        first_seen_at,
        last_seen_at,
        last_change_at,
        source_load_date,
        source_object_key,
        metadata_object_key,
        row_hash

    from (

        select
            b.*,

            row_number() over (
                partition by opensanctions_id
                order by
                    source_load_date desc,
                    last_change_at desc nulls last,
                    raw_id desc
            ) as target_rank

        from base b

    ) ranked

    where target_rank = 1

),

final as (

    select
        md5(
            concat_ws(
                '|',
                'opensanctions',
                opensanctions_id
            )
        ) as opensanctions_target_id,

        opensanctions_id,
        schema_name,

        caption,
        caption_normalized,

        aliases,
        aliases_normalized,

        birth_date_raw,

        countries,
        addresses,
        identifiers,
        sanctions,
        phones,
        emails,
        program_ids,
        datasets,

        first_seen_raw,
        last_seen_raw,
        last_change_raw,

        first_seen_at,
        last_seen_at,
        last_change_at,

        aliases is not null as has_aliases,
        countries is not null as has_countries,
        identifiers is not null as has_identifiers,
        sanctions is not null as has_sanctions,

        true as is_sanctioned,

        source_load_date,
        source_object_key,
        metadata_object_key,
        raw_id,
        row_hash,

        current_timestamp as intermediate_loaded_at

    from canonical_targets

)

select *
from final