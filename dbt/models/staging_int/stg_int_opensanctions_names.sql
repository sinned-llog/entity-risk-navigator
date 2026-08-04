{{ config(
    materialized='table',
    schema='staging',
    alias='stg_int_opensanctions_names',
    post_hook=[
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_int_opensanctions_names_id ON {{ this }} (opensanctions_name_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_opensanctions_names_target_id ON {{ this }} (opensanctions_target_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_opensanctions_names_os_id ON {{ this }} (opensanctions_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_opensanctions_names_name_norm ON {{ this }} (name_normalized);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_opensanctions_names_type ON {{ this }} (name_type);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_opensanctions_names_schema ON {{ this }} (schema_name);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_opensanctions_names_load_date ON {{ this }} (source_load_date);"
    ]
) }}

with targets as (

    select
        opensanctions_target_id,
        opensanctions_id,
        schema_name,

        caption,
        caption_normalized,

        aliases,

        source_load_date,
        source_object_key,
        metadata_object_key,
        raw_id,
        row_hash

    from {{ ref('stg_int_opensanctions_targets') }}

    where opensanctions_id is not null
      and caption_normalized is not null
      and trim(caption_normalized) <> ''

),

primary_names as (

    select
        opensanctions_target_id,
        opensanctions_id,
        schema_name,

        'primary_name' as name_type,
        'caption' as name_source_key,
        0 as name_position,

        caption as name,
        caption_normalized as name_normalized,

        source_load_date,
        source_object_key,
        metadata_object_key,
        raw_id,
        row_hash

    from targets

),

split_aliases as (

    select
        t.opensanctions_target_id,
        t.opensanctions_id,
        t.schema_name,

        'alias_name' as name_type,
        concat('alias_', a.alias_ordinal::text) as name_source_key,
        a.alias_ordinal::integer as name_position,

        nullif(
            trim(
                regexp_replace(
                    regexp_replace(
                        trim(a.alias_value),
                        '^"+|"+$',
                        '',
                        'g'
                    ),
                    '""+',
                    '"',
                    'g'
                )
            ),
            ''
        ) as name,

        nullif(
            regexp_replace(
                lower(
                    trim(
                        regexp_replace(
                            regexp_replace(
                                trim(a.alias_value),
                                '^"+|"+$',
                                '',
                                'g'
                            ),
                            '""+',
                            '"',
                            'g'
                        )
                    )
                ),
                '[[:space:]]+',
                ' ',
                'g'
            ),
            ''
        ) as name_normalized,

        t.caption_normalized,

        t.source_load_date,
        t.source_object_key,
        t.metadata_object_key,
        t.raw_id,
        t.row_hash

    from targets t

    cross join lateral regexp_split_to_table(t.aliases, '\s*;\s*')
        with ordinality as a(alias_value, alias_ordinal)

    where t.aliases is not null
      and trim(t.aliases) <> ''

),

alias_names as (

    select
        opensanctions_target_id,
        opensanctions_id,
        schema_name,
        name_type,
        name_source_key,
        name_position,
        name,
        name_normalized,
        source_load_date,
        source_object_key,
        metadata_object_key,
        raw_id,
        row_hash
    from split_aliases
    where name_normalized is not null
      and trim(name_normalized) <> ''
      /* prevent duplicates if an alias is identical to the primary name */
      and name_normalized <> caption_normalized

),

unfiltered_all_names as (

    select * from primary_names
    union all
    select * from alias_names

),

deduped_names as (

    select
        opensanctions_target_id,
        opensanctions_id,
        schema_name,
        name_type,
        name_source_key,
        name_position,
        name,
        name_normalized,
        source_load_date,
        source_object_key,
        metadata_object_key,
        raw_id,
        row_hash

    from (

        select
            u.*,
            row_number() over (
                partition by
                    opensanctions_target_id,
                    name_type,
                    name_normalized
                order by
                    name_position asc,
                    name_source_key asc
            ) as name_rank

        from unfiltered_all_names u

    ) ranked

    where name_rank = 1

),

final as (

    select
        md5(
            concat_ws(
                '|',
                opensanctions_target_id,
                name_type,
                name_source_key,
                name_normalized
            )
        ) as opensanctions_name_id,

        opensanctions_target_id,
        opensanctions_id,
        schema_name,

        name_type,
        name_source_key,
        name_position,

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
        row_hash,

        current_timestamp as intermediate_loaded_at

    from deduped_names

)

select *
from final