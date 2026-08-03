{{ config(
    materialized='table',
    schema='staging',
    alias='stg_int_eu_fsf_names',
    post_hook=[
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_int_eu_fsf_names_id ON {{ this }} (eu_fsf_name_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_eu_fsf_names_subject_id ON {{ this }} (eu_fsf_subject_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_eu_fsf_names_subject_key ON {{ this }} (eu_fsf_subject_key);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_eu_fsf_names_name_norm ON {{ this }} (name_normalized);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_eu_fsf_names_entity_type ON {{ this }} (entity_type);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_eu_fsf_names_load_date ON {{ this }} (source_load_date);"
    ]
) }}

with name_rows as (

    select
        raw_id,

        entity_logical_id,
        entity_eu_reference_number,

        coalesce(
            nullif(entity_eu_reference_number, ''),
            entity_logical_id::text
        ) as eu_fsf_subject_key,

        entity_type,
        entity_subject_type,

        name_alias_logical_id,

        -- Streamlined fallback key creation
        coalesce(
            name_alias_logical_id::text,
            md5(
                concat_ws(
                    '|',
                    entity_eu_reference_number,
                    entity_logical_id::text,
                    name_alias_whole_name_normalized
                )
            )
        ) as eu_fsf_name_source_key,

        name_alias_first_name,
        name_alias_middle_name,
        name_alias_last_name,
        name_alias_whole_name,
        name_alias_whole_name_normalized,
        name_alias_name_language,
        name_alias_gender,
        name_alias_title,
        name_alias_function,

        programme,
        regulation_type,
        regulation_number_title,
        regulation_publication_date,
        regulation_url,

        source_load_date,
        source_object_key,
        metadata_object_key,
        row_hash

    from {{ ref('stg_eu_fsf_full') }}

    where entity_logical_id is not null
      and name_alias_whole_name_normalized is not null
      and trim(name_alias_whole_name_normalized) <> ''

),

canonical_names as (

    select
        raw_id,

        entity_logical_id,
        entity_eu_reference_number,
        eu_fsf_subject_key,

        entity_type,
        entity_subject_type,

        name_alias_logical_id,
        eu_fsf_name_source_key,

        name_alias_first_name,
        name_alias_middle_name,
        name_alias_last_name,
        name_alias_whole_name,
        name_alias_whole_name_normalized,
        name_alias_name_language,
        name_alias_gender,
        name_alias_title,
        name_alias_function,

        programme,
        regulation_type,
        regulation_number_title,
        regulation_publication_date,
        regulation_url,

        source_load_date,
        source_object_key,
        metadata_object_key,
        row_hash

    from (

        select
            nr.*,

            row_number() over (
                partition by
                    eu_fsf_subject_key,
                    eu_fsf_name_source_key
                order by
                    source_load_date desc,
                    regulation_publication_date desc nulls last,
                    raw_id desc
            ) as name_rank

        from name_rows nr

    ) ranked

    where name_rank = 1

),

name_rollup as (

    select
        eu_fsf_subject_key,
        eu_fsf_name_source_key,

        string_agg(distinct programme, ';' order by programme)
            filter (
                where programme is not null
                  and trim(programme) <> ''
            ) as programmes,

        string_agg(distinct regulation_type, ';' order by regulation_type)
            filter (
                where regulation_type is not null
                  and trim(regulation_type) <> ''
            ) as regulation_types,

        string_agg(distinct regulation_number_title, ';' order by regulation_number_title)
            filter (
                where regulation_number_title is not null
                  and trim(regulation_number_title) <> ''
            ) as regulation_number_titles,

        min(regulation_publication_date) as first_regulation_publication_date,
        max(regulation_publication_date) as latest_regulation_publication_date,

        count(*) as source_row_count

    from name_rows

    group by
        eu_fsf_subject_key,
        eu_fsf_name_source_key

),

final as (

    select
        -- Guaranteed non-null elements don't need coalesce
        md5(
            concat_ws(
                '|',
                'eu_fsf',
                c.eu_fsf_subject_key,
                c.eu_fsf_name_source_key
            )
        ) as eu_fsf_name_id,

        s.eu_fsf_subject_id,
        c.eu_fsf_subject_key,

        c.entity_logical_id,
        c.entity_eu_reference_number,

        c.entity_type,
        c.entity_subject_type,

        c.name_alias_logical_id,
        c.eu_fsf_name_source_key,

        'name_alias' as name_type,

        c.name_alias_whole_name as name,
        c.name_alias_whole_name_normalized as name_normalized,

        c.name_alias_first_name,
        c.name_alias_middle_name,
        c.name_alias_last_name,

        c.name_alias_name_language as name_language,
        c.name_alias_gender as gender,
        c.name_alias_title as title,
        c.name_alias_function as function,

        r.programmes,
        r.regulation_types,
        r.regulation_number_titles,

        r.first_regulation_publication_date,
        r.latest_regulation_publication_date,

        c.regulation_type as selected_regulation_type,
        c.regulation_number_title as selected_regulation_number_title,
        c.regulation_publication_date as selected_regulation_publication_date,
        c.regulation_url as selected_regulation_url,

        r.source_row_count,

        c.source_load_date,
        c.source_object_key,
        c.metadata_object_key,
        c.raw_id,
        c.row_hash,

        current_timestamp as intermediate_loaded_at

    from canonical_names c

    left join name_rollup r
        on c.eu_fsf_subject_key = r.eu_fsf_subject_key
       and c.eu_fsf_name_source_key = r.eu_fsf_name_source_key

    left join {{ ref('stg_int_eu_fsf_subjects') }} s
        on c.eu_fsf_subject_key = s.eu_fsf_subject_key

)

select *
from final