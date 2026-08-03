{{ config(
    materialized='table',
    schema='staging',
    alias='stg_int_eu_fsf_subjects',
    post_hook=[
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_int_eu_fsf_subjects_id ON {{ this }} (eu_fsf_subject_id);",
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_int_eu_fsf_subjects_key ON {{ this }} (eu_fsf_subject_key);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_eu_fsf_subjects_reference ON {{ this }} (entity_eu_reference_number);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_eu_fsf_subjects_entity_type ON {{ this }} (entity_type);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_eu_fsf_subjects_load_date ON {{ this }} (source_load_date);"
    ]
) }}

with cleaned_source as (

    select
        raw_id,

        entity_logical_id,
        entity_eu_reference_number,

        coalesce(
            nullif(entity_eu_reference_number, ''),
            entity_logical_id::text
        ) as eu_fsf_subject_key,

        entity_united_nations_id,
        entity_designation_details,
        entity_type,
        entity_remark,
        entity_subject_type,

        -- Direkt als natives DATE übernehmen
        entity_publication_date,

        programme,
        regulation_type,
        regulation_number_title,
        regulation_publication_date,
        regulation_url,

        name_alias_logical_id,
        address_logical_id,
        birthdate_logical_id,
        identification_logical_id,
        citizenship_logical_id,

        source_load_date,
        source_object_key,
        metadata_object_key,
        row_hash

    from {{ ref('stg_eu_fsf_full') }}

    where entity_logical_id is not null

),

canonical_subject as (

    select
        raw_id,
        entity_logical_id,
        entity_eu_reference_number,
        eu_fsf_subject_key,
        entity_united_nations_id,
        entity_designation_details,
        entity_type,
        entity_remark,
        entity_subject_type,
        entity_publication_date,
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
            cs.*,
            row_number() over (
                partition by eu_fsf_subject_key
                order by
                    source_load_date desc,
                    regulation_publication_date desc nulls last,
                    raw_id desc
            ) as subject_rank

        from cleaned_source cs

    ) ranked

    where subject_rank = 1

),

subject_rollup as (

    select
        eu_fsf_subject_key,

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

        count(*) as source_row_count,

        count(distinct name_alias_logical_id)
            filter (where name_alias_logical_id is not null) as name_alias_count,

        count(distinct address_logical_id)
            filter (where address_logical_id is not null) as address_count,

        count(distinct birthdate_logical_id)
            filter (where birthdate_logical_id is not null) as birthdate_count,

        count(distinct identification_logical_id)
            filter (where identification_logical_id is not null) as identification_count,

        count(distinct citizenship_logical_id)
            filter (where citizenship_logical_id is not null) as citizenship_count

    from cleaned_source

    group by
        eu_fsf_subject_key

),

final as (

    select
        md5(concat_ws('|', 'eu_fsf', c.eu_fsf_subject_key)) as eu_fsf_subject_id,

        c.eu_fsf_subject_key,

        c.entity_logical_id,
        c.entity_eu_reference_number,
        c.entity_united_nations_id,

        c.entity_type,
        c.entity_subject_type,

        c.entity_designation_details,
        c.entity_remark,

        c.entity_publication_date,

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
        r.name_alias_count,
        r.address_count,
        r.birthdate_count,
        r.identification_count,
        r.citizenship_count,

        true as is_sanctioned,

        c.source_load_date,
        c.source_object_key,
        c.metadata_object_key,
        c.raw_id,
        c.row_hash,

        current_timestamp as intermediate_loaded_at

    from canonical_subject c

    left join subject_rollup r
        on c.eu_fsf_subject_key = r.eu_fsf_subject_key

)

select *
from final