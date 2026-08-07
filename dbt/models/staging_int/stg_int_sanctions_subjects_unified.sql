{{ config(
    materialized='table',
    schema='staging',
    alias='stg_int_sanctions_subjects_unified',
    post_hook=[
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_int_sanction_subjects_unified_id ON {{ this }} (sanction_subject_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_sanction_subjects_unified_source ON {{ this }} (sanctions_source);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_sanction_subjects_unified_entity_key ON {{ this }} (source_entity_key);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_sanction_subjects_unified_subject_id ON {{ this }} (source_subject_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_sanction_subjects_unified_entity_type ON {{ this }} (entity_type);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_sanction_subjects_unified_load_date ON {{ this }} (source_load_date);"
    ]
) }}

with eu_fsf_subjects as (

    select
        'eu_fsf' as sanctions_source,

        eu_fsf_subject_id as source_subject_id,
        eu_fsf_subject_key as source_entity_key,

        entity_logical_id::text as source_logical_id,
        entity_eu_reference_number as source_reference_number,

        entity_type,
        entity_subject_type,

        null::text as primary_name,
        null::text as primary_name_normalized,

        true as is_sanctioned,

        regulation_number_titles as source_sanction_context,
        programmes as source_programs,
        regulation_types as source_lists,
        null::text as source_datasets,

        selected_regulation_number_title as source_reference_numbers,
        selected_regulation_publication_date as source_reference_date,
        selected_regulation_url as source_reference_url,

        null::text as source_countries,
        entity_eu_reference_number as source_identifiers,

        regulation_number_titles as source_context_raw,

        null::text as source_first_seen_raw,
        null::text as source_last_seen_raw,
        null::text as source_last_change_raw,

        source_load_date,
        source_object_key,
        metadata_object_key,
        raw_id,
        row_hash

    from {{ ref('stg_int_eu_fsf_subjects') }}

    where eu_fsf_subject_id is not null
      and eu_fsf_subject_key is not null

),

opensanctions_targets as (

    select
        'opensanctions' as sanctions_source,

        opensanctions_target_id as source_subject_id,
        opensanctions_id as source_entity_key,

        opensanctions_id as source_logical_id,
        opensanctions_id as source_reference_number,

        schema_name as entity_type,
        null::text as entity_subject_type,

        caption as primary_name,
        caption_normalized as primary_name_normalized,

        is_sanctioned,

       case
            when sanctions is null then null
            when trim(sanctions) = '' then null
            when regexp_replace(trim(sanctions), '"', '', 'g') = '' then null
            else sanctions
        end as source_sanction_context,
        program_ids as source_programs,
        datasets as source_lists,
        datasets as source_datasets,

        opensanctions_id as source_reference_numbers,
        null::date as source_reference_date,
        null::text as source_reference_url,

        countries as source_countries,
        identifiers as source_identifiers,

        sanctions as source_context_raw,

        first_seen_raw as source_first_seen_raw,
        last_seen_raw as source_last_seen_raw,
        last_change_raw as source_last_change_raw,

        source_load_date,
        source_object_key,
        metadata_object_key,
        raw_id,
        row_hash

    from {{ ref('stg_int_opensanctions_targets') }}

    where opensanctions_target_id is not null
      and opensanctions_id is not null

),

all_subjects as (

    select * from eu_fsf_subjects

    union all

    select * from opensanctions_targets

),

final as (

    select
        md5(
            concat_ws(
                '|',
                sanctions_source,
                source_subject_id
            )
        ) as sanction_subject_id,

        sanctions_source,

        source_subject_id,
        source_entity_key,
        source_logical_id,
        source_reference_number,

        entity_type,
        entity_subject_type,

        primary_name,
        primary_name_normalized,

        is_sanctioned,

        source_sanction_context,
        source_programs,
        source_lists,
        source_datasets,

        source_reference_numbers,
        source_reference_date,
        source_reference_url,

        source_countries,
        source_identifiers,
        source_context_raw,

        source_first_seen_raw,
        source_last_seen_raw,
        source_last_change_raw,

        source_load_date,
        source_object_key,
        metadata_object_key,
        raw_id,
        row_hash,

        current_timestamp as intermediate_loaded_at

    from all_subjects

)

select *
from final