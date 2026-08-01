{{ config(
    materialized='table',
    schema='staging',
    alias='stg_opensanctions',
    indexes=[
      {'columns': ['opensanctions_id']},
      {'columns': ['schema_name']},
      {'columns': ['row_hash']},
      {'columns': ['source_load_date']}
    ]
) }}

with source_data as (

    select * from {{ source('raw', 'opensanctions_targets') }}

),

cleaned_and_transformed as (

    select
        -- Primary key & pipeline metadata
        raw_id,
        app_env,
        source,
        source_name,
        dataset_group,
        snapshot_type,
        file_row_number,

        -- Core identifiers & schema (schema in lowercase)
        nullif(trim(id), '')                                            as opensanctions_id,
        lower(nullif(trim(schema), ''))                                 as schema_name,
        
        -- Primary display name & normalized search variant
        nullif(trim(caption), '')                                       as caption,
        nullif(
        regexp_replace(
            lower(trim(coalesce(caption, ''))),
            '\s+',
            ' ',
            'g'
        ),
        ''
    ) as caption_normalized,
        
        -- Text attributes & aliases (original & lowercase match variant)
        nullif(trim(aliases), '')                                       as aliases,
        nullif(
        regexp_replace(
            lower(trim(coalesce(aliases, ''))),
            '\s+',
            ' ',
            'g'
        ),
        ''
    ) as aliases_normalized,
        nullif(trim(birth_date), '')                                    as birth_date,
        
        -- Codes & identifiers (country codes in UPPERCASE)
        upper(nullif(trim(countries), ''))                              as countries,
        nullif(trim(addresses), '')                                     as addresses,
        nullif(trim(identifiers), '')                                   as identifiers,
        nullif(
        regexp_replace(
            upper(trim(coalesce(identifiers, ''))),
            '\s+',
            ' ',
            'g'
        ),
        ''
    ) as identifiers_normalized,
        
        -- Sanctions, programs & contact info
        nullif(trim(sanctions), '')                                     as sanctions,
        nullif(trim(phones), '')                                        as phones,
        lower(nullif(trim(emails), ''))                                 as emails,
        upper(nullif(trim(program_ids), ''))                            as program_ids,
        lower(nullif(trim(datasets), ''))                               as datasets,

        -- Date attributes (kept as raw strings for staging)
        nullif(trim(first_seen), '')                                    as first_seen_raw,
        nullif(trim(last_seen), '')                                     as last_seen_raw,
        nullif(trim(last_change), '')                                   as last_change_raw,

        -- 1. Deterministic hash calculation (MD5) across all business raw columns
        md5(
            coalesce(trim(id), '') || '|' ||
            coalesce(trim(schema), '') || '|' ||
            coalesce(trim(caption), '') || '|' ||
            coalesce(trim(aliases), '') || '|' ||
            coalesce(trim(birth_date), '') || '|' ||
            coalesce(trim(countries), '') || '|' ||
            coalesce(trim(addresses), '') || '|' ||
            coalesce(trim(identifiers), '') || '|' ||
            coalesce(trim(sanctions), '') || '|' ||
            coalesce(trim(phones), '') || '|' ||
            coalesce(trim(emails), '') || '|' ||
            coalesce(trim(program_ids), '') || '|' ||
            coalesce(trim(datasets), '') || '|' ||
            coalesce(trim(first_seen), '') || '|' ||
            coalesce(trim(last_seen), '') || '|' ||
            coalesce(trim(last_change), '')
        ) as row_hash,

        -- 2. Reconstruction of the full raw JSONB object
        jsonb_build_object(
            'id', id,
            'schema', schema,
            'caption', caption,
            'aliases', aliases,
            'birth_date', birth_date,
            'countries', countries,
            'addresses', addresses,
            'identifiers', identifiers,
            'sanctions', sanctions,
            'phones', phones,
            'emails', emails,
            'program_ids', program_ids,
            'datasets', datasets,
            'first_seen', first_seen,
            'last_seen', last_seen,
            'last_change', last_change
        ) as raw_row,

        -- Lineage & file metadata
        nullif(trim(source_url), '')                                    as source_url,
        source_object_key,
        metadata_object_key,
        source_load_date,
        loaded_at                                                       as raw_loaded_at,
        current_timestamp                                               as staging_loaded_at

    from source_data

)

select * from cleaned_and_transformed