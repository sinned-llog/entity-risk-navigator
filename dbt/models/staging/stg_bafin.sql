{{ config(
    materialized='table',
    schema='staging',
    alias='stg_bafin_pages_full',
    post_hook=[
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_bafin_full_id ON {{ this }} (stg_bafin_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_bafin_full_institut_id ON {{ this }} (bafin_institut_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_bafin_full_candidate_id ON {{ this }} (candidate_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_bafin_full_candidate_legal_name ON {{ this }} (candidate_legal_name);",
      "CREATE INDEX IF NOT EXISTS idx_stg_bafin_full_search_name ON {{ this }} (search_name);",
      "CREATE INDEX IF NOT EXISTS idx_stg_bafin_full_row_hash ON {{ this }} (row_hash);",
      "CREATE INDEX IF NOT EXISTS idx_stg_bafin_full_source_load_date ON {{ this }} (source_load_date);"
    ]
) }}

with source_data as (

    select
        raw_id,
        app_env,
        source,
        source_name,
        dataset_group,
        snapshot_type,
        file_number,
        candidate_id,
        lei,
        bafin_institut_id,
        legal_name,
        search_name,
        jurisdiction,
        country,
        source_reason,
        priority,
        http_status,
        downloaded_bytes,
        content_hash,
        raw_content,
        raw_content_type,
        metadata_json,
        source_url,
        source_object_key,
        metadata_object_key,
        source_load_date,
        loaded_at
    from {{ source('raw', 'bafin_pages') }}
    where source_load_date = coalesce(
        nullif('{{ var("bafin_staging_load_date", "") }}', '')::date,
        (
            select max(source_load_date)
            from {{ source('raw', 'bafin_pages') }}
        )
    )

),

cleaned_data as (

    select
        raw_id,

        trim(app_env) as app_env,
        trim(source) as source,
        trim(source_name) as source_name,
        trim(dataset_group) as dataset_group,
        trim(snapshot_type) as snapshot_type,
        file_number,

        nullif(trim(candidate_id), '') as candidate_id,
        nullif(upper(trim(lei)), '') as lei,
        nullif(trim(bafin_institut_id), '') as bafin_institut_id,

        nullif(trim(legal_name), '') as candidate_legal_name,
        nullif(trim(search_name), '') as search_name,

        nullif(upper(trim(jurisdiction)), '') as jurisdiction,
        nullif(upper(trim(country)), '') as country,

        nullif(trim(source_reason), '') as source_reason,
        nullif(trim(priority), '') as priority,

        http_status,
        downloaded_bytes,
        nullif(lower(trim(content_hash)), '') as content_hash,

        raw_content,
        nullif(trim(raw_content_type), '') as raw_content_type,
        metadata_json,

        nullif(trim(source_url), '') as source_url,
        nullif(trim(source_object_key), '') as source_object_key,
        nullif(trim(metadata_object_key), '') as metadata_object_key,

        source_load_date,
        loaded_at

    from source_data

),

ranked as (

    select
        *,
        row_number() over (
            partition by
                source_load_date,
                coalesce(candidate_id, ''),
                coalesce(bafin_institut_id, ''),
                coalesce(source_object_key, '')
            order by raw_id desc
        ) as dedupe_rank
    from cleaned_data

),

final as (

    select
        md5(
            coalesce(cast(source_load_date as text), '') || '|' ||
            coalesce(candidate_id, '') || '|' ||
            coalesce(bafin_institut_id, '') || '|' ||
            coalesce(source_object_key, '')
        ) as stg_bafin_id,

        raw_id,
        app_env,
        source,
        source_name,
        dataset_group,
        snapshot_type,
        file_number,

        candidate_id,
        lei,
        bafin_institut_id,

        candidate_legal_name,

        -- Optional backward compatibility.
        candidate_legal_name as legal_name,

        search_name,

        jurisdiction,
        country,

        source_reason,
        priority,

        http_status,
        downloaded_bytes,
        content_hash,

        raw_content,
        raw_content_type,
        metadata_json,

        source_url,
        source_object_key,
        metadata_object_key,

        source_load_date,
        loaded_at,

        md5(
            coalesce(candidate_legal_name, '') || '|' ||
            coalesce(search_name, '') || '|' ||
            coalesce(bafin_institut_id, '') || '|' ||
            coalesce(lei, '') || '|' ||
            coalesce(country, '') || '|' ||
            coalesce(content_hash, '')
        ) as row_hash

    from ranked
    where dedupe_rank = 1

)

select *
from final