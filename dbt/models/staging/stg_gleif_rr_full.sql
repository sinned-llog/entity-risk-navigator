{{ config(
    schema='staging',
    materialized='table',
    alias='stg_gleif_rr_full',
    post_hook=[
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_gleif_rr_relationship_key ON {{ this }} (relationship_key);",
      "CREATE INDEX IF NOT EXISTS idx_stg_gleif_rr_start_node ON {{ this }} (start_node_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_gleif_rr_end_node ON {{ this }} (end_node_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_gleif_rr_relationship_type ON {{ this }} (relationship_type);",
      "CREATE INDEX IF NOT EXISTS idx_stg_gleif_rr_parent_lookup ON {{ this }} (relationship_type, start_node_id, source_load_date DESC, relationship_period_start DESC, last_update_date DESC, relationship_key DESC);"
    ]
) }}

with source_data as (

    select *
    from {{ source('raw', 'gleif_rr_full') }}

    {% if var('gleif_staging_load_date', none) %}
        where source_load_date = '{{ var("gleif_staging_load_date") }}'::date
    {% endif %}

),

cleaned_data as (

    select
        raw_id,

        -- Graph nodes
        upper(trim(start_node_id)) as start_node_id,
        upper(nullif(trim(start_node_id_type), '')) as start_node_id_type,
        upper(trim(end_node_id)) as end_node_id,
        upper(nullif(trim(end_node_id_type), '')) as end_node_id_type,

        -- Relationship classification and status
        upper(nullif(trim(relationship_type), '')) as relationship_type,

        md5(
            concat_ws(
                '|',
                coalesce(upper(trim(start_node_id)), ''),
                coalesce(upper(trim(end_node_id)), ''),
                coalesce(upper(nullif(trim(relationship_type), '')), '')
            )
        ) as relationship_key,

        lower(coalesce(nullif(trim(relationship_status), ''), 'unknown')) as relationship_status,
        lower(nullif(trim(relationship_period_type), '')) as relationship_period_type,

        case
            when upper(trim(coalesce(registration_status, ''))) in ('', 'NULL', 'N/A') then 'unknown'
            else lower(trim(registration_status))
        end as registration_status,

        -- Governance and validation metadata
        upper(nullif(trim(managing_lou), '')) as managing_lou,
        lower(nullif(trim(validation_sources), '')) as validation_sources,
        lower(nullif(trim(validation_documents), '')) as validation_documents,

        -- Safe date conversions
        case
            when relationship_period_start_raw ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(relationship_period_start_raw from 1 for 10)::date
            else null
        end as relationship_period_start,

        case
            when relationship_period_end_raw ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(relationship_period_end_raw from 1 for 10)::date
            else null
        end as relationship_period_end,

        case
            when initial_registration_date_raw ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(initial_registration_date_raw from 1 for 10)::date
            else null
        end as initial_registration_date,

        case
            when last_update_date_raw ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(last_update_date_raw from 1 for 10)::date
            else null
        end as last_update_date,

        case
            when next_renewal_date_raw ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(next_renewal_date_raw from 1 for 10)::date
            else null
        end as next_renewal_date,

        -- Raw date strings for hashing and reconstructed JSON
        relationship_period_start_raw,
        relationship_period_end_raw,
        initial_registration_date_raw,
        last_update_date_raw,
        next_renewal_date_raw,

        -- Dates and file lineage
        source_load_date::date as source_load_date,
        source_object_key

    from source_data

    where start_node_id is not null
      and trim(start_node_id) <> ''
      and end_node_id is not null
      and trim(end_node_id) <> ''
      and relationship_type is not null
      and trim(relationship_type) <> ''

),

transformed as (

    select distinct on (relationship_key)
        raw_id,

        relationship_key,

        start_node_id,
        start_node_id_type,
        end_node_id,
        end_node_id_type,

        relationship_type,
        relationship_status,
        relationship_period_type,
        registration_status,

        managing_lou,
        validation_sources,
        validation_documents,

        relationship_period_start,
        relationship_period_end,
        initial_registration_date,
        last_update_date,
        next_renewal_date,

        source_load_date,
        source_object_key,

        -- Deterministic hash across relevant relationship attributes for change detection
        md5(
            concat_ws(
                '|',
                coalesce(start_node_id, ''),
                coalesce(start_node_id_type, ''),
                coalesce(end_node_id, ''),
                coalesce(end_node_id_type, ''),
                coalesce(relationship_type, ''),
                coalesce(relationship_status, ''),
                coalesce(relationship_period_type, ''),
                coalesce(registration_status, ''),
                coalesce(relationship_period_start_raw, ''),
                coalesce(relationship_period_end_raw, ''),
                coalesce(initial_registration_date_raw, ''),
                coalesce(last_update_date_raw, ''),
                coalesce(next_renewal_date_raw, ''),
                coalesce(managing_lou, ''),
                coalesce(validation_sources, ''),
                coalesce(validation_documents, '')
            )
        ) as row_hash,

        -- Reconstructed JSONB object of selected raw/staging attributes
        jsonb_build_object(
            'raw_id', raw_id,
            'start_node_id', start_node_id,
            'start_node_id_type', start_node_id_type,
            'end_node_id', end_node_id,
            'end_node_id_type', end_node_id_type,
            'relationship_type', relationship_type,
            'relationship_status', relationship_status,
            'relationship_period_type', relationship_period_type,
            'registration_status', registration_status,
            'managing_lou', managing_lou,
            'validation_sources', validation_sources,
            'validation_documents', validation_documents,
            'relationship_period_start_raw', relationship_period_start_raw,
            'relationship_period_end_raw', relationship_period_end_raw,
            'initial_registration_date_raw', initial_registration_date_raw,
            'last_update_date_raw', last_update_date_raw,
            'next_renewal_date_raw', next_renewal_date_raw,
            'source_object_key', source_object_key
        ) as raw_row,

        current_timestamp as staging_loaded_at

    from cleaned_data

    order by
        relationship_key,
        source_load_date desc,
        relationship_period_start desc nulls last,
        last_update_date desc nulls last,
        raw_id desc

)

select *
from transformed