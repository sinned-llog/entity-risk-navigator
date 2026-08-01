{{ config(
    schema='staging',
    materialized='table',
    alias='stg_gleif_rr_full',
    indexes=[
      {'columns': ['relationship_key'], 'unique': True},
      {'columns': ['start_node_id']},
      {'columns': ['end_node_id']},
      {'columns': ['relationship_type']},
      {'columns': ['source_load_date']}
    ]
) }}

with source_data as (

    select *
    from {{ source('raw', 'gleif_rr_full') }}
    {% if var('gleif_staging_load_date', none) %}
        where source_load_date = '{{ var("gleif_staging_load_date") }}'::date
    {% endif %}

),

transformed as (

    select distinct on (
        md5(
            coalesce(upper(trim(start_node_id)), '') || '|' ||
            coalesce(upper(trim(end_node_id)), '') || '|' ||
            coalesce(upper(trim(relationship_type)), '')
        )
    )
        -- Primary & Source Identifiers
        raw_id,
        
        -- Unique surrogate key for the relationship (Start LEI + End LEI + Relationship Type)
        md5(
            coalesce(upper(trim(start_node_id)), '') || '|' ||
            coalesce(upper(trim(end_node_id)), '') || '|' ||
            coalesce(upper(trim(relationship_type)), '')
        )                                                               as relationship_key,
        
        -- Graph Nodes (LEIs normalized to UPPERCASE for reliable joining)
        upper(trim(start_node_id))                                      as start_node_id,
        upper(nullif(trim(start_node_id_type), ''))                     as start_node_id_type,
        upper(trim(end_node_id))                                        as end_node_id,
        upper(nullif(trim(end_node_id_type), ''))                       as end_node_id_type,
        
        -- Relationship Classification & Status
        upper(nullif(trim(relationship_type), ''))                      as relationship_type,
        lower(coalesce(nullif(trim(relationship_status), ''), 'unknown')) as relationship_status,
        lower(nullif(trim(relationship_period_type), ''))               as relationship_period_type,
        lower(nullif(trim(registration_status), ''))                    as registration_status,
        
        -- Governance & Validation Metadata
        upper(nullif(trim(managing_lou), ''))                           as managing_lou,
        lower(nullif(trim(validation_sources), ''))                     as validation_sources,
        lower(nullif(trim(validation_documents), ''))                   as validation_documents,

        -- Safe date conversions
        case 
            when relationship_period_start_raw ~ '^\d{4}-\d{2}-\d{2}' 
                then substring(relationship_period_start_raw from 1 for 10)::date 
            else null 
        end                                                             as relationship_period_start,
        
        case 
            when relationship_period_end_raw ~ '^\d{4}-\d{2}-\d{2}' 
                then substring(relationship_period_end_raw from 1 for 10)::date 
            else null 
        end                                                             as relationship_period_end,

        case 
            when initial_registration_date_raw ~ '^\d{4}-\d{2}-\d{2}' 
                then substring(initial_registration_date_raw from 1 for 10)::date 
            else null 
        end                                                             as initial_registration_date,

        case 
            when last_update_date_raw ~ '^\d{4}-\d{2}-\d{2}' 
                then substring(last_update_date_raw from 1 for 10)::date 
            else null 
        end                                                             as last_update_date,

        case 
            when next_renewal_date_raw ~ '^\d{4}-\d{2}-\d{2}' 
                then substring(next_renewal_date_raw from 1 for 10)::date 
            else null 
        end                                                             as next_renewal_date,

        -- Dates & File Lineage
        source_load_date::date                                          as source_load_date,
        source_object_key,
        
        -- Deterministic hash across row content for change detection
        md5(
            coalesce(upper(trim(start_node_id)), '') || '|' ||
            coalesce(upper(trim(end_node_id)), '') || '|' ||
            coalesce(upper(trim(relationship_type)), '') || '|' ||
            coalesce(trim(relationship_status), '') || '|' ||
            coalesce(trim(registration_status), '')
        )                                                               as row_hash,

        -- Pipeline Timestamps
        current_timestamp                                               as staging_loaded_at

    from source_data
    where start_node_id is not null 
      and trim(start_node_id) <> ''
      and end_node_id is not null
      and trim(end_node_id) <> ''
    order by 
        md5(
            coalesce(upper(trim(start_node_id)), '') || '|' ||
            coalesce(upper(trim(end_node_id)), '') || '|' ||
            coalesce(upper(trim(relationship_type)), '')
        ),
        source_load_date desc,
        raw_id desc

)

select * from transformed