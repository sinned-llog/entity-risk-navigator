{{ config(
    materialized='table',
    alias='stg_gleif_rr_test'
) }}

WITH source_data AS (
    SELECT *
    FROM {{ source('raw', 'gleif_rr_test') }}
    {% if var('gleif_staging_load_date', none) %}
        WHERE source_load_date = '{{ var("gleif_staging_load_date") }}'::date
    {% endif %}
),

transformed AS (
    SELECT
        raw_id,
        -- Einindeutiger Key für die Relationship aus Start, End & Typ
        MD5(
            COALESCE(start_node_id, '') || '|' ||
            COALESCE(end_node_id, '') || '|' ||
            COALESCE(relationship_type, '')
        ) AS relationship_key,
        
        start_node_id,
        start_node_id_type,
        end_node_id,
        end_node_id_type,
        relationship_type,
        COALESCE(relationship_status, 'UNKNOWN') AS relationship_status,
        relationship_period_type,
        registration_status,
        managing_lou,
        validation_sources,
        validation_documents,

        -- Datumsbereinigungen
        CASE 
            WHEN relationship_period_start_raw ~ '^\d{4}-\d{2}-\d{2}' 
                THEN substring(relationship_period_start_raw from 1 for 10)::date 
            ELSE NULL 
        END AS relationship_period_start,
        
        CASE 
            WHEN relationship_period_end_raw ~ '^\d{4}-\d{2}-\d{2}' 
                THEN substring(relationship_period_end_raw from 1 for 10)::date 
            ELSE NULL 
        END AS relationship_period_end,

        CASE 
            WHEN initial_registration_date_raw ~ '^\d{4}-\d{2}-\d{2}' 
                THEN substring(initial_registration_date_raw from 1 for 10)::date 
            ELSE NULL 
        END AS initial_registration_date,

        CASE 
            WHEN last_update_date_raw ~ '^\d{4}-\d{2}-\d{2}' 
                THEN substring(last_update_date_raw from 1 for 10)::date 
            ELSE NULL 
        END AS last_update_date,

        CASE 
            WHEN next_renewal_date_raw ~ '^\d{4}-\d{2}-\d{2}' 
                THEN substring(next_renewal_date_raw from 1 for 10)::date 
            ELSE NULL 
        END AS next_renewal_date,

        source_load_date::date AS source_load_date,
        source_object_key,
        
        -- Hash über die Zeile zur Änderungserkennung
        MD5(
            COALESCE(start_node_id, '') || '|' ||
            COALESCE(end_node_id, '') || '|' ||
            COALESCE(relationship_type, '') || '|' ||
            COALESCE(relationship_status, '') || '|' ||
            COALESCE(registration_status, '')
        ) AS row_hash,

        CURRENT_TIMESTAMP AS loaded_at
    FROM source_data
    WHERE start_node_id IS NOT NULL 
      AND end_node_id IS NOT NULL
)

SELECT * FROM transformed