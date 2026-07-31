{{ config(
    materialized='table',
    alias='stg_gleif_lei_test',
    indexes=[
      {'columns': ['legal_name_normalized']},
      {'columns': ['entity_status']},
      {'columns': ['source_load_date']}
    ]
) }}

WITH source_data AS (
    SELECT *
    FROM {{ source('raw', 'gleif_lei_test') }}
    {% if var('gleif_staging_load_date', none) %}
        WHERE source_load_date = '{{ var("gleif_staging_load_date") }}'::date
    {% endif %}
),

deduplicated AS (
    SELECT DISTINCT ON (lei)
        raw_id,
        lei,
        legal_name,
        
        -- Normalisierung
        NULLIF(
            regexp_replace(lower(trim(COALESCE(legal_name, ''))), '\s+', ' ', 'g'),
            ''
        ) AS legal_name_normalized,
        
        COALESCE(entity_status, 'UNKNOWN') AS entity_status,
        registration_status,
        legal_jurisdiction,
        legal_address_country,
        headquarters_address_country,
        
        -- Safe Casting für Datumsfelder
        CASE 
            WHEN next_renewal_date_raw ~ '^\d{4}-\d{2}-\d{2}' 
                THEN substring(next_renewal_date_raw from 1 for 10)::date 
            ELSE NULL 
        END AS next_renewal_date,

        CASE 
            WHEN last_update_date_raw ~ '^\d{4}-\d{2}-\d{2}' 
                THEN substring(last_update_date_raw from 1 for 10)::date 
            ELSE NULL 
        END AS last_update_date,

        source_load_date::date AS source_load_date,
        source_object_key,

        -- row_hash: MD5-Hash über fachliche Kernelemente
        MD5(
            COALESCE(lei, '') || '|' ||
            COALESCE(legal_name, '') || '|' ||
            COALESCE(entity_status, '') || '|' ||
            COALESCE(registration_status, '') || '|' ||
            COALESCE(legal_jurisdiction, '')
        ) AS row_hash,

        -- raw_row: JSONB Dump der Raw-Attribute
        jsonb_build_object(
            'raw_id', raw_id,
            'lei', lei,
            'legal_name', legal_name,
            'entity_status', entity_status,
            'registration_status', registration_status,
            'legal_jurisdiction', legal_jurisdiction,
            'legal_address_country', legal_address_country,
            'headquarters_address_country', headquarters_address_country,
            'next_renewal_date_raw', next_renewal_date_raw,
            'last_update_date_raw', last_update_date_raw,
            'source_object_key', source_object_key
        ) AS raw_row,

        CURRENT_TIMESTAMP AS loaded_at

    FROM source_data
    WHERE lei IS NOT NULL 
      AND trim(lei) <> ''
    ORDER BY 
        lei, 
        source_load_date DESC, 
        raw_id DESC
)

SELECT * FROM deduplicated