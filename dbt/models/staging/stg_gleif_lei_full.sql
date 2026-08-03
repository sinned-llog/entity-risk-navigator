{{ config(
    schema='staging',
    materialized='table',
    alias='stg_gleif_lei_full',
    indexes=[
      {'columns': ['lei'], 'unique': True},
      {'columns': ['legal_name_normalized']},
      {'columns': ['entity_status']},
      {'columns': ['source_load_date']}
    ]
) }}
with source_data as (

    select *
    from {{ source('raw', 'gleif_lei_full') }}
    {% if var('gleif_staging_load_date', none) %}
        where source_load_date = '{{ var("gleif_staging_load_date") }}'::date
    {% endif %}

),

cleaned_data as (

    select
        raw_id,
        upper(trim(lei))                                                 as lei,
        nullif(trim(legal_name), '')                                     as legal_name,
        nullif(
            regexp_replace(lower(trim(coalesce(legal_name, ''))), '[[:space:]]+', ' ', 'g'),
            ''
        )                                                                as legal_name_normalized,
        
        case
            when upper(trim(coalesce(entity_status, ''))) in ('', 'NULL', 'N/A') then 'unknown'
            else lower(trim(entity_status))
        end as entity_status,
        
        case
            when upper(trim(coalesce(registration_status, ''))) in ('', 'NULL', 'N/A') then 'unknown'
            else lower(trim(registration_status))
        end as registration_status,
        
        upper(nullif(trim(legal_jurisdiction), ''))                     as legal_jurisdiction,
        upper(nullif(trim(legal_address_country), ''))                  as legal_address_country,
        upper(nullif(trim(headquarters_address_country), ''))           as headquarters_address_country,
        
        case 
            when next_renewal_date_raw ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' 
                then substring(next_renewal_date_raw from 1 for 10)::date 
            else null 
        end                                                             as next_renewal_date,

        case 
            when last_update_date_raw ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                then substring(last_update_date_raw from 1 for 10)::date 
            else null 
        end                                                             as last_update_date,

        next_renewal_date_raw,
        last_update_date_raw,
        source_load_date::date                                          as source_load_date,
        source_object_key

    from source_data
    where lei is not null 
      and trim(lei) <> ''

),

deduplicated as (

    select distinct on (lei)
        raw_id,
        lei,
        legal_name,
        legal_name_normalized,
        entity_status,
        registration_status,
        legal_jurisdiction,
        legal_address_country,
        headquarters_address_country,
        next_renewal_date,
        last_update_date,
        source_load_date,
        source_object_key,

        md5(
            concat_ws('|', 
                coalesce(lei, ''),
                coalesce(legal_name, ''),
                coalesce(entity_status, ''),
                coalesce(registration_status, ''),
                coalesce(legal_jurisdiction, ''),
                coalesce(legal_address_country, ''),
                coalesce(headquarters_address_country, ''),
                coalesce(next_renewal_date_raw, ''),
                coalesce(last_update_date_raw, '')
            )
        ) as row_hash,

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
        ) as raw_row,

        current_timestamp as staging_loaded_at

    from cleaned_data
    order by 
        lei, 
        source_load_date desc, 
        raw_id desc

)

select * from deduplicated