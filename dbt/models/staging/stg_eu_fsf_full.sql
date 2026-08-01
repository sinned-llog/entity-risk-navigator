{{ config(
    materialized='table',
    schema='staging',
    alias='stg_eu_fsf_full',
    indexes=[
      {'columns': ['stg_eu_fsf_id'], 'unique': True},
      {'columns': ['entity_logical_id']},
      {'columns': ['source_load_date']}
    ]
) }}

with source_raw as (

    select
        raw_id,
        app_env,
        source,
        source_name,
        dataset_group,
        snapshot_type,
        row_number,
        row_hash,
        raw_row,
        source_url,
        source_object_key,
        metadata_object_key,
        source_load_date,
        loaded_at
    from {{ source('raw', 'eu_fsf_full') }}

),

extracted as (

    select
        raw_id,
        app_env,
        source,
        source_name,
        dataset_group,
        snapshot_type,
        row_number,
        row_hash,
        raw_row,
        source_url,
        source_object_key,
        metadata_object_key,
        source_load_date,
        loaded_at,

        nullif(trim(raw_row ->> 'fileGenerationDate'), '') as file_generation_date_raw,

        nullif(trim(raw_row ->> 'Entity_LogicalId'), '')::bigint as entity_logical_id,
        upper(nullif(trim(raw_row ->> 'Entity_EU_ReferenceNumber'), '')) as entity_eu_reference_number,
        upper(nullif(trim(raw_row ->> 'Entity_UnitedNationId'), '')) as entity_united_nations_id,
        nullif(trim(raw_row ->> 'Entity_DesignationDetails'), '') as entity_designation_details,
        lower(nullif(trim(raw_row ->> 'Entity_SubjectType_ClassificationCode'), '')) as entity_type,
        nullif(trim(raw_row ->> 'Entity_Remark'), '') as entity_remark,
        upper(nullif(trim(raw_row ->> 'Entity_SubjectType'), '')) as entity_subject_type,
        nullif(trim(raw_row ->> 'Entity_Regulation_PublicationDate'), '')::date as entity_publication_date,

        nullif(trim(raw_row ->> 'NameAlias_LogicalId'), '')::bigint as name_alias_logical_id,
        nullif(trim(raw_row ->> 'NameAlias_FirstName'), '') as name_alias_first_name,
        nullif(trim(raw_row ->> 'NameAlias_MiddleName'), '') as name_alias_middle_name,
        nullif(trim(raw_row ->> 'NameAlias_LastName'), '') as name_alias_last_name,
        nullif(trim(raw_row ->> 'NameAlias_WholeName'), '') as name_alias_whole_name,
        upper(nullif(trim(raw_row ->> 'NameAlias_NameLanguage'), '')) as name_alias_name_language,
        upper(nullif(trim(raw_row ->> 'NameAlias_Gender'), '')) as name_alias_gender,
        nullif(trim(raw_row ->> 'NameAlias_Title'), '') as name_alias_title,
        nullif(trim(raw_row ->> 'NameAlias_Function'), '') as name_alias_function,

        nullif(trim(raw_row ->> 'Address_LogicalId'), '')::bigint as address_logical_id,
        nullif(trim(raw_row ->> 'Address_Street'), '') as address_street,
        nullif(trim(raw_row ->> 'Address_City'), '') as address_city,
        nullif(trim(raw_row ->> 'Address_ZipCode'), '') as address_zip_code,
        upper(nullif(trim(raw_row ->> 'Address_CountryIso2Code'), '')) as address_country_iso_code,
        nullif(trim(raw_row ->> 'Address_CountryDescription'), '') as address_country_description,

        nullif(trim(raw_row ->> 'BirthDate_LogicalId'), '')::bigint as birthdate_logical_id,
        nullif(trim(raw_row ->> 'BirthDate_BirthDate'), '') as birthdate_date_raw,
        nullif(trim(raw_row ->> 'BirthDate_Year'), '')::integer as birthdate_year,
        nullif(trim(raw_row ->> 'BirthDate_City'), '') as birthdate_city,
        upper(nullif(trim(raw_row ->> 'BirthDate_CountryIso2Code'), '')) as birthdate_country_iso_code,

        nullif(trim(raw_row ->> 'Identification_LogicalId'), '')::bigint as identification_logical_id,
        nullif(trim(raw_row ->> 'Identification_Number'), '') as identification_number,
        lower(nullif(trim(raw_row ->> 'Identification_TypeDescription'), '')) as identification_type,
        upper(nullif(trim(raw_row ->> 'Identification_CountryIso2Code'), '')) as identification_country_iso_code,

        nullif(trim(raw_row ->> 'Citizenship_LogicalId'), '')::bigint as citizenship_logical_id,
        upper(nullif(trim(raw_row ->> 'Citizenship_CountryIso2Code'), '')) as citizenship_country_iso_code,
        nullif(trim(raw_row ->> 'Citizenship_CountryDescription'), '') as citizenship_country_description,
        nullif(trim(raw_row ->> 'Citizenship_Remark'), '') as citizenship_remark,
        lower(nullif(trim(raw_row ->> 'Citizenship_Regulation_Type'), '')) as citizenship_regulation_type,
        upper(nullif(trim(raw_row ->> 'Citizenship_Regulation_Programme'), '')) as citizenship_regulation_programme,
        nullif(trim(raw_row ->> 'Citizenship_Regulation_NumberTitle'), '') as citizenship_regulation_number_title,
        nullif(trim(raw_row ->> 'Citizenship_Regulation_PublicationDate'), '')::date as citizenship_regulation_publication_date,
        nullif(trim(raw_row ->> 'Citizenship_Regulation_PublicationUrl'), '') as citizenship_regulation_url,

        upper(nullif(trim(raw_row ->> 'Entity_Regulation_Programme'), '')) as programme,
        lower(nullif(trim(raw_row ->> 'Entity_Regulation_Type'), '')) as regulation_type,
        nullif(trim(raw_row ->> 'Entity_Regulation_NumberTitle'), '') as regulation_number_title,
        nullif(trim(raw_row ->> 'Entity_Regulation_PublicationDate'), '')::date as regulation_publication_date,
        nullif(trim(raw_row ->> 'Entity_Regulation_PublicationUrl'), '') as regulation_url

    from source_raw

),

cleaned_and_transformed as (

    select
        md5(
            coalesce(cast(source_load_date as text), '') || '|' ||
            coalesce(source_object_key, '') || '|' ||
            coalesce(cast(row_number as text), '')
        ) as stg_eu_fsf_id,

        raw_id,
        app_env,
        source,
        source_name,
        dataset_group,
        snapshot_type,
        row_number as file_row_number,
        row_hash,
        source_load_date,
        loaded_at as raw_loaded_at,
        current_timestamp as staging_loaded_at,

        file_generation_date_raw,

        entity_logical_id,
        entity_eu_reference_number,
        entity_united_nations_id,
        entity_designation_details,
        entity_type,
        entity_remark,
        entity_subject_type,
        entity_publication_date,

        name_alias_logical_id,
        name_alias_first_name,
        name_alias_middle_name,
        name_alias_last_name,
        name_alias_whole_name,

        nullif(
            regexp_replace(
                lower(trim(coalesce(name_alias_whole_name, ''))),
                '\s+',
                ' ',
                'g'
            ),
            ''
        ) as name_alias_whole_name_normalized,

        name_alias_name_language,
        name_alias_gender,
        name_alias_title,
        name_alias_function,

        address_logical_id,
        address_street,
        address_city,
        address_zip_code,
        address_country_iso_code,
        address_country_description,

        birthdate_logical_id,
        birthdate_date_raw,
        birthdate_year,
        birthdate_city,
        birthdate_country_iso_code,

        identification_logical_id,
        identification_number,

        nullif(
            regexp_replace(
                upper(trim(coalesce(identification_number, ''))),
                '\s+',
                ' ',
                'g'
            ),
            ''
        ) as identification_number_normalized,

        identification_type,
        identification_country_iso_code,

        citizenship_logical_id,
        citizenship_country_iso_code,
        citizenship_country_description,
        citizenship_remark,
        citizenship_regulation_type,
        citizenship_regulation_programme,
        citizenship_regulation_number_title,
        citizenship_regulation_publication_date,
        citizenship_regulation_url,

        programme,
        regulation_type,
        regulation_number_title,
        regulation_publication_date,
        regulation_url,

        raw_row,
        source_url,
        source_object_key,
        metadata_object_key

    from extracted

)

select *
from cleaned_and_transformed