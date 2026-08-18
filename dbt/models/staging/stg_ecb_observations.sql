{{ config(
    materialized='table',
    schema='staging',
    alias='stg_ecb_observations_full',
    post_hook=[
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_ecb_id ON {{ this }} (stg_ecb_id);",
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_ecb_natural_key ON {{ this }} (dataset_code, series_key, time_period_raw);",
      "CREATE INDEX IF NOT EXISTS idx_stg_ecb_series_key ON {{ this }} (series_key);",
      "CREATE INDEX IF NOT EXISTS idx_stg_ecb_ecb_key_full ON {{ this }} (ecb_key_full);",
      "CREATE INDEX IF NOT EXISTS idx_stg_ecb_obs_date ON {{ this }} (obs_date);",
      "CREATE INDEX IF NOT EXISTS idx_stg_ecb_source_load_date ON {{ this }} (source_load_date);"
    ]
) }}

with source_raw as (

    select
        raw_id,
        app_env,
        dataset_code,
        series_key,
        indicator_name,
        frequency,
        unit,
        raw_row,
        source_url,
        source_object_key,
        metadata_object_key,
        source_load_date,
        loaded_at
    from {{ source('raw', 'ecb_observations_full') }}

),

normalized as (

    select
        raw_id,
        app_env,

        upper(nullif(trim(dataset_code), '')) as dataset_code_raw,
        upper(nullif(trim(series_key), '')) as series_key_raw,
        nullif(trim(indicator_name), '') as indicator_name,
        upper(nullif(trim(frequency), '')) as frequency_raw,
        upper(nullif(trim(unit), '')) as unit_raw,

        nullif(trim(raw_row ->> 'KEY'), '') as ecb_key_full_raw,
        upper(nullif(trim(raw_row ->> 'DATAFLOW'), '')) as dataflow_raw,
        upper(nullif(trim(raw_row ->> 'FREQ'), '')) as freq_raw,
        upper(nullif(trim(raw_row ->> 'REF_AREA'), '')) as ref_area_raw,

        nullif(trim(raw_row ->> 'TIME_PERIOD'), '') as time_period_raw,
        nullif(trim(raw_row ->> 'OBS_VALUE'), '') as obs_value_raw,
        upper(nullif(trim(raw_row ->> 'OBS_STATUS'), '')) as obs_status,

        raw_row,
        source_url,
        source_object_key,
        metadata_object_key,
        source_load_date,
        loaded_at

    from source_raw

),

cleaned_and_transformed as (

    select
        md5(
            coalesce(cast(source_load_date as text), '') || '|' ||
            coalesce(source_object_key, '') || '|' ||
            coalesce(cast(raw_id as text), '')
        ) as stg_ecb_id,

        raw_id,
        app_env,
        'ECB Data Portal' as source,
        source_load_date,
        loaded_at as raw_loaded_at,
        current_timestamp as staging_loaded_at,

        coalesce(dataflow_raw, dataset_code_raw) as dataset_code,

        /*
          Full ECB key as delivered by the ECB CSV/JSON.
          Example:
          MIR.M.U2.B.A2I.AM.R.A.2240.EUR.N
        */
        upper(ecb_key_full_raw) as ecb_key_full,

        /*
          Series key without dataflow prefix.
          If raw_row.KEY starts with "MIR.", "EST.", or "YC.", remove the first segment.
          Otherwise fall back to the raw series_key column.
        */
        case
            when ecb_key_full_raw is not null
             and coalesce(dataflow_raw, dataset_code_raw) is not null
             and upper(ecb_key_full_raw) like coalesce(dataflow_raw, dataset_code_raw) || '.%'
                then substring(
                    upper(ecb_key_full_raw)
                    from length(coalesce(dataflow_raw, dataset_code_raw)) + 2
                )
            else series_key_raw
        end as series_key,

        indicator_name,
        coalesce(freq_raw, frequency_raw) as frequency,
        unit_raw as unit,

        coalesce(dataflow_raw, dataset_code_raw) as dataflow,
        coalesce(freq_raw, frequency_raw) as freq,
        ref_area_raw,
        case
            when ref_area_raw is not null then ref_area_raw

            when coalesce(dataflow_raw, dataset_code_raw) = 'EST' then 'U2'

            else null
        end as ref_area_effective,

        time_period_raw,
        obs_value_raw,
        obs_status,

        case
            when coalesce(freq_raw, frequency_raw) = 'M'
             and time_period_raw ~ '^[0-9]{4}-[0-9]{2}$'
                then to_date(time_period_raw || '-01', 'YYYY-MM-DD')

            when time_period_raw ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                then to_date(time_period_raw, 'YYYY-MM-DD')

            when coalesce(freq_raw, frequency_raw) = 'A'
             and time_period_raw ~ '^[0-9]{4}$'
                then to_date(time_period_raw || '-01-01', 'YYYY-MM-DD')

            when time_period_raw ~ '^[0-9]{4}-Q[1-4]$'
                then case substring(time_period_raw from 6 for 2)
                    when 'Q1' then to_date(substring(time_period_raw from 1 for 4) || '-01-01', 'YYYY-MM-DD')
                    when 'Q2' then to_date(substring(time_period_raw from 1 for 4) || '-04-01', 'YYYY-MM-DD')
                    when 'Q3' then to_date(substring(time_period_raw from 1 for 4) || '-07-01', 'YYYY-MM-DD')
                    when 'Q4' then to_date(substring(time_period_raw from 1 for 4) || '-10-01', 'YYYY-MM-DD')
                end

            else null
        end as obs_date,

        case
            when obs_value_raw ~ '^-?[0-9]+(\.[0-9]+)?$'
                then cast(obs_value_raw as numeric)
            else null
        end as obs_value,

        raw_row,
        source_url,
        source_object_key,
        metadata_object_key

    from normalized

),

latest_snapshot_per_observation as (

    select
        *,
        row_number() over (
            partition by dataset_code, series_key, time_period_raw
            order by
                source_load_date desc nulls last,
                raw_loaded_at desc,
                raw_id desc
        ) as snapshot_rank
    from cleaned_and_transformed

)

select *
from latest_snapshot_per_observation
where snapshot_rank = 1