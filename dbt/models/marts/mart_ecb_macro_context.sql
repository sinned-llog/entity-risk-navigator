{{ config(
    materialized='table',
    schema='marts',
    alias='mart_ecb_macro_context',
    post_hook=[
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_mart_ecb_macro_context_id ON {{ this }} (ecb_macro_context_id);",
      "CREATE INDEX IF NOT EXISTS idx_mart_ecb_macro_context_indicator ON {{ this }} (indicator_code);",
      "CREATE INDEX IF NOT EXISTS idx_mart_ecb_macro_context_dataset ON {{ this }} (dataset_code);",
      "CREATE INDEX IF NOT EXISTS idx_mart_ecb_macro_context_ref_area ON {{ this }} (reference_area);",
      "CREATE INDEX IF NOT EXISTS idx_mart_ecb_macro_context_latest_date ON {{ this }} (latest_obs_date);",
      "CREATE INDEX IF NOT EXISTS idx_mart_ecb_macro_context_load_date ON {{ this }} (source_load_date);"
    ]
) }}

with source_observations as (

    select
        dataset_code,
        dataflow,
        ecb_key_full,
        series_key,

        indicator_name,
        frequency,
        unit,

        coalesce(
            nullif(trim(ref_area_effective), ''),
            nullif(trim(ref_area_raw), ''),
            'unknown'
        ) as reference_area,

        time_period_raw,
        obs_status,
        obs_date::date as obs_date,
        obs_value::numeric as obs_value,

        source_load_date::date as source_load_date,
        source_url

    from {{ ref('stg_ecb_observations') }}

    where obs_date is not null
      and obs_value is not null
      and trim(obs_value::text) <> ''

),

series_enriched as (

    select
        *,

        count(*) over (
            partition by
                dataset_code,
                series_key,
                reference_area
        ) as observation_count,

        min(obs_date) over (
            partition by
                dataset_code,
                series_key,
                reference_area
        ) as first_obs_date,

        max(obs_date) over (
            partition by
                dataset_code,
                series_key,
                reference_area
        ) as max_obs_date,

        lag(obs_date) over (
            partition by
                dataset_code,
                series_key,
                reference_area
            order by obs_date asc
        ) as previous_obs_date,

        lag(obs_value) over (
            partition by
                dataset_code,
                series_key,
                reference_area
            order by obs_date asc
        ) as previous_obs_value,

        row_number() over (
            partition by
                dataset_code,
                series_key,
                reference_area
            order by obs_date desc
        ) as latest_rank

    from source_observations

),

latest_observations as (

    select
        *
    from series_enriched
    where latest_rank = 1

),

final as (

    select
        md5(
            concat_ws(
                '|',
                dataset_code,
                series_key,
                reference_area
            )
        ) as ecb_macro_context_id,

        case
            when indicator_name = 'Euro short-term rate'
                then 'euro_short_term_rate'
            when indicator_name = 'Cost of borrowing for corporations'
                then 'cost_of_borrowing_corporations'
            when indicator_name = 'AAA yield curve 2Y spot rate'
                then 'aaa_yield_curve_2y_spot_rate'
            when indicator_name = 'AAA yield curve 10Y spot rate'
                then 'aaa_yield_curve_10y_spot_rate'
            else lower(
                regexp_replace(
                    regexp_replace(indicator_name, '[^a-zA-Z0-9]+', '_', 'g'),
                    '^_|_$',
                    '',
                    'g'
                )
            )
        end as indicator_code,

        case
            when indicator_name = 'Euro short-term rate'
                then 1
            when indicator_name = 'Cost of borrowing for corporations'
                then 2
            when indicator_name = 'AAA yield curve 2Y spot rate'
                then 3
            when indicator_name = 'AAA yield curve 10Y spot rate'
                then 4
            else 99
        end as display_order,

        indicator_name,
        dataset_code,
        dataflow,
        ecb_key_full,
        series_key,

        reference_area,
        case
            when reference_area = 'U2' then 'Euro area'
            else reference_area
        end as reference_area_name,

        frequency,
        unit,

        first_obs_date,
        obs_date as latest_obs_date,
        time_period_raw as latest_time_period,
        obs_value as latest_obs_value,
        obs_status as latest_obs_status,

        previous_obs_date,
        previous_obs_value,

        case
            when previous_obs_value is not null
                then obs_value - previous_obs_value
            else null
        end as latest_change_abs,

        case
            when previous_obs_value is not null
             and previous_obs_value <> 0
                then round(
                    ((obs_value - previous_obs_value) / abs(previous_obs_value))::numeric,
                    6
                )
            else null
        end as latest_change_pct,

        observation_count,

        source_load_date,
        source_url,

        current_timestamp as mart_loaded_at

    from latest_observations

)

select *
from final