{{ config(
    materialized = 'table',
    schema = 'marts'
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
        ref_area_effective as reference_area,
        time_period_raw,
        obs_status,
        obs_date,
        obs_value,
        source_url,
        source_load_date
    from {{ ref('stg_ecb_observations') }}
    where obs_date is not null
      and obs_value is not null
      and indicator_name is not null

),

normalized as (

    select
        md5(
            coalesce(dataset_code, '') || '|' ||
            coalesce(series_key, '') || '|' ||
            coalesce(obs_date::text, '')
        ) as ecb_macro_timeseries_id,

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

        indicator_name,
        dataset_code,
        dataflow,
        ecb_key_full,
        series_key,
        frequency,
        unit,
        reference_area,

        case
            when reference_area = 'U2' then 'Euro area'
            when reference_area is null then null
            else reference_area
        end as reference_area_name,

        time_period_raw,
        obs_status,
        obs_date,
        obs_value,

        source_url,
        source_load_date

    from source_observations

),

with_previous as (

    select
        *,

        lag(obs_value) over (
            partition by indicator_code, reference_area, series_key
            order by obs_date asc
        ) as previous_obs_value,

        lag(obs_date) over (
            partition by indicator_code, reference_area, series_key
            order by obs_date asc
        ) as previous_obs_date,

        row_number() over (
            partition by indicator_code, reference_area, series_key
            order by obs_date desc
        ) as obs_rank_desc,

        count(*) over (
            partition by indicator_code, reference_area, series_key
        ) as observation_count

    from normalized

),

with_changes as (

    select
        *,

        obs_value - previous_obs_value as change_abs,

        case
            when previous_obs_value is null then null
            when previous_obs_value = 0 then null
            else (obs_value - previous_obs_value) / nullif(abs(previous_obs_value), 0)
        end as change_pct,

        avg(obs_value) over (
            partition by indicator_code, reference_area, series_key
            order by obs_date asc
            rows between 2 preceding and current row
        ) as rolling_avg_3_obs,

        avg(obs_value) over (
            partition by indicator_code, reference_area, series_key
            order by obs_date asc
            rows between 5 preceding and current row
        ) as rolling_avg_6_obs,

        avg(obs_value) over (
            partition by indicator_code, reference_area, series_key
            order by obs_date asc
            rows between 11 preceding and current row
        ) as rolling_avg_12_obs

    from with_previous

),

final as (

    select
        ecb_macro_timeseries_id,

        indicator_code,
        indicator_name,
        dataset_code,
        dataflow,
        ecb_key_full,
        series_key,
        frequency,
        unit,

        reference_area,
        reference_area_name,

        obs_date,
        time_period_raw,
        obs_value,
        obs_status,
        previous_obs_date,
        previous_obs_value,
        change_abs,
        change_pct,

        rolling_avg_3_obs,
        rolling_avg_6_obs,
        rolling_avg_12_obs,

        obs_rank_desc,
        observation_count,

        case
            when obs_rank_desc = 1 then true
            else false
        end as is_latest_observation,

        source_url,
        source_load_date,
        now() as mart_loaded_at

    from with_changes

)

select *
from final