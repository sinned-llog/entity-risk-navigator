{{ config(
    materialized = 'table',
    schema = 'marts'
) }}

with base as (

    select
        indicator_code,
        indicator_name,
        dataset_code,
        series_key,
        frequency,
        unit,
        reference_area,
        reference_area_name,

        obs_date,
        obs_value,
        previous_obs_value,
        change_abs,
        change_pct,

        rolling_avg_3_obs,
        rolling_avg_6_obs,
        rolling_avg_12_obs,

        obs_rank_desc,
        observation_count,
        is_latest_observation,

        source_load_date,
        mart_loaded_at as timeseries_loaded_at

    from {{ ref('mart_ecb_macro_timeseries') }}
    where obs_value is not null

),

indicator_weights as (

    select
        indicator_code,
        case
            when indicator_code = 'cost_of_borrowing_corporations' then 0.35
            when indicator_code = 'euro_short_term_rate' then 0.25
            when indicator_code = 'aaa_yield_curve_10y_spot_rate' then 0.25
            when indicator_code = 'aaa_yield_curve_2y_spot_rate' then 0.15
            else 0.00
        end as indicator_weight
    from base
    group by indicator_code

),

scored_history as (

    select
        b.*,

        cume_dist() over (
            partition by b.indicator_code, b.reference_area
            order by b.obs_value asc
        ) as current_level_percentile,

        cume_dist() over (
            partition by b.indicator_code, b.reference_area
            order by coalesce(b.change_abs, 0) asc
        ) as momentum_percentile

    from base b

),

trend_window as (

    select
        *,
        row_number() over (
            partition by indicator_code, reference_area
            order by obs_date desc
        ) as trend_rank_desc
    from scored_history

),

trend_scores as (

    select
        indicator_code,
        reference_area,

        regr_slope(
            obs_value,
            extract(epoch from obs_date)::numeric
        ) as raw_trend_slope,

        count(*) as trend_observation_count

    from trend_window
    where trend_rank_desc <= 12
    group by
        indicator_code,
        reference_area

),

latest_indicator_values as (

    select
        sh.*,
        iw.indicator_weight,
        ts.raw_trend_slope,
        ts.trend_observation_count

    from scored_history sh
    left join indicator_weights iw
        on sh.indicator_code = iw.indicator_code
    left join trend_scores ts
        on sh.indicator_code = ts.indicator_code
       and sh.reference_area = ts.reference_area

    where sh.is_latest_observation = true
      and coalesce(iw.indicator_weight, 0) > 0

),

indicator_scores as (

    select
        md5(
            coalesce(indicator_code, '') || '|' ||
            coalesce(reference_area, '') || '|' ||
            coalesce(series_key, '')
        ) as ecb_macro_pressure_indicator_id,

        indicator_code,
        indicator_name,
        dataset_code,
        series_key,
        frequency,
        unit,
        reference_area,
        reference_area_name,

        obs_date as latest_obs_date,
        obs_value as latest_obs_value,
        previous_obs_value,
        change_abs,
        change_pct,

        rolling_avg_3_obs,
        rolling_avg_6_obs,
        rolling_avg_12_obs,

        observation_count,
        source_load_date,

        round((current_level_percentile * 100)::numeric, 2) as current_level_score,
        round((momentum_percentile * 100)::numeric, 2) as momentum_score,

        raw_trend_slope,
        trend_observation_count,

        case
            when raw_trend_slope is null then 50
            when raw_trend_slope > 0 then 75
            when raw_trend_slope < 0 then 25
            else 50
        end as trend_projection_score,

        case
            when raw_trend_slope is null then 'unknown'
            when raw_trend_slope > 0 then 'upward'
            when raw_trend_slope < 0 then 'downward'
            else 'stable'
        end as trend_direction,

        round(
            (
                (0.50 * current_level_percentile * 100)
                + (0.30 * momentum_percentile * 100)
                + (0.20 * (
                    case
                        when raw_trend_slope is null then 50
                        when raw_trend_slope > 0 then 75
                        when raw_trend_slope < 0 then 25
                        else 50
                    end
                ))
            )::numeric,
            2
        ) as indicator_pressure_score,

        indicator_weight

    from latest_indicator_values

),

macro_score as (

    select
        round(
            (
                sum(indicator_pressure_score * indicator_weight)
                / nullif(sum(indicator_weight), 0)
            )::numeric,
            2
        ) as macro_pressure_score,

        round(
            (
                sum(
                    case
                        when trend_direction = 'upward' then 1
                        when trend_direction = 'downward' then -1
                        else 0
                    end * indicator_weight
                )
                / nullif(sum(indicator_weight), 0)
            )::numeric,
            4
        ) as weighted_trend_signal

    from indicator_scores

),

macro_interpretation as (

    select
        macro_pressure_score,
        weighted_trend_signal,

        case
            when macro_pressure_score >= 75 then 'high'
            when macro_pressure_score >= 50 then 'elevated'
            when macro_pressure_score >= 25 then 'moderate'
            else 'low'
        end as macro_pressure_level,

        case
            when weighted_trend_signal > 0.15 then 'upward'
            when weighted_trend_signal < -0.15 then 'downward'
            else 'stable'
        end as macro_trend_direction

    from macro_score

),

final as (

    select
        i.*,

        m.macro_pressure_score,
        m.macro_pressure_level,
        m.macro_trend_direction,
        m.weighted_trend_signal,

        case
            when m.macro_pressure_level = 'high'
                then 'Euro area macro pressure is currently high. Current ECB indicators are elevated and recent trend signals point to continued pressure.'
            when m.macro_pressure_level = 'elevated'
                then 'Euro area macro pressure is elevated. ECB indicators show increased financing and rate pressure compared with their historical range.'
            when m.macro_pressure_level = 'moderate'
                then 'Euro area macro pressure is moderate. ECB indicators show some pressure, but not at elevated or high levels.'
            else 'Euro area macro pressure is currently low based on the selected ECB indicators.'
        end as macro_pressure_summary,

        now() as mart_loaded_at

    from indicator_scores i
    cross join macro_interpretation m

)

select *
from final
order by
    indicator_weight desc,
    indicator_code