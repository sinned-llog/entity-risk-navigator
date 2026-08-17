{{ config(
    materialized = 'table',
    schema = 'marts'
) }}

with entity_master as (

    select
        entity_candidate_id,
        lei,
        legal_name,
        legal_name_normalized,
        country,
        source_load_date
    from {{ ref('mart_entity_master') }}

),

macro_pressure as (

    select
        macro_pressure_score,
        macro_pressure_level,
        macro_trend_direction,
        weighted_trend_signal,
        macro_pressure_summary,
        max(latest_obs_date) as latest_ecb_obs_date,
        max(source_load_date) as ecb_source_load_date,
        max(mart_loaded_at) as macro_pressure_loaded_at
    from {{ ref('mart_ecb_macro_pressure_score') }}
    group by
        macro_pressure_score,
        macro_pressure_level,
        macro_trend_direction,
        weighted_trend_signal,
        macro_pressure_summary

),

entity_applicability as (

    select
        e.*,

        case
            when upper(e.country) in (
                'AT', 'BE', 'HR', 'CY', 'EE', 'FI', 'FR', 'DE', 'GR',
                'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PT', 'SK',
                'SI', 'ES'
            )
                then true
            else false
        end as is_euro_area_country,

        case
            when e.country is null or trim(e.country) = ''
                then 'unknown'
            when upper(e.country) in (
                'AT', 'BE', 'HR', 'CY', 'EE', 'FI', 'FR', 'DE', 'GR',
                'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PT', 'SK',
                'SI', 'ES'
            )
                then 'applicable'
            else 'reduced'
        end as macro_context_applicability,

        case
            when e.country is null or trim(e.country) = ''
                then 0.30
            when upper(e.country) in (
                'AT', 'BE', 'HR', 'CY', 'EE', 'FI', 'FR', 'DE', 'GR',
                'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PT', 'SK',
                'SI', 'ES'
            )
                then 1.00
            else 0.35
        end as macro_applicability_weight,

        case
            when e.country is null or trim(e.country) = ''
                then 'Applicability unknown because the entity jurisdiction is missing or could not be mapped. ECB indicators are shown as general macroeconomic context.'

            when upper(e.country) in (
                'AT', 'BE', 'HR', 'CY', 'EE', 'FI', 'FR', 'DE', 'GR',
                'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PT', 'SK',
                'SI', 'ES'
            )
                then 'Entity jurisdiction is part of the euro area. ECB indicators are relevant as regional macroeconomic context.'

            else 'Reduced applicability because the entity jurisdiction is outside the euro area. ECB indicators are shown as broader macroeconomic context only.'
        end as macro_context_applicability_reason

    from entity_master e

),

final as (

    select
        md5(
            coalesce(e.entity_candidate_id, '') || '|ecb_macro_context'
        ) as entity_macro_context_id,

        e.entity_candidate_id,
        e.lei,
        e.legal_name,
        e.legal_name_normalized,
        e.country,

        e.is_euro_area_country,
        e.macro_context_applicability,
        e.macro_applicability_weight,
        e.macro_context_applicability_reason,

        m.macro_pressure_score,
        m.macro_pressure_level,
        m.macro_trend_direction,
        m.weighted_trend_signal,
        m.macro_pressure_summary,

        round(
            (m.macro_pressure_score * e.macro_applicability_weight)::numeric,
            2
        ) as entity_macro_context_score,

        case
            when e.macro_context_applicability = 'applicable'
                then m.macro_pressure_summary

            when e.macro_context_applicability = 'reduced'
                then 'ECB macro indicators are shown with reduced applicability for this entity because its jurisdiction is outside the euro area.'

            else 'ECB macro indicators are shown as general context because applicability could not be determined.'
        end as entity_macro_context_summary,

        m.latest_ecb_obs_date,
        m.ecb_source_load_date,
        e.source_load_date as entity_source_load_date,
        m.macro_pressure_loaded_at,
        now() as mart_loaded_at

    from entity_applicability e
    cross join macro_pressure m

)

select *
from final