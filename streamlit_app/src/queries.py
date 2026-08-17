# streamlit_app/src/queries.py

from src.db import read_sql
from unidecode import unidecode
import pandas as pd

############################################################################
#Overview Queries
############################################################################


def to_latin_display(value: object) -> str:
    """
    Converts a text value into a Latin-script display approximation.

    Important:
    - This is only for dashboard display.
    - Do not use this for matching, risk scoring, or audit logic.
    - The original source value must remain available.
    """
    if value is None:
        return ""

    text = str(value).strip()

    if not text:
        return ""

    transliterated = unidecode(text).strip()

    return transliterated if transliterated else text

def add_latin_display_columns(df):
    """
    Adds Latin-script display helper columns for dashboard usability.

    Original source fields remain unchanged.
    """
    if df is None or df.empty:
        return df
    if "legal_name" in df.columns:
        df["legal_name_latin_display"] = df["legal_name"].apply(to_latin_display)
    if "legal_name_normalized" in df.columns:
        df["legal_name_normalized_latin_display"] = df["legal_name_normalized"].apply(to_latin_display)
    if "sanctions_name" in df.columns:
        df["sanctions_name_latin_display"] = df["sanctions_name"].apply(to_latin_display)
    if "sanctions_subject_primary_name" in df.columns:
        df["sanctions_subject_primary_name_latin_display"] = df["sanctions_subject_primary_name"].apply(to_latin_display)

    return df


def get_risk_overview_metrics():
    query = """
        select
            count(*) as total_entities,
            count(*) filter (where has_sanctions_match) as entities_with_sanctions_match,
            count(*) filter (where risk_tier = 'high') as high_risk_entities,
            count(*) filter (where risk_tier = 'medium') as medium_risk_entities,
            count(*) filter (where risk_tier = 'review') as review_required_entities,
            count(*) filter (where risk_tier = 'low_or_no_known_match') as low_or_no_known_match_entities,
            coalesce(sum(total_sanctions_match_count), 0) as total_sanctions_matches
        from marts.mart_entity_risk_score
    """
    return read_sql(query)


def get_risk_tier_distribution():
    query = """
        select
            risk_tier,
            count(*) as entity_count
        from marts.mart_entity_risk_score
        group by risk_tier
        order by
            case risk_tier
                when 'high' then 1
                when 'medium' then 2
                when 'review' then 3
                when 'low_or_no_known_match' then 4
                else 5
            end
    """
    return read_sql(query)

def get_top_risk_entities(limit: int = 25):
    query = """
        select
            lei,
            legal_name_normalized,
            country,
            entity_status,
            registration_status,
            risk_score,
            risk_tier,
            total_sanctions_match_count,
            high_confidence_match_count,
            medium_confidence_match_count,
            review_required_match_count,
            source_entity_keys
        from marts.mart_entity_risk_score
        where risk_tier <> 'low_or_no_known_match'
        order by
            risk_score desc,
            total_sanctions_match_count desc,
            legal_name_normalized asc
        limit :limit
    """

    df = read_sql(query, params={"limit": limit})
    return add_latin_display_columns(df)

############################################################################
#Entity_Detail Queries
############################################################################

def search_entities(search_term: str, limit: int = 50):
    query = """
        select
            m.entity_candidate_id,
            m.lei,
            m.legal_name_normalized,
            m.country,
            m.entity_status,
            m.registration_status,
            m.legal_jurisdiction,

            coalesce(r.risk_score, 0) as risk_score,
            coalesce(r.risk_tier, 'low_or_no_known_match') as risk_tier,
            coalesce(r.total_sanctions_match_count, 0) as total_sanctions_match_count,
            coalesce(r.high_confidence_match_count, 0) as high_confidence_match_count,
            coalesce(r.medium_confidence_match_count, 0) as medium_confidence_match_count,
            coalesce(r.review_required_match_count, 0) as review_required_match_count

        from marts.mart_entity_master m
        left join marts.mart_entity_risk_score r
            on m.entity_candidate_id = r.entity_candidate_id

        where
            :search_term is not null
            and length(trim(:search_term)) >= 2
            and (
                m.lei ilike '%' || :search_term || '%'
                or m.legal_name ilike '%' || :search_term || '%'
                or m.legal_name_normalized ilike '%' || :search_term || '%'
            )

        order by
            case
                when m.lei ilike :search_term || '%' then 1
                when m.legal_name_normalized ilike :search_term || '%' then 2
                when m.legal_name ilike :search_term || '%' then 3
                else 4
            end,
            coalesce(r.risk_score, 0) desc,
            coalesce(r.total_sanctions_match_count, 0) desc,
            m.legal_name_normalized asc

        limit :limit
    """

    df = read_sql(
        query,
        params={
            "search_term": search_term.strip(),
            "limit": limit,
        },
    )
    return add_latin_display_columns(df)

def get_entity_detail_summary(entity_candidate_id: str):
    query = """
        select
            m.entity_candidate_id,
            m.candidate_source,
            m.candidate_source_id,
            m.lei,
            m.legal_name_normalized,
            m.country,
            m.entity_status,
            m.registration_status,
            m.legal_jurisdiction,
            m.legal_address_country,
            m.headquarters_address_country,

            m.direct_parent_lei,
            m.direct_parent_name,
            m.ultimate_parent_lei,
            m.ultimate_parent_name,
            m.has_direct_parent,
            m.has_ultimate_parent,
            m.has_any_parent,

            m.next_renewal_date,
            m.last_update_date,
            m.source_load_date,

            coalesce(r.risk_score, 0) as risk_score,
            coalesce(r.risk_tier, 'low_or_no_known_match') as risk_tier,
            r.risk_reasons,
            r.match_tier_summary,
            coalesce(r.total_sanctions_match_count, 0) as total_sanctions_match_count,
            coalesce(r.high_confidence_match_count, 0) as high_confidence_match_count,
            coalesce(r.medium_confidence_match_count, 0) as medium_confidence_match_count,
            coalesce(r.review_required_match_count, 0) as review_required_match_count,
            r.source_entity_keys,
            r.sanctions_sources,
            r.distinct_sanction_subject_count,
            r.mart_loaded_at as risk_mart_loaded_at

        from marts.mart_entity_master m
        left join marts.mart_entity_risk_score r
            on m.entity_candidate_id = r.entity_candidate_id

        where m.entity_candidate_id = :entity_candidate_id

        limit 1
    """

    df = read_sql(
            query,
            params={
                "entity_candidate_id": entity_candidate_id,
            },
        )
    return add_latin_display_columns(df)

def get_entity_sanctions_matches(entity_candidate_id: str):
    query = """
        select
            entity_sanctions_match_id,
            entity_candidate_id,
            lei,
            legal_name,
            legal_name_normalized,
            country,

            sanctions_source,
            sanctions_entity_type,
            sanction_subject_id,
            source_subject_id,
            source_entity_key,
            sanctions_name,
            sanctions_subject_primary_name,
            source_name_type,
            is_primary_name,

            match_type,
            match_score,
            match_quality_score,
            match_quality_tier,
            match_quality_reasons,
            match_quality_rank,

            is_potential_risk_match,
            is_high_confidence_match,
            is_medium_confidence_match,
            is_review_required_match,
            is_short_match_key,
            is_generic_match_key,
            has_country_overlap,
            country_overlap_status,
            has_identifier_overlap,

            sanction_context_summary,
            source_programs,
            source_lists,
            source_countries,
            source_reference_url,
            source_load_date

        from marts.mart_entity_sanctions_screening

        where entity_candidate_id = :entity_candidate_id

        order by
            match_quality_rank asc,
            match_quality_score desc,
            sanctions_source asc,
            sanctions_name asc
    """

    df = read_sql(
        query,
        params={"entity_candidate_id": entity_candidate_id},
    )

    return add_latin_display_columns(df)

############################################################################
# Pipeline Status Queries
############################################################################

def get_pipeline_table_status_metrics():
    query = """
        with expected_tables as (
            select
                pipeline_layer,
                target_table,
                display_name,
                is_active,
                is_required_for_mvp,
                sort_order
            from config.pipeline_expected_tables
            where is_active = true
        ),

        latest_audit as (
            select
                a.*,
                row_number() over (
                    partition by a.target_table
                    order by
                        case a.pipeline_health_status
                            when 'not_healthy' then 1
                            when 'latest_failed_previous_success_available' then 2
                            when 'healthy' then 3
                            else 4
                        end asc,
                        a.is_failed_or_incomplete_latest_run desc,
                        a.started_at desc nulls last,
                        a.job_run_id desc nulls last
                ) as row_num
            from marts.mart_pipeline_audit_status a
        ),

        table_status as (
            select
                e.pipeline_layer,
                e.target_table,
                e.display_name,
                coalesce(a.pipeline_health_status, 'missing_run') as pipeline_health_status,
                coalesce(a.total_run_count, 0) as total_run_count,
                coalesce(a.is_successful_latest_run, false) as is_successful_latest_run,
                coalesce(a.is_failed_or_incomplete_latest_run, false) as is_failed_or_incomplete_latest_run
            from expected_tables e
            left join latest_audit a
                on e.target_table = a.target_table
               and a.row_num = 1
        )

        select
            count(*) as monitored_tables,
            coalesce(sum(total_run_count), 0) as total_runs,
            count(*) filter (
                where pipeline_health_status = 'healthy'
            ) as healthy_tables,
            count(*) filter (
                where pipeline_health_status = 'latest_failed_previous_success_available'
            ) as warning_tables,
            count(*) filter (
                where pipeline_health_status = 'not_healthy'
            ) as not_healthy_tables,
            count(*) filter (
                where pipeline_health_status = 'missing_run'
            ) as missing_run_tables,
            count(*) filter (
                where is_failed_or_incomplete_latest_run
            ) as tables_with_failed_latest_run
        from table_status
    """
    return read_sql(query)


def get_pipeline_table_health_details():
    query = """
        with expected_tables as (
            select
                pipeline_layer,
                target_table,
                display_name,
                is_active,
                is_required_for_mvp,
                sort_order
            from config.pipeline_expected_tables
            where is_active = true
        ),

        latest_audit as (
            select
                a.*,
                row_number() over (
                    partition by a.target_table
                    order by
                        case a.pipeline_health_status
                            when 'not_healthy' then 1
                            when 'latest_failed_previous_success_available' then 2
                            when 'healthy' then 3
                            else 4
                        end asc,
                        a.is_failed_or_incomplete_latest_run desc,
                        a.started_at desc nulls last,
                        a.job_run_id desc nulls last
                ) as row_num
            from marts.mart_pipeline_audit_status a
        )

        select
            e.pipeline_layer,
            e.target_table,
            e.display_name,
            e.sort_order,

            a.job_name,
            a.job_type,
            a.source,
            a.target_system,
            a.app_env,
            a.status,

            coalesce(a.pipeline_health_status, 'missing_run') as pipeline_health_status,
            coalesce(a.freshness_status, 'unknown') as freshness_status,
            a.snapshot_age_days,
            coalesce(a.is_successful_latest_run, false) as is_successful_latest_run,
            coalesce(a.is_failed_or_incomplete_latest_run, false) as is_failed_or_incomplete_latest_run,

            coalesce(a.total_run_count, 0) as total_run_count,
            coalesce(a.success_run_count, 0) as success_run_count,
            coalesce(a.failed_or_incomplete_run_count, 0) as failed_or_incomplete_run_count,

            a.rows_read,
            a.rows_inserted,
            a.started_at,
            a.finished_at,
            a.duration_seconds,
            a.last_success_at,
            a.last_failure_at,
            a.error_message,
            a.mart_loaded_at

        from expected_tables e
        left join latest_audit a
            on e.target_table = a.target_table
           and a.row_num = 1

        order by
            case coalesce(a.pipeline_health_status, 'missing_run')
                when 'not_healthy' then 1
                when 'missing_run' then 2
                when 'latest_failed_previous_success_available' then 3
                when 'healthy' then 4
                else 5
            end,
            coalesce(a.is_failed_or_incomplete_latest_run, false) desc,
            e.sort_order asc,
            e.pipeline_layer asc,
            e.target_table asc
    """
    return read_sql(query)


def get_pipeline_latest_loads():
    query = """
        select
            a.job_name,
            a.job_type,
            a.source,
            a.target_system,
            a.target_table,
            e.display_name,
            e.pipeline_layer,
            a.app_env,
            a.status,
            a.pipeline_health_status,
            a.is_successful_latest_run,
            a.is_failed_or_incomplete_latest_run,
            a.started_at,
            a.finished_at,
            a.duration_seconds,
            a.freshness_status,
            a.snapshot_age_days,
            a.rows_read,
            a.rows_inserted,
            a.total_run_count,
            a.success_run_count,
            a.failed_or_incomplete_run_count,
            a.last_success_at,
            a.last_failure_at,
            a.has_successful_run,
            a.error_message,
            a.mart_loaded_at
        from marts.mart_pipeline_audit_status a
        inner join config.pipeline_expected_tables e
            on a.target_table = e.target_table
           and e.is_active = true
        order by
            case a.pipeline_health_status
                when 'not_healthy' then 1
                when 'latest_failed_previous_success_available' then 2
                when 'healthy' then 3
                else 4
            end,
            a.is_failed_or_incomplete_latest_run desc,
            a.started_at desc nulls last,
            e.sort_order asc,
            a.job_name asc
    """
    return read_sql(query)

############################################################################
# Entity Relationship Queries
############################################################################

def get_entity_relationship_context(entity_candidate_id: str):
    query = """
        select
            entity_candidate_id,
            lei,
            legal_name,
            legal_name_normalized,
            country,

            direct_parent_lei,
            direct_parent_name,
            ultimate_parent_lei,
            ultimate_parent_name,

            has_direct_parent,
            has_ultimate_parent,
            has_any_parent,

            known_child_count,
            total_descendant_count,
            same_parent_entity_count,
            parent_chain_depth,
            parent_path_count,

            furthest_known_ancestor_lei,
            furthest_known_ancestor_name,

            has_known_children,
            relationship_context_summary,

            source_load_date,
            mart_loaded_at
        from marts.mart_entity_relationship_context
        where entity_candidate_id = :entity_candidate_id
        limit 1
    """

    return read_sql(
        query,
        params={"entity_candidate_id": entity_candidate_id},
    )


def get_entity_child_entities(lei: str, limit: int = 1000):
    query = """
        select
            p.root_entity_candidate_id as entity_candidate_id,
            p.root_lei as lei,
            p.root_legal_name as legal_name,
            p.root_legal_name_normalized as legal_name_normalized,
            p.root_country as country,

            m.entity_status,
            m.registration_status,
            m.direct_parent_lei,
            m.direct_parent_name,

            p.relationship_depth,
            case
                when p.relationship_depth = 1 then 'direct_child'
                else 'indirect_descendant'
            end as relationship_level,

            p.lei_path_text

        from marts.mart_entity_parent_paths p
        left join marts.mart_entity_master m
            on p.root_entity_candidate_id = m.entity_candidate_id

        where p.ancestor_lei = :lei
          and p.root_lei <> :lei

        order by
            p.relationship_depth asc,
            p.root_legal_name_normalized asc

        limit :limit
    """

    return read_sql(
        query,
        params={
            "lei": lei,
            "limit": limit,
        },
    )

def get_entity_same_parent_entities(
    entity_candidate_id: str,
    direct_parent_lei: str,
    limit: int = 1000,
):
    query = """
        select
            entity_candidate_id,
            lei,
            legal_name,
            legal_name_normalized,
            country,
            entity_status,
            registration_status,
            direct_parent_lei,
            direct_parent_name
        from marts.mart_entity_master
        where direct_parent_lei = :direct_parent_lei
          and entity_candidate_id <> :entity_candidate_id
        order by
            legal_name_normalized asc
        limit :limit
    """

    return read_sql(
        query,
        params={
            "entity_candidate_id": entity_candidate_id,
            "direct_parent_lei": direct_parent_lei,
            "limit": limit,
        },
    )


def get_entity_parent_path(entity_candidate_id: str):
    query = """
        select
            root_entity_candidate_id,
            root_lei,
            root_legal_name,
            root_legal_name_normalized,

            ancestor_lei,
            ancestor_name,
            relationship_depth,
            lei_path_text,
            is_furthest_known_ancestor,

            source_load_date,
            mart_loaded_at
        from marts.mart_entity_parent_paths
        where root_entity_candidate_id = :entity_candidate_id
        order by
            relationship_depth asc
    """

    return read_sql(
        query,
        params={"entity_candidate_id": entity_candidate_id},
    )

############################################################################
# ECB Macro Time Series Queries
############################################################################

def get_ecb_macro_indicator_options():
    query = """
        select distinct
            indicator_code,
            indicator_name,
            frequency,
            unit,
            reference_area_name
        from marts.mart_ecb_macro_timeseries
        order by indicator_name
    """

    return read_sql(query)


def get_ecb_macro_timeseries(indicator_code: str | None = None):
    query = """
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
            is_latest_observation,
            source_load_date,
            mart_loaded_at
        from marts.mart_ecb_macro_timeseries
        where (:indicator_code is null or indicator_code = :indicator_code)
        order by
            indicator_name,
            obs_date
    """

    return read_sql(
        query,
        params={"indicator_code": indicator_code},
    )


def get_ecb_macro_latest_observations():
    query = """
        select
            indicator_code,
            indicator_name,
            frequency,
            unit,
            reference_area_name,
            obs_date as latest_obs_date,
            obs_value as latest_obs_value,
            previous_obs_value,
            change_abs,
            change_pct,
            rolling_avg_3_obs,
            rolling_avg_6_obs,
            rolling_avg_12_obs,
            source_load_date
        from marts.mart_ecb_macro_timeseries
        where is_latest_observation = true
        order by indicator_name
    """

    return read_sql(query)

def get_ecb_macro_pressure_summary():
    query = """
        select
            macro_pressure_score,
            macro_pressure_level,
            macro_trend_direction,
            weighted_trend_signal,
            macro_pressure_summary,
            max(latest_obs_date) as latest_obs_date,
            max(source_load_date) as source_load_date,
            max(mart_loaded_at) as mart_loaded_at
        from marts.mart_ecb_macro_pressure_score
        group by
            macro_pressure_score,
            macro_pressure_level,
            macro_trend_direction,
            weighted_trend_signal,
            macro_pressure_summary
        limit 1
    """

    return read_sql(query)

def get_ecb_macro_pressure_indicators():
    query = """
        select
            indicator_code,
            indicator_name,
            latest_obs_date,
            latest_obs_value,
            previous_obs_value,
            change_abs,
            change_pct,
            current_level_score,
            momentum_score,
            trend_projection_score,
            trend_direction,
            indicator_pressure_score,
            indicator_weight,
            frequency,
            unit,
            reference_area_name,
            source_load_date
        from marts.mart_ecb_macro_pressure_score
        order by
            indicator_weight desc,
            indicator_code asc
    """

    return read_sql(query)

def get_entity_macro_context(entity_candidate_id: str):
    query = """
        select
            entity_candidate_id,
            lei,
            legal_name,
            country,
            is_euro_area_country,
            macro_context_applicability,
            macro_applicability_weight,
            macro_context_applicability_reason,
            macro_pressure_score,
            macro_pressure_level,
            macro_trend_direction,
            entity_macro_context_score,
            entity_macro_context_summary,
            macro_pressure_summary,
            macro_pressure_loaded_at,
            mart_loaded_at
        from marts.mart_entity_macro_context
        where entity_candidate_id = :entity_candidate_id
        limit 1
    """

    return read_sql(
        query,
        params={"entity_candidate_id": entity_candidate_id},
    )