# streamlit_app/src/queries.py

from src.db import read_sql
from unidecode import unidecode


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
            legal_name,
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
            sanctions_sources,
            sanctions_programs,
            sanctions_lists,
            risk_reasons
        from marts.mart_entity_risk_score
        order by
            risk_score desc,
            total_sanctions_match_count desc,
            legal_name_normalized asc
        limit :limit
    """
    df = read_sql(query, params={"limit": limit})

    return add_latin_display_columns(df)