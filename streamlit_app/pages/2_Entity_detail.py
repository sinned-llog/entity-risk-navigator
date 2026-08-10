# streamlit_app/pages/2_Entity_Detail.py

import pandas as pd
import streamlit as st

from src.queries import (
    search_entities,
    get_entity_detail_summary,
    get_entity_sanctions_matches,
)


st.set_page_config(
    page_title="Entity Detail | EntityRisk Navigator",
    page_icon="🧭",
    layout="wide",
)

st.title("Entity Detail")
st.caption("Search for an entity and review its risk score, sanctions matches, and master data.")


def format_int(value):
    if pd.isna(value):
        return "0"
    return f"{int(value):,}"


def render_entity_header(row):
    legal_name = row.get("legal_name") or row.get("legal_name_normalized") or "-"
    lei = row.get("lei") or "-"
    country = row.get("country") or "-"
    entity_status = row.get("entity_status") or "-"
    registration_status = row.get("registration_status") or "-"

    st.header(legal_name)
    st.caption(
        f"LEI: {lei} | Country: {country} | "
        f"Entity status: {entity_status} | Registration status: {registration_status}"
    )


def render_risk_cards(row):
    st.subheader("Risk Summary")

    col1, col2, col3, col4, col5, col6 = st.columns(6)

    col1.metric("Risk Score", format_int(row.get("risk_score")))
    col2.metric("Risk Tier", row.get("risk_tier") or "-")
    col3.metric("Total Matches", format_int(row.get("total_sanctions_match_count")))
    col4.metric("High", format_int(row.get("high_confidence_match_count")))
    col5.metric("Medium", format_int(row.get("medium_confidence_match_count")))
    col6.metric("Review", format_int(row.get("review_required_match_count")))

    risk_reasons = row.get("risk_reasons")
    match_tier_summary = row.get("match_tier_summary")

    if risk_reasons or match_tier_summary:
        st.caption(
            f"{risk_reasons or ''}"
            + (" | " if risk_reasons and match_tier_summary else "")
            + f"{match_tier_summary or ''}"
        )


def render_sanctions_matches(matches_df):
    st.subheader("Sanctions Matches")

    if matches_df.empty:
        st.info("No sanctions matches found for this entity.")
        return

    display_columns = [
        "match_quality_tier",
        "match_quality_score",
        "sanctions_source",
        "sanctions_name",
        "sanctions_subject_primary_name",
        "source_name_type",
        "match_quality_reasons",
        "country_overlap_status",
        "has_identifier_overlap",
        "source_programs",
        "source_lists",
        "source_countries",
        "sanction_context_summary",
        "source_reference_url",
    ]

    existing_columns = [
        column for column in display_columns if column in matches_df.columns
    ]

    if not existing_columns:
        st.warning("No expected match detail columns found.")
        st.dataframe(matches_df, use_container_width=True)
        return

    df_display = matches_df[existing_columns].rename(
        columns={
            "match_quality_tier": "Quality",
            "match_quality_score": "Quality Score",
            "sanctions_source": "Source",
            "sanctions_name": "Matched Sanctions Name",
            "sanctions_subject_primary_name": "Subject Primary Name",
            "source_name_type": "Name Type",
            "match_quality_reasons": "Match Quality Explanation",
            "country_overlap_status": "Country Overlap Status",
            "has_identifier_overlap": "Identifier Overlap",
            "source_programs": "Program / Regime Context",
            "source_lists": "Source Lists / Datasets",
            "source_countries": "Source Country Context",
            "sanction_context_summary": "Sanction Context",
            "source_reference_url": "Source Reference",
        }
    )

    st.dataframe(df_display, use_container_width=True, hide_index=True)


def render_master_data(row):
    master_data = {
        "Entity Candidate ID": row.get("entity_candidate_id"),
        "Candidate Source": row.get("candidate_source"),
        "Candidate Source ID": row.get("candidate_source_id"),
        "LEI": row.get("lei"),
        "Legal Name": row.get("legal_name"),
        "Normalized Name": row.get("legal_name_normalized"),
        "Country": row.get("country"),
        "Legal Jurisdiction": row.get("legal_jurisdiction"),
        "Entity Status": row.get("entity_status"),
        "Registration Status": row.get("registration_status"),
        "Legal Address Country": row.get("legal_address_country"),
        "Headquarters Country": row.get("headquarters_address_country"),
        "Next Renewal Date": row.get("next_renewal_date"),
        "Last Update Date": row.get("last_update_date"),
        "Source Load Date": row.get("source_load_date"),
    }

    df = pd.DataFrame(
        [{"Field": key, "Value": value} for key, value in master_data.items()]
    )

    st.dataframe(df, use_container_width=True, hide_index=True)


def render_parent_information(row):
    parent_data = {
        "Has Direct Parent": row.get("has_direct_parent"),
        "Direct Parent LEI": row.get("direct_parent_lei"),
        "Direct Parent Name": row.get("direct_parent_name"),
        "Has Ultimate Parent": row.get("has_ultimate_parent"),
        "Ultimate Parent LEI": row.get("ultimate_parent_lei"),
        "Ultimate Parent Name": row.get("ultimate_parent_name"),
        "Has Any Parent": row.get("has_any_parent"),
    }

    df = pd.DataFrame(
        [{"Field": key, "Value": value} for key, value in parent_data.items()]
    )

    st.dataframe(df, use_container_width=True, hide_index=True)


try:
    search_term = st.text_input(
        "Search entity by LEI or name",
        placeholder="e.g. 2rivers",
    )

    if not search_term or len(search_term.strip()) < 2:
        st.info("Enter at least 2 characters to search for an entity.")
        st.stop()

    results_df = search_entities(search_term=search_term, limit=50)

    if results_df.empty:
        st.warning("No entities found.")
        st.stop()

    results_df = results_df.copy()

    results_df["select_label"] = (
        results_df["lei"].fillna("")
        + " | "
        + results_df["legal_name_normalized"].fillna("")
        + " | "
        + results_df["country"].fillna("")
        + " | "
        + results_df["risk_tier"].fillna("")
        + " | score="
        + results_df["risk_score"].fillna(0).astype(int).astype(str)
        + " | matches="
        + results_df["total_sanctions_match_count"].fillna(0).astype(int).astype(str)
    )

    selected_label = st.selectbox(
        "Select matching entity",
        results_df["select_label"].tolist(),
    )

    selected_entity_candidate_id = results_df.loc[
        results_df["select_label"] == selected_label,
        "entity_candidate_id",
    ].iloc[0]

    summary_df = get_entity_detail_summary(
        entity_candidate_id=selected_entity_candidate_id
    )

    if summary_df.empty:
        st.warning("No detail data found for selected entity.")
        st.stop()

    matches_df = get_entity_sanctions_matches(
        entity_candidate_id=selected_entity_candidate_id
    )

    row = summary_df.iloc[0]

    st.divider()

    render_entity_header(row)

    st.divider()

    render_risk_cards(row)

    st.divider()

    render_sanctions_matches(matches_df)

    st.divider()

    with st.expander("Master Data", expanded=False):
        render_master_data(row)

    with st.expander("Parent Information", expanded=False):
        render_parent_information(row)

except Exception as exc:
    st.error("Failed to load entity detail data.")
    st.exception(exc)