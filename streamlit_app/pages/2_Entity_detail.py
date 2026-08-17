# streamlit_app/pages/2_Entity_Detail.py

import pandas as pd
import streamlit as st

from src.queries import (
    search_entities,
    get_entity_detail_summary,
    get_entity_sanctions_matches,
    get_entity_relationship_context,
    get_entity_child_entities,
    get_entity_same_parent_entities,
    get_entity_parent_path,
    get_entity_macro_context,
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


def render_relationship_context(
    relationship_df,
    child_entities_df,
    same_parent_entities_df,
    parent_path_df,
):
    st.subheader("Relationship Context")

    if relationship_df.empty:
        st.info("No relationship context found for this entity.")
        return

    rel = relationship_df.iloc[0]

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Parent Chain Depth",
        format_int(rel.get("parent_chain_depth")),
    )

    col2.metric(
        "Known Children / Descendants",
        format_int(rel.get("total_descendant_count")),
    )

    col3.metric(
        "Same-Parent Peers",
        format_int(rel.get("same_parent_entity_count")),
    )

    col4.metric(
        "Has Parent",
        "Yes" if rel.get("has_any_parent") else "No",
    )

    summary = rel.get("relationship_context_summary")

    if summary:
        st.caption(summary)

    st.markdown("#### Parent Overview")

    parent_data = {
        "Direct Parent LEI": rel.get("direct_parent_lei"),
        "Direct Parent Name": rel.get("direct_parent_name"),
        "Ultimate Parent LEI": rel.get("ultimate_parent_lei"),
        "Ultimate Parent Name": rel.get("ultimate_parent_name"),
        "Furthest Known Ancestor LEI": rel.get("furthest_known_ancestor_lei"),
        "Furthest Known Ancestor Name": rel.get("furthest_known_ancestor_name"),
        "Source Load Date": rel.get("source_load_date"),
    }

    parent_display = pd.DataFrame(
        [
            {"Field": key, "Value": value}
            for key, value in parent_data.items()
            if value is not None and str(value).strip() != ""
        ]
    )

    if parent_display.empty:
        st.info("No parent details available.")
    else:
        st.dataframe(
            parent_display,
            use_container_width=True,
            hide_index=True,
        )

    if not parent_path_df.empty:
        st.markdown("#### Parent Path")

        parent_path_columns = [
            "relationship_depth",
            "ancestor_lei",
            "ancestor_name",
            "lei_path_text",
            "is_furthest_known_ancestor",
        ]

        existing_parent_path_columns = [
            column for column in parent_path_columns
            if column in parent_path_df.columns
        ]

        parent_path_display = parent_path_df[
            existing_parent_path_columns
        ].rename(
            columns={
                "relationship_depth": "Depth",
                "ancestor_lei": "Ancestor LEI",
                "ancestor_name": "Ancestor Name",
                "lei_path_text": "LEI Path",
                "is_furthest_known_ancestor": "Furthest Known Ancestor",
            }
        )

        st.dataframe(
            parent_path_display,
            use_container_width=True,
            hide_index=True,
        )

    if not child_entities_df.empty:
        st.markdown("#### Known Child and Descendant Entities")

        child_display_columns = [
            "relationship_level",
            "relationship_depth",
            "lei",
            "legal_name",
            "legal_name_normalized",
            "country",
            "entity_status",
            "registration_status",
            "direct_parent_name",
            "lei_path_text",
        ]

        existing_child_columns = [
            column for column in child_display_columns
            if column in child_entities_df.columns
        ]

        child_display = child_entities_df[existing_child_columns].rename(
            columns={
                "relationship_level": "Relationship Level",
                "relationship_depth": "Depth",
                "lei": "LEI",
                "legal_name": "Legal Name",
                "legal_name_normalized": "Normalized Name",
                "country": "Country",
                "entity_status": "Entity Status",
                "registration_status": "Registration Status",
                "direct_parent_name": "Direct Parent",
                "lei_path_text": "Relationship Path",
            }
        )

        st.dataframe(
            child_display,
            use_container_width=True,
            hide_index=True,
        )

    if not same_parent_entities_df.empty:
        st.markdown("#### Same-Parent Entities")

        peer_display_columns = [
            "lei",
            "legal_name",
            "legal_name_normalized",
            "country",
            "entity_status",
            "registration_status",
            "direct_parent_name",
        ]

        existing_peer_columns = [
            column for column in peer_display_columns
            if column in same_parent_entities_df.columns
        ]

        peer_display = same_parent_entities_df[existing_peer_columns].rename(
            columns={
                "lei": "LEI",
                "legal_name": "Legal Name",
                "legal_name_normalized": "Normalized Name",
                "country": "Country",
                "entity_status": "Entity Status",
                "registration_status": "Registration Status",
                "direct_parent_name": "Shared Direct Parent",
            }
        )

        st.dataframe(
            peer_display,
            use_container_width=True,
            hide_index=True,
        )

def render_macro_context(macro_context_df):
    st.subheader("Macro Environment Context")

    if macro_context_df.empty:
        st.info("No macro context available for this entity.")
        return

    macro = macro_context_df.iloc[0]

    macro_pressure_score = macro.get("macro_pressure_score")
    macro_pressure_level = macro.get("macro_pressure_level")
    macro_trend_direction = macro.get("macro_trend_direction")
    entity_macro_context_score = macro.get("entity_macro_context_score")
    applicability = macro.get("macro_context_applicability")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Macro Pressure",
        str(macro_pressure_level).replace("_", " ").title()
        if pd.notna(macro_pressure_level)
        else "n/a",
    )

    col2.metric(
        "Pressure Score",
        f"{macro_pressure_score:.2f}"
        if pd.notna(macro_pressure_score)
        else "n/a",
    )

    col3.metric(
        "Trend",
        str(macro_trend_direction).replace("_", " ").title()
        if pd.notna(macro_trend_direction)
        else "n/a",
    )

    col4.metric(
        "Entity Context Score",
        f"{entity_macro_context_score:.2f}"
        if pd.notna(entity_macro_context_score)
        else "n/a",
    )

    if applicability == "applicable":
        st.success("Applicability: Applicable")
    elif applicability == "reduced":
        st.warning("Applicability: Reduced")
    else:
        st.info("Applicability: Unknown")

    reason = macro.get("macro_context_applicability_reason")
    if reason:
        st.caption(reason)

    summary = macro.get("entity_macro_context_summary")
    if summary:
        st.info(summary)

    st.caption(
        "ECB macro indicators are shown as contextual information and are not part "
        "of the current entity sanctions risk score."
    )

try:
    search_term = st.text_input(
        "Search entity by LEI or name",
        placeholder="e.g. 2rivers",
    )

    if not search_term or len(search_term.strip()) < 2:
        st.info("Enter at least 2 characters to search for an entity.")
        st.stop()

    results_df = search_entities(search_term=search_term, limit=1000)

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

    macro_context_df = get_entity_macro_context(
        entity_candidate_id=selected_entity_candidate_id
    )

    matches_df = get_entity_sanctions_matches(
        entity_candidate_id=selected_entity_candidate_id
    )
    relationship_df = get_entity_relationship_context(
    entity_candidate_id=selected_entity_candidate_id
    )

    child_entities_df = pd.DataFrame()
    same_parent_entities_df = pd.DataFrame()
    parent_path_df = pd.DataFrame()

    if not relationship_df.empty:
        relationship_row = relationship_df.iloc[0]

        selected_lei_for_relationships = relationship_row.get("lei")
        selected_direct_parent_lei = relationship_row.get("direct_parent_lei")

        if selected_lei_for_relationships:
            child_entities_df = get_entity_child_entities(
                lei=selected_lei_for_relationships,
                limit=1000,
            )

        if selected_direct_parent_lei:
            same_parent_entities_df = get_entity_same_parent_entities(
                entity_candidate_id=selected_entity_candidate_id,
                direct_parent_lei=selected_direct_parent_lei,
                limit=1000,
            )

        parent_path_df = get_entity_parent_path(
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

    with st.expander("Relationship Context", expanded=True):
        render_relationship_context(
            relationship_df=relationship_df,
            child_entities_df=child_entities_df,
            same_parent_entities_df=same_parent_entities_df,
            parent_path_df=parent_path_df,
        )

    st.divider()

    with st.expander("Macro Environment Context", expanded=True):
            render_macro_context(macro_context_df)

    st.divider()

    with st.expander("Master Data", expanded=False):
        render_master_data(row)

except Exception as exc:
    st.error("Failed to load entity detail data.")
    st.exception(exc)