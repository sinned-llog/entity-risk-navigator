# streamlit_app/pages/1_Overview.py

import plotly.express as px
import streamlit as st

from src.queries import (
    get_risk_overview_metrics,
    get_risk_tier_distribution,
    get_top_risk_entities,
)


st.set_page_config(
    page_title="Overview | EntityRisk Navigator",
    page_icon="🧭",
    layout="wide",
)

st.title("Overview")
st.caption("High-level overview of entity risk and sanctions screening results.")

try:
    metrics_df = get_risk_overview_metrics()
    metrics = metrics_df.iloc[0]

    total_entities = int(metrics["total_entities"])
    entities_with_sanctions_match = int(metrics["entities_with_sanctions_match"])
    high_risk_entities = int(metrics["high_risk_entities"])
    medium_risk_entities = int(metrics["medium_risk_entities"])
    review_required_entities = int(metrics["review_required_entities"])
    low_or_no_known_match_entities = int(metrics["low_or_no_known_match_entities"])
    total_sanctions_matches = int(metrics["total_sanctions_matches"])

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total Entities", f"{total_entities:,}")
    col2.metric("Entities with Match", f"{entities_with_sanctions_match:,}")
    col3.metric("Total Sanctions Matches", f"{total_sanctions_matches:,}")
    col4.metric("Low / No Known Match", f"{low_or_no_known_match_entities:,}")

    col5, col6, col7 = st.columns(3)

    col5.metric("High Risk", f"{high_risk_entities:,}")
    col6.metric("Medium Risk", f"{medium_risk_entities:,}")
    col7.metric("Review Required", f"{review_required_entities:,}")

    st.divider()

    st.subheader("Risk Tier Distribution")

    risk_dist_df = get_risk_tier_distribution()

    fig = px.bar(
        risk_dist_df,
        x="risk_tier",
        y="entity_count",
        text="entity_count",
        labels={
            "risk_tier": "Risk Tier",
            "entity_count": "Entity Count",
        },
        title="Entities by Risk Tier",
    )

    fig.update_traces(textposition="outside")
    fig.update_layout(showlegend=False)

    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    st.subheader("Top Risk Entities")

    df = get_top_risk_entities(limit=25)

    display_columns = [
        "lei",
        "legal_name_normalized_latin_display",
        "legal_name_normalized",
        "country",
        "entity_status",
        "registration_status",
        "risk_score",
        "risk_tier",
        "total_sanctions_match_count",
        "high_confidence_match_count",
        "medium_confidence_match_count",
        "review_required_match_count",
        "source_entity_keys",
    ]

    missing_columns = [
    column for column in display_columns if column not in df.columns
    ]
    
    if missing_columns:
        st.warning(
            "Some expected columns are missing from the query result: "
            + ", ".join(missing_columns)
        )
        st.dataframe(df, use_container_width=True)
    else:
        df_display = df[display_columns].rename(
            columns={
                "lei": "LEI",
                "legal_name_normalized_latin_display": "Legal name, Latin",
                "legal_name_normalized": "Legal name, original normalized",
                "country": "Country",
                "entity_status": "Entity status",
                "registration_status": "Registration status",
                "risk_score": "Risk score",
                "risk_tier": "Risk tier",
                "total_sanctions_match_count": "Total matches",
                "high_confidence_match_count": "High",
                "medium_confidence_match_count": "Medium",
                "review_required_match_count": "Review",
                "source_entity_keys": "Source entity keys",
            }
        )
        st.dataframe(df_display, use_container_width=True)

except Exception as exc:
    st.error("Failed to load overview data.")
    st.exception(exc)