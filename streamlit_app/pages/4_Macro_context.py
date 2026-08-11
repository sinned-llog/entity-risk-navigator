# streamlit_app/pages/4_Macro_Context.py

import pandas as pd
import streamlit as st

from src.queries import get_ecb_macro_context


st.set_page_config(
    page_title="Macro Context | EntityRisk Navigator",
    page_icon="🧭",
    layout="wide",
)

st.title("Macro Context")
st.caption(
    "ECB macro indicators for dashboard context only. "
    "These values are not used in entity risk scoring."
)


def format_number(value, decimals=4):
    if pd.isna(value):
        return "-"
    return f"{float(value):,.{decimals}f}"


def format_pct(value):
    if pd.isna(value):
        return "-"
    return f"{float(value) * 100:,.2f}%"


def format_change(value):
    if pd.isna(value):
        return "-"
    number = float(value)
    sign = "+" if number > 0 else ""
    return f"{sign}{number:,.4f}"


try:
    df = get_ecb_macro_context()

    if df.empty:
        st.warning("No ECB macro context data found.")
        st.stop()

    df = df.copy()

    st.subheader("Latest ECB Indicators")

    for _, row in df.iterrows():
        indicator_name = row.get("indicator_name") or "-"
        latest_value = row.get("latest_obs_value")
        latest_date = row.get("latest_obs_date")
        unit = row.get("unit") or "-"
        change_abs = row.get("latest_change_abs")
        change_pct = row.get("latest_change_pct")
        previous_value = row.get("previous_obs_value")
        previous_date = row.get("previous_obs_date")

        with st.container(border=True):
            st.markdown(f"### {indicator_name}")

            col1, col2, col3, col4 = st.columns(4)

            col1.metric(
                "Latest value",
                format_number(latest_value),
                delta=format_change(change_abs),
            )

            col2.metric(
                "Latest change %",
                format_pct(change_pct),
            )

            col3.metric(
                "Previous value",
                format_number(previous_value),
            )

            col4.metric(
                "Observations",
                f"{int(row.get('observation_count') or 0):,}",
            )

            st.caption(
                f"Latest date: {latest_date} | "
                f"Previous date: {previous_date} | "
                f"Unit: {unit} | "
                f"Reference area: {row.get('reference_area_name') or row.get('reference_area') or '-'}"
            )

    st.divider()

    st.subheader("ECB Macro Context Table")

    display_columns = [
        "display_order",
        "indicator_name",
        "reference_area_name",
        "frequency",
        "unit",
        "latest_obs_date",
        "latest_time_period",
        "latest_obs_value",
        "previous_obs_date",
        "previous_obs_value",
        "latest_change_abs",
        "latest_change_pct",
        "observation_count",
        "source_load_date",
        "source_url",
        "mart_loaded_at",
    ]

    existing_columns = [
        column for column in display_columns if column in df.columns
    ]

    df_display = df[existing_columns].rename(
        columns={
            "display_order": "Display order",
            "indicator_name": "Indicator",
            "reference_area_name": "Reference area",
            "frequency": "Frequency",
            "unit": "Unit",
            "latest_obs_date": "Latest date",
            "latest_time_period": "Latest period",
            "latest_obs_value": "Latest value",
            "previous_obs_date": "Previous date",
            "previous_obs_value": "Previous value",
            "latest_change_abs": "Change abs",
            "latest_change_pct": "Change %",
            "observation_count": "Observations",
            "source_load_date": "Source load date",
            "source_url": "Source URL",
            "mart_loaded_at": "Mart loaded at",
        }
    )

    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
    )

except Exception as exc:
    st.error("Failed to load ECB macro context data.")
    st.exception(exc)