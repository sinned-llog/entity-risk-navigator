import pandas as pd
import streamlit as st
import altair as alt

from src.queries import (
    get_ecb_macro_indicator_options,
    get_ecb_macro_timeseries,
    get_ecb_macro_latest_observations,
    get_ecb_macro_pressure_summary,
    get_ecb_macro_pressure_indicators,
)


# -------------------------------------------------------------------
# Page configuration
# -------------------------------------------------------------------

st.set_page_config(
    page_title="Macro Environment",
    layout="wide",
)

st.title("Macro Environment")

st.caption(
    "ECB macro time series are used as contextual indicators for the current "
    "macro-economic environment. They are not part of the current entity "
    "sanctions risk score."
)


# -------------------------------------------------------------------
# Cached data loading
# -------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_indicator_options():
    return get_ecb_macro_indicator_options()


@st.cache_data(ttl=300)
def load_timeseries(indicator_code):
    return get_ecb_macro_timeseries(indicator_code=indicator_code)


@st.cache_data(ttl=300)
def load_latest_observations():
    return get_ecb_macro_latest_observations()


@st.cache_data(ttl=300)
def load_macro_pressure_summary():
    return get_ecb_macro_pressure_summary()


@st.cache_data(ttl=300)
def load_macro_pressure_indicators():
    return get_ecb_macro_pressure_indicators()


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def format_number(value, decimals=2):
    if pd.isna(value):
        return "n/a"

    return f"{value:.{decimals}f}"


def format_date(value):
    if pd.isna(value):
        return "n/a"

    return pd.to_datetime(value).date().isoformat()


def format_level(value):
    if pd.isna(value):
        return "n/a"

    return str(value).replace("_", " ").title()


def render_pressure_badge(level):
    if pd.isna(level):
        st.info("Macro pressure level is not available.")
        return

    normalized_level = str(level).lower()

    if normalized_level in {"critical", "high"}:
        st.error(f"Macro Pressure Level: {format_level(level)}")
    elif normalized_level in {"elevated"}:
        st.warning(f"Macro Pressure Level: {format_level(level)}")
    elif normalized_level in {"moderate"}:
        st.info(f"Macro Pressure Level: {format_level(level)}")
    else:
        st.success(f"Macro Pressure Level: {format_level(level)}")


# -------------------------------------------------------------------
# Load data
# -------------------------------------------------------------------

indicator_options_df = load_indicator_options()
latest_df = load_latest_observations()
pressure_summary_df = load_macro_pressure_summary()
pressure_indicators_df = load_macro_pressure_indicators()


if indicator_options_df.empty:
    st.warning("No ECB macro time series data available.")
    st.stop()


# -------------------------------------------------------------------
# Macro pressure assessment
# -------------------------------------------------------------------

st.subheader("Macro Pressure Assessment")

if pressure_summary_df.empty:
    st.info("No macro pressure score available yet.")
else:
    pressure = pressure_summary_df.iloc[0]

    macro_pressure_score = pressure.get("macro_pressure_score")
    macro_pressure_level = pressure.get("macro_pressure_level")
    macro_trend_direction = pressure.get("macro_trend_direction")
    latest_obs_date = pressure.get("latest_obs_date")
    source_load_date = pressure.get("source_load_date")
    macro_pressure_summary = pressure.get("macro_pressure_summary")

    render_pressure_badge(macro_pressure_level)

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Macro Pressure Score",
        format_number(macro_pressure_score, decimals=2),
    )

    col2.metric(
        "Pressure Level",
        format_level(macro_pressure_level),
    )

    col3.metric(
        "Trend Direction",
        format_level(macro_trend_direction),
    )

    col4.metric(
        "Latest ECB Observation",
        format_date(latest_obs_date),
    )

    if macro_pressure_summary:
        st.info(macro_pressure_summary)

    if pd.notna(source_load_date):
        st.caption(
            f"ECB source load date: {format_date(source_load_date)}"
        )


# -------------------------------------------------------------------
# Macro pressure indicator details
# -------------------------------------------------------------------

if not pressure_indicators_df.empty:
    with st.expander("Indicator contribution details", expanded=False):
        indicator_display = pressure_indicators_df.rename(
            columns={
                "indicator_code": "Indicator code",
                "indicator_name": "Indicator",
                "latest_obs_date": "Latest observation date",
                "latest_obs_value": "Latest value",
                "current_level_score": "Current level score",
                "momentum_score": "Momentum score",
                "trend_projection_score": "Trend projection score",
                "indicator_pressure_score": "Indicator pressure score",
                "indicator_trend_direction": "Trend direction",
                "indicator_weight": "Weight",
                "unit": "Unit",
                "frequency": "Frequency",
                "reference_area_name": "Reference area",
            }
        )

        st.dataframe(
            indicator_display,
            use_container_width=True,
            hide_index=True,
        )


# -------------------------------------------------------------------
# Latest ECB indicators
# -------------------------------------------------------------------

st.divider()

st.subheader("Latest ECB Indicators")

if latest_df.empty:
    st.info("No latest ECB observations available.")
else:
    latest_display_columns = [
        "indicator_name",
        "latest_obs_date",
        "latest_obs_value",
        "previous_obs_value",
        "change_abs",
        "change_pct",
        "frequency",
        "unit",
        "reference_area_name",
        "source_load_date",
    ]

    existing_latest_columns = [
        column for column in latest_display_columns
        if column in latest_df.columns
    ]

    latest_display = latest_df[existing_latest_columns].rename(
        columns={
            "indicator_name": "Indicator",
            "latest_obs_date": "Latest observation date",
            "latest_obs_value": "Latest value",
            "previous_obs_value": "Previous value",
            "change_abs": "Change abs",
            "change_pct": "Change pct",
            "frequency": "Frequency",
            "unit": "Unit",
            "reference_area_name": "Reference area",
            "source_load_date": "Source load date",
        }
    )

    st.dataframe(
        latest_display,
        use_container_width=True,
        hide_index=True,
    )


# -------------------------------------------------------------------
# Time series explorer
# -------------------------------------------------------------------

st.divider()

st.subheader("Time Series Explorer")

indicator_label_map = {
    row["indicator_name"]: row["indicator_code"]
    for _, row in indicator_options_df.iterrows()
}

selected_indicator_name = st.selectbox(
    "Select ECB indicator",
    options=list(indicator_label_map.keys()),
)

selected_indicator_code = indicator_label_map[selected_indicator_name]

timeseries_df = load_timeseries(
    indicator_code=selected_indicator_code,
)

if timeseries_df.empty:
    st.info("No observations found for selected indicator.")
    st.stop()

timeseries_df = timeseries_df.copy()
timeseries_df["obs_date"] = pd.to_datetime(timeseries_df["obs_date"])

timeseries_df = timeseries_df.sort_values("obs_date")

latest_row = timeseries_df.iloc[-1]

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Latest value",
    format_number(latest_row.get("obs_value"), decimals=4),
)

col2.metric(
    "Latest change",
    format_number(latest_row.get("change_abs"), decimals=4),
)

col3.metric(
    "Latest date",
    format_date(latest_row.get("obs_date")),
)

col4.metric(
    "Observations",
    f"{len(timeseries_df):,}",
)


# -------------------------------------------------------------------
# Chart
# -------------------------------------------------------------------

chart_base = alt.Chart(timeseries_df).encode(
    x=alt.X("obs_date:T", title="Observation date"),
)

line_chart = chart_base.mark_line().encode(
    y=alt.Y("obs_value:Q", title="Observation value"),
    tooltip=[
        alt.Tooltip("obs_date:T", title="Date"),
        alt.Tooltip("obs_value:Q", title="Value"),
        alt.Tooltip("change_abs:Q", title="Change abs"),
        alt.Tooltip("rolling_avg_3_obs:Q", title="Rolling avg 3 obs"),
        alt.Tooltip("rolling_avg_6_obs:Q", title="Rolling avg 6 obs"),
        alt.Tooltip("rolling_avg_12_obs:Q", title="Rolling avg 12 obs"),
    ],
)

rolling_layers = [line_chart]

if "rolling_avg_6_obs" in timeseries_df.columns:
    rolling_chart = chart_base.mark_line(
        strokeDash=[5, 5],
        color="orange",
    ).encode(
        y=alt.Y("rolling_avg_6_obs:Q", title="Observation value"),
        tooltip=[
            alt.Tooltip("obs_date:T", title="Date"),
            alt.Tooltip("rolling_avg_6_obs:Q", title="Rolling avg 6 obs"),
        ],
    )

    rolling_layers.append(rolling_chart)

st.altair_chart(
    alt.layer(*rolling_layers).properties(
        height=420,
        title=selected_indicator_name,
    ),
    use_container_width=True,
)


# -------------------------------------------------------------------
# Observation data
# -------------------------------------------------------------------

with st.expander("Observation data", expanded=False):
    observation_columns = [
        "obs_date",
        "obs_value",
        "previous_obs_value",
        "change_abs",
        "change_pct",
        "rolling_avg_3_obs",
        "rolling_avg_6_obs",
        "rolling_avg_12_obs",
        "frequency",
        "unit",
        "reference_area_name",
        "source_load_date",
    ]

    existing_observation_columns = [
        column for column in observation_columns
        if column in timeseries_df.columns
    ]

    display_df = timeseries_df[existing_observation_columns].rename(
        columns={
            "obs_date": "Observation date",
            "obs_value": "Value",
            "previous_obs_value": "Previous value",
            "change_abs": "Change abs",
            "change_pct": "Change pct",
            "rolling_avg_3_obs": "Rolling avg 3 obs",
            "rolling_avg_6_obs": "Rolling avg 6 obs",
            "rolling_avg_12_obs": "Rolling avg 12 obs",
            "frequency": "Frequency",
            "unit": "Unit",
            "reference_area_name": "Reference area",
            "source_load_date": "Source load date",
        }
    )

    st.dataframe(
        display_df.sort_values("Observation date", ascending=False),
        use_container_width=True,
        hide_index=True,
    )