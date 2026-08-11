# streamlit_app/pages/3_Pipeline_Status.py

import pandas as pd
import streamlit as st

from src.queries import (
    get_pipeline_table_status_metrics,
    get_pipeline_table_health_details,
    get_pipeline_latest_loads,
)


st.set_page_config(
    page_title="Pipeline Status | EntityRisk Navigator",
    page_icon="🧭",
    layout="wide",
)

st.title("Pipeline Status")
st.caption(
    "Table-level pipeline health across raw, staging, and mart layers. "
    "Problem tables are shown first."
)


HEALTH_LABELS = {
    "healthy": "Healthy",
    "latest_failed_previous_success_available": "Warning: latest run failed",
    "not_healthy": "Not healthy",
    "missing_run": "Missing run",
}


def format_int(value):
    if pd.isna(value):
        return "0"
    return f"{int(value):,}"


def format_duration(value):
    if pd.isna(value):
        return "-"
    seconds = int(value)

    if seconds < 60:
        return f"{seconds}s"

    minutes = seconds // 60
    remaining_seconds = seconds % 60

    if minutes < 60:
        return f"{minutes}m {remaining_seconds}s"

    hours = minutes // 60
    remaining_minutes = minutes % 60

    return f"{hours}h {remaining_minutes}m"


def add_health_label(df):
    if df is None or df.empty:
        return df

    df = df.copy()

    if "pipeline_health_status" in df.columns:
        df["health_status_display"] = df["pipeline_health_status"].map(
            HEALTH_LABELS
        ).fillna(df["pipeline_health_status"])

    return df


try:
    metrics_df = get_pipeline_table_status_metrics()

    if metrics_df.empty:
        st.warning("No pipeline status data found.")
        st.stop()

    metrics = metrics_df.iloc[0]

    monitored_tables = int(metrics["monitored_tables"])
    total_runs = int(metrics["total_runs"])
    healthy_tables = int(metrics["healthy_tables"])
    warning_tables = int(metrics["warning_tables"])
    not_healthy_tables = int(metrics["not_healthy_tables"])
    tables_with_failed_latest_run = int(metrics["tables_with_failed_latest_run"])
    missing_run_tables = int(metrics["missing_run_tables"])

    st.subheader("Pipeline Status Overview")

    col1, col2, col3, col4, col5, col6 = st.columns(6)

    col1.metric("Monitored Tables", f"{monitored_tables:,}")
    col2.metric("Total Runs", f"{total_runs:,}")
    col3.metric("Healthy Tables", f"{healthy_tables:,}")
    col4.metric("Warning Tables", f"{warning_tables:,}")
    col5.metric("Not Healthy Tables", f"{not_healthy_tables:,}")
    col6.metric("Missing Run Tables", f"{missing_run_tables:,}")

    if tables_with_failed_latest_run > 0:
        st.warning(
            f"{tables_with_failed_latest_run:,} table(s) have a failed or incomplete latest run."
        )
    else:
        st.success("No failed or incomplete latest table runs detected.")

    st.divider()

    st.subheader("Table Health Details")
    st.caption(
        "One row per target table. Failed and warning tables are shown first."
    )

    table_health_df = get_pipeline_table_health_details()
    table_health_df = add_health_label(table_health_df)

    if table_health_df.empty:
        st.info("No table-level pipeline details found.")
    else:
        table_health_df = table_health_df.copy()

        if "duration_seconds" in table_health_df.columns:
            table_health_df["duration_display"] = table_health_df[
                "duration_seconds"
            ].apply(format_duration)

        display_columns = [
            "pipeline_layer",
            "target_table",
            "job_name",
            "job_type",
            "status",
            "health_status_display",
            "is_successful_latest_run",
            "is_failed_or_incomplete_latest_run",
            "total_run_count",
            "success_run_count",
            "failed_or_incomplete_run_count",
            "rows_read",
            "rows_inserted",
            "started_at",
            "finished_at",
            "duration_display",
            "last_success_at",
            "last_failure_at",
            "effective_load_date",
            "error_message",
        ]

        existing_columns = [
            column for column in display_columns
            if column in table_health_df.columns
        ]

        df_display = table_health_df[existing_columns].rename(
            columns={
                "pipeline_layer": "Layer",
                "target_table": "Table",
                "job_name": "Job",
                "job_type": "Job type",
                "status": "Latest status",
                "health_status_display": "Health",
                "is_successful_latest_run": "Latest successful",
                "is_failed_or_incomplete_latest_run": "Latest failed / incomplete",
                "total_run_count": "Total runs",
                "success_run_count": "Successful runs",
                "failed_or_incomplete_run_count": "Failed / incomplete runs",
                "rows_read": "Rows read",
                "rows_inserted": "Rows inserted",
                "started_at": "Started at",
                "finished_at": "Finished at",
                "duration_display": "Duration",
                "last_success_at": "Last success at",
                "last_failure_at": "Last failure at",
                "effective_load_date": "Effective load date",
                "error_message": "Error message",
            }
        )

        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True,
        )

    st.divider()

    st.subheader("Latest Loads")
    st.caption(
        "Detailed latest load and run metadata from the pipeline audit mart."
    )

    latest_loads_df = get_pipeline_latest_loads()
    latest_loads_df = add_health_label(latest_loads_df)

    if latest_loads_df.empty:
        st.info("No latest load details found.")
        st.stop()

    latest_loads_df = latest_loads_df.copy()

    if "duration_seconds" in latest_loads_df.columns:
        latest_loads_df["duration_display"] = latest_loads_df[
            "duration_seconds"
        ].apply(format_duration)

    latest_columns = [
        "job_name",
        "job_type",
        "source",
        "target_system",
        "target_table",
        "app_env",
        "status",
        "health_status_display",
        "is_successful_latest_run",
        "is_failed_or_incomplete_latest_run",
        "started_at",
        "finished_at",
        "duration_display",
        "effective_load_date",
        "rows_read",
        "rows_inserted",
        "total_run_count",
        "success_run_count",
        "failed_or_incomplete_run_count",
        "last_success_at",
        "last_failure_at",
        "has_successful_run",
        "error_message",
        "mart_loaded_at",
    ]

    existing_latest_columns = [
        column for column in latest_columns
        if column in latest_loads_df.columns
    ]

    latest_display = latest_loads_df[existing_latest_columns].rename(
        columns={
            "job_name": "Job",
            "job_type": "Job type",
            "source": "Source",
            "target_system": "Target system",
            "target_table": "Target table",
            "app_env": "Environment",
            "status": "Latest status",
            "health_status_display": "Health",
            "is_successful_latest_run": "Latest successful",
            "is_failed_or_incomplete_latest_run": "Latest failed / incomplete",
            "started_at": "Started at",
            "finished_at": "Finished at",
            "duration_display": "Duration",
            "effective_load_date": "Effective load date",
            "rows_read": "Rows read",
            "rows_inserted": "Rows inserted",
            "total_run_count": "Total runs",
            "success_run_count": "Successful runs",
            "failed_or_incomplete_run_count": "Failed / incomplete runs",
            "last_success_at": "Last success at",
            "last_failure_at": "Last failure at",
            "has_successful_run": "Has successful run",
            "error_message": "Error message",
            "mart_loaded_at": "Mart loaded at",
        }
    )

    st.dataframe(
        latest_display,
        use_container_width=True,
        hide_index=True,
    )

except Exception as exc:
    st.error("Failed to load pipeline status data.")
    st.exception(exc)