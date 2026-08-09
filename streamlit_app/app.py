# streamlit_app/app.py

import streamlit as st


st.set_page_config(
    page_title="EntityRisk Navigator",
    page_icon="🧭",
    layout="wide",
)

st.title("EntityRisk Navigator")
st.caption("MVP Dashboard for entity risk and sanctions screening")

st.markdown("""
Welcome to the EntityRisk Navigator.

This dashboard provides a consolidated view on:

- Legal entity master data
- Sanctions screening matches
- Entity-level risk scores
- Pipeline audit status
- ECB macro context

Use the navigation menu on the left to open the dashboard pages.
""")

st.info(
    "ECB macro indicators are shown as contextual information only "
    "and are not used in the entity risk score."
)