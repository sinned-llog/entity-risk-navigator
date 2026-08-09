# streamlit_app/src/db.py

import os
import pandas as pd
import streamlit as st

from sqlalchemy import create_engine, text


def get_database_url() -> str:
    """
    Build PostgreSQL connection URL from environment variables.

    Inside Docker Compose, the Postgres host is the service name: postgres.
    """
    user = os.getenv("POSTGRES_USER", "risk_user")
    password = os.getenv("POSTGRES_PASSWORD", "change_me")
    db = os.getenv("POSTGRES_DB", "risk_radar")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")

    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


@st.cache_resource
def get_engine():
    """
    Create a cached SQLAlchemy engine for the Streamlit app.
    """
    database_url = get_database_url()
    return create_engine(database_url, pool_pre_ping=True)


def read_sql(query: str, params: dict | None = None) -> pd.DataFrame:
    """
    Execute a SQL query and return a pandas DataFrame.
    """
    engine = get_engine()

    with engine.connect() as connection:
        return pd.read_sql_query(
            sql=text(query),
            con=connection,
            params=params or {},
        )