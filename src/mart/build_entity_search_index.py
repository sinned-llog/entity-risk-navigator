import os
import re
from datetime import datetime, timezone

import pandas as pd
from unidecode import unidecode

from scr.common.postgres_client import PostgresClient


CHUNKSIZE = int(os.getenv("ENTITY_SEARCH_CHUNKSIZE", "50000"))


def to_latin_display(value: object) -> str:
    """
    Converts a text value into a Latin-script display approximation.

    Important:
    - This is only for dashboard display/search usability.
    - Do not use this for sanctions matching, risk scoring, or audit logic.
    - Original source values remain unchanged.
    """
    if value is None:
        return ""

    text_value = str(value).strip()

    if not text_value:
        return ""

    transliterated = unidecode(text_value).strip()

    return transliterated if transliterated else text_value


def to_search_key(value: object) -> str:
    """
    Converts a value into a compact Latin search key.

    Example:
    - "Bei Jing" -> "beijing"
    - "2RIVERS PTE. LTD." -> "2riverspteltd"

    This is only for dashboard search convenience.
    """
    latin_value = to_latin_display(value).lower().strip()
    return re.sub(r"[^a-z0-9]+", "", latin_value)


def prepare_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds dashboard-only Latin display and search helper columns.
    """
    df = df.copy()

    df["legal_name_latin_display"] = df["legal_name"].apply(to_latin_display)
    df["legal_name_normalized_latin_display"] = df[
        "legal_name_normalized"
    ].apply(to_latin_display)

    df["legal_name_latin_search_key"] = df["legal_name"].apply(to_search_key)
    df["legal_name_normalized_latin_search_key"] = df[
        "legal_name_normalized"
    ].apply(to_search_key)

    df["search_index_loaded_at"] = datetime.now(timezone.utc)

    return df


def build_entity_search_index() -> None:
    read_db = PostgresClient.from_env()
    write_db = PostgresClient.from_env()

    source_query = """
        select
            m.entity_candidate_id,
            m.lei,
            m.legal_name,
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
    """

    ddl_init = """
        create schema if not exists marts;

        create extension if not exists pg_trgm;

        drop table if exists marts.mart_entity_search_new;

        create table marts.mart_entity_search_new (
            entity_candidate_id text,
            lei text,
            legal_name text,
            legal_name_normalized text,
            country text,
            entity_status text,
            registration_status text,
            legal_jurisdiction text,

            risk_score integer,
            risk_tier text,
            total_sanctions_match_count bigint,
            high_confidence_match_count bigint,
            medium_confidence_match_count bigint,
            review_required_match_count bigint,

            legal_name_latin_display text,
            legal_name_normalized_latin_display text,
            legal_name_latin_search_key text,
            legal_name_normalized_latin_search_key text,

            search_index_loaded_at timestamp with time zone
        );
    """

    ddl_finalize = """
        create unique index idx_mart_entity_search_new_entity_candidate_id
            on marts.mart_entity_search_new (entity_candidate_id);

        create index idx_mart_entity_search_new_lei
            on marts.mart_entity_search_new (lei);

        create index idx_mart_entity_search_new_legal_name_trgm
            on marts.mart_entity_search_new
            using gin (legal_name gin_trgm_ops);

        create index idx_mart_entity_search_new_legal_name_norm_trgm
            on marts.mart_entity_search_new
            using gin (legal_name_normalized gin_trgm_ops);

        create index idx_mart_entity_search_new_legal_name_latin_trgm
            on marts.mart_entity_search_new
            using gin (legal_name_latin_display gin_trgm_ops);

        create index idx_mart_entity_search_new_legal_name_norm_latin_trgm
            on marts.mart_entity_search_new
            using gin (legal_name_normalized_latin_display gin_trgm_ops);

        create index idx_mart_entity_search_new_legal_name_latin_key
            on marts.mart_entity_search_new (legal_name_latin_search_key);

        create index idx_mart_entity_search_new_legal_name_norm_latin_key
            on marts.mart_entity_search_new (legal_name_normalized_latin_search_key);

        drop table if exists marts.mart_entity_search;

        alter table marts.mart_entity_search_new
            rename to mart_entity_search;

        analyze marts.mart_entity_search;
    """

    read_db.connect()
    write_db.connect()

    total_rows = 0

    try:
        with write_db.transaction():
            write_db.execute(ddl_init, commit=False)

            chunks = pd.read_sql_query(
                sql=source_query,
                con=read_db.conn,
                chunksize=CHUNKSIZE,
            )

            for chunk in chunks:
                prepared_chunk = prepare_chunk(chunk)

                # Convert pandas NaN/NaT values to real SQL NULL values for COPY.
                prepared_chunk = prepared_chunk.where(
                    pd.notna(prepared_chunk),
                    None,
                )

                columns = list(prepared_chunk.columns)
                rows = [
                    tuple(row)
                    for row in prepared_chunk.itertuples(index=False, name=None)
                ]

                inserted_rows = write_db.copy_rows(
                    table_name="marts.mart_entity_search_new",
                    columns=columns,
                    rows=rows,
                    commit=False,
                )

                total_rows += inserted_rows
                print(
                    f"Inserted {total_rows:,} rows into "
                    "marts.mart_entity_search_new..."
                )

            print("Building indexes and replacing marts.mart_entity_search...")
            write_db.execute(ddl_finalize, commit=False)

        print(f"Finished marts.mart_entity_search with {total_rows:,} rows")

    finally:
        read_db.close()
        write_db.close()


if __name__ == "__main__":
    build_entity_search_index()