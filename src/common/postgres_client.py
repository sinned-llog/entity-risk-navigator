import os
import psycopg2
from psycopg2.extras import execute_values
from typing import Sequence, Any
from io import StringIO


class PostgresClient:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = None

    @classmethod
    def from_env(cls) -> "PostgresClient":
        return cls(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )

    def connect(self) -> None:
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password,
            )

    def close(self) -> None:
        if self.conn is not None and not self.conn.closed:
            self.conn.close()

    def execute(
        self,
        sql: str,
        params: tuple | None = None,
        commit: bool = True,
    ) -> None:
        self.connect()

        with self.conn.cursor() as cursor:
            cursor.execute(sql, params)

        if commit:
            self.conn.commit()

    def fetch_all(
        self,
        sql: str,
        params: tuple | None = None,
    ) -> list:
        self.connect()

        with self.conn.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

    def fetch_one(
        self,
        sql: str,
        params: tuple | None = None,
    ) -> tuple | None:
        self.connect()

        with self.conn.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone()

    def execute_values(
        self,
        sql: str,
        rows: Sequence[tuple],
        page_size: int = 1000,
        commit: bool = True,
    ) -> int:
        if not rows:
            return 0

        self.connect()

        with self.conn.cursor() as cursor:
            execute_values(
                cursor,
                sql,
                rows,
                page_size=page_size,
            )

        if commit:
            self.conn.commit()

        return len(rows)

    def ensure_schemas(self) -> None:
        self.execute(
            """
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE SCHEMA IF NOT EXISTS staging;
            CREATE SCHEMA IF NOT EXISTS mart;
            CREATE SCHEMA IF NOT EXISTS audit;
            """
        )

    def copy_rows(
        self,
        table_name: str,
        columns: list[str],
        rows: list[tuple],
        null_marker: str = "\\N",
    ) -> int:
        if not rows:
            return 0

        self.connect()

        buffer = StringIO()

        # Performantes Erzeugen von TSV-Zeilen (Tab-Separated Values)
        for row in rows:
            formatted_values = []
            for val in row:
                if val is None:
                    formatted_values.append(null_marker)
                else:
                    # Strings für Postgres Text-Format escapen (\, \t, \n, \r)
                    val_str = (
                        str(val)
                        .replace("\\", "\\\\")
                        .replace("\t", "\\t")
                        .replace("\n", "\\n")
                        .replace("\r", "\\r")
                    )
                    formatted_values.append(val_str)
            
            buffer.write("\t".join(formatted_values) + "\n")

        buffer.seek(0)
        columns_sql = ", ".join(columns)

        # Schnelles Postgres TEXT COPY (ohne CSV-Overhead)
        copy_sql = f"""
            COPY {table_name} ({columns_sql})
            FROM STDIN
            WITH (
                FORMAT text,
                NULL '{null_marker}'
            )
        """

        with self.conn.cursor() as cursor:
            cursor.copy_expert(copy_sql, buffer)

        self.conn.commit()

        return len(rows)