from __future__ import annotations

from typing import Any

from src.snowflake_ops import open_snowflake_connection
from src.utils import PROJECT_ROOT

TECHNICAL_SQL_DIR = PROJECT_ROOT / "sql" / "dml" / "technical"


def _read_sql(file_name: str) -> str:
    return (TECHNICAL_SQL_DIR / file_name).read_text(encoding="utf-8")


def _fetch_next_extract_id(conn: Any) -> int:
    with conn.cursor() as cursor:
        cursor.execute(_read_sql("technical.next_extract_id.sql"))
        row = cursor.fetchone()
        if row is None:
            raise ValueError("Failed to fetch extract_id from TECHNICAL.EXTRACT_ID_SEQ")
        return int(row[0])


def get_next_extract_id(conn: Any | None = None) -> int:
    if conn is not None:
        return _fetch_next_extract_id(conn)

    own_conn = open_snowflake_connection()
    try:
        return _fetch_next_extract_id(own_conn)
    finally:
        own_conn.close()


def upsert_technical_log(
    conn: Any,
    extract_id: int,
    step_name: str,
    source_id: str,
    source_file: str | None,
    target_table: str | None,
    status: str,
    error_message: str | None,
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            _read_sql("technical.log_upsert.sql"),
            (
                extract_id,
                step_name,
                source_id,
                source_file,
                target_table,
                status,
                error_message,
            ),
        )
