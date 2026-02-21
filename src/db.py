from __future__ import annotations

from contextlib import contextmanager
from typing import Generator, Any

import snowflake.connector

from src.utils import load_snowflake_config


@contextmanager
def snowflake_connection() -> Generator:
    conn = snowflake.connector.connect(**load_snowflake_config())
    try:
        yield conn
    finally:
        conn.close()


def get_next_extract_id(conn: Any) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT TECHNICAL.EXTRACT_ID_SEQ.NEXTVAL")
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("Failed to fetch extract_id from TECHNICAL.EXTRACT_ID_SEQ")
        return int(row[0])