from __future__ import annotations

from pathlib import Path
from typing import Any

from src.utils import load_snowflake_config


def open_snowflake_connection() -> Any:
    import snowflake.connector

    return snowflake.connector.connect(**load_snowflake_config())


def execute_sql_file(
    conn: Any, sql_path: Path, params: dict[str, str] | None = None
) -> None:
    sql_text = sql_path.read_text(encoding="utf-8")
    if params is not None:
        sql_text = sql_text.format(**params)
    for cursor in conn.execute_string(sql_text):
        cursor.close()
