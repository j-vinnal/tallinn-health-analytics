from __future__ import annotations

import logging
from typing import Any


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logging.getLogger("snowflake.connector.connection").setLevel(logging.WARNING)


def log(
    conn: Any,
    logger: logging.Logger,
    extract_id: int,
    step_name: str,
    source_id: str,
    status: str,
    *,
    source_file: str | None = None,
    target_table: str | None = None,
    error_message: str | None = None,
) -> None:
    level = logging.ERROR if status == "FAILED" else logging.INFO
    logger.log(level, "[%s] %s | %s: %s", step_name, source_id, target_table or "-", status)
    if error_message:
        logger.error(error_message)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO TECHNICAL.LOG
                (EXTRACT_ID, STEP_NAME, SOURCE_ID, SOURCE_FILE, TARGET_TABLE, STATUS, ERROR_MESSAGE)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (extract_id, step_name, source_id, source_file, target_table, status, error_message),
        )