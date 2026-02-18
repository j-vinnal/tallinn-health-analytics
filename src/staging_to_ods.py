from __future__ import annotations

import logging
from typing import Any

from src.pipeline_constants import (
    STATUS_FAILED,
    STATUS_SKIPPED,
    STATUS_SUCCESS,
    STEP_STAGING_TO_ODS,
)
from src.snowflake_ops import execute_sql_file, open_snowflake_connection
from src.source_registry import get_source_spec
from src.technical_log import get_next_extract_id, upsert_technical_log


def _get_staging_stats(conn: Any, staging_table: str) -> tuple[int, int | None]:
    with conn.cursor() as cursor:
        cursor.execute(
            f"SELECT COUNT(*) AS row_count, MAX(extract_id) AS max_extract_id FROM {staging_table}"
        )
        row = cursor.fetchone()
        if row is None:
            return 0, None
        row_count = int(row[0])
        max_extract_id = None if row[1] is None else int(row[1])
        return row_count, max_extract_id


def run_staging_to_ods(source_id: str, logger: logging.Logger) -> None:
    source_spec = get_source_spec(source_id)
    sql_paths = source_spec.ods_dml_sql
    staging_table = source_spec.staging_table
    target_table = source_spec.ods_target_tables

    conn = open_snowflake_connection()
    extract_id: int | None = None

    try:
        row_count, max_extract_id = _get_staging_stats(conn, staging_table)

        if row_count == 0:
            logger.warning("No rows found in %s for source: %s", staging_table, source_id)
            extract_id = get_next_extract_id(conn)
            upsert_technical_log(
                conn,
                extract_id,
                STEP_STAGING_TO_ODS,
                source_id,
                None,
                target_table,
                STATUS_SKIPPED,
                None,
            )
            return

        extract_id = int(max_extract_id) if max_extract_id is not None else get_next_extract_id(conn)

        for sql_path in sql_paths:
            execute_sql_file(conn, sql_path)

        upsert_technical_log(
            conn,
            extract_id,
            STEP_STAGING_TO_ODS,
            source_id,
            None,
            target_table,
            STATUS_SUCCESS,
            None,
        )
        logger.info("Loaded: %s -> %s", source_id, target_table)
    except Exception as exc:
        if extract_id is None:
            extract_id = get_next_extract_id(conn)
        upsert_technical_log(
            conn,
            extract_id,
            STEP_STAGING_TO_ODS,
            source_id,
            None,
            target_table,
            STATUS_FAILED,
            str(exc)[:4000],
        )
        raise
    finally:
        conn.close()
