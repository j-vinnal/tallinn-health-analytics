from __future__ import annotations

import logging
from pathlib import Path
import shutil
from typing import Any

from src.utils import PROJECT_ROOT, load_snowflake_config

STEP_RAW_TO_STAGING = "raw_to_staging"
DML_BY_SOURCE = {
    "SR57": PROJECT_ROOT / "sql" / "dml" / "staging" / "staging.load_tai_sr57_st1.sql",
    "PKH2": PROJECT_ROOT / "sql" / "dml" / "staging" / "staging.load_tai_pkh2_st1.sql",
}
TABLE_BY_SOURCE = {
    "SR57": "STAGING.TAI_SR57_ST1",
    "PKH2": "STAGING.TAI_PKH2_ST1",
}


def _run_sql_template(conn: Any, sql_path: Path, params: dict[str, str]) -> None:
    sql_text = sql_path.read_text(encoding="utf-8").format(**params)
    for cursor in conn.execute_string(sql_text):
        cursor.close()


def _move_to_processed(file_path: Path, source_id: str, processed_root: Path) -> Path:
    target_dir = processed_root / source_id
    target_dir.mkdir(parents=True, exist_ok=True)
    moved_to = shutil.move(str(file_path), str(target_dir / file_path.name))
    return Path(moved_to)


def _insert_technical_log(
    conn: Any,
    extract_id: int,
    source_id: str,
    source_file: str | None,
    target_table: str | None,
    status: str,
    error_message: str | None,
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO TECHNICAL.LOG (
                EXTRACT_ID,
                STEP_NAME,
                SOURCE_ID,
                SOURCE_FILE,
                TARGET_TABLE,
                STATUS,
                ERROR_MESSAGE
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                extract_id,
                STEP_RAW_TO_STAGING,
                source_id,
                source_file,
                target_table,
                status,
                error_message,
            ),
        )


def get_next_extract_id() -> int:
    import snowflake.connector

    conn = snowflake.connector.connect(**load_snowflake_config())
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT TECHNICAL.EXTRACT_ID_SEQ.NEXTVAL")
            row = cursor.fetchone()
            if row is None:
                raise ValueError("Failed to fetch extract_id from TECHNICAL.EXTRACT_ID_SEQ")
            return int(row[0])
    finally:
        conn.close()


def run_raw_to_staging(
    source_id: str, extract_id: int, api_config: dict[str, Any], logger: logging.Logger
) -> None:
    import snowflake.connector

    raw_root = Path(api_config["tai_api"].get("output_path", "data/raw"))
    processed_root = Path(api_config["tai_api"].get("processed_path", "data/processed"))
    snowflake_config = load_snowflake_config()
    conn = snowflake.connector.connect(**snowflake_config)
    source_file: str | None = None
    target_table = TABLE_BY_SOURCE.get(source_id)

    try:
        files = sorted((raw_root / source_id).glob("*.csv"))
        if not files:
            logger.warning("No CSV file found for source: %s", source_id)
            _insert_technical_log(
                conn, extract_id, source_id, source_file, target_table, "SKIPPED", None
            )
            return
        file_path = files[0]
        source_file = file_path.name

        sql_path = DML_BY_SOURCE.get(source_id)
        if sql_path is None:
            raise ValueError(f"No staging DML SQL mapping for source: {source_id}")
        if target_table is None:
            raise ValueError(f"No staging table mapping for source: {source_id}")

        _run_sql_template(
            conn,
            sql_path,
            {
                "local_file_uri": f"'{file_path.resolve().as_uri()}'",
                "source_file": file_path.name,
                "extract_id": str(extract_id),
            },
        )
        _move_to_processed(file_path, source_id, processed_root)
        _insert_technical_log(
            conn, extract_id, source_id, source_file, target_table, "SUCCESS", None
        )
        logger.info("Loaded: %s -> %s", file_path.name, target_table)
    except Exception as exc:
        _insert_technical_log(
            conn,
            extract_id,
            source_id,
            source_file,
            target_table,
            "FAILED",
            str(exc)[:4000],
        )
        raise
    finally:
        conn.close()
