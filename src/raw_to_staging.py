from __future__ import annotations

import logging
from pathlib import Path
import shutil
from typing import Any

from src.pipeline_constants import (
    STATUS_FAILED,
    STATUS_SKIPPED,
    STATUS_SUCCESS,
    STEP_RAW_TO_STAGING,
)
from src.source_registry import get_source_spec
from src.snowflake_ops import execute_sql_file, open_snowflake_connection
from src.technical_log import upsert_technical_log


def _move_to_processed(file_path: Path, source_id: str, processed_root: Path) -> Path:
    target_dir = processed_root / source_id
    target_dir.mkdir(parents=True, exist_ok=True)
    moved_to = shutil.move(str(file_path), str(target_dir / file_path.name))
    return Path(moved_to)


def run_raw_to_staging(
    source_id: str, extract_id: int, api_config: dict[str, Any], logger: logging.Logger
) -> None:
    raw_root = Path(api_config["tai_api"].get("output_path", "data/raw"))
    processed_root = Path(api_config["tai_api"].get("processed_path", "data/processed"))
    conn = open_snowflake_connection()
    source_file: str | None = None
    source_spec = get_source_spec(source_id)
    target_table = source_spec.staging_table

    try:
        files = sorted((raw_root / source_id).glob("*.csv"))
        if not files:
            logger.warning("No CSV file found for source: %s", source_id)
            upsert_technical_log(
                conn,
                extract_id,
                STEP_RAW_TO_STAGING,
                source_id,
                source_file,
                target_table,
                STATUS_SKIPPED,
                None,
            )
            return
        file_path = files[0] # todo: handle multiple files if needed
        source_file = file_path.name

        execute_sql_file(
            conn,
            source_spec.staging_dml_sql,
            {
                "local_file_uri": f"'{file_path.resolve().as_uri()}'",
                "gz_name": f"{file_path.name}.gz",
                "extract_id": str(extract_id),
            },
        )
        _move_to_processed(file_path, source_id, processed_root)
        upsert_technical_log(
            conn,
            extract_id,
            STEP_RAW_TO_STAGING,
            source_id,
            source_file,
            target_table,
            STATUS_SUCCESS,
            None,
        )
        logger.info("Loaded: %s -> %s", file_path.name, target_table)
    except Exception as exc:
        upsert_technical_log(
            conn,
            extract_id,
            STEP_RAW_TO_STAGING,
            source_id,
            source_file,
            target_table,
            STATUS_FAILED,
            str(exc)[:4000],
        )
        raise
    finally:
        conn.close()
