from __future__ import annotations

import logging
from pathlib import Path
import shutil
from typing import Any

from src.utils import PROJECT_ROOT, load_snowflake_config

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


def run_raw_to_staging(
    source_id: str, api_config: dict[str, Any], logger: logging.Logger
) -> None:
    import snowflake.connector

    raw_root = Path(api_config["tai_api"].get("output_path", "data/raw"))
    processed_root = Path(api_config["tai_api"].get("processed_path", "data/processed"))
    snowflake_config = load_snowflake_config()
    conn = snowflake.connector.connect(**snowflake_config)
    
    try:
        files = sorted((raw_root / source_id).glob("*.csv"))
        if not files:
            logger.warning("No CSV file found for source: %s", source_id)
            return
        file_path = files[0]

        sql_path = DML_BY_SOURCE.get(source_id)
        if sql_path is None:
            raise ValueError(f"No staging DML SQL mapping for source: {source_id}")
        table_name = TABLE_BY_SOURCE.get(source_id)
        if table_name is None:
            raise ValueError(f"No staging table mapping for source: {source_id}")

        _run_sql_template(
            conn,
            sql_path,
            {
                "local_file_uri": f"'{file_path.resolve().as_uri()}'",
                "source_file": file_path.name,
            },
        )
        _move_to_processed(file_path, source_id, processed_root)
        logger.info("Loaded: %s -> %s", file_path.name, table_name)
    finally:
        conn.close()
