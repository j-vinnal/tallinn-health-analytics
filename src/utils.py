from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any
import tomllib

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_API_CONFIG_PATH = PROJECT_ROOT / "conf" / "api_sources.toml"
DEFAULT_SNOWFLAKE_CONFIG_PATH = PROJECT_ROOT / "conf" / "snowflake.toml"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def build_parser(supported_steps: list[str]) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TAI pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run one or more pipeline steps.")
    run_parser.add_argument(
        "--step",
        action="append",
        choices=supported_steps,
        help="Step to run. Repeat to run multiple steps in order. If omitted, runs all steps.",
    )
    run_parser.add_argument(
        "--sources",
        nargs="*",
        default=None,
        help="Optional source IDs for api_to_raw/raw_to_staging (e.g., SR57 PKH2).",
    )

    return parser


def load_api_config(config_path: Path | None = None) -> dict:
    path = config_path or DEFAULT_API_CONFIG_PATH
    with path.open("rb") as handle:
        return tomllib.load(handle)


def load_snowflake_config(config_path: Path | None = None) -> dict:
    path = config_path or DEFAULT_SNOWFLAKE_CONFIG_PATH
    with path.open("rb") as handle:
        config = tomllib.load(handle)
    return config["connection"]


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


def insert_technical_log(
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
                step_name,
                source_id,
                source_file,
                target_table,
                status,
                error_message,
            ),
        )


def validate_sources(
    selected: list[str] | None, available: dict[str, Any]
) -> list[str]:
    if not available:
        raise ValueError("No sources configured.")

    if not selected:
        return list(available.keys())

    unknown = [source_id for source_id in selected if source_id not in available]
    if unknown:
        known = ", ".join(available.keys())
        missing = ", ".join(unknown)
        raise ValueError(f"Unknown source(s): {missing}. Available sources: {known}")

    seen: set[str] = set()
    resolved: list[str] = []
    for source_id in selected:
        if source_id in seen:
            continue
        seen.add(source_id)
        resolved.append(source_id)

    return resolved
