from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any
import tomllib

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_API_CONFIG_PATH = PROJECT_ROOT / "conf" / "api_sources.toml"


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
        required=True,
        choices=supported_steps,
        help="Step to run. Repeat to run multiple steps in order.",
    )
    run_parser.add_argument(
        "--sources",
        nargs="*",
        default=None,
        help="Optional source IDs for api_to_raw (e.g., SR57 PKH2).",
    )

    return parser


def load_api_config(config_path: Path | None = None) -> dict:
    path = config_path or DEFAULT_API_CONFIG_PATH
    with path.open("rb") as handle:
        return tomllib.load(handle)


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
