from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any
import tomllib

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_api_config() -> dict:
    path = PROJECT_ROOT / "conf" / "api.toml"
    with path.open("rb") as f:
        return tomllib.load(f)


def load_sources() -> dict:
    path = PROJECT_ROOT / "conf" / "sources.toml"
    with path.open("rb") as f:
        return tomllib.load(f)


def load_snowflake_config() -> dict:
    path = PROJECT_ROOT / "conf" / "snowflake.toml"
    with path.open("rb") as f:
        return tomllib.load(f)["connection"]


def build_parser(supported_steps: list[str]) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TAI pipeline CLI")
    sub = parser.add_subparsers(dest="command", required=True)
    run_p = sub.add_parser("run", help="Run pipeline steps.")
    run_p.add_argument("--step", action="append", choices=supported_steps)
    run_p.add_argument("--sources", nargs="*", default=None)
    return parser


def validate_sources(selected: list[str] | None, available: dict[str, Any]) -> list[str]:
    if not available:
        raise ValueError("No sources configured.")
    if not selected:
        return list(available.keys())
    unknown = [s for s in selected if s not in available]
    if unknown:
        raise ValueError(f"Unknown source(s): {', '.join(unknown)}. Available: {', '.join(available)}")
    return list(dict.fromkeys(selected))  # dedup, säilitades järjekorra
