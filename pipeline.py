from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable

from src.db import get_next_extract_id, snowflake_connection
from src.logger import configure_logging
from src.steps.api_to_raw import run as run_api_to_raw
from src.steps.raw_to_staging import run as run_raw_to_staging
from src.steps.staging_to_ods import run as run_staging_to_ods
from src.utils import build_parser, load_api_config, load_sources, validate_sources

STEPS = ["api_to_raw", "raw_to_staging", "staging_to_ods"]

StepFn = Callable[["RunContext"], None]

STEP_REGISTRY: dict[str, StepFn] = {
    "api_to_raw": run_api_to_raw,
    "raw_to_staging": run_raw_to_staging,
    "staging_to_ods": run_staging_to_ods,
}


@dataclass
class RunContext:
    conn: Any
    extract_id: int
    sources: list[str]
    logger: logging.Logger
    api_config: dict
    sources_config: dict


def main() -> None:
    configure_logging()
    parser = build_parser(STEPS)
    args = parser.parse_args()
    logger = logging.getLogger("pipeline")

    try:
        api_config = load_api_config()
        sources_config = load_sources()
        selected_sources = validate_sources(args.sources, sources_config)
        step_names = args.step or STEPS

        with snowflake_connection() as conn:
            extract_id = get_next_extract_id(conn)
            ctx = RunContext(
                conn=conn,
                extract_id=extract_id,
                sources=selected_sources,
                logger=logger,
                api_config=api_config,
                sources_config=sources_config,
            )
            for step_name in step_names:
                logger.info("Step %s started (extract_id=%s).", step_name, extract_id)
                STEP_REGISTRY[step_name](ctx)
                logger.info("Step %s completed.", step_name)

    except Exception as exc:
        logger.error("Pipeline failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
