from __future__ import annotations

import argparse
import logging
from typing import Callable

from src.pipeline_constants import (
    STEP_API_TO_RAW,
    STEP_RAW_TO_STAGING,
    STEP_STAGING_TO_ODS,
    SUPPORTED_STEPS,
)
from src.extract_from_api import fetch_data_to_file
from src.raw_to_staging import run_raw_to_staging
from src.staging_to_ods import run_staging_to_ods
from src.technical_log import get_next_extract_id
from src.utils import (
    build_parser,
    configure_logging,
    load_api_config,
    validate_sources,
)

StepFn = Callable[[argparse.Namespace, logging.Logger], None]


def _resolve_selected_sources(args: argparse.Namespace) -> tuple[dict, list[str]]:
    config = load_api_config()
    sources = config["sources"]
    selected_sources = validate_sources(args.sources, sources)
    return config, selected_sources


def step_api_to_raw(args: argparse.Namespace, logger: logging.Logger) -> None:
    config, selected_sources = _resolve_selected_sources(args)
    sources = config["sources"]

    logger.info("Source(s): %d (%s)", len(selected_sources), ", ".join(selected_sources))

    for source_id in selected_sources:
        logger.info("Saved: %s", fetch_data_to_file(source_id, config, sources[source_id]))


def step_raw_to_staging(args: argparse.Namespace, logger: logging.Logger) -> None:
    config, selected_sources = _resolve_selected_sources(args)
    extract_id = get_next_extract_id()

    logger.info("Source(s): %d (%s)", len(selected_sources), ", ".join(selected_sources))
    
    for source_id in selected_sources:
        run_raw_to_staging(source_id, extract_id, config, logger)


def step_staging_to_ods(args: argparse.Namespace, logger: logging.Logger) -> None:
    _, selected_sources = _resolve_selected_sources(args)

    logger.info("Source(s): %d (%s)", len(selected_sources), ", ".join(selected_sources))

    for source_id in selected_sources:
        run_staging_to_ods(source_id, logger)


def run_steps(args: argparse.Namespace, logger: logging.Logger) -> None:
    step_registry: dict[str, StepFn] = {
        STEP_API_TO_RAW: step_api_to_raw,
        STEP_RAW_TO_STAGING: step_raw_to_staging,
        STEP_STAGING_TO_ODS: step_staging_to_ods,
    }

    step_names = args.step or SUPPORTED_STEPS
    for step_name in step_names:
        step_fn = step_registry.get(step_name)
        if step_fn is None:
            raise ValueError(f"Unsupported step: {step_name}")
        logger.info("Step %s started.", step_name)
        step_fn(args, logger)
        logger.info("Step %s completed.", step_name)


def main() -> None:
    configure_logging()
    parser = build_parser(SUPPORTED_STEPS)
    args = parser.parse_args()
    logger = logging.getLogger("pipeline")

    try:
        if args.command == "run":
            run_steps(args, logger)
            return
        parser.error(f"Unsupported command: {args.command}")
    except Exception as exc:
        logger.error("Pipeline failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
