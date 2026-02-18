from __future__ import annotations

import argparse
import logging
from typing import Callable

from src.extract_from_api import fetch_data_to_file
from src.raw_to_staging import run_raw_to_staging
from src.utils import (
    build_parser,
    configure_logging,
    get_next_extract_id,
    load_api_config,
    validate_sources,
)

STEP_API_TO_RAW = "api_to_raw"
STEP_RAW_TO_STAGING = "raw_to_staging"
SUPPORTED_STEPS = [STEP_API_TO_RAW, STEP_RAW_TO_STAGING]

StepFn = Callable[[argparse.Namespace, logging.Logger], None]


def step_api_to_raw(args: argparse.Namespace, logger: logging.Logger) -> None:
    config = load_api_config()
    sources = config["sources"]
    selected_sources = validate_sources(args.sources, sources)

    logger.info("Source(s): %d (%s)", len(selected_sources), ", ".join(selected_sources))

    for source_id in selected_sources:
        logger.info("Saved: %s", fetch_data_to_file(source_id, config, sources[source_id]))


def step_raw_to_staging(args: argparse.Namespace, logger: logging.Logger) -> None:
    config = load_api_config()
    sources = config["sources"]
    selected_sources = validate_sources(args.sources, sources)
    extract_id = get_next_extract_id()

    logger.info("Source(s): %d (%s)", len(selected_sources), ", ".join(selected_sources))
    
    for source_id in selected_sources:
        run_raw_to_staging(source_id, extract_id, config, logger)


def run_steps(args: argparse.Namespace, logger: logging.Logger) -> None:
    step_registry: dict[str, StepFn] = {
        STEP_API_TO_RAW: step_api_to_raw,
        STEP_RAW_TO_STAGING: step_raw_to_staging,
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
