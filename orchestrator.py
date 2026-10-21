"""
orchestrator.py - Data Ingestion Orchestrator CLI Entrypoint

Thin CLI entrypoint delegating Single-Batch gating, state tracking, parallel
TPT worker execution, post-ingestion hooks, and Batch Capsule archiving to
`lib.orchestrator_service.OrchestratorService` and `dataloader_core`.
"""

from __future__ import annotations

import argparse
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
COMMON_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "COMMON"))
if COMMON_DIR not in sys.path:
    sys.path.insert(0, COMMON_DIR)
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from dataloader_core.config import expand_env_vars
from dataloader_core.exceptions import DataLoaderError
from dataloader_core.logging_utils import setup_orchestrator_console_logger
from lib.orchestrator_service import (
    BatchContext,
    OrchestratorService,
    process_entity_task,
    strip_header_in_place,
)


def main() -> None:
    logger = setup_orchestrator_console_logger()
    parser = argparse.ArgumentParser(description="Generic Data Ingestion Orchestrator - Single Batch Architecture")
    parser.add_argument("--config", type=str, required=True, help="Path to configuration file")
    parser.add_argument(
        "--skip-post-tasks",
        action="store_true",
        default=False,
        help="Skip execution of downstream post-ingestion tasks (e.g. dbt)",
    )
    parser.add_argument("--source", type=str, default=None, help="Explicit data source override")
    parser.add_argument("--all-sources", action="store_true", default=False, help="Process batches across all sources")
    parser.add_argument("--sources", type=str, default=None, help="Comma-separated list of sources to orchestrate")
    parser.add_argument(
        "--post-tasks-file",
        type=str,
        default=None,
        help="Path to JSON file containing source-specific post-ingestion tasks",
    )
    args = parser.parse_args()

    sources_list = [s.strip().upper() for s in args.sources.split(",") if s.strip()] if args.sources else None

    try:
        service = OrchestratorService(
            config_path=args.config,
            source_override=args.source,
            skip_post_tasks=args.skip_post_tasks,
            post_tasks_file=args.post_tasks_file,
            all_sources=args.all_sources,
            sources_list=sources_list,
        )
        sys.exit(service.run())
    except DataLoaderError as exc:
        logger.critical(f"Failed to initialize or execute Orchestrator: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
