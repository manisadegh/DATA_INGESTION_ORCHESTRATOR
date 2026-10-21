"""
lib/logger.py - Orchestrator Logging Wrapper

Delegates console and Batch Log Capsule file handler setup to
`dataloader_core.logging_utils` (which uses `CapsulePathResolver` exact
path-segment matching).
"""

from __future__ import annotations

import os
import sys

_COMMON_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "COMMON"))
if _COMMON_DIR not in sys.path:
    sys.path.insert(0, _COMMON_DIR)

from dataloader_core.logging_utils import (
    ORCHESTRATOR_LOG_FORMAT as LOG_FORMAT,
    add_batch_file_logger,
    setup_orchestrator_console_logger as setup_logger,
)

__all__ = ["LOG_FORMAT", "setup_logger", "add_batch_file_logger"]
