"""
lib/post_tasks.py - Chained Post-Ingestion Task Runner

Executes post-ingestion hooks ('dbt', 'shell', 'python') with explicit timeouts,
process group termination, log streaming to `<batch_log_dir>/POST_TASKS/<n>_<type>.log`,
and fail-safe validation (F2.9).
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
from typing import Any, Dict, List, Optional, Union

_COMMON_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "COMMON"))
if _COMMON_DIR not in sys.path:
    sys.path.insert(0, _COMMON_DIR)

from dataloader_core.config import expand_env_vars
from dataloader_core.exceptions import ConfigurationError

logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")


def validate_post_tasks(tasks: Any) -> None:
    """
    Validates post-ingestion task structures before batch movement begins (F2.9).
    Enforces task type in {'dbt', 'shell', 'python'}, required fields ('select' for dbt),
    and positive integer timeouts.
    """
    if not tasks:
        return

    if isinstance(tasks, dict):
        # Could be a source-mapped dictionary {source: [tasks], "default": [tasks]}
        for src, task_list in tasks.items():
            if isinstance(task_list, list):
                validate_post_tasks(task_list)
        return

    if not isinstance(tasks, list):
        raise ConfigurationError(f"post_ingestion_tasks must be a list or dict of lists, got {type(tasks).__name__}")

    for idx, task in enumerate(tasks, 1):
        if not isinstance(task, dict):
            raise ConfigurationError(f"Post-task #{idx} must be a dictionary, got {type(task).__name__}")

        t_type = task.get("type")
        if t_type not in ("dbt", "shell", "python"):
            raise ConfigurationError(
                f"Unsupported post-task type '{t_type}' in task #{idx} (supported: 'dbt', 'shell', 'python')"
            )

        if t_type == "dbt":
            for field in ("project_dir", "profiles_dir", "select"):
                val = task.get(field)
                if not val or not str(val).strip():
                    raise ConfigurationError(f"dbt post-task #{idx} requires non-empty '{field}' parameter.")
        elif t_type == "shell":
            if not task.get("command") or not str(task.get("command")).strip():
                raise ConfigurationError(f"shell post-task #{idx} requires non-empty 'command' parameter.")
        elif t_type == "python":
            if not task.get("script") or not str(task.get("script")).strip():
                raise ConfigurationError(f"python post-task #{idx} requires non-empty 'script' parameter.")

        timeout = task.get("timeout_seconds")
        if timeout is not None:
            try:
                t_int = int(timeout)
                if t_int <= 0:
                    raise ValueError()
            except (ValueError, TypeError):
                raise ConfigurationError(
                    f"Invalid timeout_seconds '{timeout}' in task #{idx} (must be integer > 0)."
                )


def _kill_group(pid: int, sig) -> None:
    """Kills entire process group if available, falling back to process-level kill."""
    if hasattr(os, "killpg"):
        try:
            os.killpg(pid, sig)
        except OSError:
            pass
    else:
        try:
            os.kill(pid, sig)
        except OSError:
            pass


def _tail_file(path: str, n: int = 50) -> str:
    """Reads the last n lines of a file."""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            return "".join(fh.readlines()[-n:])
    except OSError:
        return ""


def _run_task_command(
    cmd: Union[List[str], str],
    run_env: Dict[str, str],
    log_path: str,
    timeout_sec: int = 3600,
    grace_seconds: int = 30,
    shell: bool = False,
    task_desc: str = "Task",
) -> bool:
    """
    Executes a post-task command with output streamed to log_path and process-group timeout enforcement.
    """
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as out:
        out.write(f"=== Starting {task_desc} (Timeout: {timeout_sec}s) ===\n")
        out.flush()

        proc = subprocess.Popen(
            cmd,
            stdout=out,
            stderr=subprocess.STDOUT,
            env=run_env,
            shell=shell,
            start_new_session=True,
        )

        try:
            rc = proc.wait(timeout=timeout_sec)
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout ({timeout_sec}s) exceeded for {task_desc}; terminating process group {proc.pid}")
            _kill_group(proc.pid, signal.SIGTERM)
            try:
                rc = proc.wait(timeout=grace_seconds)
            except subprocess.TimeoutExpired:
                _kill_group(proc.pid, signal.SIGKILL)
                rc = proc.wait()
            rc = -1 if rc == 0 else rc

        out.write(f"\n=== Completed {task_desc} with exit code {rc} ===\n")

    if rc != 0:
        tail_output = _tail_file(log_path, 50)
        logger.error(
            f"{task_desc} failed with exit code {rc}. Last 50 lines of log ({log_path}):\n{tail_output}"
        )
        return False

    return True


def run_post_tasks(
    post_tasks_config: List[Dict[str, Any]],
    context_env: Optional[Dict[str, str]] = None,
    batch_log_dir: Optional[str] = None,
) -> bool:
    """
    Executes a list of post-ingestion tasks based on their type ('dbt', 'shell', 'python').
    Merges `context_env` into a copy of `os.environ` passed to subprocesses without
    mutating global `os.environ`. Streams output to `<batch_log_dir>/POST_TASKS/<n>_<type>.log` (F2.9).
    """
    if not post_tasks_config:
        logger.info("No post-ingestion tasks configured.")
        return True

    validate_post_tasks(post_tasks_config)

    run_env = os.environ.copy()
    if context_env:
        for k, v in context_env.items():
            if v is not None:
                run_env[str(k)] = str(v)

    log_root = os.path.join(batch_log_dir, "POST_TASKS") if batch_log_dir else os.path.join(os.getcwd(), "POST_TASKS")

    for idx, task in enumerate(post_tasks_config, 1):
        task_type = task.get("type")
        timeout_sec = int(task.get("timeout_seconds", 3600))
        log_file = os.path.join(log_root, f"{idx}_{task_type}.log")
        logger.info(f"Starting post-ingestion task #{idx} of type: '{task_type}' (log: {log_file})")

        if task_type == "dbt":
            dbt_dir = expand_env_vars(task["project_dir"], env=run_env)
            profiles_dir = expand_env_vars(task["profiles_dir"], env=run_env)
            select_spec = expand_env_vars(task["select"], env=run_env)

            cmd_run = ["dbt", "run", "--select", select_spec, "--project-dir", dbt_dir, "--profiles-dir", profiles_dir]
            if not _run_task_command(cmd_run, run_env, log_file, timeout_sec, task_desc=f"dbt run #{idx}"):
                return False

            cmd_test = ["dbt", "test", "--select", select_spec, "--project-dir", dbt_dir, "--profiles-dir", profiles_dir]
            if not _run_task_command(cmd_test, run_env, log_file, timeout_sec, task_desc=f"dbt test #{idx}"):
                return False

        elif task_type == "shell":
            command = expand_env_vars(task["command"], env=run_env)
            if not _run_task_command(command, run_env, log_file, timeout_sec, shell=True, task_desc=f"shell task #{idx}"):
                return False

        elif task_type == "python":
            script_path = expand_env_vars(task["script"], env=run_env)
            if not os.path.exists(script_path):
                logger.error(f"Python script '{script_path}' not found.")
                return False
            cmd_py = [sys.executable, script_path]
            if not _run_task_command(cmd_py, run_env, log_file, timeout_sec, task_desc=f"python task #{idx}"):
                return False

    logger.info("All post-ingestion tasks completed successfully.")
    return True
