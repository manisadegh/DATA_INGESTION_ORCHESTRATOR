"""
lib/post_tasks.py - Chained Post-Ingestion Task Runner

Executes post-ingestion hooks ('dbt', 'shell', 'python') passing batch context
(`CURRENT_BATCH_ID`, `CURRENT_SOURCE`, `CURRENT_MANIFEST`) via explicit subprocess
`env` dictionaries rather than mutating global `os.environ`.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from typing import Any, Dict, List, Optional

_COMMON_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "COMMON"))
if _COMMON_DIR not in sys.path:
    sys.path.insert(0, _COMMON_DIR)

from dataloader_core.config import expand_env_vars

logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")


def run_post_tasks(
    post_tasks_config: List[Dict[str, Any]],
    context_env: Optional[Dict[str, str]] = None,
) -> bool:
    """
    Executes a list of post-ingestion tasks based on their type ('dbt', 'shell', 'python').
    Merges `context_env` into a copy of `os.environ` passed to subprocesses without
    mutating global `os.environ`.
    """
    if not post_tasks_config:
        logger.info("No post-ingestion tasks configured.")
        return True

    run_env = os.environ.copy()
    if context_env:
        for k, v in context_env.items():
            if v is not None:
                run_env[str(k)] = str(v)

    for task in post_tasks_config:
        task_type = task.get("type")
        logger.info(f"Starting post-ingestion task of type: {task_type}")

        if task_type == "dbt":
            if not _run_dbt_task(task, run_env):
                return False
        elif task_type == "shell":
            if not _run_shell_task(task, run_env):
                return False
        elif task_type == "python":
            if not _run_python_task(task, run_env):
                return False
        else:
            logger.warning(f"Unknown task type: {task_type}. Skipping.")

    logger.info("All post-ingestion tasks completed successfully.")
    return True


def _run_dbt_task(task: Dict[str, Any], run_env: Dict[str, str]) -> bool:
    dbt_dir = task.get("project_dir")
    profiles_dir = task.get("profiles_dir")

    if not dbt_dir or not profiles_dir:
        logger.error("DBT directories not configured properly in task.")
        return False

    dbt_dir = expand_env_vars(dbt_dir, env=run_env)
    profiles_dir = expand_env_vars(profiles_dir, env=run_env)

    logger.info(f"Running DBT in {dbt_dir}...")
    cmd_run = ["dbt", "run", "--project-dir", dbt_dir, "--profiles-dir", profiles_dir]
    res_run = subprocess.run(cmd_run, capture_output=True, text=True, env=run_env)

    if res_run.returncode != 0:
        logger.error(f"dbt run failed!\n{res_run.stdout}\n{res_run.stderr}")
        return False

    cmd_test = ["dbt", "test", "--project-dir", dbt_dir, "--profiles-dir", profiles_dir]
    res_test = subprocess.run(cmd_test, capture_output=True, text=True, env=run_env)

    if res_test.returncode != 0:
        logger.error(f"dbt test failed!\n{res_test.stdout}\n{res_test.stderr}")
        return False

    return True


def _run_shell_task(task: Dict[str, Any], run_env: Dict[str, str]) -> bool:
    command = task.get("command")
    if not command:
        logger.error("Shell command not provided.")
        return False
    command = expand_env_vars(command, env=run_env)

    logger.info(f"Executing shell command: {command}")
    res = subprocess.run(command, shell=True, capture_output=True, text=True, env=run_env)
    if res.returncode != 0:
        logger.error(f"Shell task failed (RC {res.returncode}):\n{res.stdout}\n{res.stderr}")
        return False
    logger.info(f"Shell task output:\n{res.stdout}")
    return True


def _run_python_task(task: Dict[str, Any], run_env: Dict[str, str]) -> bool:
    script_path = task.get("script")
    if script_path:
        script_path = expand_env_vars(script_path, env=run_env)
    if not script_path or not os.path.exists(script_path):
        logger.error(f"Python script {script_path} not found.")
        return False

    logger.info(f"Executing python script: {script_path}")
    res = subprocess.run([sys.executable, script_path], capture_output=True, text=True, env=run_env)
    if res.returncode != 0:
        logger.error(f"Python task failed (RC {res.returncode}):\n{res.stdout}\n{res.stderr}")
        return False
    logger.info(f"Python task output:\n{res.stdout}")
    return True
