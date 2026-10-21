"""
lib/db_state.py - Teradata Technical State Machine (BATCH_RUNS & BATCH_TASKS)

Manages batch and entity task lifecycles in Teradata Vantage using pooled,
thread-safe connections from `dataloader_core.db.TeradataConnectionManager`.
"""

from __future__ import annotations

import contextlib
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

_COMMON_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "COMMON"))
if _COMMON_DIR not in sys.path:
    sys.path.insert(0, _COMMON_DIR)

from dataloader_core.db import TeradataConnectionManager, validate_sql_identifier

logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")


def current_timestamp_str() -> str:
    """Returns current timestamp formatted for Teradata TIMESTAMP(0) without microseconds."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _require_source(source: Optional[str]) -> str:
    """Validates that a non-empty source identifier is provided for (BATCH_ID, SOURCE) Primary Index scoping."""
    if not source or not str(source).strip():
        raise ValueError("Mandatory 'source' parameter is missing or empty for TeradataStateDB query.")
    return str(source).strip()


class TeradataStateDB:
    """
    Manages state transitions in `${TDENV}SIDFINTCH.BATCH_RUNS` and `BATCH_TASKS`
    using a persistent, thread-safe `TeradataConnectionManager` pool.
    """

    def __init__(
        self,
        td_config: Optional[Dict[str, Any]] = None,
        conn_mgr: Optional[TeradataConnectionManager] = None,
    ) -> None:
        if td_config is None:
            td_config = {}

        self.conn_mgr = conn_mgr or TeradataConnectionManager(td_config)
        self.host = self.conn_mgr.connect_kwargs["host"]
        self.user = self.conn_mgr.connect_kwargs["user"]
        self.password = self.conn_mgr.connect_kwargs.get("password")
        self.logmech = self.conn_mgr.connect_kwargs.get("logmech", "TD2")

        default_tech_db = f"{os.environ.get('TDENV', '')}{os.environ.get('DB_TCH', 'SIDFINTCH')}"
        raw_tech_db = td_config.get("technical_database") or os.environ.get("TD_TECH_DB", default_tech_db)
        self.tech_db = validate_sql_identifier(raw_tech_db)

    def get_connection(self):
        """
        Borrows a pooled TeradataConnection context manager from `self.conn_mgr`.
        Also preserves compatibility with unit tests patching `db.get_connection`.
        """
        return self.conn_mgr.acquire_connection()

    @contextlib.contextmanager
    def _cursor(self):
        with self.get_connection() as con:
            cur_obj = con.cursor()
            if hasattr(cur_obj, "__enter__"):
                with cur_obj as cur:
                    yield cur
            else:
                try:
                    yield cur_obj
                finally:
                    if hasattr(cur_obj, "close"):
                        cur_obj.close()

    def close(self) -> None:
        """Closes all pooled Teradata connections managed by `self.conn_mgr`."""
        self.conn_mgr.close_all()

    def initialize_batch(
        self,
        batch_id: str,
        source: str,
        extract_timestamp: str,
        tasks_list: List[Tuple[str, str]],
    ) -> None:
        """
        Initializes batch in BATCH_RUNS and tasks in BATCH_TASKS.
        tasks_list: list of tuples (entity_name, file_name)
        """
        src = _require_source(source)
        run_date = datetime.now().strftime("%Y-%m-%d")
        start_time = current_timestamp_str()
        current_pid = os.getpid()

        with self._cursor() as cur:
            cur.execute(
                f"SELECT STATUS FROM {self.tech_db}.BATCH_RUNS WHERE BATCH_ID = ? AND SOURCE = ?",
                (batch_id, src),
            )
            row = cur.fetchone()

            if row is None:
                logger.info(
                    f"Initializing new batch {batch_id} (Source: {src}, ExtractTS: {extract_timestamp}, PID: {current_pid})"
                )
                cur.execute(
                    f"""
                    INSERT INTO {self.tech_db}.BATCH_RUNS 
                    (BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, RUN_DATE, START_TIME, STATUS, PROCESS_PID) 
                    VALUES (?, ?, ?, ?, ?, 'RUNNING', ?)
                """,
                    (batch_id, src, extract_timestamp, run_date, start_time, current_pid),
                )

                for entity_name, file_name in tasks_list:
                    cur.execute(
                        f"""
                        INSERT INTO {self.tech_db}.BATCH_TASKS 
                        (BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, ENTITY_NAME, FILE_NAME, STATUS, RETRY_COUNT) 
                        VALUES (?, ?, ?, ?, ?, 'PENDING', 0)
                    """,
                        (batch_id, src, extract_timestamp, entity_name, file_name),
                    )
            elif str(row[0]).strip().upper() == "SUCCESS":
                logger.info(
                    f"Batch {batch_id} (Source: {src}) previously completed with status SUCCESS. "
                    f"Resetting state to RUNNING/PENDING for re-ingestion (PID: {current_pid})."
                )
                cur.execute(
                    f"""
                    UPDATE {self.tech_db}.BATCH_RUNS 
                    SET EXTRACT_TIMESTAMP = ?, RUN_DATE = ?, START_TIME = ?, END_TIME = NULL, STATUS = 'RUNNING', PROCESS_PID = ? 
                    WHERE BATCH_ID = ? AND SOURCE = ?
                """,
                    (extract_timestamp, run_date, start_time, current_pid, batch_id, src),
                )
                cur.execute(
                    f"DELETE FROM {self.tech_db}.BATCH_TASKS WHERE BATCH_ID = ? AND SOURCE = ?",
                    (batch_id, src),
                )
                for entity_name, file_name in tasks_list:
                    cur.execute(
                        f"""
                        INSERT INTO {self.tech_db}.BATCH_TASKS 
                        (BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, ENTITY_NAME, FILE_NAME, STATUS, RETRY_COUNT) 
                        VALUES (?, ?, ?, ?, ?, 'PENDING', 0)
                    """,
                        (batch_id, src, extract_timestamp, entity_name, file_name),
                    )
            else:
                logger.info(f"Batch {batch_id} (Source: {src}) already exists with status {row[0]}. Attempting recovery.")
                cur.execute(
                    f"""
                    UPDATE {self.tech_db}.BATCH_RUNS 
                    SET STATUS = 'RUNNING', PROCESS_PID = ? WHERE BATCH_ID = ? AND SOURCE = ?
                """,
                    (current_pid, batch_id, src),
                )

                cur.execute(
                    f"SELECT FILE_NAME FROM {self.tech_db}.BATCH_TASKS WHERE BATCH_ID = ? AND SOURCE = ?",
                    (batch_id, src),
                )
                existing_files = set(r[0] for r in cur.fetchall())
                for entity_name, file_name in tasks_list:
                    if file_name not in existing_files:
                        cur.execute(
                            f"""
                            INSERT INTO {self.tech_db}.BATCH_TASKS 
                            (BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, ENTITY_NAME, FILE_NAME, STATUS, RETRY_COUNT) 
                            VALUES (?, ?, ?, ?, ?, 'PENDING', 0)
                        """,
                            (batch_id, src, extract_timestamp, entity_name, file_name),
                        )

    def get_pending_tasks(self, batch_id: str, source: Optional[str] = None):
        """Returns list of (ENTITY_NAME, FILE_NAME, RETRY_COUNT) for PENDING or FAILED tasks."""
        src = _require_source(source)
        with self._cursor() as cur:
            cur.execute(
                f"""
                SELECT ENTITY_NAME, FILE_NAME, RETRY_COUNT 
                FROM {self.tech_db}.BATCH_TASKS 
                WHERE BATCH_ID = ? AND SOURCE = ? AND STATUS IN ('PENDING', 'FAILED')
            """,
                (batch_id, src),
            )
            return cur.fetchall()

    def update_task_start(
        self,
        batch_id: str,
        entity_name: str,
        file_name: Optional[str] = None,
        source: Optional[str] = None,
    ) -> None:
        """Updates task status to RUNNING. If file_name is None, updates all files for this entity."""
        src = _require_source(source)
        ts_now = current_timestamp_str()
        with self._cursor() as cur:
            if file_name:
                cur.execute(
                    f"""
                    UPDATE {self.tech_db}.BATCH_TASKS 
                    SET STATUS = 'RUNNING', START_TIME = ?, RETRY_COUNT = RETRY_COUNT + 1 
                    WHERE BATCH_ID = ? AND SOURCE = ? AND ENTITY_NAME = ? AND FILE_NAME = ?
                """,
                    (ts_now, batch_id, src, entity_name, file_name),
                )
            else:
                cur.execute(
                    f"""
                    UPDATE {self.tech_db}.BATCH_TASKS 
                    SET STATUS = 'RUNNING', START_TIME = ?, RETRY_COUNT = RETRY_COUNT + 1 
                    WHERE BATCH_ID = ? AND SOURCE = ? AND ENTITY_NAME = ?
                """,
                    (ts_now, batch_id, src, entity_name),
                )

    def update_task_end(
        self,
        batch_id: str,
        entity_name: str,
        status: str,
        file_name: Optional[str] = None,
        error_msg: str = "",
        source: Optional[str] = None,
    ) -> None:
        """Updates task status to SUCCESS or FAILED. If file_name is None, updates all files for this entity."""
        src = _require_source(source)
        ts_now = current_timestamp_str()
        with self._cursor() as cur:
            truncated_err = str(error_msg)[:1000] if error_msg else None
            if file_name:
                cur.execute(
                    f"""
                    UPDATE {self.tech_db}.BATCH_TASKS 
                    SET STATUS = ?, END_TIME = ?, ERROR_MESSAGE = ? 
                    WHERE BATCH_ID = ? AND SOURCE = ? AND ENTITY_NAME = ? AND FILE_NAME = ?
                """,
                    (status, ts_now, truncated_err, batch_id, src, entity_name, file_name),
                )
            else:
                cur.execute(
                    f"""
                    UPDATE {self.tech_db}.BATCH_TASKS 
                    SET STATUS = ?, END_TIME = ?, ERROR_MESSAGE = ? 
                    WHERE BATCH_ID = ? AND SOURCE = ? AND ENTITY_NAME = ?
                """,
                    (status, ts_now, truncated_err, batch_id, src, entity_name),
                )

    def get_batch_summary(
        self,
        batch_id: str,
        expected_total_files: int,
        source: Optional[str] = None,
    ) -> bool:
        """Returns True if all expected files for this batch are SUCCESS, False otherwise."""
        src = _require_source(source)
        with self._cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*) FROM {self.tech_db}.BATCH_TASKS 
                WHERE BATCH_ID = ? AND SOURCE = ? AND STATUS = 'SUCCESS'
            """,
                (batch_id, src),
            )
            success_count = cur.fetchone()[0]
            return success_count == expected_total_files

    def finish_batch(
        self,
        batch_id: str,
        final_status: str,
        source: Optional[str] = None,
    ) -> None:
        src = _require_source(source)
        ts_now = current_timestamp_str()
        with self._cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self.tech_db}.BATCH_RUNS 
                SET STATUS = ?, END_TIME = ? 
                WHERE BATCH_ID = ? AND SOURCE = ?
            """,
                (final_status, ts_now, batch_id, src),
            )

    def get_active_batch_run(self, source: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Returns active batch dict (including PROCESS_PID) if any batch has STATUS = 'RUNNING'."""
        with self._cursor() as cur:
            if source:
                cur.execute(
                    f"""
                    SELECT BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, START_TIME, PROCESS_PID 
                    FROM {self.tech_db}.BATCH_RUNS 
                    WHERE STATUS = 'RUNNING' AND SOURCE = ?
                """,
                    (source,),
                )
            else:
                cur.execute(
                    f"""
                    SELECT BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, START_TIME, PROCESS_PID 
                    FROM {self.tech_db}.BATCH_RUNS 
                    WHERE STATUS = 'RUNNING'
                """
                )
            row = cur.fetchone()
            if row:
                return {
                    "batch_id": row[0],
                    "source": row[1],
                    "extract_timestamp": row[2],
                    "start_time": row[3],
                    "process_pid": row[4],
                }
            return None

    def mark_batch_failed(self, batch_id: str, source: Optional[str] = None) -> None:
        """Marks an active or abandoned batch as FAILED."""
        src = _require_source(source)
        ts_now = current_timestamp_str()
        with self._cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self.tech_db}.BATCH_RUNS 
                SET STATUS = 'FAILED', END_TIME = ? 
                WHERE BATCH_ID = ? AND SOURCE = ? AND STATUS = 'RUNNING'
            """,
                (ts_now, batch_id, src),
            )
