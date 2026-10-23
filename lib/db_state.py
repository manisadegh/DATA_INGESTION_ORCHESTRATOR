"""
lib/db_state.py - Teradata Technical State Machine (BATCH_RUNS & BATCH_TASKS)

Manages batch execution attempts and entity-level task lifecycles in Teradata Vantage
using pooled, thread-safe connections from `dataloader_core.db.TeradataConnectionManager`.
Enforces full history retention without in-place row overwrites or deletions (F2.11).
"""

from __future__ import annotations

import contextlib
import logging
import os
import socket
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

_COMMON_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "COMMON"))
if _COMMON_DIR not in sys.path:
    sys.path.insert(0, _COMMON_DIR)

from dataloader_core.db import TeradataConnectionManager, validate_sql_identifier
from dataloader_core.exceptions import DataLoaderError
from dataloader_core.locking import is_pid_alive

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
        td_config: Optional[Any] = None,
        conn_mgr: Optional[TeradataConnectionManager] = None,
        tech_db: Optional[str] = None,
        technical_database: Optional[str] = None,
        default_source: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        if td_config is None:
            td_config = {}

        self.default_source = default_source or kwargs.get("source")

        if not isinstance(td_config, dict):
            self.conn_mgr = td_config
            self.host = "localhost"
            self.user = "mock"
            self.password = None
            self.logmech = "TD2"
            raw_tech_db = tech_db or technical_database or "SIDFINTCH"
        else:
            self.conn_mgr = conn_mgr or TeradataConnectionManager(td_config)
            self.host = getattr(self.conn_mgr, "connect_kwargs", {}).get("host", "localhost")
            self.user = getattr(self.conn_mgr, "connect_kwargs", {}).get("user", "mock")
            self.password = getattr(self.conn_mgr, "connect_kwargs", {}).get("password")
            self.logmech = getattr(self.conn_mgr, "connect_kwargs", {}).get("logmech", "TD2")
            default_tech_db = f"{os.environ.get('TDENV', '')}{os.environ.get('DB_TCH', 'SIDFINTCH')}"
            raw_tech_db = tech_db or technical_database or td_config.get("technical_database") or os.environ.get("TD_TECH_DB", default_tech_db)

        self.tech_db = validate_sql_identifier(raw_tech_db)
        self.current_attempt: Dict[Tuple[str, str], int] = {}

    def _resolve_source(self, source: Optional[str]) -> str:
        src = source or self.default_source
        return _require_source(src)

    def get_connection(self):
        """
        Borrows a pooled TeradataConnection context manager from `self.conn_mgr`.
        Also preserves compatibility with unit tests passing a mock connection.
        """
        if isinstance(self.conn_mgr, TeradataConnectionManager):
            return self.conn_mgr.acquire_connection()
        if hasattr(self.conn_mgr, "acquire_connection") and not hasattr(self.conn_mgr, "cursor"):
            return self.conn_mgr.acquire_connection()
        return self.conn_mgr

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
        if hasattr(self.conn_mgr, "close_all"):
            self.conn_mgr.close_all()

    def _migrate_technical_schema(self) -> None:
        """Applies Version 3.0 technical table migration in Teradata if columns are missing (F2.11)."""
        logger.warning(f"Version 3.0 columns missing in {self.tech_db}. Applying auto-migration...")
        with self.get_connection() as con:
            cur_obj = con.cursor()
            try:
                for sql in [
                    f"ALTER TABLE {self.tech_db}.BATCH_RUNS ADD RUN_ATTEMPT INTEGER DEFAULT 1 NOT NULL",
                    f"ALTER TABLE {self.tech_db}.BATCH_RUNS ADD PROCESS_HOST VARCHAR(255) CHARACTER SET UNICODE NOT CASESPECIFIC",
                    f"ALTER TABLE {self.tech_db}.BATCH_TASKS ADD EXTRACT_TIMESTAMP VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC",
                    f"ALTER TABLE {self.tech_db}.BATCH_TASKS ADD RUN_ATTEMPT INTEGER DEFAULT 1 NOT NULL",
                    f"ALTER TABLE {self.tech_db}.BATCH_TASKS ADD FILE_COUNT INTEGER DEFAULT 1",
                    f"ALTER TABLE {self.tech_db}.BATCH_TASKS ADD TPT_JOB_NAME VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC",
                    f"ALTER TABLE {self.tech_db}.BATCH_TASKS DROP FILE_NAME",
                ]:
                    try:
                        cur_obj.execute(sql)
                    except Exception:
                        pass
            finally:
                if hasattr(cur_obj, "close"):
                    cur_obj.close()

    def get_attempt(self, batch_id: str, source: Optional[str] = None, run_attempt: Optional[int] = None) -> int:
        """Returns the attempt number, falling back to cache or database max attempt."""
        if run_attempt is not None and run_attempt > 0:
            return run_attempt
        src = self._resolve_source(source)
        cached = self.current_attempt.get((batch_id, src))
        if cached is not None and cached > 0:
            return cached
        try:
            return self._query_attempt(batch_id, src)
        except Exception as exc:
            err_msg = str(exc).upper()
            if "5628" in err_msg or "RUN_ATTEMPT" in err_msg:
                self._migrate_technical_schema()
                return self._query_attempt(batch_id, src)
            raise

    def _query_attempt(self, batch_id: str, src: str) -> int:
        with self._cursor() as cur:
            cur.execute(
                f"SELECT COALESCE(MAX(RUN_ATTEMPT), 1) FROM {self.tech_db}.BATCH_RUNS WHERE BATCH_ID = ? AND SOURCE = ?",
                (batch_id, src),
            )
            row = cur.fetchone()
            att = int(row[0]) if row and row[0] is not None else 1
            self.current_attempt[(batch_id, src)] = att
            return att

    def start_batch_run(
        self,
        batch_id: str,
        source: Optional[str] = None,
        extract_timestamp: str = "",
        entities_files: Optional[Union[Dict[str, Any], List[Tuple[str, str]]]] = None,
        **kwargs: Any,
    ) -> int:
        """Alias for initialize_batch to support attempt history tracking (F2.11)."""
        src = self._resolve_source(source)
        return self.initialize_batch(
            batch_id=batch_id,
            source=src,
            extract_timestamp=extract_timestamp,
            entity_file_counts=entities_files or {},
        )

    def initialize_batch(
        self,
        batch_id: str,
        source: Optional[str],
        extract_timestamp: str,
        entity_file_counts: Union[Dict[str, Any], List[Tuple[str, str]]],
    ) -> int:
        """
        Initializes a new attempt for batch in BATCH_RUNS and entity tasks in BATCH_TASKS (F2.11).
        Carries over previously SUCCESS entities so recoveries do not reload loaded data.
        Returns the new attempt number.
        """
        src = self._resolve_source(source)
        try:
            return self._do_initialize_batch(batch_id, src, extract_timestamp, entity_file_counts)
        except Exception as exc:
            err_msg = str(exc).upper()
            if "5628" in err_msg or "RUN_ATTEMPT" in err_msg or "PROCESS_HOST" in err_msg or "FILE_COUNT" in err_msg:
                self._migrate_technical_schema()
                return self._do_initialize_batch(batch_id, src, extract_timestamp, entity_file_counts)
            raise

    def _do_initialize_batch(
        self,
        batch_id: str,
        src: str,
        extract_timestamp: str,
        entity_file_counts: Union[Dict[str, Any], List[Tuple[str, str]]],
    ) -> int:
        run_date = datetime.now().strftime("%Y-%m-%d")
        start_time = current_timestamp_str()
        current_pid = os.getpid()
        current_host = socket.gethostname()

        if isinstance(entity_file_counts, list):
            counts: Dict[str, int] = {}
            for item in entity_file_counts:
                e_name = item[0]
                counts[e_name] = counts.get(e_name, 0) + 1
        elif isinstance(entity_file_counts, dict):
            counts = {}
            for k, v in entity_file_counts.items():
                if isinstance(v, list):
                    counts[k] = len(v)
                else:
                    counts[k] = int(v)
        else:
            counts = dict(entity_file_counts)

        with self._cursor() as cur:
            cur.execute(
                f"SELECT COALESCE(MAX(RUN_ATTEMPT), 0) FROM {self.tech_db}.BATCH_RUNS WHERE BATCH_ID = ? AND SOURCE = ?",
                (batch_id, src),
            )
            row = cur.fetchone()
            prev_attempt = int(row[0]) if (row and row[0] is not None) else 0
            new_attempt = prev_attempt + 1

            success_entities: Dict[str, Tuple[Any, ...]] = {}
            if prev_attempt > 0:
                cur.execute(
                    f"SELECT STATUS, PROCESS_PID, PROCESS_HOST FROM {self.tech_db}.BATCH_RUNS WHERE BATCH_ID = ? AND SOURCE = ? AND RUN_ATTEMPT = ?",
                    (batch_id, src, prev_attempt),
                )
                prev_run = cur.fetchone()
                if prev_run and str(prev_run[0]).strip().upper() == "RUNNING":
                    prev_pid, prev_host = prev_run[1], prev_run[2]
                    if (prev_host is None or prev_host == current_host) and is_pid_alive(prev_pid):
                        raise DataLoaderError(
                            f"Batch {batch_id} (Source: {src}) attempt {prev_attempt} is currently RUNNING with active PID {prev_pid} on {prev_host}."
                        )

                # Query previously SUCCESS entities to carry forward
                cur.execute(
                    f"""
                    SELECT ENTITY_NAME, FILE_COUNT, RETRY_COUNT, TPT_JOB_NAME, START_TIME, END_TIME 
                    FROM {self.tech_db}.BATCH_TASKS 
                    WHERE BATCH_ID = ? AND SOURCE = ? AND RUN_ATTEMPT = ? AND STATUS = 'SUCCESS'
                """,
                    (batch_id, src, prev_attempt),
                )
                for r in cur.fetchall():
                    success_entities[r[0]] = r

            logger.info(
                f"Initializing batch {batch_id} (Source: {src}, Attempt: {new_attempt}, Host: {current_host}, PID: {current_pid})"
            )
            cur.execute(
                f"""
                INSERT INTO {self.tech_db}.BATCH_RUNS 
                (BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, RUN_DATE, START_TIME, STATUS, PROCESS_PID, RUN_ATTEMPT, PROCESS_HOST) 
                VALUES (?, ?, ?, ?, ?, 'RUNNING', ?, ?, ?)
            """,
                (batch_id, src, extract_timestamp, run_date, start_time, current_pid, new_attempt, current_host),
            )

            try:
                self._insert_tasks(cur, batch_id, src, extract_timestamp, new_attempt, counts, success_entities)
            except Exception as exc:
                err_msg = str(exc).upper()
                if "FILE_NAME" in err_msg and "NOT NULL" in err_msg:
                    self._insert_tasks_with_filename(cur, batch_id, src, extract_timestamp, new_attempt, counts, success_entities)
                else:
                    raise

        self.current_attempt[(batch_id, src)] = new_attempt
        return new_attempt

    def _insert_tasks(
        self,
        cur,
        batch_id: str,
        src: str,
        extract_timestamp: str,
        new_attempt: int,
        counts: Dict[str, int],
        success_entities: Dict[str, Tuple[Any, ...]],
    ) -> None:
        for entity_name, file_count in counts.items():
            if entity_name in success_entities:
                st = success_entities[entity_name]
                logger.info(
                    f"[{entity_name}] Reusing previously SUCCESS entity task from attempt {new_attempt - 1} (carried over)."
                )
                cur.execute(
                    f"""
                    INSERT INTO {self.tech_db}.BATCH_TASKS 
                    (BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, RUN_ATTEMPT, ENTITY_NAME, FILE_COUNT, STATUS, START_TIME, END_TIME, RETRY_COUNT, TPT_JOB_NAME) 
                    VALUES (?, ?, ?, ?, ?, ?, 'SUCCESS', ?, ?, ?, ?)
                """,
                    (batch_id, src, extract_timestamp, new_attempt, entity_name, st[1], st[4], st[5], st[2], st[3]),
                )
            else:
                cur.execute(
                    f"""
                    INSERT INTO {self.tech_db}.BATCH_TASKS 
                    (BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, RUN_ATTEMPT, ENTITY_NAME, FILE_COUNT, STATUS, RETRY_COUNT) 
                    VALUES (?, ?, ?, ?, ?, ?, 'PENDING', 0)
                """,
                    (batch_id, src, extract_timestamp, new_attempt, entity_name, file_count),
                )

    def _insert_tasks_with_filename(
        self,
        cur,
        batch_id: str,
        src: str,
        extract_timestamp: str,
        new_attempt: int,
        counts: Dict[str, int],
        success_entities: Dict[str, Tuple[Any, ...]],
    ) -> None:
        for entity_name, file_count in counts.items():
            fname = f"{entity_name} ({file_count} files)"
            if entity_name in success_entities:
                st = success_entities[entity_name]
                cur.execute(
                    f"""
                    INSERT INTO {self.tech_db}.BATCH_TASKS 
                    (BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, RUN_ATTEMPT, ENTITY_NAME, FILE_COUNT, FILE_NAME, STATUS, START_TIME, END_TIME, RETRY_COUNT, TPT_JOB_NAME) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'SUCCESS', ?, ?, ?, ?)
                """,
                    (batch_id, src, extract_timestamp, new_attempt, entity_name, st[1], fname, st[4], st[5], st[2], st[3]),
                )
            else:
                cur.execute(
                    f"""
                    INSERT INTO {self.tech_db}.BATCH_TASKS 
                    (BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, RUN_ATTEMPT, ENTITY_NAME, FILE_COUNT, FILE_NAME, STATUS, RETRY_COUNT) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', 0)
                """,
                    (batch_id, src, extract_timestamp, new_attempt, entity_name, file_count, fname),
                )

    def get_pending_tasks(
        self,
        batch_id: str,
        source: Optional[str] = None,
        run_attempt: Optional[int] = None,
    ) -> List[Tuple[str, int]]:
        """Returns list of (ENTITY_NAME, RETRY_COUNT) for PENDING or FAILED tasks for current attempt."""
        src = self._resolve_source(source)
        att = self.get_attempt(batch_id, src, run_attempt)
        with self._cursor() as cur:
            cur.execute(
                f"""
                SELECT ENTITY_NAME, RETRY_COUNT 
                FROM {self.tech_db}.BATCH_TASKS 
                WHERE BATCH_ID = ? AND SOURCE = ? AND RUN_ATTEMPT = ? AND STATUS IN ('PENDING', 'FAILED')
            """,
                (batch_id, src, att),
            )
            return [(r[0], r[1]) for r in cur.fetchall()]

    def update_task_start(
        self,
        batch_id: str,
        entity_name: str,
        file_name: Optional[str] = None,
        source: Optional[str] = None,
        tpt_job_name: Optional[str] = None,
        run_attempt: Optional[int] = None,
    ) -> None:
        """Updates entity task status to RUNNING for current attempt (F1.4, F2.11)."""
        src = self._resolve_source(source)
        att = self.get_attempt(batch_id, src, run_attempt)
        ts_now = current_timestamp_str()
        with self._cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self.tech_db}.BATCH_TASKS 
                SET STATUS = 'RUNNING', START_TIME = ?, RETRY_COUNT = RETRY_COUNT + 1, TPT_JOB_NAME = ?
                WHERE BATCH_ID = ? AND SOURCE = ? AND ENTITY_NAME = ? AND RUN_ATTEMPT = ?
            """,
                (ts_now, tpt_job_name, batch_id, src, entity_name, att),
            )

    def update_task_end(
        self,
        batch_id: str,
        entity_name: str,
        status: str,
        file_name: Optional[str] = None,
        error_msg: str = "",
        source: Optional[str] = None,
        run_attempt: Optional[int] = None,
    ) -> None:
        """Updates entity task status to SUCCESS or FAILED for current attempt (F2.11)."""
        src = self._resolve_source(source)
        att = self.get_attempt(batch_id, src, run_attempt)
        ts_now = current_timestamp_str()
        with self._cursor() as cur:
            truncated_err = str(error_msg)[:1000] if error_msg else None
            cur.execute(
                f"""
                UPDATE {self.tech_db}.BATCH_TASKS 
                SET STATUS = ?, END_TIME = ?, ERROR_MESSAGE = ? 
                WHERE BATCH_ID = ? AND SOURCE = ? AND ENTITY_NAME = ? AND RUN_ATTEMPT = ?
            """,
                (status, ts_now, truncated_err, batch_id, src, entity_name, att),
            )

    def get_batch_summary(
        self,
        batch_id: str,
        expected_entity_count: int,
        source: Optional[str] = None,
        run_attempt: Optional[int] = None,
    ) -> bool:
        """Returns True if all expected entities for current attempt are SUCCESS, False otherwise."""
        src = self._resolve_source(source)
        att = self.get_attempt(batch_id, src, run_attempt)
        with self._cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*) FROM {self.tech_db}.BATCH_TASKS 
                WHERE BATCH_ID = ? AND SOURCE = ? AND RUN_ATTEMPT = ? AND STATUS = 'SUCCESS'
            """,
                (batch_id, src, att),
            )
            row = cur.fetchone()
            success_count = int(row[0]) if (row and row[0] is not None) else 0
            return success_count == expected_entity_count

    def finish_batch(
        self,
        batch_id: str,
        final_status: str,
        source: Optional[str] = None,
        run_attempt: Optional[int] = None,
    ) -> None:
        """Marks current attempt as finished (SUCCESS or FAILED) without deleting history (F2.11)."""
        src = self._resolve_source(source)
        att = self.get_attempt(batch_id, src, run_attempt)
        ts_now = current_timestamp_str()
        with self._cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self.tech_db}.BATCH_RUNS 
                SET STATUS = ?, END_TIME = ? 
                WHERE BATCH_ID = ? AND SOURCE = ? AND RUN_ATTEMPT = ?
            """,
                (final_status, ts_now, batch_id, src, att),
            )

    def get_active_batch_run(self, source: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Returns active batch dict (including PROCESS_PID, PROCESS_HOST, RUN_ATTEMPT) if any attempt is RUNNING."""
        src = source or self.default_source
        try:
            return self._query_active_batch_run(src)
        except Exception as exc:
            err_msg = str(exc).upper()
            if "5628" in err_msg or "RUN_ATTEMPT" in err_msg or "PROCESS_HOST" in err_msg:
                self._migrate_technical_schema()
                return self._query_active_batch_run(src)
            raise

    def _query_active_batch_run(self, src: Optional[str]) -> Optional[Dict[str, Any]]:
        with self._cursor() as cur:
            if src:
                cur.execute(
                    f"""
                    SELECT BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, START_TIME, PROCESS_PID, RUN_ATTEMPT, PROCESS_HOST 
                    FROM {self.tech_db}.BATCH_RUNS 
                    WHERE STATUS = 'RUNNING' AND SOURCE = ?
                    ORDER BY RUN_ATTEMPT DESC
                """,
                    (src,),
                )
            else:
                cur.execute(
                    f"""
                    SELECT BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, START_TIME, PROCESS_PID, RUN_ATTEMPT, PROCESS_HOST 
                    FROM {self.tech_db}.BATCH_RUNS 
                    WHERE STATUS = 'RUNNING'
                    ORDER BY RUN_ATTEMPT DESC
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
                    "run_attempt": row[5],
                    "process_host": row[6],
                }
            return None

    def get_latest_attempt(self, batch_id: str, source: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Returns the latest attempt row for the given batch and source (F2.8, F2.11)."""
        src = self._resolve_source(source)
        try:
            return self._query_latest_attempt(batch_id, src)
        except Exception as exc:
            err_msg = str(exc).upper()
            if "5628" in err_msg or "RUN_ATTEMPT" in err_msg or "PROCESS_HOST" in err_msg:
                self._migrate_technical_schema()
                return self._query_latest_attempt(batch_id, src)
            raise

    def _query_latest_attempt(self, batch_id: str, src: str) -> Optional[Dict[str, Any]]:
        with self._cursor() as cur:
            cur.execute(
                f"""
                SELECT RUN_ATTEMPT, STATUS, PROCESS_PID, PROCESS_HOST 
                FROM {self.tech_db}.BATCH_RUNS 
                WHERE BATCH_ID = ? AND SOURCE = ? 
                ORDER BY RUN_ATTEMPT DESC
            """,
                (batch_id, src),
            )
            row = cur.fetchone()
            if row:
                return {
                    "run_attempt": row[0],
                    "status": str(row[1]).strip().upper(),
                    "process_pid": row[2],
                    "process_host": row[3],
                }
            return None

    def mark_batch_failed(
        self,
        batch_id: str,
        source: Optional[str] = None,
        run_attempt: Optional[int] = None,
    ) -> None:
        """Marks current RUNNING attempt as FAILED."""
        src = self._resolve_source(source)
        att = self.get_attempt(batch_id, src, run_attempt)
        ts_now = current_timestamp_str()
        with self._cursor() as cur:
            cur.execute(
                f"""
                UPDATE {self.tech_db}.BATCH_RUNS 
                SET STATUS = 'FAILED', END_TIME = ? 
                WHERE BATCH_ID = ? AND SOURCE = ? AND RUN_ATTEMPT = ? AND STATUS = 'RUNNING'
            """,
                (ts_now, batch_id, src, att),
            )
