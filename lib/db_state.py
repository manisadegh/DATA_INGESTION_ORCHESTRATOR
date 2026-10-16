import os
import logging
from datetime import datetime
import teradatasql
import re

logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")

def current_timestamp_str():
    """Returns current timestamp formatted for Teradata TIMESTAMP(0) without microseconds."""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

class TeradataStateDB:
    def __init__(self, td_config):
        if td_config is None:
            td_config = {}
        self.host = td_config.get('host') or td_config.get('server') or td_config.get('tdpid') or os.environ.get('TDPID')
        if not self.host:
            raise ValueError("Teradata host/server not configured. Provide 'host' (or 'server') in config.json or export TDPID.")

        env_user = f"{os.environ.get('TDENV', '')}{os.environ.get('SID_ADM_USER', '')}" if os.environ.get("SID_ADM_USER") else os.environ.get("USER")
        self.user = td_config.get('user') or td_config.get('username') or env_user
        if not self.user:
            raise ValueError("Teradata user not configured. Provide 'user' in config.json or export TDENV and SID_ADM_USER.")
        
        # Support either plain-text password, direct ENCRYPTED_PASSWORD, or encrypted pass/key files (TJEncryptPassword)
        if 'key_file' in td_config and 'pass_file' in td_config:
            self.password = f"ENCRYPTED_PASSWORD(file:{td_config['key_file']},file:{td_config['pass_file']})"
        elif 'pass_file' in td_config and 'key_file' in td_config:
            self.password = f"ENCRYPTED_PASSWORD(file:{td_config['key_file']},file:{td_config['pass_file']})"
        elif td_config.get('password'):
            self.password = td_config.get('password')
        else:
            dbt_dir = os.environ.get("DBT_PROFILE_DIR") or os.path.expanduser("~/.dbt")
            auto_key = os.path.join(dbt_dir, f"{self.user}_Key.properties")
            auto_pass = os.path.join(dbt_dir, f"{self.user}_Pass.properties")
            if os.path.exists(auto_key) and os.path.exists(auto_pass):
                self.password = f"ENCRYPTED_PASSWORD(file:{auto_key},file:{auto_pass})"
            else:
                self.password = os.environ.get('TD_PASSWORD')
            
        self.logmech = td_config.get('logmech', 'TD2')
        default_tech_db = f"{os.environ.get('TDENV', '')}{os.environ.get('DB_TCH', 'SIDFINTCH')}"
        self.tech_db = td_config.get('technical_database') or os.environ.get('TD_TECH_DB', default_tech_db)
        if not re.match(r'^[A-Za-z0-9_]+$', self.tech_db):
            raise ValueError(f"Invalid database identifier: '{self.tech_db}'")

    def get_connection(self):
        return teradatasql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            logmech=self.logmech
        )

    def initialize_batch(self, batch_id, source, extract_timestamp, tasks_list):
        """
        Initializes batch in BATCH_RUNS and tasks in BATCH_TASKS.
        tasks_list: list of tuples (entity_name, file_name)
        """
        run_date = datetime.now().strftime('%Y-%m-%d')
        start_time = current_timestamp_str()

        current_pid = os.getpid()
        with self.get_connection() as con:
            with con.cursor() as cur:
                # Check if batch exists for this source
                cur.execute(f"SELECT STATUS FROM {self.tech_db}.BATCH_RUNS WHERE BATCH_ID = ? AND SOURCE = ?", (batch_id, source))
                row = cur.fetchone()

                if row is None:
                    logger.info(f"Initializing new batch {batch_id} (Source: {source}, ExtractTS: {extract_timestamp}, PID: {current_pid})")
                    # Insert batch run with PROCESS_PID (with fallback if column missing)
                    try:
                        cur.execute(f"""
                            INSERT INTO {self.tech_db}.BATCH_RUNS 
                            (BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, RUN_DATE, START_TIME, STATUS, PROCESS_PID) 
                            VALUES (?, ?, ?, ?, ?, 'RUNNING', ?)
                        """, (batch_id, source, extract_timestamp, run_date, start_time, current_pid))
                    except Exception:
                        cur.execute(f"""
                            INSERT INTO {self.tech_db}.BATCH_RUNS 
                            (BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, RUN_DATE, START_TIME, STATUS) 
                            VALUES (?, ?, ?, ?, ?, 'RUNNING')
                        """, (batch_id, source, extract_timestamp, run_date, start_time))

                    # Insert tasks for each file
                    for entity_name, file_name in tasks_list:
                        cur.execute(f"""
                            INSERT INTO {self.tech_db}.BATCH_TASKS 
                            (BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, ENTITY_NAME, FILE_NAME, STATUS, RETRY_COUNT) 
                            VALUES (?, ?, ?, ?, ?, 'PENDING', 0)
                        """, (batch_id, source, extract_timestamp, entity_name, file_name))
                else:
                    logger.info(f"Batch {batch_id} (Source: {source}) already exists with status {row[0]}. Attempting recovery.")
                    try:
                        cur.execute(f"""
                            UPDATE {self.tech_db}.BATCH_RUNS 
                            SET STATUS = 'RUNNING', PROCESS_PID = ? WHERE BATCH_ID = ? AND SOURCE = ?
                        """, (current_pid, batch_id, source))
                    except Exception:
                        cur.execute(f"""
                            UPDATE {self.tech_db}.BATCH_RUNS 
                            SET STATUS = 'RUNNING' WHERE BATCH_ID = ? AND SOURCE = ?
                        """, (batch_id, source))
                    
                    # For tasks, ensure all current files are recorded
                    cur.execute(f"SELECT FILE_NAME FROM {self.tech_db}.BATCH_TASKS WHERE BATCH_ID = ? AND SOURCE = ?", (batch_id, source))
                    existing_files = set(r[0] for r in cur.fetchall())
                    for entity_name, file_name in tasks_list:
                        if file_name not in existing_files:
                            cur.execute(f"""
                                INSERT INTO {self.tech_db}.BATCH_TASKS 
                                (BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, ENTITY_NAME, FILE_NAME, STATUS, RETRY_COUNT) 
                                VALUES (?, ?, ?, ?, ?, 'PENDING', 0)
                            """, (batch_id, source, extract_timestamp, entity_name, file_name))

    def get_pending_tasks(self, batch_id, source=None):
        """Returns list of (ENTITY_NAME, FILE_NAME, RETRY_COUNT) for PENDING or FAILED tasks."""
        with self.get_connection() as con:
            with con.cursor() as cur:
                if source:
                    cur.execute(f"""
                        SELECT ENTITY_NAME, FILE_NAME, RETRY_COUNT 
                        FROM {self.tech_db}.BATCH_TASKS 
                        WHERE BATCH_ID = ? AND SOURCE = ? AND STATUS IN ('PENDING', 'FAILED')
                    """, (batch_id, source))
                else:
                    cur.execute(f"""
                        SELECT ENTITY_NAME, FILE_NAME, RETRY_COUNT 
                        FROM {self.tech_db}.BATCH_TASKS 
                        WHERE BATCH_ID = ? AND STATUS IN ('PENDING', 'FAILED')
                    """, (batch_id,))
                return cur.fetchall()

    def update_task_start(self, batch_id, entity_name, file_name=None, source=None):
        """Updates task status to RUNNING. If file_name is None, updates all files for this entity."""
        ts_now = current_timestamp_str()
        with self.get_connection() as con:
            with con.cursor() as cur:
                if file_name and source:
                    cur.execute(f"""
                        UPDATE {self.tech_db}.BATCH_TASKS 
                        SET STATUS = 'RUNNING', START_TIME = ?, RETRY_COUNT = RETRY_COUNT + 1 
                        WHERE BATCH_ID = ? AND SOURCE = ? AND ENTITY_NAME = ? AND FILE_NAME = ?
                    """, (ts_now, batch_id, source, entity_name, file_name))
                elif file_name:
                    cur.execute(f"""
                        UPDATE {self.tech_db}.BATCH_TASKS 
                        SET STATUS = 'RUNNING', START_TIME = ?, RETRY_COUNT = RETRY_COUNT + 1 
                        WHERE BATCH_ID = ? AND ENTITY_NAME = ? AND FILE_NAME = ?
                    """, (ts_now, batch_id, entity_name, file_name))
                elif source:
                    cur.execute(f"""
                        UPDATE {self.tech_db}.BATCH_TASKS 
                        SET STATUS = 'RUNNING', START_TIME = ?, RETRY_COUNT = RETRY_COUNT + 1 
                        WHERE BATCH_ID = ? AND SOURCE = ? AND ENTITY_NAME = ?
                    """, (ts_now, batch_id, source, entity_name))
                else:
                    cur.execute(f"""
                        UPDATE {self.tech_db}.BATCH_TASKS 
                        SET STATUS = 'RUNNING', START_TIME = ?, RETRY_COUNT = RETRY_COUNT + 1 
                        WHERE BATCH_ID = ? AND ENTITY_NAME = ?
                    """, (ts_now, batch_id, entity_name))

    def update_task_end(self, batch_id, entity_name, status, file_name=None, error_msg="", source=None):
        """Updates task status to SUCCESS or FAILED. If file_name is None, updates all files for this entity."""
        ts_now = current_timestamp_str()
        with self.get_connection() as con:
            with con.cursor() as cur:
                error_msg = str(error_msg)[:1999] if error_msg else None
                if file_name and source:
                    cur.execute(f"""
                        UPDATE {self.tech_db}.BATCH_TASKS 
                        SET STATUS = ?, END_TIME = ?, ERROR_MESSAGE = ? 
                        WHERE BATCH_ID = ? AND SOURCE = ? AND ENTITY_NAME = ? AND FILE_NAME = ?
                    """, (status, ts_now, error_msg, batch_id, source, entity_name, file_name))
                elif file_name:
                    cur.execute(f"""
                        UPDATE {self.tech_db}.BATCH_TASKS 
                        SET STATUS = ?, END_TIME = ?, ERROR_MESSAGE = ? 
                        WHERE BATCH_ID = ? AND ENTITY_NAME = ? AND FILE_NAME = ?
                    """, (status, ts_now, error_msg, batch_id, entity_name, file_name))
                elif source:
                    cur.execute(f"""
                        UPDATE {self.tech_db}.BATCH_TASKS 
                        SET STATUS = ?, END_TIME = ?, ERROR_MESSAGE = ? 
                        WHERE BATCH_ID = ? AND SOURCE = ? AND ENTITY_NAME = ?
                    """, (status, ts_now, error_msg, batch_id, source, entity_name))
                else:
                    cur.execute(f"""
                        UPDATE {self.tech_db}.BATCH_TASKS 
                        SET STATUS = ?, END_TIME = ?, ERROR_MESSAGE = ? 
                        WHERE BATCH_ID = ? AND ENTITY_NAME = ?
                    """, (status, ts_now, error_msg, batch_id, entity_name))

    def get_batch_summary(self, batch_id, expected_total_files, source=None):
        """Returns True if all expected files for this batch are SUCCESS, False otherwise."""
        with self.get_connection() as con:
            with con.cursor() as cur:
                if source:
                    cur.execute(f"""
                        SELECT COUNT(*) FROM {self.tech_db}.BATCH_TASKS 
                        WHERE BATCH_ID = ? AND SOURCE = ? AND STATUS = 'SUCCESS'
                    """, (batch_id, source))
                else:
                    cur.execute(f"""
                        SELECT COUNT(*) FROM {self.tech_db}.BATCH_TASKS 
                        WHERE BATCH_ID = ? AND STATUS = 'SUCCESS'
                    """, (batch_id,))
                success_count = cur.fetchone()[0]
                return success_count == expected_total_files

    def finish_batch(self, batch_id, final_status, source=None):
        ts_now = current_timestamp_str()
        with self.get_connection() as con:
            with con.cursor() as cur:
                try:
                    if source:
                        cur.execute(f"""
                            UPDATE {self.tech_db}.BATCH_RUNS 
                            SET STATUS = ?, END_TIME = ?, PROCESS_PID = NULL 
                            WHERE BATCH_ID = ? AND SOURCE = ?
                        """, (final_status, ts_now, batch_id, source))
                    else:
                        cur.execute(f"""
                            UPDATE {self.tech_db}.BATCH_RUNS 
                            SET STATUS = ?, END_TIME = ?, PROCESS_PID = NULL 
                            WHERE BATCH_ID = ?
                        """, (final_status, ts_now, batch_id))
                except Exception:
                    if source:
                        cur.execute(f"""
                            UPDATE {self.tech_db}.BATCH_RUNS 
                            SET STATUS = ?, END_TIME = ? 
                            WHERE BATCH_ID = ? AND SOURCE = ?
                        """, (final_status, ts_now, batch_id, source))
                    else:
                        cur.execute(f"""
                            UPDATE {self.tech_db}.BATCH_RUNS 
                            SET STATUS = ?, END_TIME = ? 
                            WHERE BATCH_ID = ?
                        """, (final_status, ts_now, batch_id))

    def get_active_batch_run(self, source=None):
        """Returns active batch dict (including PROCESS_PID) if any batch has STATUS = 'RUNNING'."""
        with self.get_connection() as con:
            with con.cursor() as cur:
                try:
                    if source:
                        cur.execute(f"""
                            SELECT BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, START_TIME, PROCESS_PID 
                            FROM {self.tech_db}.BATCH_RUNS 
                            WHERE STATUS = 'RUNNING' AND SOURCE = ?
                        """, (source,))
                    else:
                        cur.execute(f"""
                            SELECT BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, START_TIME, PROCESS_PID 
                            FROM {self.tech_db}.BATCH_RUNS 
                            WHERE STATUS = 'RUNNING'
                        """)
                    row = cur.fetchone()
                    if row:
                        return {
                            "batch_id": row[0],
                            "source": row[1],
                            "extract_timestamp": row[2],
                            "start_time": row[3],
                            "process_pid": row[4]
                        }
                    return None
                except Exception:
                    # Fallback if PROCESS_PID column not yet added to BATCH_RUNS DDL
                    if source:
                        cur.execute(f"""
                            SELECT BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, START_TIME 
                            FROM {self.tech_db}.BATCH_RUNS 
                            WHERE STATUS = 'RUNNING' AND SOURCE = ?
                        """, (source,))
                    else:
                        cur.execute(f"""
                            SELECT BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, START_TIME 
                            FROM {self.tech_db}.BATCH_RUNS 
                            WHERE STATUS = 'RUNNING'
                        """)
                    row = cur.fetchone()
                    if row:
                        return {
                            "batch_id": row[0],
                            "source": row[1],
                            "extract_timestamp": row[2],
                            "start_time": row[3],
                            "process_pid": None
                        }
                    return None

    def mark_batch_failed(self, batch_id, source=None):
        """Marks an active or abandoned batch as FAILED and clears PROCESS_PID."""
        ts_now = current_timestamp_str()
        with self.get_connection() as con:
            with con.cursor() as cur:
                try:
                    if source:
                        cur.execute(f"""
                            UPDATE {self.tech_db}.BATCH_RUNS 
                            SET STATUS = 'FAILED', END_TIME = ?, PROCESS_PID = NULL 
                            WHERE BATCH_ID = ? AND SOURCE = ? AND STATUS = 'RUNNING'
                        """, (ts_now, batch_id, source))
                    else:
                        cur.execute(f"""
                            UPDATE {self.tech_db}.BATCH_RUNS 
                            SET STATUS = 'FAILED', END_TIME = ?, PROCESS_PID = NULL 
                            WHERE BATCH_ID = ? AND STATUS = 'RUNNING'
                        """, (ts_now, batch_id))
                except Exception:
                    if source:
                        cur.execute(f"""
                            UPDATE {self.tech_db}.BATCH_RUNS 
                            SET STATUS = 'FAILED', END_TIME = ? 
                            WHERE BATCH_ID = ? AND SOURCE = ? AND STATUS = 'RUNNING'
                        """, (ts_now, batch_id, source))
                    else:
                        cur.execute(f"""
                            UPDATE {self.tech_db}.BATCH_RUNS 
                            SET STATUS = 'FAILED', END_TIME = ? 
                            WHERE BATCH_ID = ? AND STATUS = 'RUNNING'
                        """, (ts_now, batch_id))

