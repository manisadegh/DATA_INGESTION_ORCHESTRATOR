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
        self.host = td_config['host']
        self.user = td_config['user']
        
        # Support either plain-text password or encrypted pass/key files (TJEncryptPassword)
        if 'pass_file' in td_config and 'key_file' in td_config:
            self.password = f"ENCRYPTED_PASSWORD(file:{td_config['pass_file']},file:{td_config['key_file']})"
        else:
            self.password = td_config.get('password')
            
        self.logmech = td_config.get('logmech', 'TD2')
        self.tech_db = td_config['technical_database']
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

        with self.get_connection() as con:
            with con.cursor() as cur:
                # Check if batch exists
                cur.execute(f"SELECT STATUS FROM {self.tech_db}.BATCH_RUNS WHERE BATCH_ID = ?", (batch_id,))
                row = cur.fetchone()

                if row is None:
                    logger.info(f"Initializing new batch {batch_id} (Source: {source}, ExtractTS: {extract_timestamp})")
                    # Insert batch run
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
                    logger.info(f"Batch {batch_id} already exists with status {row[0]}. Attempting recovery.")
                    cur.execute(f"""
                        UPDATE {self.tech_db}.BATCH_RUNS 
                        SET STATUS = 'RUNNING' WHERE BATCH_ID = ?
                    """, (batch_id,))
                    
                    # For tasks, ensure all current files are recorded
                    cur.execute(f"SELECT FILE_NAME FROM {self.tech_db}.BATCH_TASKS WHERE BATCH_ID = ?", (batch_id,))
                    existing_files = set(r[0] for r in cur.fetchall())
                    for entity_name, file_name in tasks_list:
                        if file_name not in existing_files:
                            cur.execute(f"""
                                INSERT INTO {self.tech_db}.BATCH_TASKS 
                                (BATCH_ID, SOURCE, EXTRACT_TIMESTAMP, ENTITY_NAME, FILE_NAME, STATUS, RETRY_COUNT) 
                                VALUES (?, ?, ?, ?, ?, 'PENDING', 0)
                            """, (batch_id, source, extract_timestamp, entity_name, file_name))

    def get_pending_tasks(self, batch_id):
        """Returns list of (ENTITY_NAME, FILE_NAME, RETRY_COUNT) for PENDING or FAILED tasks."""
        with self.get_connection() as con:
            with con.cursor() as cur:
                cur.execute(f"""
                    SELECT ENTITY_NAME, FILE_NAME, RETRY_COUNT 
                    FROM {self.tech_db}.BATCH_TASKS 
                    WHERE BATCH_ID = ? AND STATUS IN ('PENDING', 'FAILED')
                """, (batch_id,))
                return cur.fetchall()

    def update_task_start(self, batch_id, entity_name, file_name=None):
        """Updates task status to RUNNING. If file_name is None, updates all files for this entity."""
        ts_now = current_timestamp_str()
        with self.get_connection() as con:
            with con.cursor() as cur:
                if file_name:
                    cur.execute(f"""
                        UPDATE {self.tech_db}.BATCH_TASKS 
                        SET STATUS = 'RUNNING', START_TIME = ?, RETRY_COUNT = RETRY_COUNT + 1 
                        WHERE BATCH_ID = ? AND ENTITY_NAME = ? AND FILE_NAME = ?
                    """, (ts_now, batch_id, entity_name, file_name))
                else:
                    cur.execute(f"""
                        UPDATE {self.tech_db}.BATCH_TASKS 
                        SET STATUS = 'RUNNING', START_TIME = ?, RETRY_COUNT = RETRY_COUNT + 1 
                        WHERE BATCH_ID = ? AND ENTITY_NAME = ?
                    """, (ts_now, batch_id, entity_name))

    def update_task_end(self, batch_id, entity_name, status, file_name=None, error_msg=""):
        """Updates task status to SUCCESS or FAILED. If file_name is None, updates all files for this entity."""
        ts_now = current_timestamp_str()
        with self.get_connection() as con:
            with con.cursor() as cur:
                error_msg = str(error_msg)[:1999] if error_msg else None
                if file_name:
                    cur.execute(f"""
                        UPDATE {self.tech_db}.BATCH_TASKS 
                        SET STATUS = ?, END_TIME = ?, ERROR_MESSAGE = ? 
                        WHERE BATCH_ID = ? AND ENTITY_NAME = ? AND FILE_NAME = ?
                    """, (status, ts_now, error_msg, batch_id, entity_name, file_name))
                else:
                    cur.execute(f"""
                        UPDATE {self.tech_db}.BATCH_TASKS 
                        SET STATUS = ?, END_TIME = ?, ERROR_MESSAGE = ? 
                        WHERE BATCH_ID = ? AND ENTITY_NAME = ?
                    """, (status, ts_now, error_msg, batch_id, entity_name))

    def get_batch_summary(self, batch_id, expected_total_files):
        """Returns True if all expected files for this batch are SUCCESS, False otherwise."""
        with self.get_connection() as con:
            with con.cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(*) FROM {self.tech_db}.BATCH_TASKS 
                    WHERE BATCH_ID = ? AND STATUS = 'SUCCESS'
                """, (batch_id,))
                success_count = cur.fetchone()[0]
                return success_count == expected_total_files

    def finish_batch(self, batch_id, final_status):
        ts_now = current_timestamp_str()
        with self.get_connection() as con:
            with con.cursor() as cur:
                cur.execute(f"""
                    UPDATE {self.tech_db}.BATCH_RUNS 
                    SET STATUS = ?, END_TIME = ? 
                    WHERE BATCH_ID = ?
                """, (final_status, ts_now, batch_id))
