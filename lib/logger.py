import logging
import sys
import os
from datetime import datetime

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(threadName)-12s | %(message)s"

def setup_logger():
    """Initializes bootstrap console logger."""
    logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logger.addHandler(stdout_handler)
    return logger

def add_batch_file_logger(logs_dir, source, extract_timestamp, batch_id):
    """
    Attaches a dedicated run-specific FileHandler in logs_dir.
    Naming: LOGS/orchestrator_<SOURCE>_<EXTRACT_TIMESTAMP>_<BATCH_ID>_<EXECUTION_TS>.log
    """
    logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")
    os.makedirs(logs_dir, exist_ok=True)
    
    execution_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Clean any special characters from components
    clean_src = str(source).replace("/", "_").replace("\\", "_")
    clean_ext = str(extract_timestamp).replace(":", "").replace("-", "").replace(" ", "_")
    clean_batch = str(batch_id).replace("/", "_").replace("\\", "_")
    
    log_filename = f"orchestrator_{clean_src}_{clean_ext}_{clean_batch}_{execution_ts}.log"
    log_filepath = os.path.join(logs_dir, log_filename).replace("\\", "/")
    
    file_handler = logging.FileHandler(log_filepath, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(file_handler)
    
    logger.info(f"Initialized run log file: {log_filepath}")
    return log_filepath
