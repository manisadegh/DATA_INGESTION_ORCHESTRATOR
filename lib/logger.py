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
    Standardized Naming: LOGS/<SOURCE>/<YYYY-MM>/BATCH_<ID>_<TS>/LOG_ORCHESTRATOR_<SOURCE>_<EXTRACT_TS>_<BATCH_ID>_<EXEC_TS>.log
    """
    logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")
    clean_src = str(source).replace("/", "_").replace("\\", "_")
    clean_ext = str(extract_timestamp).replace(":", "").replace("-", "").replace(" ", "_")
    clean_batch = str(batch_id).replace("/", "_").replace("\\", "_")
    year_month = f"{clean_ext[:4]}-{clean_ext[4:6]}" if len(clean_ext) >= 6 else "UNKNOWN_DATE"
    capsule_name = f"BATCH_{clean_batch}_{clean_ext}"

    # Partition logs directory by source and capsule if not already done
    target_dir = logs_dir
    if source and clean_src.lower() not in target_dir.lower():
        target_dir = os.path.join(target_dir, clean_src)
    if capsule_name not in target_dir:
        target_dir = os.path.join(target_dir, year_month, capsule_name)
    os.makedirs(target_dir, exist_ok=True)
    
    execution_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"LOG_ORCHESTRATOR_{clean_src}_{clean_ext}_{clean_batch}_{execution_ts}.log"
    log_filepath = os.path.join(target_dir, log_filename)
    
    file_handler = logging.FileHandler(log_filepath, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(file_handler)
    
    logger.info(f"Initialized run log file: {log_filepath}")
    return log_filepath
