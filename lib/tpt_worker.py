import logging
import subprocess
import os
from typing import Optional

logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")

def run_tpt_loader(
    entity,
    file_path_or_pattern,
    retry_count,
    batch_id,
    tpt_config=None,
    orch_config=None,
    db_state=None,
    load_mode="replace",
    skip_rows=1,
    skip_rows_every_file="N",
    extract_timestamp=None,
    source: Optional[str] = None,
    skip_precheck: bool = False,
    has_meta: int = 2,
    work_zone: Optional[str] = None,
    delimiter: Optional[str] = None,
):
    """
    Executes the bash wrapper for TPT in a subprocess with auto-healing.
    Operates strictly in the isolated LOADING_ZONE for a Single Batch.
    Parameters are decoupled via tpt_config (with backward compatibility fallback to orch_config).
    """
    if tpt_config is None:
        tpt_config = {}
    if orch_config is None:
        orch_config = {}

    ts_info = f", ExtractTS '{extract_timestamp}'" if extract_timestamp else ""
    logger.info(
        f"Starting TPT for entity '{entity}' (Batch {batch_id}{ts_info}, Retry {retry_count}, Mode {load_mode}, "
        f"SkipRows {skip_rows}, SkipRowsEveryFile {skip_rows_every_file}, SkipPrecheck {skip_precheck}, HasMeta {has_meta})"
    )
    
    # Mark task start in DB
    if db_state:
        db_state.update_task_start(batch_id, entity, source=source)
    
    # Resolve TPT specific variables from tpt_config, falling back to orch_config
    script_path = tpt_config.get('wrapper_script') or orch_config.get('tpt_wrapper_script')
    unlock_script = tpt_config.get('unlock_script') or orch_config.get('tpt_unlock_script')
    timeout_sec = tpt_config.get('timeout_seconds') or orch_config.get('tpt_timeout_seconds', 3600)
    target_db = tpt_config.get('target_db') or orch_config.get('target_db')
    work_db = tpt_config.get('work_db') or orch_config.get('work_db')
    work_dir = tpt_config.get('work_dir') or orch_config.get('tpt_work_dir')
    
    tpt_env = tpt_config.get('tpt_env') or orch_config.get('tpt_env')
    if tpt_env is None:
        tpt_env = os.environ.copy()
    else:
        tpt_env = tpt_env.copy()
        
    loading_zone = work_zone or orch_config.get('loading_zone', orch_config.get('landing_zone'))
    
    # Check if full path already provided or relative to loading_zone
    if os.path.isabs(file_path_or_pattern) or file_path_or_pattern.startswith('/tmp') or file_path_or_pattern.startswith('//'):
        actual_file_path = file_path_or_pattern
    else:
        actual_file_path = os.path.join(loading_zone, file_path_or_pattern)
                
    bash_bin = "/bin/bash"

    cmd = [
        bash_bin, script_path, 
        "-f", actual_file_path, 
        "-t", entity, 
        "-B", str(batch_id),
        "-m", load_mode,
        "-S", str(skip_rows),
        "-Y", str(skip_rows_every_file)
    ]
    if extract_timestamp:
        cmd.extend(["-T", str(extract_timestamp)])
    
    if skip_precheck:
        cmd.append("-C")
        cmd.extend(["-M", str(has_meta)])

    if work_dir:
        os.makedirs(work_dir, exist_ok=True)
        cmd.extend(["-w", str(work_dir)])

    eff_delimiter = delimiter or tpt_config.get('delimiter') or orch_config.get('delimiter')
    if eff_delimiter:
        cmd.extend(["-s", str(eff_delimiter)])
    
    if target_db and work_db:
        cmd.extend([
            "-d", target_db,
            "-k", work_db
        ])
    if tpt_env and 'USER' in tpt_env:
        cmd.extend(["-u", tpt_env['USER']])
        
    # Build unlock command if script provided
    unlock_cmd = None
    if unlock_script:
        unlock_cmd = [
            bash_bin, unlock_script,
            "-t", entity,
            "-m", load_mode,
            "-B", str(batch_id)
        ]
        if extract_timestamp:
            unlock_cmd.extend(["-T", str(extract_timestamp)])
        if target_db and work_db:
            unlock_cmd.extend([
                "-d", target_db,
                "-k", work_db
            ])
        if work_dir:
            unlock_cmd.extend(["-w", str(work_dir)])
        if tpt_env and 'USER' in tpt_env:
            unlock_cmd.extend(["-u", tpt_env['USER']])
    
    # 1. Pre-emptive Unlock
    if retry_count > 0 and unlock_cmd:
        logger.warning(f"[{entity}] Previous failure detected (Retry {retry_count}). Triggering pre-emptive unlock.")
        try:
            subprocess.run(unlock_cmd, capture_output=True, env=tpt_env, timeout=600)
        except subprocess.TimeoutExpired:
            logger.error(f"[{entity}] Pre-emptive unlock timed out after 600 seconds.")
    
    try:
        logger.info(f"Executing: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, errors='replace', env=tpt_env, timeout=timeout_sec)
        
        # 2. Reactive Unlock (Error 2652 detected)
        stdout_str = result.stdout or ""
        stderr_str = result.stderr or ""
        if result.returncode not in [0, 4] and ("2652" in stdout_str or "2652" in stderr_str or "Table is being Loaded" in stderr_str):
            if unlock_cmd:
                logger.warning(f"[{entity}] Error 2652 detected. Triggering reactive unlock and retrying...")
                try:
                    unlock_res = subprocess.run(unlock_cmd, capture_output=True, text=True, errors='replace', env=tpt_env, timeout=600)
                    if unlock_res.returncode in [0, 4] and load_mode == "replace":
                        logger.info(f"[{entity}] Resumed job completed successfully via unlock script.")
                        result = unlock_res
                    else:
                        result = subprocess.run(cmd, capture_output=True, text=True, errors='replace', env=tpt_env, timeout=timeout_sec) # Retry once
                except subprocess.TimeoutExpired:
                    logger.error(f"[{entity}] Reactive unlock or retry timed out.")
                    if db_state:
                        db_state.update_task_end(batch_id, entity, 'FAILED', error_msg="Timeout during unlock or retry.", source=source)
                    return entity, False
            else:
                logger.warning(f"[{entity}] Error 2652 detected but no unlock script configured.")
        
        # Exit code 0 or 4 (Warning) are success for TPT
        if result.returncode in [0, 4]:
            logger.info(f"Successfully loaded entity '{entity}'")
            if db_state:
                db_state.update_task_end(batch_id, entity, 'SUCCESS', source=source)
            return entity, True
        else:
            err_msg = result.stderr if result.stderr else result.stdout
            logger.error(f"Failed to load entity '{entity}' (RC {result.returncode}). Msg: {err_msg}")
            if db_state:
                db_state.update_task_end(batch_id, entity, 'FAILED', error_msg=err_msg, source=source)
            return entity, False
            
    except subprocess.TimeoutExpired:
        logger.error(f"[{entity}] TPT execution timed out after {timeout_sec} seconds.")
        if db_state:
            db_state.update_task_end(batch_id, entity, 'FAILED', error_msg=f"TimeoutExpired: Process exceeded {timeout_sec}s limit.", source=source)
        return entity, False
    except Exception as e:
        logger.exception(f"Exception while loading entity '{entity}': {e}")
        if db_state:
            db_state.update_task_end(batch_id, entity, 'FAILED', error_msg=str(e), source=source)
        return entity, False
