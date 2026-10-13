import logging
import subprocess
import os

logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")

def run_tpt_loader(entity, file_path_or_pattern, retry_count, batch_id, tpt_config=None, orch_config=None, db_state=None, load_mode="replace", skip_rows=1, skip_rows_every_file="N"):
    """
    Executes the bash wrapper for TPT in a subprocess with auto-healing.
    Operates strictly in the isolated LOADING_ZONE for a Single Batch.
    Parameters are decoupled via tpt_config (with backward compatibility fallback to orch_config).
    """
    if tpt_config is None:
        tpt_config = {}
    if orch_config is None:
        orch_config = {}
    if hasattr(orch_config, 'update_task_start') and db_state is None:
        # Legacy positional signature: (entity, file, retry, batch, orch_config, db_state)
        db_state = orch_config
        orch_config = tpt_config
        tpt_config = {}

    logger.info(f"Starting TPT for entity '{entity}' (Batch {batch_id}, Retry {retry_count}, Mode {load_mode}, SkipRows {skip_rows})")
    
    # Mark task start in DB
    if db_state:
        db_state.update_task_start(batch_id, entity)
    
    # Resolve TPT specific variables from tpt_config, falling back to orch_config
    script_path = tpt_config.get('wrapper_script') or orch_config.get('tpt_wrapper_script')
    unlock_script = tpt_config.get('unlock_script') or orch_config.get('tpt_unlock_script')
    timeout_sec = tpt_config.get('timeout_seconds') or orch_config.get('tpt_timeout_seconds', 3600)
    target_db = tpt_config.get('target_db') or orch_config.get('target_db')
    work_db = tpt_config.get('work_db') or orch_config.get('work_db')
    
    tpt_env = tpt_config.get('tpt_env') or orch_config.get('tpt_env')
    if tpt_env is None:
        tpt_env = os.environ.copy()
    else:
        tpt_env = tpt_env.copy()
        
    if os.name == 'nt':
        tpt_env['MSYS'] = 'noglob'
    
    loading_zone = orch_config.get('loading_zone', orch_config.get('landing_zone'))
    
    # Check if full path already provided or relative to loading_zone
    if os.path.isabs(file_path_or_pattern) or file_path_or_pattern.startswith('/tmp') or file_path_or_pattern.startswith('//'):
        actual_file_path = file_path_or_pattern
    else:
        actual_file_path = f"{loading_zone}/{file_pattern_clean(file_path_or_pattern)}"
                
    bash_bin = "bash"
    # Linux / POSIX is the primary production target.
    # On Windows dev machines only, resolve Git Bash dynamically if present.
    if os.name == 'nt':
        local_app_data = os.environ.get('LOCALAPPDATA', '')
        program_files = os.environ.get('ProgramFiles', r'C:\Program Files')
        for candidate in [
            os.path.join(local_app_data, r"Programs\Git\bin\bash.exe") if local_app_data else None,
            os.path.join(program_files, r"Git\bin\bash.exe"),
            os.path.join(program_files, r"Git\usr\bin\bash.exe")
        ]:
            if candidate and os.path.exists(candidate):
                bash_bin = candidate
                break

    cmd = [
        bash_bin, script_path, 
        "-f", actual_file_path, 
        "-t", entity, 
        "-B", str(batch_id),
        "-m", load_mode,
        "-S", str(skip_rows)
    ]
    
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
        unlock_cmd = [bash_bin, unlock_script, "-t", entity, "-m", "replace"]
        if target_db and work_db:
            unlock_cmd.extend([
                "-d", target_db,
                "-k", work_db
            ])
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
                    unlock_res = subprocess.run(unlock_cmd, capture_output=True, text=True, env=tpt_env, timeout=600)
                    if unlock_res.returncode in [0, 4] and load_mode == "replace":
                        logger.info(f"[{entity}] Resumed job completed successfully via unlock script.")
                        result = unlock_res
                    else:
                        result = subprocess.run(cmd, capture_output=True, text=True, env=tpt_env, timeout=timeout_sec) # Retry once
                except subprocess.TimeoutExpired:
                    logger.error(f"[{entity}] Reactive unlock or retry timed out.")
                    db_state.update_task_end(batch_id, entity, 'FAILED', error_msg="Timeout during unlock or retry.")
                    return entity, False
            else:
                logger.warning(f"[{entity}] Error 2652 detected but no unlock script configured.")
        
        # Exit code 0 or 4 (Warning) are success for TPT
        if result.returncode in [0, 4]:
            logger.info(f"Successfully loaded entity '{entity}'")
            db_state.update_task_end(batch_id, entity, 'SUCCESS')
            return entity, True
        else:
            err_msg = result.stderr if result.stderr else result.stdout
            logger.error(f"Failed to load entity '{entity}' (RC {result.returncode}). Msg: {err_msg}")
            db_state.update_task_end(batch_id, entity, 'FAILED', error_msg=err_msg)
            return entity, False
            
    except subprocess.TimeoutExpired:
        logger.error(f"[{entity}] TPT execution timed out after {timeout_sec} seconds.")
        db_state.update_task_end(batch_id, entity, 'FAILED', error_msg=f"TimeoutExpired: Process exceeded {timeout_sec}s limit.")
        return entity, False
    except Exception as e:
        logger.exception(f"Exception while loading entity '{entity}': {e}")
        db_state.update_task_end(batch_id, entity, 'FAILED', error_msg=str(e))
        return entity, False

def file_pattern_clean(f):
    """Ensures forward slash consistency."""
    return f.replace('\\', '/')
