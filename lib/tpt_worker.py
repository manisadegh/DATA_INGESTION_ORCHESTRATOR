import logging
import os
import re
import signal
import subprocess
import time
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")

_LOCK_RE = re.compile(
    r"\b(?:RDBMS error|Error)\s+2652\b|\bis being Loaded\b|\bis being MLoaded\b",
    re.IGNORECASE,
)


@dataclass
class RunResult:
    returncode: int
    output_path: str

    def tail(self, n=50):
        try:
            with open(self.output_path, "r", encoding="utf-8", errors="replace") as fh:
                return "".join(fh.readlines()[-n:])
        except OSError:
            return ""


def _is_table_locked(text: str) -> bool:
    return _LOCK_RE.search(text) is not None


def _kill_group(pid: int, sig) -> None:
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


def _run(cmd, env, timeout, output_path, grace_seconds=30) -> RunResult:
    with open(output_path, "a", encoding="utf-8") as out:
        proc = subprocess.Popen(
            cmd,
            stdout=out,
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True,
        )
        try:
            rc = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout after {timeout}s; terminating process group {proc.pid}")
            _kill_group(proc.pid, signal.SIGTERM)
            try:
                rc = proc.wait(timeout=grace_seconds)
            except subprocess.TimeoutExpired:
                _kill_group(proc.pid, signal.SIGKILL)
                rc = proc.wait()
            rc = -1 if rc == 0 else rc
    return RunResult(rc, output_path)


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
    has_meta: Optional[int] = 2,
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

    # Compute deterministic unique TPT job name (F1.4)
    safe_entity = re.sub(r"[^A-Za-z0-9_]", "_", entity)
    tpt_job_name = f"TPT_{safe_entity}_{time.strftime('%Y%m%d_%H%M%S')}_{os.getpid()}"

    ts_info = f", ExtractTS '{extract_timestamp}'" if extract_timestamp else ""
    logger.info(
        f"Starting TPT for entity '{entity}' [Job: {tpt_job_name}] (Batch {batch_id}{ts_info}, Retry {retry_count}, "
        f"Mode {load_mode}, SkipRows {skip_rows}, SkipRowsEveryFile {skip_rows_every_file}, "
        f"SkipPrecheck {skip_precheck}, HasMeta {has_meta})"
    )
    
    # Mark task start in DB with exact job name
    if db_state:
        db_state.update_task_start(batch_id, entity, source=source, tpt_job_name=tpt_job_name)
    
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
        
    loading_zone = work_zone or (orch_config.get('loading_zone', orch_config.get('landing_zone')) if orch_config else None)
    
    # Resolve file argument: -L for file lists (F2.2), -f for single files / legacy patterns
    if file_path_or_pattern.endswith(".lst") or os.path.basename(file_path_or_pattern) == "files.lst":
        file_arg = ["-L", file_path_or_pattern]
    elif os.path.isabs(file_path_or_pattern):
        file_arg = ["-f", file_path_or_pattern]
    elif loading_zone:
        file_arg = ["-f", os.path.join(loading_zone, file_path_or_pattern)]
    else:
        file_arg = ["-f", file_path_or_pattern]
                
    bash_bin = "/bin/bash"

    if work_dir:
        os.makedirs(work_dir, exist_ok=True)
    else:
        work_dir = os.getcwd()
    output_path = os.path.join(work_dir, f"{tpt_job_name}.out")

    cmd = [
        bash_bin, script_path, 
        *file_arg,
        "-t", entity, 
        "-B", str(batch_id),
        "-m", load_mode,
        "-S", str(skip_rows),
        "-Y", str(skip_rows_every_file),
        "-j", tpt_job_name,
    ]
    if extract_timestamp:
        cmd.extend(["-T", str(extract_timestamp).replace("T", " ")])
    
    if skip_precheck:
        cmd.append("-C")
    if has_meta is not None:
        cmd.extend(["-M", str(has_meta)])

    if work_dir:
        cmd.extend(["-w", str(work_dir)])

    eff_delimiter = delimiter or tpt_config.get('delimiter') or orch_config.get('delimiter')
    if eff_delimiter:
        cmd.extend(["-s", str(eff_delimiter)])
    
    if target_db and work_db:
        cmd.extend([
            "-d", target_db,
            "-k", work_db
        ])
    if tpt_env and 'TD_LOGON_USER' in tpt_env:
        cmd.extend(["-u", tpt_env['TD_LOGON_USER']])
        
    # Build unlock command if script provided
    unlock_cmd = None
    if unlock_script:
        unlock_cmd = [
            bash_bin, unlock_script,
            "-t", entity,
            "-m", load_mode,
            "-j", tpt_job_name,
            "-B", str(batch_id),
        ]
        if extract_timestamp:
            unlock_cmd.extend(["-T", str(extract_timestamp).replace("T", " ")])
        if target_db and work_db:
            unlock_cmd.extend([
                "-d", target_db,
                "-k", work_db
            ])
        if work_dir:
            unlock_cmd.extend(["-w", str(work_dir)])
        if tpt_env and 'TD_LOGON_USER' in tpt_env:
            unlock_cmd.extend(["-u", tpt_env['TD_LOGON_USER']])
    
    # 1. Pre-emptive Unlock
    if retry_count > 0 and unlock_cmd:
        logger.warning(f"[{entity}] Previous failure detected (Retry {retry_count}). Triggering pre-emptive unlock.")
        _run(unlock_cmd, env=tpt_env, timeout=600, output_path=output_path)
    
    try:
        logger.info(f"Executing: {' '.join(cmd)}")
        result = _run(cmd, env=tpt_env, timeout=timeout_sec, output_path=output_path)
        
        # 2. Reactive Unlock (Error 2652 detected)
        output_text = result.tail(500)
        if result.returncode not in (0, 4) and _is_table_locked(output_text):
            if unlock_cmd:
                logger.warning(f"[{entity}] Table lock (2652) detected. Running unlock then retrying the load once.")
                unlock_res = _run(unlock_cmd, env=tpt_env, timeout=600, output_path=output_path)
                if unlock_res.returncode not in (0, 4):
                    logger.error(f"[{entity}] Unlock failed (rc={unlock_res.returncode}); not retrying.")
                else:
                    result = _run(cmd, env=tpt_env, timeout=timeout_sec, output_path=output_path)
            else:
                logger.warning(f"[{entity}] Table lock detected but no unlock script configured.")
        
        # Exit code 0 or 4 (Warning) are success for TPT
        if result.returncode in (0, 4):
            logger.info(f"Successfully loaded entity '{entity}'")
            if db_state:
                db_state.update_task_end(batch_id, entity, 'SUCCESS', source=source)
            return entity, True
        else:
            err_msg = result.tail(50)
            logger.error(f"Failed to load entity '{entity}' (RC {result.returncode}). Msg: {err_msg}")
            if db_state:
                db_state.update_task_end(batch_id, entity, 'FAILED', error_msg=err_msg, source=source)
            return entity, False
            
    except Exception as e:
        logger.exception(f"Exception while loading entity '{entity}': {e}")
        if db_state:
            db_state.update_task_end(batch_id, entity, 'FAILED', error_msg=str(e), source=source)
        return entity, False
