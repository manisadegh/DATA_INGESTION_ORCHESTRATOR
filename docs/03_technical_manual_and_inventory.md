# DATA_INGESTION_ORCHESTRATOR - Technical Manual & Inventory

## 1. System Context & Component Architecture

The **DATA_INGESTION_ORCHESTRATOR** (`orchestrator.py`) is deployed on the Linux application server at:
```
/sg/local/sid/home/Dsidadm/DATA_LOADER/PROCESSING_TOOLS/DATA_INGESTION_ORCHESTRATOR/
```
It controls the atomic ingress, decompression, header normalization, and parallel ingestion of single batches into Teradata Vantage, interfacing directly with `load_csv_generic.sh` and tracking execution state in technical tables.

```
DATA_INGESTION_ORCHESTRATOR/
├── orchestrator.py             # Master orchestrator entrypoint & concurrency sentry
├── config.json                 # Deployment configuration file
├── config_template.json        # Template configuration with concurrency defaults
├── entity_mapping.csv          # Entity-to-table signature mapping definitions
├── ddl_technical_tables.sql    # Teradata DDL for BATCH_RUNS & BATCH_TASKS
├── docs/                       # Technical and operational documentation suite
└── lib/
    ├── sensor.py               # Landing zone discovery, gating & checksum verification
    ├── db_state.py             # Teradata state persistence (BATCH_RUNS / TASKS)
    ├── tpt_worker.py           # Subprocess execution & Error 2652 reactive lock recovery
    ├── post_tasks.py           # Source and global post-task execution engine
    └── logger.py               # Batch log capsule and execution logging setup
```

---

## 2. Module Inventory & Code Signatures

### 2.1 Entrypoint: `orchestrator.py`
- **Path**: `orchestrator.py`
- **Role**: Master batch ingress controller, concurrency driver, archive manager.

#### Functions in `orchestrator.py`:
1. `is_pid_alive(pid: int) -> bool`
   - **Signature**: `is_pid_alive(pid: int) -> bool`
   - **Description**: Verifies if an OS Process ID is actively running. On POSIX/Linux, executes `os.kill(pid, 0)` with error trapping for dead processes vs permission boundaries. On Windows NT, uses Win32 `kernel32.OpenProcess`.
   - **Return**: `True` if process is active; `False` if dead or non-existent.

2. `strip_header_in_place(file_path: str) -> None`
   - **Signature**: `strip_header_in_place(file_path: str) -> None`
   - **Description**: Strips line 1 (header) from a delimited text file in-place using a temporary swap file (`.tmp`). Writes a `.<filename>.header_stripped` marker file to guarantee idempotency across task retries.
   - **Side Effects**: Modifies file in-place; creates hidden marker file.

3. `process_entity_task(entity, entity_info, batch_id, extract_timestamp, tpt_config, orch_config, db) -> bool`
   - **Signature**: `process_entity_task(entity, entity_info, batch_id, extract_timestamp, tpt_config, orch_config, db) -> bool`
   - **Description**: ThreadPool worker target. Safely unpacks archives (`.zip` / `.gz`) inside `LOADING_ZONE`, determines single vs multi-chunk structure, applies `strip_header_in_place` on Chunk 1 if multi-chunk delimited text, configures `SkipRows` (0 for normalized multi-chunks, 1 for single files), invokes `run_tpt_loader()`, and returns task success boolean.

4. `expand_env_vars(data: Any) -> Any`
   - **Signature**: `expand_env_vars(data: Any) -> Any`
   - **Description**: Recursively expands `${VAR}` or `$VAR` environment variables in configuration strings, dictionaries, and lists.

5. `main() -> None`
   - **Signature**: `main() -> None`
   - **Execution Sequence**:
     1. **Configuration & Paths**: Loads `config.json`, resolves relative paths, applies `--source` partition overrides to `LANDING_ZONE`, `LOADING_ZONE`, `ARCHIVE_ZONE`, and `LOGS`.
     2. **Teradata Pre-Flight Check**: Queries `BATCH_RUNS` for active batches of this source (`get_active_batch_run(source)`). If active PID is alive, enters active polling wait loop (`poll_interval_seconds: 5`). If PID is dead, reclassifies orphaned batch as `FAILED` (`mark_batch_failed`).
     3. **Process Lock Sentry**: Inspects `LOADING_ZONE/<SOURCE>/.orchestrator.pid`. If an active PID is alive, polls until completion. Acquires lock and registers `atexit` cleanup.
     4. **In-Flight Crash Check**: Inspects `LOADING_ZONE/<SOURCE>` for uncompleted `.MF`. If found, activates crash recovery (`is_in_flight = True`), bypassing checksum validation and resuming remaining tasks.
     5. **Landing Zone Gating**: If no in-flight batch, scans `LANDING_ZONE/<SOURCE>` via `scan_landing_zone()`. Enforces single-batch invariant (OPS-01) and orphan-free integrity.
     6. **Batch Log Capsule Setup**: Creates `LOGS/<SOURCE>/<YYYY-MM>/BATCH_<ID>_<TS>/` and sets `tpt_config['work_dir'] = .../TPT/`.
     7. **Payload Validation & Move**: Validates checksums, maps files to entities (`verify_and_map_batch`), and moves payload files from `LANDING_ZONE` to `LOADING_ZONE`.
     8. **State Initialization**: Inserts or updates batch in `BATCH_RUNS` (`STATUS = 'RUNNING'`, records `PROCESS_PID`), and registers all entity files in `BATCH_TASKS` as `PENDING`.
     9. **Parallel Worker Dispatch**: Executes entity tasks in parallel via `ThreadPoolExecutor(max_workers=max_concurrent_jobs)`.
     10. **Completeness Barrier**: Verifies that 100% of expected entity files succeeded. If any task failed after maximum retries, marks `BATCH_RUNS` as `FAILED` and aborts.
     11. **Post-Tasks Execution**: Merges source-specific tasks (`--post-tasks-file`) with global tasks and executes them synchronously.
     12. **Archiving & Final Commit**: Moves data payload and manifest to `ARCHIVE_ZONE/<SOURCE>/<YYYY-MM>/BATCH_<ID>_<TS>/`, cleans temporary markers, updates `BATCH_RUNS` to `SUCCESS` (permanently retaining `PROCESS_PID`), and removes `.orchestrator.pid`.

---

### 2.2 Module: `lib/sensor.py`
- **Path**: `lib/sensor.py`
- **Role**: Gating enforcement, manifest parsing, checksum computation, and entity mapping.

#### Functions in `lib/sensor.py`:
1. `scan_landing_zone(landing_zone: str, manifest_pattern: str = "MANIFEST_*.MF") -> Tuple[str, Optional[str], List[str]]`
   - **Description**: Inspects directory contents. Returns status (`'IDLE'`, `'ERROR_MISSING_MANIFEST'`, `'ERROR_MULTIPLE_MANIFESTS'`, `'VALID'`), manifest path, and data files list.

2. `parse_manifest_metadata(manifest_path: str, manifest_regex: Optional[str] = None) -> Dict[str, str]`
   - **Description**: Extracts `source`, `extract_timestamp`, and `batch_id` from manifest filename. Normalizes timestamp to `YYYY-MM-DD HH:MM:SS`.

3. `parse_manifest_content(manifest_path: str) -> List[Dict[str, str]]`
   - **Description**: Reads manifest CSV content (`filename,checksum`), skipping empty lines and comments.

4. `compute_file_checksum(file_path: str, algorithm: str = "md5") -> str`
   - **Description**: Reads file in 64 KB blocks and computes cryptographic MD5 or SHA256 checksum.

5. `verify_and_map_batch(...) -> Tuple[Dict[str, Dict], List[str]]`
   - **Description**: Enforces OPS-01 (orphan guard). Validates declared checksums. Maps declared files to target tables using `entity_mapping.csv`. Returns entity workload dictionary and list of validated files.

---

### 2.3 Module: `lib/db_state.py`
- **Path**: `lib/db_state.py`
- **Role**: State persistence in Teradata Vantage (`BATCH_RUNS` and `BATCH_TASKS`).

#### Class: `TeradataStateDB`
1. `__init__(td_config: dict)`: Resolves host, user, encrypted password (`Key`/`Pass` files or `TD_PASSWORD`), logmech, and technical database name.
2. `get_connection() -> teradatasql.TeradataConnection`: Establishes Teradata connection.
3. `initialize_batch(batch_id, source, extract_timestamp, tasks_list) -> None`: Inserts or updates a row in `BATCH_RUNS` with `STATUS = 'RUNNING'` and `PROCESS_PID = os.getpid()`. Inserts task records in `BATCH_TASKS` as `PENDING`. Formats dates strictly to `YYYY-MM-DD` and `YYYY-MM-DD HH:MM:SS` to avoid Teradata Error 5404.
4. `get_pending_tasks(batch_id, source=None) -> List[Tuple]`: Returns list of `(ENTITY_NAME, FILE_NAME, RETRY_COUNT)` for `PENDING` or `FAILED` tasks.
5. `update_task_start(batch_id, entity_name, file_name=None, source=None)`: Commits task status as `RUNNING` with current timestamp.
6. `update_task_success(batch_id, entity_name, file_name=None, source=None)`: Commits task status as `SUCCESS` with end timestamp.
7. `update_task_failure(batch_id, entity_name, error_msg, file_name=None, source=None)`: Increments `RETRY_COUNT`, records error message, and sets status to `FAILED`.
8. `get_batch_summary(batch_id, expected_total_files, source=None) -> bool`: Returns `True` if `COUNT(*) WHERE STATUS = 'SUCCESS'` equals expected files count.
9. `finish_batch(batch_id, final_status, source=None)`: Updates `BATCH_RUNS` status (`SUCCESS` or `FAILED`) and commits `END_TIME` while permanently preserving `PROCESS_PID`.
10. `get_active_batch_run(source=None) -> Optional[Dict]`: Queries for any batch with `STATUS = 'RUNNING'` (optionally filtered by source), returning `BATCH_ID`, `SOURCE`, `EXTRACT_TIMESTAMP`, and `PROCESS_PID`.
11. `mark_batch_failed(batch_id, source=None)`: Auto-heals orphaned batches by setting `STATUS = 'FAILED'`.

---

### 2.4 Module: `lib/tpt_worker.py`
- **Path**: `lib/tpt_worker.py`
- **Role**: Subprocess execution of `load_csv_generic.sh` with auto-healing and pre-emptive table unlocking.

#### Functions in `lib/tpt_worker.py`:
1. `run_tpt_loader(entity, file_path_or_pattern, retry_count, batch_id, ...) -> Tuple[int, bool]`
   - **Description**: Dispatches `TPT_LOADER/load_csv_generic.sh`.
   - **Auto-Healing**:
     - On retry ($>0$), issues a pre-emptive unlock via `unlock_tpt_table.sh`.
     - Intercepts return code `3` or output containing Error `2652` (`Table is being Loaded / Paused`), executes reactive unlock, and retries.
     - Safely replaces non-UTF8 characters in output streams (`errors='replace'`).

---

### 2.5 Module: `lib/post_tasks.py`
- **Path**: `lib/post_tasks.py`
- **Role**: Post-ingestion task execution engine.

#### Functions in `lib/post_tasks.py`:
1. `run_post_tasks(post_tasks_config: list, batch_id: str, source: str, extract_timestamp: str, ...) -> bool`
   - **Description**: Merges source-specific tasks (loaded from `--post-tasks-file`) with global tasks from `config.json`.
   - **Variable Injection**: Automatically exports `$CURRENT_BATCH_ID`, `$CURRENT_SOURCE`, and `$CURRENT_EXTRACT_TIMESTAMP` into subprocess environments.

---

## 3. Exit Codes & Process Termination Matrix

The orchestrator adheres to standard POSIX exit semantics:

| Exit Code | Classification | Cause / Trigger | Operational Impact |
|:---:|:---|:---|:---|
| **`0`** | `SUCCESS / IDLE` | - Batch loaded, verified, transformed, and archived with 100% success.<br>- `LANDING_ZONE` is completely empty (`IDLE`). | System ready for subsequent batch. |
| **`1`** | `FATAL / ABORT` | - Boundary violation OPS-01 (multiple manifests or unmanifested orphan files).<br>- Checksum mismatch on data payloads.<br>- Process lock wait timeout (`lock_timeout_seconds`) exceeded.<br>- TPT entity load failed after exhausting all retries.<br>- Downstream post-task failed. | Batch marked `FAILED` in `BATCH_RUNS`. Ingress halts immediately. |
