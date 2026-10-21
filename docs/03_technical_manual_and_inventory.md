# DATA_INGESTION_ORCHESTRATOR - Technical Manual & Inventory

## 1. System Context & Component Architecture

The **DATA_INGESTION_ORCHESTRATOR** (`orchestrator.py`) is deployed on the Linux application server at:
```
/sg/local/sid/home/Dsidadm/DATA_LOADER/PROCESSING_TOOLS/DATA_INGESTION_ORCHESTRATOR/
```
It controls the atomic ingress, decompression, header normalization, and parallel ingestion of single batches into Teradata Vantage, interfacing directly with `load_csv_generic.sh` and tracking execution state in technical tables.

```
DATA_INGESTION_ORCHESTRATOR/
├── orchestrator.py             # Thin CLI entrypoint (<80 lines) delegating to OrchestratorService
├── config.json                 # Deployment configuration file
├── config_template.json        # Template configuration with concurrency & pool defaults
├── entity_mapping.csv          # Entity-to-table signature mapping definitions
├── ddl_technical_tables.sql    # Teradata DDL for BATCH_RUNS & BATCH_TASKS
├── docs/                       # Technical and operational documentation suite
└── lib/
    ├── orchestrator_service.py # OrchestratorService & BatchContext (imports dataloader_core)
    ├── sensor.py               # Landing zone discovery, gating & checksum verification
    ├── db_state.py             # Pooled Teradata state persistence (BATCH_RUNS / TASKS)
    ├── tpt_worker.py           # Subprocess execution & Error 2652 reactive lock recovery
    ├── post_tasks.py           # Source and global post-task execution engine (context_env)
    └── logger.py               # Batch log capsule and execution logging wrapper
```

---

## 2. Module Inventory & Code Signatures

### 2.1 CLI Entrypoint (`orchestrator.py`) & Service (`lib/orchestrator_service.py`)
- **Paths**: `orchestrator.py`, `lib/orchestrator_service.py`
- **Role**: Thin CLI entrypoint (`orchestrator.py`) and cohesive service class (`OrchestratorService`) backed by `COMMON/dataloader_core` (`ConfigLoader`, `ProcessLock`, `TeradataConnectionManager`, `EntityMapper`, `ManifestHandler`, `CapsulePathResolver`, `LoggingFactory`).

#### Class `OrchestratorService` & Helpers (`lib/orchestrator_service.py`):
1. `strip_header_in_place(file_path: str) -> None`
   - **Description**: Strips line 1 (header) from a delimited text file in-place using a temporary swap file (`.tmp`). Writes a `.<filename>.header_stripped` marker file after staging the swap file and before `os.replace()` to guarantee crash-safe idempotency across task retries.

2. `process_entity_task(entity, entity_info, batch_id, source, extract_timestamp, tpt_config, orch_config, db) -> bool`
   - **Description**: ThreadPool worker target. Safely unpacks archives (`.zip` / `.gz`) inside `LOADING_ZONE`, determines single vs multi-chunk structure, applies `strip_header_in_place` on Chunk 1 if multi-chunk delimited text, configures `SkipRows` (`0` for normalized multi-chunks, `1` for single files), invokes `run_tpt_loader(..., source=source)`, and returns task success boolean.

3. `OrchestratorService.wait_or_recover_active_db_batch() -> None`
   - **Description**: Queries `BATCH_RUNS` via `db.get_active_batch_run(source)`. Waits via `wait_for_pid_completion()` if `PROCESS_PID` is alive, or reclassifies orphaned batches as `FAILED` if `PROCESS_PID` is dead.

4. `OrchestratorService.detect_and_validate_batch() -> Optional[BatchContext]`
   - **Description**: Detects in-flight batches in `LOADING_ZONE` or scans `LANDING_ZONE` under Single-Batch rules (`OPS-01`), sets up Batch Capsule logging via `CapsulePathResolver.get_log_capsule_dir()`, and validates checksums and entity mappings (`verify_and_map_batch`). Returns `None` when `IDLE`.

5. `OrchestratorService.transition_to_loading_zone(ctx: BatchContext) -> None`
   - **Description**: Moves manifest and validated payload files atomically from `LANDING_ZONE` to `LOADING_ZONE`.

6. `OrchestratorService.execute_entity_tasks(ctx: BatchContext) -> bool`
   - **Description**: Registers batch in `BATCH_RUNS`/`BATCH_TASKS` and dispatches entity workers concurrently via `ThreadPoolExecutor` sharing pooled connections from `TeradataStateDB`.

7. `OrchestratorService.run_completeness_barrier_and_archive(ctx: BatchContext) -> int`
   - **Description**: Verifies 100% task completion in `BATCH_TASKS`, executes post-ingestion tasks passing `context_env` (`CURRENT_BATCH_ID`, `CURRENT_SOURCE`, `CURRENT_MANIFEST`) without mutating global `os.environ`, archives deliverables into `ARCHIVE_ZONE/<SOURCE>/<YYYY-MM>/BATCH_<ID>_<TS>/`, cleans `LOADING_ZONE`, and marks `BATCH_RUNS` as `SUCCESS`.

8. `OrchestratorService.run(skip_post_tasks: Optional[bool] = None, post_tasks_file: Optional[str] = None) -> int`
   - **Description**: Acquires `dataloader_core.locking.ProcessLock` (`fcntl.flock` + PID tracking) on `LOADING_ZONE/<SOURCE>/.orchestrator.pid`, executes all lifecycle phases, and closes all pooled Teradata connections in `finally`.

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
- **Role**: State persistence in Teradata Vantage (`BATCH_RUNS` and `BATCH_TASKS`) backed by `dataloader_core.db.TeradataConnectionManager` connection pooling.

#### Class: `TeradataStateDB`
1. `__init__(td_config: dict, conn_mgr: Optional[TeradataConnectionManager] = None)`: Instantiates or accepts a `TeradataConnectionManager` pool (default `connection_pool_size = 6`) and validates `technical_database` via `validate_sql_identifier`.
2. `get_connection() -> ContextManager[teradatasql.TeradataConnection]`: Borrows a live pooled connection from `self.conn_mgr.acquire_connection()`, returning it to the pool on exit without closing the TCP session.
3. `close() -> None`: Closes all pooled Teradata connections at orchestrator shutdown.
3. `_require_source(source: str) -> str`: Enforces that `source` is a non-empty string (`ValueError` raised if missing) so all `BATCH_RUNS` and `BATCH_TASKS` queries use the composite `(BATCH_ID, SOURCE)` primary index.
4. `initialize_batch(batch_id, source, extract_timestamp, tasks_list) -> None`: Inserts or updates a row in `BATCH_RUNS` with `STATUS = 'RUNNING'` and `PROCESS_PID = os.getpid()`. Inserts task records in `BATCH_TASKS` as `PENDING`. Formats dates strictly to `YYYY-MM-DD` and `YYYY-MM-DD HH:MM:SS` to avoid Teradata Error 5404.
5. `get_pending_tasks(batch_id, source) -> List[Tuple]`: Returns list of `(ENTITY_NAME, FILE_NAME, RETRY_COUNT)` for `PENDING` or `FAILED` tasks scoped by `BATCH_ID` and `SOURCE`.
6. `update_task_start(batch_id, entity_name, file_name=None, source=None)`: Commits task status as `RUNNING` with current timestamp (requires `source`).
7. `update_task_end(batch_id, entity_name, status, retry_count, error_message, file_name=None, source=None)`: Updates `STATUS` (`SUCCESS` or `FAILED`), `RETRY_COUNT`, `END_TIME`, and `ERROR_MESSAGE` (truncated to 1000 characters; requires `source`).
8. `get_batch_summary(batch_id, expected_total_files, source) -> bool`: Returns `True` if `COUNT(*) WHERE BATCH_ID = ? AND SOURCE = ? AND STATUS = 'SUCCESS'` equals expected files count.
9. `finish_batch(batch_id, final_status, source)`: Updates `BATCH_RUNS` status (`SUCCESS` or `FAILED`) and commits `END_TIME` while permanently preserving `PROCESS_PID` (requires `source`).
10. `get_active_batch_run(source=None) -> Optional[Dict]`: Queries for any batch with `STATUS = 'RUNNING'` (filtered by `SOURCE` when provided), returning `BATCH_ID`, `SOURCE`, `EXTRACT_TIMESTAMP`, and `PROCESS_PID`.
11. `mark_batch_failed(batch_id, source)`: Auto-heals orphaned batches by setting `STATUS = 'FAILED'` (requires `source`).

---

### 2.4 Module: `lib/tpt_worker.py`
- **Path**: `lib/tpt_worker.py`
- **Role**: Subprocess execution of `load_csv_generic.sh` with auto-healing and pre-emptive table unlocking.

#### Functions in `lib/tpt_worker.py`:
1. `run_tpt_loader(entity, file_path_or_pattern, retry_count, batch_id, tpt_config, orch_config, db_state, extract_timestamp=None, skip_rows=1, skip_rows_every_file="N", delimiter=",", source=None) -> Tuple[int, bool]`
   - **Description**: Dispatches `TPT_LOADER/load_csv_generic.sh` forwarding `-S <skip_rows>`, `-Y <skip_rows_every_file>`, `-s <delimiter>`, `-B <batch_id>`, and `-T <extract_timestamp>`, and records task state transitions with mandatory `source` scoping.
   - **Auto-Healing**:
     - On retry ($>0$), issues a pre-emptive unlock via `unlock_tpt_table.sh -t <entity> -m <load_mode> -B <batch_id> -T <extract_timestamp>`.
     - Intercepts return code `3` or output containing Error `2652` (`Table is being Loaded / Paused`), executes reactive unlock with `-m <load_mode> -B <batch_id> -T <extract_timestamp>`, and retries.
     - Safely replaces non-UTF8 characters in output streams (`errors='replace'`).

---

### 2.5 Module: `lib/post_tasks.py`
- **Path**: `lib/post_tasks.py`
- **Role**: Post-ingestion task execution engine.

#### Functions in `lib/post_tasks.py`:
1. `run_post_tasks(post_tasks_config: list, context_env: Optional[Dict[str, str]] = None) -> bool`
   - **Description**: Executes source-specific and global post-ingestion hooks (`dbt`, `shell`, `python`).
   - **Isolated Environment Injection**: Merges `context_env` (`CURRENT_BATCH_ID`, `CURRENT_SOURCE`, `CURRENT_MANIFEST`) into a subprocess environment dictionary without mutating global `os.environ`.

---

## 3. Exit Codes & Process Termination Matrix

The orchestrator adheres to standard POSIX exit semantics:

| Exit Code | Classification | Cause / Trigger | Operational Impact |
|:---:|:---|:---|:---|
| **`0`** | `SUCCESS / IDLE` | - Batch loaded, verified, transformed, and archived with 100% success.<br>- `LANDING_ZONE` is completely empty (`IDLE`). | System ready for subsequent batch. |
| **`1`** | `FATAL / ABORT` | - Boundary violation OPS-01 (multiple manifests or unmanifested orphan files).<br>- Checksum mismatch on data payloads.<br>- Process lock wait timeout (`lock_timeout_seconds`) exceeded.<br>- TPT entity load failed after exhausting all retries.<br>- Downstream post-task failed. | Batch marked `FAILED` in `BATCH_RUNS`. Ingress halts immediately. |
