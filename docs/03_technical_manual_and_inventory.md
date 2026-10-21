# DATA_INGESTION_ORCHESTRATOR - Technical Manual & Inventory

## 1. System Context & Component Architecture

The **DATA_INGESTION_ORCHESTRATOR** (`orchestrator.py`) is deployed on the Linux application server at:
```
/sg/local/sid/home/Dsidadm/DATA_LOADER/PROCESSING_TOOLS/DATA_INGESTION_ORCHESTRATOR/
```
It controls multi-batch queue discovery, atomic capsule staging, decompression, header normalization, Just-In-Time (JIT) Schema Guard Auto-DDL, and parallel ingestion of batches into Teradata Vantage, interfacing directly with `load_csv_generic.sh` (`-C -M 2`) and tracking execution state in technical tables.

```
DATA_INGESTION_ORCHESTRATOR/
├── orchestrator.py             # Thin CLI entrypoint (<80 lines) delegating to OrchestratorService
├── config.json                 # Deployment configuration file
├── config_template.json        # Template configuration with concurrency, pool & schema guard defaults
├── entity_mapping.csv          # Entity-to-table signature mapping definitions
├── ddl_technical_tables.sql    # Teradata DDL for BATCH_RUNS & BATCH_TASKS
├── docs/                       # Technical and operational documentation suite
└── lib/
    ├── orchestrator_service.py # OrchestratorService & BatchContext (imports dataloader_core)
    ├── schema_guard.py         # JIT Schema Guard & Auto-DDL (DBC.ColumnsV vs CSV headers)
    ├── sensor.py               # Multi-batch queue discovery (scan_landing_queue, O(1) verify)
    ├── db_state.py             # Pooled Teradata state persistence (BATCH_RUNS / TASKS)
    ├── tpt_worker.py           # Subprocess execution (-C -M 2) & Error 2652 reactive lock recovery
    ├── post_tasks.py           # Source-keyed and global post-task execution engine (context_env)
    └── logger.py               # Batch log capsule and execution logging wrapper
```

---

## 2. Module Inventory & Code Signatures

### 2.1 CLI Entrypoint (`orchestrator.py`) & Service (`lib/orchestrator_service.py`)
- **Paths**: `orchestrator.py`, `lib/orchestrator_service.py`
- **Role**: Thin CLI entrypoint (`orchestrator.py`, strictly $<80$ lines) and cohesive service class (`OrchestratorService`) backed by `COMMON/dataloader_core` (`ConfigLoader`, `ProcessLock`, `UtilitySlotGovernor`, `TeradataConnectionManager`, `EntityMapper`, `ManifestHandler`, `CapsulePathResolver`, `LoggingFactory`, `detach_batch_file_logger`).

#### CLI Arguments (`orchestrator.py`):
- `--config <path>`: Path to JSON configuration file (required).
- `--skip-post-tasks`: Flag to bypass all post-ingestion tasks.
- `--source <source>`: Specific source partition code (e.g., `ORACLE_FUSION`, `CRM`).
- `--sources <S1,S2,...>`: Comma-separated list of sources to process sequentially.
- `--all-sources`: Scans `LANDING_ZONE` and automatically sweeps all discovered source partitions.
- `--post-tasks-file <path>`: External JSON file with supplementary post-tasks.

#### Data Structure `BatchContext`:
Stores metadata and workload for an active batch in the pipeline:
- `batch_id: str`: Extracted batch identifier.
- `source: str`: Source system partition code.
- `extract_ts: str`: Formatted extract timestamp (`YYYY-MM-DD HH:MM:SS`).
- `clean_ts: str`: Normalized alphanumeric timestamp token (`YYYYMMDD_HHMMSS`).
- `manifest_path: str`: Full path to standardized manifest.
- `manifest_basename: str`: Basename of manifest file.
- `active_zone: str`: Working processing directory (`LOADING_ZONE/<SOURCE>`).
- `is_in_flight: bool`: `True` if batch was resumed from an in-flight crash.
- `entities_workload: Dict[str, Dict[str, Any]]`: Per-entity workload dictionary containing `target_table`, `signature`, `files`, `delimiter`, `load_mode`, `skip_rows`, `header_mode`, and pre-extracted `header_columns`.
- `validated_files: List[str]`: Full list of validated payload data files.
- `batch_log_dir: str`: Directory for batch-specific log capsule (`LOGS/<SOURCE>/<YYYY-MM>/BATCH_<ID>_<TS>/`).
- `run_log_file: str`: Path to execution log file.
- `raw_dir: Optional[str]`: Path to capsule `RAW/` directory containing vaulted raw manifest and intake receipt.
- `descriptor_path: Optional[str]`: Path to `batch_descriptor.json`.
- `descriptor: Optional[BatchDescriptor]`: Parsed `BatchDescriptor` object.

#### Class `OrchestratorService` & Key Methods (`lib/orchestrator_service.py`):
1. `strip_header_in_place(file_path: str) -> None`
   - **Description**: Strips line 1 (header) from a delimited text file in-place using a temporary swap file (`.tmp`). Writes a `.<filename>.header_stripped` marker file after staging the swap file and before `os.replace()` to guarantee crash-safe idempotency across task retries.

2. `process_entity_task(entity, entity_info, batch_id, source, extract_timestamp, tpt_config, orch_config, db, has_meta=2, skip_precheck=True) -> bool`
   - **Description**: ThreadPool worker target. Extracts entity mapping attributes (`delimiter`, `load_mode`, `skip_rows`, `header_mode`), dearchives `.zip`/`.gz` payloads into `LOADING_ZONE`, configures `SkipRows` and `SkipRowsEveryFile` according to `header_mode` (`first_chunk_only` strips chunk 1, `every_file` passes `-Y Y`, `none` passes `-S 0`), acquires a slot via `UtilitySlotGovernor.acquire_slot(entity, source)`, and executes `run_tpt_loader(...)`.

3. `OrchestratorService.wait_or_recover_active_db_batch() -> None`
   - **Description**: Queries `BATCH_RUNS` via `db.get_active_batch_run(source)`. Waits via `wait_for_pid_completion()` if `PROCESS_PID` is alive, or reclassifies orphaned batches as `FAILED` if `PROCESS_PID` is dead.

4. `OrchestratorService.scan_and_order_queue() -> List[QueuedBatchItem]`
   - **Description**: Invokes `sensor.scan_landing_queue()` to discover all queued Batch Capsules in `LANDING_ZONE/<SOURCE>/` and sorts them chronologically (FIFO).

5. `OrchestratorService.process_batch(ctx: BatchContext) -> int`
   - **Description**: Executes the complete lifecycle for a single batch: JIT Schema Guard synchronization, parallel entity loading via TPT workers under `UtilitySlotGovernor`, completeness barrier verification, source-keyed and global post-tasks execution, Batch Capsule archiving to `ARCHIVE_ZONE`, and cleanup. Detaches batch file log handlers on exit via `detach_batch_file_logger()`.

6. `OrchestratorService.run(skip_post_tasks: Optional[bool] = None, post_tasks_file: Optional[str] = None) -> int`
   - **Description**: Acquires `dataloader_core.locking.ProcessLock` (`fcntl.flock` + PID tracking) on `LOADING_ZONE/<SOURCE>/.orchestrator.pid`, checks for in-flight batches in `LOADING_ZONE`, scans queued capsules, loops over queued batches enforcing **`FAIL_STOP`**, and closes pooled Teradata connections in `finally`.

7. `OrchestratorService.run_all_sources(sources: Optional[List[str]] = None, skip_post_tasks: Optional[bool] = None, post_tasks_file: Optional[str] = None) -> int`
   - **Description**: Discovers all active source subdirectories in `LANDING_ZONE` (or filters by `sources` list) and invokes `run()` for each source sequentially, exiting immediately on any failure.

---

### 2.2 Module: `lib/schema_guard.py`
- **Path**: `lib/schema_guard.py`
- **Role**: Just-In-Time (JIT) schema synchronization and Auto-DDL management right before entity loading.

#### Class `SchemaGuard`:
1. `__init__(connection_manager: TeradataConnectionManager, target_database: str, auto_migrate_ddl: bool = True, character_set: str = "UNICODE")`
   - **Description**: Initializes schema guard using shared Teradata connection pool. Validates SQL identifiers for target database.
2. `inspect_table_columns(table_name: str) -> List[str]`
   - **Description**: Queries `DBC.ColumnsV` for `ColumnName WHERE DatabaseName = ? AND TableName = ? ORDER BY ColumnId ASC`. Returns uppercase list of existing column names.
3. `generate_table_ddl(table_name: str, csv_columns: List[str]) -> str`
   - **Description**: Generates Teradata DDL for `NOPI` table with all CSV columns declared as `VARCHAR(4000) CHARACTER SET <character_set> NOT CASESPECIFIC` plus mandatory audit columns (`INFA_LAST_REPLICATED TIMESTAMP(0)` and `INFA_BATCH_ID VARCHAR(100)`).
4. `sync_entity_schema_if_drifted(entity_name: str, csv_columns: List[str], load_mode: str = "replace") -> bool`
   - **Description**: Compares sanitized CSV columns against existing table columns in `DBC.ColumnsV`.
   - If table is missing or columns differ:
     - If `load_mode == 'replace'` and `auto_migrate_ddl == True`: drops table (if exists) and executes `generate_table_ddl`. Returns `True` (migrated).
     - If `load_mode == 'append'`: raises `SchemaSyncError` to prevent destructive alterations on historical data.
     - If `auto_migrate_ddl == False`: raises `SchemaSyncError` to block unauthorized structural changes.
   - If columns match: returns `False` (no drift).

---

### 2.3 Module: `lib/sensor.py`
- **Path**: `lib/sensor.py`
- **Role**: Multi-batch queue discovery, fast-path $O(1)$ batch descriptor verification, manifest parsing, and entity mapping.

#### Functions in `lib/sensor.py`:
1. `scan_landing_queue(landing_zone: str, manifest_pattern: str = "MANIFEST_*.MF") -> List[Dict[str, Any]]`
   - **Description**: Discovers 1..N queued Batch Capsules (`BATCH_<CLEAN_TS>_<BATCH_ID>/`) and legacy flat manifests. Extracts timestamps and batch IDs, returning a list sorted chronologically in ascending FIFO order.
2. `verify_descriptor_batch(active_zone: str, descriptor: BatchDescriptor, verify_checksum: bool = False) -> Tuple[Dict[str, Dict[str, Any]], List[str]]`
   - **Description**: Fast-path $O(1)$ verification. Reads pre-computed `BatchDescriptor`, verifying declared file existence and byte sizes in $O(1)$ time without expensive MD5 recalculations. Supports both `files` (metadata dicts) and `data_files` list keys with automatic fallback. Extracts per-entity routing attributes (`delimiter`, `load_mode`, `skip_rows`, `header_mode`, `header_columns`) into `entities_workload`.
3. `scan_landing_zone(landing_zone: str, manifest_pattern: str = "MANIFEST_*.MF") -> Tuple[str, Optional[str], List[str]]`
   - **Description**: Legacy flat directory scanner. Returns status (`'IDLE'`, `'ERROR_MISSING_MANIFEST'`, `'ERROR_MULTIPLE_MANIFESTS'`, `'VALID'`), manifest path, and data files list.
4. `parse_manifest_metadata(manifest_path: str, manifest_regex: Optional[str] = None) -> Dict[str, str]`
   - **Description**: Extracts `source`, `extract_timestamp`, and `batch_id` from manifest filename. Normalizes timestamp to `YYYY-MM-DD HH:MM:SS`.
5. `parse_manifest_content(manifest_path: str) -> List[Dict[str, str]]`
   - **Description**: Reads manifest CSV content (`filename,checksum`), skipping empty lines and comments.
6. `verify_and_map_batch(...) -> Tuple[Dict[str, Dict], List[str]]`
   - **Description**: Enforces OPS-02 (orphan guard). Validates declared files. Maps declared files to target tables using `entity_mapping.csv`. Returns entity workload dictionary and list of validated files.

---

### 2.4 Module: `lib/db_state.py`
- **Path**: `lib/db_state.py`
- **Role**: State persistence in Teradata Vantage (`BATCH_RUNS` and `BATCH_TASKS`) backed by `dataloader_core.db.TeradataConnectionManager` connection pooling.

#### Class: `TeradataStateDB`
1. `__init__(td_config: dict, conn_mgr: Optional[TeradataConnectionManager] = None)`: Instantiates or accepts a `TeradataConnectionManager` pool and validates `technical_database` via `validate_sql_identifier`.
2. `get_connection() -> ContextManager[teradatasql.TeradataConnection]`: Borrows a live pooled connection from `self.conn_mgr.acquire_connection()`, returning it to the pool on exit without closing the TCP session.
3. `close() -> None`: Closes all pooled Teradata connections at orchestrator shutdown.
4. `_require_source(source: str) -> str`: Enforces that `source` is a non-empty string (`ValueError` raised if missing) so all queries use the composite `(BATCH_ID, SOURCE)` primary index.
5. `initialize_batch(batch_id, source, extract_timestamp, tasks_list) -> None`: Inserts or updates a row in `BATCH_RUNS` with `STATUS = 'RUNNING'` and `PROCESS_PID = os.getpid()`. Inserts task records in `BATCH_TASKS` as `PENDING`. Formats dates strictly to `YYYY-MM-DD` and `YYYY-MM-DD HH:MM:SS` to avoid Teradata Error 5404.
6. `get_pending_tasks(batch_id, source) -> List[Tuple]`: Returns list of `(ENTITY_NAME, FILE_NAME, RETRY_COUNT)` for `PENDING` or `FAILED` tasks scoped by `BATCH_ID` and `SOURCE`.
7. `update_task_start(batch_id, entity_name, file_name=None, source=None)`: Commits task status as `RUNNING` with current timestamp (requires `source`).
8. `update_task_end(batch_id, entity_name, status, retry_count, error_message, file_name=None, source=None)`: Updates `STATUS` (`SUCCESS` or `FAILED`), `RETRY_COUNT`, `END_TIME`, and `ERROR_MESSAGE` (truncated to 1000 characters; requires `source`).
9. `get_batch_summary(batch_id, expected_total_files, source) -> bool`: Returns `True` if `COUNT(*) WHERE BATCH_ID = ? AND SOURCE = ? AND STATUS = 'SUCCESS'` equals expected files count.
10. `finish_batch(batch_id, final_status, source)`: Updates `BATCH_RUNS` status (`SUCCESS` or `FAILED`) and commits `END_TIME` while permanently preserving `PROCESS_PID` (requires `source`).
11. `get_active_batch_run(source=None) -> Optional[Dict]`: Queries for any batch with `STATUS = 'RUNNING'` (filtered by `SOURCE` when provided), returning `BATCH_ID`, `SOURCE`, `EXTRACT_TIMESTAMP`, and `PROCESS_PID`.
12. `mark_batch_failed(batch_id, source)`: Auto-heals orphaned batches by setting `STATUS = 'FAILED'` (requires `source`).

---

### 2.5 Module: `lib/tpt_worker.py`
- **Path**: `lib/tpt_worker.py`
- **Role**: Subprocess execution of `load_csv_generic.sh` with fast-path bypass (`-C -M 2`) and reactive Error 2652 table unlocking.

#### Functions in `lib/tpt_worker.py`:
1. `run_tpt_loader(entity, file_path_or_pattern, retry_count, batch_id, tpt_config, orch_config, db_state, extract_timestamp=None, skip_rows=1, skip_rows_every_file="N", delimiter=",", source=None, skip_precheck=True, has_meta=2) -> Tuple[int, bool]`
   - **Description**: Dispatches `TPT_LOADER/load_csv_generic.sh` forwarding `-S <skip_rows>`, `-Y <skip_rows_every_file>`, `-s <delimiter>`, `-B <batch_id>`, `-T <extract_timestamp>`, `-C` (when `skip_precheck=True`), and `-M <has_meta>`.
   - **Fast-Path Schema Pre-check Bypass**: Passing `-C -M 2` instructs `load_csv_generic.sh` to bypass redundant BTEQ table checks (since Schema Guard pre-synchronized the table) and pads audit columns in-memory during TPT streaming.
   - **Auto-Healing**:
     - On retry ($>0$), issues a pre-emptive unlock via `unlock_tpt_table.sh -t <entity> -m <load_mode> -B <batch_id> -T <extract_timestamp>`.
     - Intercepts return code `3` or output containing Error `2652` (`Table is being Loaded / Paused`), executes reactive unlock with `-m <load_mode> -B <batch_id> -T <extract_timestamp>`, and retries.
     - Safely replaces non-UTF8 characters in output streams (`errors='replace'`).

---

### 2.6 Module: `lib/post_tasks.py`
- **Path**: `lib/post_tasks.py`
- **Role**: Post-ingestion task execution engine.

#### Functions in `lib/post_tasks.py`:
1. `run_post_tasks(post_tasks_config: Union[dict, list], context_env: Optional[Dict[str, str]] = None, source: Optional[str] = None) -> bool`
   - **Description**: Resolves tasks for the given `source` (`post_tasks_config[source]` + `post_tasks_config["default"]`, or direct list) and executes hooks (`dbt`, `shell`, `python`).
   - **Isolated Environment Injection**: Merges `context_env` (`CURRENT_BATCH_ID`, `CURRENT_SOURCE`, `CURRENT_TIMESTAMP`, `TDENV`) into a subprocess environment dictionary without mutating global `os.environ`.

---

## 3. Exit Codes & Process Termination Matrix

The orchestrator adheres to standard POSIX exit semantics:

| Exit Code | Classification | Cause / Trigger | Operational Impact |
|:---:|:---|:---|:---|
| **`0`** | `SUCCESS / IDLE` | - All queued batches loaded, verified, transformed, and archived with 100% success.<br>- `LANDING_ZONE` contains no queued capsules (`IDLE`). | System ready for subsequent batch. |
| **`1`** | `FATAL / ABORT` | - Boundary violation OPS-02 (unmanifested orphan files).<br>- Checksum / file size mismatch on data payloads.<br>- Schema drift when `auto_migrate_ddl = false` or `load_mode = 'append'`.<br>- Process lock wait timeout (`lock_timeout_seconds`) exceeded.<br>- TPT entity load failed after exhausting all retries.<br>- Downstream post-task failed. | Batch marked `FAILED` in `BATCH_RUNS`. Queue halts immediately (**`FAIL_STOP`**). |
