"""
lib/orchestrator_service.py - Cohesive Data Ingestion Orchestrator Service

Encapsulates the Single-Batch Orchestrator lifecycle into a structured,
unit-testable service class (`OrchestratorService`) backed by `dataloader_core`.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import shutil
import sys
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

_SCRIPT_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
_COMMON_DIR = os.path.normpath(os.path.join(_SCRIPT_DIR, "..", "COMMON"))
if _COMMON_DIR not in sys.path:
    sys.path.insert(0, _COMMON_DIR)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

from dataloader_core.capsule import CapsulePathResolver
from dataloader_core.config import ConfigLoader, expand_env_vars
from dataloader_core.db import build_tpt_environment
from dataloader_core.exceptions import (
    BatchValidationError,
    ConfigurationError,
    DataLoaderError,
)
from dataloader_core.locking import ProcessLock, is_pid_alive, wait_for_pid_completion
from dataloader_core.logging_utils import add_batch_file_logger, setup_orchestrator_console_logger

from lib.db_state import TeradataStateDB
from lib.post_tasks import run_post_tasks
from lib.sensor import (
    parse_manifest_content,
    parse_manifest_metadata,
    scan_landing_zone,
    verify_and_map_batch,
)
from lib.tpt_worker import run_tpt_loader


@dataclass
class BatchContext:
    """Holds validated metadata and file workload for a single batch execution."""

    batch_id: str
    source: str
    extract_ts: str
    clean_ts: str
    manifest_path: str
    manifest_basename: str
    active_zone: str
    is_in_flight: bool
    entities_workload: Dict[str, Dict[str, Any]]
    validated_files: List[str]
    batch_log_dir: str
    run_log_file: str


def strip_header_in_place(file_path: str) -> None:
    """
    Removes the header (first line) of a CSV file in-place (Solution A).
    Writes the marker file after staging the stripped content to temp_path
    so a crash can never strip line 1 twice on retry.
    """
    marker = os.path.join(os.path.dirname(file_path), "." + os.path.basename(file_path) + ".header_stripped")
    temp_path = file_path + ".tmp"
    if os.path.exists(marker):
        if os.path.exists(temp_path):
            os.replace(temp_path, file_path)
        return

    with open(file_path, "r", encoding="utf-8", errors="replace") as fin, open(
        temp_path, "w", encoding="utf-8"
    ) as fout:
        _ = fin.readline()  # Strip header line
        for line in fin:
            fout.write(line)

    with open(marker, "w", encoding="utf-8") as f:
        f.write("STRIPPED\n")
    os.replace(temp_path, file_path)


def process_entity_task(
    entity: str,
    entity_info: Dict[str, Any],
    batch_id: str,
    source: str,
    extract_timestamp: str,
    tpt_config: Dict[str, Any],
    orch_config: Dict[str, Any],
    db: TeradataStateDB,
) -> bool:
    """
    Ingests all files for a given entity in the isolated LOADING_ZONE.
    Handles single files vs multi-chunk extracts and header normalization.
    """
    logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")
    loading_zone = orch_config.get("loading_zone", orch_config["landing_zone"])
    files = sorted(entity_info["files"])
    signature = entity_info.get("signature", entity.lower())

    pending_tasks = db.get_pending_tasks(batch_id, source=source)
    entity_retries = [t[2] for t in pending_tasks if t[0] == entity]
    retry_count = max(entity_retries) if entity_retries else 0

    load_mode = tpt_config.get("default_load_mode") or orch_config.get("default_load_mode", "replace")

    any_archive = any(f.lower().endswith((".zip", ".gz")) for f in files)
    if any_archive:
        extracted_files = []
        for f in files:
            f_path = os.path.join(loading_zone, f)
            if f.lower().endswith(".zip"):
                with zipfile.ZipFile(f_path, "r") as z:
                    for m in z.namelist():
                        if m.lower().endswith((".csv", ".pecsv")):
                            base_m = os.path.basename(m)
                            if not base_m:
                                continue
                            out_p = os.path.join(loading_zone, base_m)
                            if not os.path.exists(out_p):
                                with z.open(m) as zf, open(out_p, "wb") as out_f:
                                    shutil.copyfileobj(zf, out_f)
                            extracted_files.append(base_m)
            elif f.lower().endswith(".gz"):
                base_name = os.path.basename(f)
                if base_name.lower().endswith((".csv.gz", ".pecsv.gz")):
                    out_name = base_name[:-3]
                elif base_name.lower().endswith(".gz"):
                    out_name = base_name[:-3] + ".csv"
                else:
                    out_name = base_name + ".csv"
                out_p = os.path.join(loading_zone, out_name)
                if not os.path.exists(out_p):
                    with gzip.open(f_path, "rb") as gf, open(out_p, "wb") as out_f:
                        shutil.copyfileobj(gf, out_f)
                extracted_files.append(out_name)
            else:
                extracted_files.append(f)
        data_files = sorted(list(set(extracted_files)))
        data_ext = os.path.splitext(data_files[0])[1].lower() if data_files else ".csv"
    else:
        data_files = files
        data_ext = os.path.splitext(data_files[0])[1].lower() if data_files else ".csv"

    if len(data_files) == 1:
        target_spec = data_files[0]
        skip_rows = 1
        skip_rows_every_file = "N"
    else:
        all_delimited = all(f.lower().endswith((".csv", ".pecsv")) for f in data_files)
        if all_delimited:
            chunk_1_path = os.path.join(loading_zone, data_files[0])
            strip_header_in_place(chunk_1_path)
            target_spec = f"*{signature}*{data_ext}"
            skip_rows = 0
            skip_rows_every_file = "N"
            logger.info(
                f"[{entity}] Multi-chunk detected ({len(data_files)} files). First chunk header stripped; loading with SkipRows=0."
            )
        else:
            target_spec = f"*{signature}*{data_ext}"
            skip_rows = 1
            skip_rows_every_file = "N"

    _, success = run_tpt_loader(
        entity=entity,
        file_path_or_pattern=target_spec,
        retry_count=retry_count,
        batch_id=batch_id,
        tpt_config=tpt_config,
        orch_config=orch_config,
        db_state=db,
        load_mode=load_mode,
        skip_rows=skip_rows,
        skip_rows_every_file=skip_rows_every_file,
        extract_timestamp=extract_timestamp,
        source=source,
    )

    return success


class OrchestratorService:
    """Service managing Single-Batch ingestion orchestration, TPT workers, post-tasks, and archiving."""

    def __init__(
        self,
        config_path: str,
        source_override: Optional[str] = None,
        skip_post_tasks: bool = False,
        post_tasks_file: Optional[str] = None,
    ) -> None:
        self.logger = setup_orchestrator_console_logger()
        self.config_path = config_path
        self.source_override = source_override
        self.skip_post_tasks = skip_post_tasks
        self.post_tasks_file = post_tasks_file

        self.config = ConfigLoader.load_json_config(config_path)
        self.base_dir = os.path.dirname(os.path.abspath(config_path))

        self.tpt_config = self.config.get("tpt", {})
        self.orch_config = self.config.get("orchestration", {})
        self.td_config = self.config.get("teradata", {})

        for key in ["landing_zone", "loading_zone", "archive_zone", "logs_dir"]:
            if self.orch_config.get(key) and not os.path.isabs(self.orch_config[key]):
                self.orch_config[key] = os.path.normpath(os.path.join(self.base_dir, self.orch_config[key]))

        landing_cfg = self.orch_config.get("landing_zone")
        if not landing_cfg:
            self.logger.critical("Configuration error: 'landing_zone' is required.")
            raise ConfigurationError("Configuration error: 'landing_zone' is required.")

        loading_cfg = self.orch_config.get("loading_zone", os.path.normpath(os.path.join(self.base_dir, "../LOADING_ZONE")))
        archive_cfg = self.orch_config.get("archive_zone", os.path.normpath(os.path.join(self.base_dir, "../ARCHIVE_ZONE")))
        logs_cfg = self.orch_config.get("logs_dir", os.path.normpath(os.path.join(self.base_dir, "../LOGS")))

        if self.source_override:
            self.landing_zone = ConfigLoader.resolve_path(
                self.base_dir, landing_cfg, source=self.source_override, ensure_exists=True
            )
            self.loading_zone = ConfigLoader.resolve_path(
                self.base_dir, loading_cfg, source=self.source_override, ensure_exists=True
            )
            self.archive_zone = ConfigLoader.resolve_path(
                self.base_dir, archive_cfg, source=self.source_override, ensure_exists=True
            )
            self.logs_dir = ConfigLoader.resolve_path(
                self.base_dir, logs_cfg, source=self.source_override, ensure_exists=True
            )
        else:
            self.landing_zone = landing_cfg
            self.loading_zone = loading_cfg
            self.archive_zone = archive_cfg
            self.logs_dir = logs_cfg
            os.makedirs(self.loading_zone, exist_ok=True)

        self.orch_config["landing_zone"] = self.landing_zone
        self.orch_config["loading_zone"] = self.loading_zone
        self.orch_config["archive_zone"] = self.archive_zone
        self.orch_config["logs_dir"] = self.logs_dir

        for key in ["wrapper_script", "unlock_script"]:
            if self.tpt_config.get(key) and not os.path.isabs(self.tpt_config[key]):
                self.tpt_config[key] = os.path.normpath(os.path.join(self.base_dir, self.tpt_config[key]))

        mapping_cfg = self.orch_config.get("entity_mapping_file", "entity_mapping.csv")
        self.mapping_file = (
            mapping_cfg
            if os.path.isabs(mapping_cfg)
            else os.path.normpath(os.path.join(self.base_dir, mapping_cfg))
        )

        tpt_env = build_tpt_environment(self.td_config)
        self.orch_config["tpt_env"] = tpt_env
        self.tpt_config["tpt_env"] = tpt_env

        self.manifest_pattern = self.orch_config.get("manifest_pattern", "MANIFEST_*.MF")
        concurrency_cfg = self.orch_config.get("concurrency", {})
        self.poll_interval = concurrency_cfg.get("poll_interval_seconds", 5)
        self.lock_timeout = concurrency_cfg.get("lock_timeout_seconds", 7200)

        self.db: Optional[TeradataStateDB] = None

    def _get_db(self) -> TeradataStateDB:
        if self.db is None:
            max_workers = int(self.orch_config.get("max_concurrent_jobs", 5))
            td_cfg_with_pool = dict(self.td_config)
            td_cfg_with_pool.setdefault("connection_pool_size", max(6, max_workers + 1))
            self.db = TeradataStateDB(td_cfg_with_pool)
        return self.db

    def wait_or_recover_active_db_batch(self) -> None:
        """
        Queries Teradata BATCH_RUNS for any active batch run for `self.source_override`:
          - Waits in active polling loop if `PROCESS_PID` is alive.
          - Reclassifies batch as `FAILED` if `PROCESS_PID` is dead.
        """
        try:
            db = self._get_db()
            active_db_batch = db.get_active_batch_run(source=self.source_override)
            if active_db_batch:
                db_pid = active_db_batch.get("process_pid")
                b_id = active_db_batch.get("batch_id")
                if db_pid and is_pid_alive(int(db_pid)) and int(db_pid) != os.getpid():
                    wait_for_pid_completion(
                        pid=int(db_pid),
                        poll_interval_sec=self.poll_interval,
                        timeout_sec=self.lock_timeout,
                        logger=self.logger,
                        context_label=f"batch {b_id}",
                        initial_message=(
                            f"Active batch {b_id} detected in BATCH_RUNS with running PID {db_pid}. "
                            "Entering active waiting loop..."
                        ),
                        poll_message=f"Batch {b_id} (PID {db_pid}) still active in BATCH_RUNS. Waiting {self.poll_interval}s...",
                        completion_message=f"Active batch {b_id} (PID {db_pid}) has completed.",
                    )
                elif db_pid and not is_pid_alive(int(db_pid)):
                    self.logger.warning(
                        f"Batch {b_id} was marked RUNNING in BATCH_RUNS with dead PID {db_pid}. Reclassifying as FAILED."
                    )
                    db.mark_batch_failed(b_id, source=active_db_batch.get("source"))
        except DataLoaderError:
            raise
        except Exception as exc:
            self.logger.debug(f"Pre-flight BATCH_RUNS active check skipped or non-blocking: {exc}")

    def detect_and_validate_batch(self) -> Optional[BatchContext]:
        """
        Detects an in-flight batch in LOADING_ZONE or scans LANDING_ZONE under Single-Batch
        rules, parses manifest metadata & entries, sets up Batch Capsule logging, and
        validates checksums & entity mappings. Returns `None` when IDLE.
        """
        in_flight_manifests = (
            [os.path.join(self.loading_zone, f) for f in os.listdir(self.loading_zone) if f.endswith(".MF")]
            if os.path.exists(self.loading_zone)
            else []
        )

        if in_flight_manifests:
            self.logger.info(f"In-flight batch detected in LOADING_ZONE: {in_flight_manifests[0]}. Resuming execution.")
            manifest_path = in_flight_manifests[0]
            active_zone = self.loading_zone
            is_in_flight = True
        else:
            active_zone = self.landing_zone
            is_in_flight = False
            scan_status, manifest_path, data_files = scan_landing_zone(self.landing_zone, self.manifest_pattern)

            if scan_status == "IDLE":
                self.logger.info("Landing zone is empty. Exiting gracefully with RC 0.")
                return None
            if scan_status == "ERROR_MISSING_MANIFEST":
                msg = f"FATAL: Found {len(data_files)} data files in landing zone, but no manifest file. Aborting."
                self.logger.critical(msg)
                raise BatchValidationError(msg)
            if scan_status == "ERROR_MULTIPLE_MANIFESTS":
                msg = f"FATAL: Single-Batch rule violation: Multiple manifests detected in landing zone: {manifest_path}. Aborting."
                self.logger.critical(msg)
                raise BatchValidationError(msg)

        try:
            manifest_meta = parse_manifest_metadata(manifest_path, self.orch_config.get("manifest_regex"))
        except Exception as exc:
            msg = f"Failed to parse manifest filename '{manifest_path}': {exc}"
            self.logger.critical(msg)
            raise BatchValidationError(msg) from exc

        source = manifest_meta["source"]
        extract_ts = manifest_meta["extract_timestamp"]
        batch_id = manifest_meta["batch_id"]
        clean_ts = CapsulePathResolver.normalize_timestamp_token(extract_ts)

        batch_log_dir = CapsulePathResolver.get_log_capsule_dir(
            self.logs_dir, source, batch_id, clean_ts, ensure_exists=True
        )
        self.tpt_config["work_dir"] = os.path.join(batch_log_dir, "TPT")
        os.makedirs(self.tpt_config["work_dir"], exist_ok=True)

        run_log_file = add_batch_file_logger(batch_log_dir, source, extract_ts, batch_id)
        self.logger.info(f"Processing Batch {batch_id} (Source: {source}, Extract: {extract_ts}) [In-Flight: {is_in_flight}]")

        try:
            manifest_entries = parse_manifest_content(manifest_path)
        except Exception as exc:
            msg = f"Failed to parse manifest content for {manifest_path}: {exc}"
            self.logger.critical(msg)
            raise BatchValidationError(msg) from exc

        verify_checksum = False if is_in_flight else self.orch_config.get("verify_checksum", True)
        allow_orphan = self.orch_config.get("allow_orphan_files", False)

        try:
            entities_workload, validated_files = verify_and_map_batch(
                landing_zone=active_zone,
                manifest_entries=manifest_entries,
                mapping_file=self.mapping_file,
                verify_checksum=verify_checksum,
                allow_orphan_files=allow_orphan,
                source=source,
            )
        except Exception as exc:
            msg = f"Batch validation failed for batch {batch_id}: {exc}"
            self.logger.critical(msg)
            raise BatchValidationError(msg) from exc

        return BatchContext(
            batch_id=batch_id,
            source=source,
            extract_ts=extract_ts,
            clean_ts=clean_ts,
            manifest_path=manifest_path,
            manifest_basename=os.path.basename(manifest_path),
            active_zone=active_zone,
            is_in_flight=is_in_flight,
            entities_workload=entities_workload,
            validated_files=validated_files,
            batch_log_dir=batch_log_dir,
            run_log_file=run_log_file,
        )

    acquire_guards_and_detect_batch = detect_and_validate_batch

    def transition_to_loading_zone(self, ctx: BatchContext) -> None:
        """Atomically moves manifest and validated payload files from LANDING_ZONE to LOADING_ZONE."""
        if not ctx.is_in_flight:
            os.makedirs(self.loading_zone, exist_ok=True)
            self.logger.info(f"Moving validated batch files from '{self.landing_zone}' to isolated '{self.loading_zone}'...")

            shutil.move(ctx.manifest_path, os.path.join(self.loading_zone, ctx.manifest_basename))

            for f in ctx.validated_files:
                src_file = os.path.join(self.landing_zone, f)
                dst_file = os.path.join(self.loading_zone, f)
                shutil.move(src_file, dst_file)

            self.logger.info(
                f"Successfully transferred manifest and {len(ctx.validated_files)} data files to '{self.loading_zone}'."
            )

        self.orch_config["loading_zone"] = self.loading_zone

    def execute_entity_tasks(self, ctx: BatchContext) -> bool:
        """Registers batch state in Teradata and executes parallel entity ingestion workers."""
        db = self._get_db()
        tasks_list = []
        for entity, info in ctx.entities_workload.items():
            for fname in info["files"]:
                tasks_list.append((entity, fname))

        try:
            db.initialize_batch(ctx.batch_id, ctx.source, ctx.extract_ts, tasks_list)
        except Exception as exc:
            msg = f"Database initialization failed for batch {ctx.batch_id}: {exc}"
            self.logger.critical(msg)
            raise DataLoaderError(msg) from exc

        pending_tasks = db.get_pending_tasks(ctx.batch_id, source=ctx.source)
        if not pending_tasks:
            self.logger.info(f"No pending tasks to execute for batch {ctx.batch_id}.")
            return True

        pending_entities = sorted(list(set(t[0] for t in pending_tasks)))
        self.logger.info(f"Found pending tasks for {len(pending_entities)} distinct entities in batch {ctx.batch_id}.")

        all_workers_ok = True
        max_workers = self.orch_config.get("max_concurrent_jobs", 5)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for entity in pending_entities:
                entity_info = ctx.entities_workload[entity]
                futures[
                    executor.submit(
                        process_entity_task,
                        entity,
                        entity_info,
                        ctx.batch_id,
                        ctx.source,
                        ctx.extract_ts,
                        self.tpt_config,
                        self.orch_config,
                        db,
                    )
                ] = entity

            for future in as_completed(futures):
                entity = futures[future]
                try:
                    success = future.result()
                    if not success:
                        all_workers_ok = False
                        self.logger.error(f"Ingestion for entity '{entity}' reported failure.")
                except Exception as exc:
                    all_workers_ok = False
                    self.logger.error(f"Worker for entity '{entity}' raised exception: {exc}")

        return all_workers_ok

    execute_entity_workload = execute_entity_tasks

    def run_completeness_barrier_and_archive(self, ctx: BatchContext) -> int:
        """
        Verifies 100% task completion in Teradata, executes post-ingestion tasks
        (passing batch context via `context_env` without mutating `os.environ`),
        archives batch deliverables into the Batch Capsule, cleans LOADING_ZONE,
        and updates `BATCH_RUNS`.
        """
        db = self._get_db()
        expected_total_files = len(ctx.validated_files)

        if not db.get_batch_summary(ctx.batch_id, expected_total_files, source=ctx.source):
            db.finish_batch(ctx.batch_id, "FAILED", source=ctx.source)
            self.logger.error(
                f"[ERROR] Completeness barrier failed for batch {ctx.batch_id}. "
                f"Files retained in '{self.loading_zone}' for recovery."
            )
            return 1

        self.logger.info(f"[SUCCESS] All {expected_total_files} file tasks for batch {ctx.batch_id} ingested successfully.")

        post_tasks: List[Dict[str, Any]] = []
        if self.post_tasks_file and os.path.exists(self.post_tasks_file):
            try:
                with open(self.post_tasks_file, "r", encoding="utf-8") as ptf:
                    pt_data = expand_env_vars(json.load(ptf))
                    if isinstance(pt_data, list):
                        post_tasks.extend(pt_data)
                    elif isinstance(pt_data, dict):
                        src_specific = pt_data.get(ctx.source, pt_data.get("default", []))
                        post_tasks.extend(src_specific)
                self.logger.info(
                    f"Loaded {len(post_tasks)} source-specific post-task(s) from intake controller ({self.post_tasks_file})."
                )
                try:
                    capsule_pt = os.path.join(ctx.batch_log_dir, os.path.basename(self.post_tasks_file))
                    if os.path.abspath(self.post_tasks_file) != os.path.abspath(capsule_pt):
                        shutil.copy2(self.post_tasks_file, capsule_pt)
                except Exception:
                    pass
            except Exception as exc:
                self.logger.warning(f"Could not load post-tasks from file {self.post_tasks_file}: {exc}")

        orch_post_tasks = self.orch_config.get("post_ingestion_tasks", [])
        if isinstance(orch_post_tasks, dict):
            src_tasks = orch_post_tasks.get(ctx.source, orch_post_tasks.get("default", []))
            post_tasks.extend(src_tasks)
        elif isinstance(orch_post_tasks, list):
            post_tasks.extend(orch_post_tasks)

        context_env = {
            "CURRENT_BATCH_ID": str(ctx.batch_id),
            "CURRENT_SOURCE": str(ctx.source),
            "CURRENT_MANIFEST": os.path.join(self.loading_zone, ctx.manifest_basename),
        }

        if self.skip_post_tasks:
            self.logger.info(
                "[INFO] Ingestion completed with 100% success. Post-tasks execution skipped via --skip-post-tasks flag."
            )
            post_success = True
        else:
            post_success = run_post_tasks(post_tasks, context_env=context_env) if post_tasks else True

        if not post_success:
            db.finish_batch(ctx.batch_id, "FAILED", source=ctx.source)
            self.logger.error(f"[ERROR] Pipeline failed during post-ingestion tasks for batch {ctx.batch_id}.")
            return 1

        batch_archive_dir = CapsulePathResolver.get_archive_capsule_dir(
            self.archive_zone, ctx.source, ctx.batch_id, ctx.clean_ts, ensure_exists=True
        )
        self.logger.info(f"Archiving processed batch files into Batch Capsule: '{batch_archive_dir}'...")

        shutil.move(
            os.path.join(self.loading_zone, ctx.manifest_basename),
            os.path.join(batch_archive_dir, ctx.manifest_basename),
        )
        for f in ctx.validated_files:
            f_path = os.path.join(self.loading_zone, f)
            if os.path.exists(f_path):
                shutil.move(f_path, os.path.join(batch_archive_dir, f))
            marker_path = os.path.join(self.loading_zone, "." + f + ".header_stripped")
            if os.path.exists(marker_path):
                os.remove(marker_path)

        # Clean up any remaining temporary extracted files or markers in loading_zone (preserving active .orchestrator.pid until lock exit)
        if os.path.exists(self.loading_zone):
            for item in os.listdir(self.loading_zone):
                if item == ".orchestrator.pid":
                    continue
                item_path = os.path.join(self.loading_zone, item)
                if os.path.isfile(item_path):
                    try:
                        os.remove(item_path)
                    except Exception as exc:
                        self.logger.warning(f"Could not remove temporary file {item_path}: {exc}")

        db.finish_batch(ctx.batch_id, "SUCCESS", source=ctx.source)
        self.logger.info(f"[SUCCESS] Pipeline completed successfully for batch {ctx.batch_id}!")
        return 0

    finalize_and_archive = run_completeness_barrier_and_archive

    def run(
        self,
        skip_post_tasks: Optional[bool] = None,
        post_tasks_file: Optional[str] = None,
    ) -> int:
        """Executes the full Single-Batch Orchestrator lifecycle and returns exit code (0 or 1)."""
        if skip_post_tasks is not None:
            self.skip_post_tasks = skip_post_tasks
        if post_tasks_file is not None:
            self.post_tasks_file = post_tasks_file

        pid_file = os.path.join(self.loading_zone, ".orchestrator.pid")
        lock = ProcessLock(
            lock_file=pid_file,
            poll_interval_sec=self.poll_interval,
            timeout_sec=self.lock_timeout,
            logger=self.logger,
            label="Orchestrator",
        )

        try:
            self.wait_or_recover_active_db_batch()
            with lock:
                ctx = self.detect_and_validate_batch()
                if ctx is None:
                    return 0
                self.transition_to_loading_zone(ctx)
                self.execute_entity_tasks(ctx)
                return self.run_completeness_barrier_and_archive(ctx)
        except DataLoaderError:
            return 1
        except Exception as exc:
            self.logger.critical(f"FATAL: Unhandled exception in Orchestrator: {exc}")
            return 1
        finally:
            if self.db is not None:
                self.db.close()
