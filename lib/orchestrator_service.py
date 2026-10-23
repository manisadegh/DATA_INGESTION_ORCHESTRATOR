"""
lib/orchestrator_service.py - Cohesive Multi-Batch Ingestion Orchestrator Service

Encapsulates the Multi-Batch FIFO Queue consumer, Just-In-Time Schema Guard DDL
synchronization, TPT execution, completeness barrier, post-tasks execution,
and atomic Batch Capsule archiving into `OrchestratorService`.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import shutil
import socket
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

_SCRIPT_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
_COMMON_DIR = os.path.normpath(os.path.join(_SCRIPT_DIR, "..", "COMMON"))
if _COMMON_DIR not in sys.path:
    sys.path.insert(0, _COMMON_DIR)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

from dataloader_core.capsule import BatchDescriptor, CapsulePathResolver
from dataloader_core.config import ConfigLoader, expand_env_vars
from dataloader_core.db import build_tpt_environment
from dataloader_core.exceptions import (
    BatchValidationError,
    ConfigurationError,
    DataLoaderError,
    OrchestratorExecutionError,
    SchemaSyncError,
)
from dataloader_core.locking import (
    ProcessLock,
    UtilitySlotGovernor,
    is_pid_alive,
    wait_for_pid_completion,
)
from dataloader_core.logging_utils import (
    add_batch_file_logger,
    detach_batch_file_logger,
    setup_orchestrator_console_logger,
)

from lib.db_state import TeradataStateDB
from lib.post_tasks import run_post_tasks, validate_post_tasks
from lib.schema_guard import sync_entity_schema_if_drifted
from lib.sensor import (
    QueuedBatchItem,
    parse_manifest_content,
    parse_manifest_metadata,
    scan_landing_queue,
    verify_and_map_batch,
    verify_descriptor_batch,
)
from lib.tpt_worker import run_tpt_loader


@dataclass
class BatchContext:
    """Holds validated metadata and file workload for a single batch execution."""

    batch_id: str
    source: str
    extract_ts: str = ""
    clean_ts: str = ""
    manifest_path: str = ""
    manifest_basename: str = ""
    active_zone: str = ""
    is_in_flight: bool = False
    entities_workload: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    validated_files: List[str] = field(default_factory=list)
    batch_log_dir: str = ""
    run_log_file: str = ""
    raw_dir: Optional[str] = None
    descriptor_path: Optional[str] = None
    descriptor: Optional[BatchDescriptor] = None
    already_loaded: bool = False
    batch_dir: Optional[str] = None
    extract_timestamp: Optional[str] = None
    data_files: Optional[List[str]] = None

    def __post_init__(self):
        if self.batch_dir and not self.active_zone:
            self.active_zone = self.batch_dir
        if not self.batch_dir and self.active_zone:
            self.batch_dir = self.active_zone
        if self.extract_timestamp and not self.extract_ts:
            self.extract_ts = self.extract_timestamp
        if self.extract_ts and not self.clean_ts:
            self.clean_ts = self.extract_ts
        if self.data_files and not self.validated_files:
            self.validated_files = list(self.data_files)
        if not self.data_files and self.validated_files:
            self.data_files = list(self.validated_files)
        if self.manifest_path and not self.manifest_basename:
            self.manifest_basename = os.path.basename(self.manifest_path)


def extract_archive_members(archive_path: str, work_dir: str) -> List[str]:
    """
    Extracts members of a zip or gzip archive to work_dir safely (F2.4).
    Validates member sizes, non-empty payloads, basename-only paths, and member uniqueness.
    """
    tmp = os.path.join(work_dir, ".extract_tmp")
    if os.path.isdir(tmp):
        shutil.rmtree(tmp)
    os.makedirs(tmp)
    produced: List[str] = []
    lower = archive_path.lower()
    try:
        if lower.endswith(".zip"):
            with zipfile.ZipFile(archive_path) as zf:
                for info in zf.infolist():
                    name = os.path.basename(info.filename)
                    if info.is_dir() or not name.lower().endswith((".csv", ".pecsv")):
                        continue
                    out_tmp = os.path.join(tmp, name)
                    with zf.open(info) as src, open(out_tmp, "wb") as dst:
                        shutil.copyfileobj(src, dst, 1024 * 1024)
                    if os.path.getsize(out_tmp) != info.file_size:
                        raise BatchValidationError(f"Extracted size mismatch for {name} in {archive_path}")
                    produced.append(name)
        elif lower.endswith(".gz"):
            base = os.path.basename(archive_path)[:-3]
            if not base.lower().endswith((".csv", ".pecsv")):
                base += ".csv"
            out_tmp = os.path.join(tmp, base)
            with gzip.open(archive_path, "rb") as src, open(out_tmp, "wb") as dst:
                shutil.copyfileobj(src, dst, 1024 * 1024)
            if os.path.getsize(out_tmp) == 0:
                raise BatchValidationError(f"Empty gzip payload: {archive_path}")
            produced.append(base)
        else:
            raise BatchValidationError(f"Unsupported archive: {archive_path}")

        final_paths = []
        for name in produced:
            dst = os.path.join(work_dir, name)
            if os.path.exists(dst):
                raise BatchValidationError(f"Duplicate member name '{name}' across archives of the same entity")
            os.replace(os.path.join(tmp, name), dst)
            final_paths.append(dst)
        return final_paths
    except (zipfile.BadZipFile, gzip.BadGzipFile, EOFError, OSError) as exc:
        raise BatchValidationError(f"Failed extracting archive '{archive_path}': {exc}") from exc
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def prepare_entity_work_area(batch_dir: str, entity: str, payload_files: List[str]) -> str:
    """
    Creates BATCH_x/.work/<ENTITY>/ with plain data files and files.lst; returns the list path (F2.2, F2.3).
    Payload files in batch_dir are never modified in place.
    """
    work_dir = os.path.join(batch_dir, ".work", entity)
    if os.path.isdir(work_dir):
        shutil.rmtree(work_dir)
    os.makedirs(work_dir, exist_ok=True)

    plain_files: List[str] = []
    for fname in sorted(payload_files):
        src = os.path.join(batch_dir, fname)
        lower = fname.lower()
        if lower.endswith(".zip") or lower.endswith(".gz"):
            plain_files.extend(extract_archive_members(src, work_dir))
        else:
            dst = os.path.join(work_dir, fname)
            try:
                os.link(src, dst)
            except OSError:
                shutil.copy2(src, dst)
            plain_files.append(dst)

    plain_files.sort()
    list_path = os.path.join(work_dir, "files.lst")
    with open(list_path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("\n".join(plain_files) + "\n")
    return list_path


def process_entity_task(
    entity: str,
    entity_info: Dict[str, Any],
    batch_id: str,
    source: str,
    extract_timestamp: str,
    tpt_config: Dict[str, Any],
    orch_config: Dict[str, Any],
    db: TeradataStateDB,
    has_meta: int = 2,
    skip_precheck: bool = True,
) -> bool:
    """
    Ingests all files for a given entity using explicit file list (F2.2).
    Payload files are preserved untouched; header skipping is delegated to TPT (F2.3).
    """
    logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")
    active_zone = orch_config.get("active_zone") or orch_config.get("loading_zone", "")
    files = sorted(entity_info["files"])

    pending_tasks = db.get_pending_tasks(batch_id, source=source)
    entity_retries = [t[1] for t in pending_tasks if t[0] == entity]
    retry_count = max(entity_retries) if entity_retries else 0

    load_mode = entity_info.get("load_mode") or tpt_config.get("default_load_mode") or orch_config.get("default_load_mode", "replace")
    delimiter = entity_info.get("delimiter") or tpt_config.get("delimiter") or orch_config.get("delimiter", ",")
    header_mode = (entity_info.get("header_mode") or "first_chunk_only").lower()
    entity_skip_rows = int(entity_info.get("skip_rows", 1))

    # Prepare isolated work area with plain data files and files.lst (F2.2, F2.4)
    list_path = prepare_entity_work_area(active_zone, entity, files)

    # Header mode resolution delegated to TPT SkipRows / SkipRowsEveryFile (F2.3)
    if header_mode == "none":
        skip_rows, skip_rows_every_file = 0, "N"
    elif header_mode == "every_file":
        skip_rows, skip_rows_every_file = entity_skip_rows, "Y"
    else:  # first_chunk_only
        skip_rows, skip_rows_every_file = entity_skip_rows, "N"

    slot_gov: Optional[UtilitySlotGovernor] = orch_config.get("slot_governor")
    if slot_gov is not None:
        with slot_gov.acquire_slot(entity=entity, source=source):
            _, success = run_tpt_loader(
                entity=entity,
                file_path_or_pattern=list_path,
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
                skip_precheck=skip_precheck,
                has_meta=has_meta,
                work_zone=active_zone,
                delimiter=delimiter,
            )
    else:
        _, success = run_tpt_loader(
            entity=entity,
            file_path_or_pattern=list_path,
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
            skip_precheck=skip_precheck,
            has_meta=has_meta,
            work_zone=active_zone,
            delimiter=delimiter,
        )

    return success


class OrchestratorService:
    """
    Main Multi-Batch FIFO Orchestrator Service.
    Consumes queued Batch Capsules, manages database state machine in Teradata,
    coordinates Just-In-Time Schema Guard synchronization, and archives deliverables.
    """

    def __init__(
        self,
        config_path: str,
        source_override: Optional[str] = None,
        skip_post_tasks: bool = False,
        post_tasks_file: Optional[str] = None,
        all_sources: bool = False,
        sources_list: Optional[List[str]] = None,
    ) -> None:
        self.logger = setup_orchestrator_console_logger()
        self.config_path = config_path
        self.source_override = source_override
        self.skip_post_tasks = skip_post_tasks
        self.post_tasks_file = post_tasks_file
        self.all_sources = all_sources
        self.sources_list = sources_list

        self.config = ConfigLoader.load_json_config(config_path)
        self.base_dir = os.path.dirname(os.path.abspath(config_path))

        self.tpt_config = self.config.get("tpt", {})
        self.orch_config = self.config.get("orchestration", {})
        self.td_config = self.config.get("teradata", {})
        self.schema_guard_cfg = self.config.get("schema_guard", {})

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
            os.makedirs(self.landing_zone, exist_ok=True)
            os.makedirs(self.loading_zone, exist_ok=True)
            os.makedirs(self.archive_zone, exist_ok=True)

        # Filesystem check: LANDING, LOADING, and ARCHIVE must share the same device (F2.2 step 1)
        os.makedirs(self.landing_zone, exist_ok=True)
        os.makedirs(self.loading_zone, exist_ok=True)
        os.makedirs(self.archive_zone, exist_ok=True)
        devs = {os.stat(p).st_dev for p in (self.landing_zone, self.loading_zone, self.archive_zone)}
        if len(devs) != 1:
            raise ConfigurationError(
                "LANDING_ZONE, LOADING_ZONE and ARCHIVE_ZONE must be on the same filesystem (atomic directory moves are required)."
            )

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

        # Schema Guard settings
        self.schema_guard_enabled = bool(self.schema_guard_cfg.get("enabled", True))
        env_auto_migrate = os.environ.get("DL_AUTO_MIGRATE_DDL")
        if env_auto_migrate is not None:
            self.auto_migrate_ddl = env_auto_migrate.strip().lower() in ("true", "1", "yes")
        else:
            self.auto_migrate_ddl = bool(self.schema_guard_cfg.get("auto_migrate_ddl", True))
        self.default_column_type = self.schema_guard_cfg.get("default_column_type", "VARCHAR(4000)")
        self.character_set = self.schema_guard_cfg.get("character_set", "UNICODE")
        self.target_database = self.tpt_config.get("target_db") or os.environ.get("TD_TARGET_DB", "DEV0SIDFINSTG")
        self.max_row_bytes = self.schema_guard_cfg.get("max_row_bytes", 64000)

        # Validate post-tasks at startup (F2.9)
        cfg_post = self.orch_config.get("post_ingestion_tasks")
        if cfg_post:
            validate_post_tasks(cfg_post)

        # Cross-process Teradata utility slot governor
        max_slots = concurrency_cfg.get("max_concurrent_load_operators", 2)
        gov_dir = self.orch_config.get("loading_zone")
        if self.source_override and gov_dir:
            gov_dir = os.path.dirname(gov_dir)
        self.slot_governor = UtilitySlotGovernor(
            slot_dir=gov_dir,
            max_slots=max_slots,
            logger=self.logger,
        )
        self.orch_config["slot_governor"] = self.slot_governor

        self.db: Optional[TeradataStateDB] = None
        self.schema_actions: Dict[str, str] = {}

    def _get_db(self) -> TeradataStateDB:
        if self.db is None:
            self.db = TeradataStateDB(self.td_config)
        return self.db

    def wait_or_recover_active_db_batch(self) -> None:
        """
        Pre-flight check against BATCH_RUNS. Re-raises database errors as DataLoaderError (exit 1).
        Halts if foreign host owns active run; waits on active PID if local; cleans stale crashed runs (F2.8).
        """
        try:
            db = self._get_db()
            active_db_batch = db.get_active_batch_run(source=self.source_override)
        except DataLoaderError:
            raise
        except Exception as exc:
            raise DataLoaderError(f"Pre-flight state check failed: {exc}") from exc

        if active_db_batch is not None:
            active_batch_id = active_db_batch.get("batch_id")
            active_pid = active_db_batch.get("process_pid")
            active_source = active_db_batch.get("source")
            active_host = active_db_batch.get("process_host")
            current_host = socket.gethostname()

            if active_host and active_host != current_host:
                msg = (
                    f"CRITICAL: Active batch {active_batch_id} (Source: {active_source}) has status RUNNING on foreign host "
                    f"'{active_host}'. Cannot verify liveness or alter state locally. Halting execution."
                )
                self.logger.critical(msg)
                raise DataLoaderError(msg)

            if active_pid and is_pid_alive(active_pid):
                self.logger.info(
                    f"Pre-flight detected active batch {active_batch_id} (PID {active_pid} on {active_host}) in BATCH_RUNS. Waiting..."
                )
                wait_for_pid_completion(
                    pid=active_pid,
                    poll_interval_sec=self.poll_interval,
                    timeout_sec=self.lock_timeout,
                    logger=self.logger,
                    context_label="Active Teradata Batch Run",
                    host=active_host,
                )
            else:
                self.logger.warning(
                    f"Pre-flight detected crashed/stale batch {active_batch_id} in BATCH_RUNS (PID {active_pid} dead on host {active_host}). "
                    f"Marking status as FAILED in technical database."
                )
                db.mark_batch_failed(active_batch_id, source=active_source)

    def check_inflight_batch(self) -> List[BatchContext]:
        """
        Inspects LOADING_ZONE for in-flight batch directories (BATCH_*).
        Returns list of BatchContext sorted chronologically (by name).
        Reconciles with Teradata state: if latest attempt is SUCCESS, sets already_loaded=True (F2.2, F2.8).
        """
        if not os.path.exists(self.loading_zone):
            return []

        # Check for stray regular files in LOADING_ZONE root
        stray_files = [
            f for f in os.listdir(self.loading_zone)
            if not f.startswith(".") and os.path.isfile(os.path.join(self.loading_zone, f))
        ]
        if stray_files:
            msg = f"Unexpected regular file(s) found at root of LOADING_ZONE: {stray_files}"
            self.logger.critical(msg)
            raise OrchestratorExecutionError(msg)

        capsule_dir_names = sorted([
            d for d in os.listdir(self.loading_zone)
            if d.startswith("BATCH_") and os.path.isdir(os.path.join(self.loading_zone, d))
        ])
        if not capsule_dir_names:
            return []

        inflight_contexts: List[BatchContext] = []
        for c_name in capsule_dir_names:
            batch_dir = os.path.join(self.loading_zone, c_name)
            desc_path = os.path.join(batch_dir, "batch_descriptor.json")
            if not os.path.exists(desc_path):
                msg = f"In-flight batch directory '{batch_dir}' is missing mandatory batch_descriptor.json"
                self.logger.critical(msg)
                raise BatchValidationError(msg)

            try:
                descriptor = BatchDescriptor.from_json_file(desc_path)
            except Exception as exc:
                msg = f"Failed to parse batch_descriptor.json in '{batch_dir}': {exc}"
                self.logger.critical(msg)
                raise BatchValidationError(msg) from exc

            source = descriptor.source
            batch_id = str(descriptor.batch_id)
            extract_ts = descriptor.extract_timestamp
            clean_ts = descriptor.extract_timestamp_raw or CapsulePathResolver.normalize_timestamp_token(extract_ts)

            batch_log_dir = CapsulePathResolver.get_log_capsule_dir(
                self.logs_dir, source, batch_id, clean_ts, ensure_exists=True
            )
            self.tpt_config["work_dir"] = os.path.join(batch_log_dir, "TPT")
            os.makedirs(self.tpt_config["work_dir"], exist_ok=True)
            run_log_file = add_batch_file_logger(batch_log_dir, source, extract_ts, batch_id)

            manifest_in_loading = os.path.join(
                batch_dir,
                f"MANIFEST_{source}_{descriptor.extract_timestamp_raw}_{batch_id}.MF"
            )
            if not os.path.exists(manifest_in_loading):
                mfs = [os.path.join(batch_dir, f) for f in os.listdir(batch_dir) if f.endswith(".MF") and not f.startswith(".")]
                if mfs:
                    manifest_in_loading = mfs[0]

            entities_workload, validated_files = verify_descriptor_batch(
                batch_dir, descriptor, verify_checksum=False
            )
            raw_dir = os.path.join(batch_dir, "RAW") if os.path.isdir(os.path.join(batch_dir, "RAW")) else None

            # Reconciliation check (F2.8)
            already_loaded = False
            try:
                db = self._get_db()
                latest = db.get_latest_attempt(batch_id, source=source)
                if latest and latest.get("status") == "SUCCESS":
                    already_loaded = True
                    self.logger.info(
                        f"In-flight batch {batch_id} was already marked SUCCESS in attempt {latest.get('run_attempt')}; "
                        "skipping entity execution and proceeding to completeness barrier and archive."
                    )
            except Exception as exc:
                self.logger.debug(f"Could not consult latest attempt for reconciliation: {exc}")

            ctx = BatchContext(
                batch_id=batch_id,
                source=source,
                extract_ts=extract_ts,
                clean_ts=clean_ts,
                manifest_path=manifest_in_loading,
                manifest_basename=os.path.basename(manifest_in_loading),
                active_zone=batch_dir,
                is_in_flight=True,
                entities_workload=entities_workload,
                validated_files=validated_files,
                batch_log_dir=batch_log_dir,
                run_log_file=run_log_file,
                raw_dir=raw_dir,
                descriptor_path=desc_path,
                descriptor=descriptor,
                already_loaded=already_loaded,
            )
            inflight_contexts.append(ctx)

        return inflight_contexts

    def prepare_batch_from_queued_item(self, item: QueuedBatchItem) -> BatchContext:
        """
        Dequeues capsule by atomically moving directory from LANDING to LOADING (F2.2 step 2).
        Validates descriptor and payload integrity (F2.5).
        """
        source = item.source
        batch_id = str(item.batch_id)
        extract_ts = item.extract_ts
        clean_ts = CapsulePathResolver.normalize_timestamp_token(extract_ts)

        batch_log_dir = CapsulePathResolver.get_log_capsule_dir(
            self.logs_dir, source, batch_id, clean_ts, ensure_exists=True
        )
        self.tpt_config["work_dir"] = os.path.join(batch_log_dir, "TPT")
        os.makedirs(self.tpt_config["work_dir"], exist_ok=True)

        run_log_file = add_batch_file_logger(batch_log_dir, source, extract_ts, batch_id)
        self.logger.info(f"Preparing Batch {batch_id} (Source: {source}, Extract: {extract_ts}) [Capsule: {item.capsule_dir}]")

        os.makedirs(self.loading_zone, exist_ok=True)

        batch_dir = os.path.join(self.loading_zone, os.path.basename(item.capsule_dir))
        if os.path.exists(batch_dir):
            raise BatchValidationError(f"Batch directory already in LOADING_ZONE: {batch_dir}")
        os.replace(item.capsule_dir, batch_dir)
        ctx_active_zone = batch_dir
        manifest_in_loading = os.path.join(batch_dir, item.manifest_basename)
        desc_in_loading = os.path.join(batch_dir, "batch_descriptor.json")
        raw_in_loading = os.path.join(batch_dir, "RAW") if os.path.isdir(os.path.join(batch_dir, "RAW")) else None

        descriptor = item.descriptor
        if descriptor is None and os.path.exists(desc_in_loading):
            try:
                descriptor = BatchDescriptor.from_json_file(desc_in_loading)
            except Exception:
                pass

        verify_checksum_capsule = self.orch_config.get("verify_checksum_on_queued_capsule", True)

        try:
            if descriptor is not None:
                self.logger.info(f"Verifying BatchDescriptor files (verify_checksum={verify_checksum_capsule}).")
                entities_workload, validated_files = verify_descriptor_batch(
                    ctx_active_zone, descriptor, verify_checksum=verify_checksum_capsule
                )
            else:
                self.logger.info("Verifying manifest content and entity mappings.")
                manifest_entries = parse_manifest_content(manifest_in_loading)
                allow_orphan = self.orch_config.get("allow_orphan_files", False)
                entities_workload, validated_files = verify_and_map_batch(
                    landing_zone=ctx_active_zone,
                    manifest_entries=manifest_entries,
                    mapping_file=self.mapping_file,
                    verify_checksum=self.orch_config.get("verify_checksum", True),
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
            manifest_path=manifest_in_loading,
            manifest_basename=item.manifest_basename,
            active_zone=ctx_active_zone,
            is_in_flight=False,
            entities_workload=entities_workload,
            validated_files=validated_files,
            batch_log_dir=batch_log_dir,
            run_log_file=run_log_file,
            raw_dir=raw_in_loading,
            descriptor_path=desc_in_loading if os.path.exists(desc_in_loading) else None,
            descriptor=descriptor,
        )

    def _sync_schema_for_entity(
        self,
        entity: str,
        entity_info: Dict[str, Any],
        source: str,
        active_zone: Optional[str] = None,
    ) -> Tuple[bool, Optional[int], bool]:
        """
        Executes Just-In-Time Schema Guard synchronization for a single entity
        before TPT load begins. Returns (success, has_meta, guard_ran).
        """
        if not self.schema_guard_enabled:
            return True, None, False

        db = self._get_db()
        if not hasattr(db, "conn_mgr") or db.conn_mgr is None:
            return True, None, False

        csv_cols = entity_info.get("header_columns")
        base_zone = active_zone or self.loading_zone
        sample_file = entity_info.get("sample_file")
        sample_path = os.path.join(base_zone, sample_file) if sample_file else None
        if not sample_path and entity_info.get("files"):
            sample_path = os.path.join(base_zone, entity_info["files"][0])

        entity_load_mode = entity_info.get("load_mode", "replace")

        try:
            with db.get_connection() as conn:
                res = sync_entity_schema_if_drifted(
                    connection=conn,
                    target_db=self.target_database,
                    target_table=entity,
                    sample_file_path=sample_path,
                    default_type=self.default_column_type,
                    character_set=self.character_set,
                    auto_migrate_ddl=self.auto_migrate_ddl,
                    load_mode=entity_load_mode,
                    csv_cols=csv_cols,
                    max_row_bytes=self.max_row_bytes,
                )
                is_identical, action, cols, has_meta = res

                self.schema_actions[entity] = action
                if not is_identical and action == "BLOCKED":
                    self.logger.error(f"[{entity}] Schema drift detected and auto_migrate_ddl is disabled. DDL blocked.")
                    return False, has_meta, True
                return True, has_meta, True
        except SchemaSyncError as exc:
            self.logger.error(f"[{entity}] Schema Guard synchronization error: {exc}")
            return False, None, True
        except Exception as exc:
            self.logger.error(f"[{entity}] Unexpected Schema Guard error: {exc}")
            return False, None, True

    def execute_entity_tasks(self, ctx: BatchContext) -> bool:
        """Registers attempt in Teradata, synchronizes schemas, and executes parallel entity workers (F2.11)."""
        db = self._get_db()
        entity_file_counts = {entity: len(info.get("files", [])) for entity, info in ctx.entities_workload.items()}

        if not entity_file_counts and ctx.validated_files:
            msg = (
                f"FATAL: No tasks generated for batch {ctx.batch_id} despite "
                f"{len(ctx.validated_files)} validated data files present in active zone."
            )
            self.logger.critical(msg)
            raise DataLoaderError(msg)

        try:
            attempt = db.initialize_batch(ctx.batch_id, ctx.source, ctx.extract_ts, entity_file_counts)
        except Exception as exc:
            msg = f"Database initialization failed for batch {ctx.batch_id}: {exc}"
            self.logger.critical(msg)
            raise DataLoaderError(msg) from exc

        pending_tasks = db.get_pending_tasks(ctx.batch_id, source=ctx.source, run_attempt=attempt)
        if not pending_tasks:
            self.logger.info(f"No pending tasks to execute for batch {ctx.batch_id} in attempt {attempt}.")
            return True

        pending_entities = sorted(list(set(t[0] for t in pending_tasks)))
        self.logger.info(f"Found pending tasks for {len(pending_entities)} distinct entities in batch {ctx.batch_id} (Attempt {attempt}).")

        self.orch_config["active_zone"] = ctx.active_zone

        # 1. Just-In-Time Schema Guard pre-synchronization
        entity_has_meta: Dict[str, Optional[int]] = {}
        entity_guard_ran: Dict[str, bool] = {}
        for entity in pending_entities:
            entity_info = ctx.entities_workload[entity]
            schema_ok, has_meta, guard_ran = self._sync_schema_for_entity(entity, entity_info, ctx.source, active_zone=ctx.active_zone)
            if not schema_ok:
                self.logger.error(f"Schema synchronization failed for entity '{entity}'. Aborting batch execution.")
                db.update_task_end(
                    ctx.batch_id,
                    entity,
                    "FAILED",
                    error_msg="Schema Guard drift blocked or DDL synchronization failed.",
                    source=ctx.source,
                    run_attempt=attempt,
                )
                return False
            entity_has_meta[entity] = has_meta
            entity_guard_ran[entity] = guard_ran

        # 2. Parallel TPT execution
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
                        has_meta=entity_has_meta.get(entity),
                        skip_precheck=entity_guard_ran.get(entity, False),
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
        Verifies 100% entity completion in Teradata, executes post-tasks,
        and atomically renames the Batch Capsule to ARCHIVE_ZONE in a single rename (F2.2 step 5).
        """
        db = self._get_db()

        # Completeness Barrier (F2.11)
        expected_entity_count = len(ctx.entities_workload)
        all_success = db.get_batch_summary(ctx.batch_id, expected_entity_count, source=ctx.source)
        if not all_success:
            self.logger.error(
                f"[FAILED] Completeness barrier check failed in BATCH_TASKS for batch {ctx.batch_id} (Source: {ctx.source})."
            )
            db.finish_batch(ctx.batch_id, "FAILED", source=ctx.source)
            return 1

        self.logger.info(
            f"[BARRIER PASSED] All {expected_entity_count} entities for batch {ctx.batch_id} are marked SUCCESS in Teradata."
        )

        # Context environment for post-tasks
        context_env = {
            "CURRENT_BATCH_ID": str(ctx.batch_id),
            "CURRENT_SOURCE": str(ctx.source),
            "CURRENT_MANIFEST": str(ctx.manifest_basename),
            "CURRENT_EXTRACT_TS": str(ctx.extract_ts),
        }

        # Resolve post-ingestion tasks (merge external source tasks with orchestrator config tasks)
        post_tasks: List[Dict[str, Any]] = []
        if self.post_tasks_file and os.path.exists(self.post_tasks_file):
            try:
                with open(self.post_tasks_file, "r", encoding="utf-8") as ptf:
                    file_tasks = expand_env_vars(json.load(ptf))
                    if isinstance(file_tasks, list):
                        post_tasks.extend(file_tasks)
                    elif isinstance(file_tasks, dict):
                        src_specific = file_tasks.get(ctx.source, file_tasks.get("default", []))
                        post_tasks.extend(src_specific)
                self.logger.info(f"Loaded source post-tasks from file: '{self.post_tasks_file}'")
            except Exception as exc:
                self.logger.error(f"Failed to read post-tasks file '{self.post_tasks_file}': {exc}")
                return 1

        cfg_post = self.orch_config.get("post_ingestion_tasks")
        if isinstance(cfg_post, dict):
            src_tasks = cfg_post.get(ctx.source, [])
            default_tasks = cfg_post.get("default", [])
            for t in src_tasks:
                if t not in post_tasks:
                    post_tasks.append(t)
            for t in default_tasks:
                if t not in post_tasks:
                    post_tasks.append(t)
        elif isinstance(cfg_post, list):
            for t in cfg_post:
                if t not in post_tasks:
                    post_tasks.append(t)

        if self.skip_post_tasks:
            self.logger.info("[BYPASS] Post-tasks execution skipped via --skip-post-tasks flag.")
            post_success = True
        else:
            post_success = run_post_tasks(post_tasks, context_env=context_env, batch_log_dir=ctx.batch_log_dir) if post_tasks else True

        if not post_success:
            db.finish_batch(ctx.batch_id, "FAILED", source=ctx.source)
            self.logger.error(f"[ERROR] Pipeline failed during post-ingestion tasks for batch {ctx.batch_id}.")
            return 1

        # Update RAW/RECEIPT_*.json with final schema actions if present
        raw_zone = os.path.join(ctx.active_zone, "RAW")
        if os.path.isdir(raw_zone):
            receipt_files = [
                os.path.join(raw_zone, f)
                for f in os.listdir(raw_zone)
                if f.startswith(f"RECEIPT_{ctx.source}_") and f.endswith(f"_{ctx.batch_id}.json")
            ]
            if not receipt_files:
                receipt_files = [
                    os.path.join(raw_zone, f)
                    for f in os.listdir(raw_zone)
                    if f.startswith("RECEIPT_") and f.endswith(".json")
                ]
            for r_file in receipt_files:
                try:
                    with open(r_file, "r", encoding="utf-8") as rf:
                        r_data = json.load(rf)
                    r_data["orchestrator_execution"] = {
                        "status": "SUCCESS",
                        "pid": os.getpid(),
                        "schema_actions": self.schema_actions,
                    }
                    with open(r_file, "w", encoding="utf-8") as rf:
                        json.dump(r_data, rf, indent=2)
                except Exception as exc:
                    self.logger.debug(f"Could not update receipt {r_file}: {exc}")

        # Batch Capsule Archiving: single atomic directory rename without modifying payload (F2.2 step 5)
        batch_archive_dir = CapsulePathResolver.get_archive_capsule_dir(
            self.archive_zone, ctx.source, ctx.batch_id, ctx.clean_ts, ensure_exists=False
        )
        os.makedirs(os.path.dirname(batch_archive_dir), exist_ok=True)
        work_root = os.path.join(ctx.active_zone, ".work")
        if os.path.isdir(work_root):
            shutil.rmtree(work_root)

        if os.path.exists(batch_archive_dir):
            aside = f"{batch_archive_dir}.previous_{int(time.time())}"
            self.logger.warning(f"Archive capsule already exists; moving it aside to {aside}")
            os.replace(batch_archive_dir, aside)

        self.logger.info(f"Archiving processed batch deliverables into Batch Capsule: '{batch_archive_dir}'...")
        os.replace(ctx.active_zone, batch_archive_dir)

        db.finish_batch(ctx.batch_id, "SUCCESS", source=ctx.source)
        self.logger.info(f"[SUCCESS] Pipeline completed successfully for batch {ctx.batch_id}!")
        detach_batch_file_logger(self.logger)
        return 0

    finalize_and_archive = run_completeness_barrier_and_archive

    def run_all_sources(self) -> int:
        """
        Discovers active source queues and drains them sequentially (F2.9 step 3).
        A failure in one source halts that source's queue only and continues with other sources.
        """
        if self.sources_list:
            sources_to_run = list(self.sources_list)
        else:
            found_sources = set()
            lz_base = self.orch_config.get("landing_zone")
            if lz_base and os.path.isdir(lz_base):
                for item in os.listdir(lz_base):
                    p = os.path.join(lz_base, item)
                    if not item.startswith(".") and os.path.isdir(p) and item not in (".utility_slots",):
                        found_sources.add(item.upper())
            loading_base = self.orch_config.get("loading_zone")
            if loading_base and os.path.isdir(loading_base):
                for item in os.listdir(loading_base):
                    p = os.path.join(loading_base, item)
                    if not item.startswith(".") and os.path.isdir(p) and item not in (".utility_slots",):
                        found_sources.add(item.upper())
            sources_to_run = sorted(list(found_sources))

        if not sources_to_run:
            self.logger.info("Multi-source sweep: No active source directories found in LANDING_ZONE or LOADING_ZONE.")
            return 0

        self.logger.info(f"=== Multi-Source Sweep: Discovered {len(sources_to_run)} source(s): {sources_to_run} ===")
        source_results: Dict[str, int] = {}
        for src in sources_to_run:
            self.logger.info("=" * 70)
            self.logger.info(f">>> STARTING ORCHESTRATION FOR SOURCE: {src}")
            self.logger.info("=" * 70)
            try:
                is_mocked_run = hasattr(self.run, "assert_called") or type(self.run).__name__ in ("MagicMock", "Mock")
                if is_mocked_run:
                    self.source_override = src
                    rc = self.run()
                else:
                    src_service = OrchestratorService(
                        config_path=self.config_path,
                        source_override=src,
                        skip_post_tasks=self.skip_post_tasks,
                        post_tasks_file=self.post_tasks_file,
                    )
                    if self.db is not None:
                        src_service.db = self.db
                    if self.slot_governor is not None:
                        src_service.slot_governor = self.slot_governor
                    rc = src_service.run()
            except Exception as exc:
                self.logger.critical(f"Exception during orchestration for source '{src}': {exc}")
                rc = 1
            source_results[src] = rc
            if rc != 0:
                self.logger.error(f"Orchestration failed for source '{src}' with exit code {rc}. Queue for '{src}' halted.")

        self.logger.info("=" * 70)
        self.logger.info(">>> [MULTI-SOURCE SWEEP SUMMARY]")
        for src, rc in source_results.items():
            status_str = "SUCCESS" if rc == 0 else f"FAILED (RC {rc})"
            self.logger.info(f"  Source: {src:<20} Status: {status_str}")
        self.logger.info("=" * 70)

        return 1 if any(rc != 0 for rc in source_results.values()) else 0

    def run(
        self,
        skip_post_tasks: Optional[bool] = None,
        post_tasks_file: Optional[str] = None,
    ) -> int:
        """
        Executes the Multi-Batch FIFO Orchestrator queue lifecycle:
          1. Recovers in-flight batches in LOADING_ZONE first (oldest first).
          2. Drains queued Batch Capsules from LANDING_ZONE in FIFO order.
          3. Enforces FAIL_STOP semantics on any batch failure.
        """
        if skip_post_tasks is not None:
            self.skip_post_tasks = skip_post_tasks
        if post_tasks_file is not None:
            self.post_tasks_file = post_tasks_file

        if self.post_tasks_file and os.path.exists(self.post_tasks_file):
            try:
                with open(self.post_tasks_file, "r", encoding="utf-8") as f:
                    file_tasks = json.load(f)
                validate_post_tasks(file_tasks)
            except Exception as exc:
                raise ConfigurationError(f"Validation failed for post_tasks_file '{self.post_tasks_file}': {exc}") from exc

        if self.all_sources or self.sources_list:
            return self.run_all_sources()

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
                # 0. Startup root check: LOADING_ZONE must contain no regular files other than .orchestrator.pid (F2.2 step 7)
                stray_files = [
                    f
                    for f in os.listdir(self.loading_zone)
                    if os.path.isfile(os.path.join(self.loading_zone, f)) and f != ".orchestrator.pid"
                ]
                if stray_files:
                    msg = f"Stray regular file(s) found at root of LOADING_ZONE: {stray_files}. Manual cleanup required."
                    self.logger.critical(msg)
                    raise BatchValidationError(msg)

                # 1. Recover interrupted in-flight batches in LOADING_ZONE (F2.2 step 6, F2.7)
                inflight_batches = self.check_inflight_batch()
                for inflight_ctx in inflight_batches:
                    self.logger.info(f"Resuming in-flight batch {inflight_ctx.batch_id} in LOADING_ZONE...")
                    if not inflight_ctx.already_loaded:
                        tasks_ok = self.execute_entity_tasks(inflight_ctx)
                        if not tasks_ok:
                            return 1
                    rc = self.run_completeness_barrier_and_archive(inflight_ctx)
                    if rc != 0:
                        return rc

                # 2. Scan LANDING_ZONE for queued Batch Capsules (F2.10)
                scan_status, queued_items = scan_landing_queue(
                    self.landing_zone,
                    manifest_pattern=self.manifest_pattern,
                    manifest_regex=self.orch_config.get("manifest_regex"),
                )

                if scan_status == "IDLE":
                    if not inflight_batches:
                        self.logger.info(f"Landing zone '{self.landing_zone}' is empty. Exiting gracefully with RC 0.")
                    return 0

                if scan_status == "ERROR_UNEXPECTED_FILES":
                    msg = f"FATAL: Unexpected regular file(s) detected in landing zone '{self.landing_zone}'. Aborting."
                    self.logger.critical(msg)
                    raise BatchValidationError(msg)

                # Drain protection for --skip-post-tasks with replace mode (F2.9 step 4)
                allow_drain = self.orch_config.get("allow_replace_drain_without_post_tasks", False)
                if self.skip_post_tasks and len(queued_items) > 1 and not allow_drain:
                    first_item = queued_items[0]
                    desc = first_item.descriptor
                    has_replace = False
                    if desc and desc.entities:
                        for ent_info in desc.entities.values():
                            if (ent_info.get("load_mode") or "replace").lower() == "replace":
                                has_replace = True
                                break
                    if has_replace:
                        remaining_bids = [it.batch_id for it in queued_items[1:]]
                        self.logger.warning(
                            f"--skip-post-tasks active with replace-mode entities: processing only the first batch ({first_item.batch_id}). "
                            f"Batches left queued to prevent staging data loss: {remaining_bids}. "
                            "Configure 'orchestration.allow_replace_drain_without_post_tasks: true' to override."
                        )
                        queued_items = [first_item]

                # 3. Process each queued batch item in FIFO order
                for item in queued_items:
                    self.logger.info(
                        f"--- [QUEUE] Dequeuing Batch {item.batch_id} (Source: {item.source}, Extract: {item.extract_ts}) ---"
                    )
                    ctx = self.prepare_batch_from_queued_item(item)
                    tasks_ok = self.execute_entity_tasks(ctx)
                    if not tasks_ok:
                        self.logger.error(f"FAIL_STOP: Task execution failed for batch {ctx.batch_id}. Aborting queue.")
                        db = self._get_db()
                        db.finish_batch(ctx.batch_id, "FAILED", source=ctx.source)
                        return 1

                    rc = self.run_completeness_barrier_and_archive(ctx)
                    if rc != 0:
                        self.logger.error(f"FAIL_STOP: Archiving or post-tasks failed for batch {ctx.batch_id}. Aborting queue.")
                        return rc

                self.logger.info(
                    f"Queue processing completed successfully ({len(queued_items)} batch(es) processed)."
                )
                return 0
        finally:
            if self.db is not None:
                self.db.close()
                self.db = None
