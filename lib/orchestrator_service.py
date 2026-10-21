"""
lib/orchestrator_service.py - Cohesive Multi-Batch Ingestion Orchestrator Service

Encapsulates the Multi-Batch FIFO Queue consumer, Just-In-Time Schema Guard DDL
synchronization, TPT execution, completeness barrier, post-tasks execution,
and unified Batch Capsule archiving into `OrchestratorService`.
"""

from __future__ import annotations

import glob
import gzip
import json
import logging
import os
import shutil
import sys
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
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
from lib.post_tasks import run_post_tasks
from lib.schema_guard import sync_entity_schema_if_drifted
from lib.sensor import (
    QueuedBatchItem,
    parse_manifest_content,
    parse_manifest_metadata,
    scan_landing_queue,
    scan_landing_zone,
    verify_and_map_batch,
    verify_descriptor_batch,
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
    raw_dir: Optional[str] = None
    descriptor_path: Optional[str] = None
    descriptor: Optional[BatchDescriptor] = None


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
    has_meta: int = 2,
    skip_precheck: bool = True,
) -> bool:
    """
    Ingests all files for a given entity in the isolated LOADING_ZONE.
    Handles single files vs multi-chunk extracts and header normalization.
    """
    logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")
    loading_zone = orch_config.get("loading_zone") or orch_config.get("landing_zone", "")
    files = sorted(entity_info["files"])
    signature = entity_info.get("signature", entity.lower())

    pending_tasks = db.get_pending_tasks(batch_id, source=source)
    entity_retries = [t[2] for t in pending_tasks if t[0] == entity]
    retry_count = max(entity_retries) if entity_retries else 0

    load_mode = entity_info.get("load_mode") or tpt_config.get("default_load_mode") or orch_config.get("default_load_mode", "replace")
    delimiter = entity_info.get("delimiter") or tpt_config.get("delimiter") or orch_config.get("delimiter", ",")
    header_mode = (entity_info.get("header_mode") or "first_chunk_only").lower()
    entity_skip_rows = int(entity_info.get("skip_rows", 1))

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
        if header_mode == "none":
            skip_rows = 0
            skip_rows_every_file = "N"
        else:
            skip_rows = entity_skip_rows
            skip_rows_every_file = "N"
    else:
        target_spec = f"*{signature}*{data_ext}"
        if header_mode == "first_chunk_only":
            all_delimited = all(f.lower().endswith((".csv", ".pecsv")) for f in data_files)
            if all_delimited:
                chunk_1_path = os.path.join(loading_zone, data_files[0])
                strip_header_in_place(chunk_1_path)
                skip_rows = 0
                skip_rows_every_file = "N"
                logger.info(
                    f"[{entity}] Multi-chunk detected ({len(data_files)} files, header_mode=first_chunk_only). First chunk header stripped; loading with SkipRows=0."
                )
            else:
                skip_rows = 1
                skip_rows_every_file = "N"
        elif header_mode == "every_file":
            skip_rows = entity_skip_rows
            skip_rows_every_file = "Y"
            logger.info(
                f"[{entity}] Multi-chunk detected ({len(data_files)} files, header_mode=every_file). Preserving headers; loading with SkipRows={skip_rows} and SkipRowsEveryFile='Y'."
            )
        elif header_mode == "none":
            skip_rows = 0
            skip_rows_every_file = "N"
            logger.info(
                f"[{entity}] Multi-chunk detected ({len(data_files)} files, header_mode=none). Loading headless with SkipRows=0."
            )
        else:
            skip_rows = entity_skip_rows
            skip_rows_every_file = "N"

    slot_gov: Optional[UtilitySlotGovernor] = orch_config.get("slot_governor")
    if slot_gov is not None:
        with slot_gov.acquire_slot(entity=entity, source=source):
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
                skip_precheck=skip_precheck,
                has_meta=has_meta,
                delimiter=delimiter,
            )
    else:
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
            skip_precheck=skip_precheck,
            has_meta=has_meta,
            delimiter=delimiter,
        )

    return success


class OrchestratorService:
    """Service managing Multi-Batch FIFO ingestion orchestration, JIT Schema Guard, TPT workers, post-tasks, and archiving."""

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

        # Schema Guard settings
        self.schema_guard_enabled = bool(self.schema_guard_cfg.get("enabled", True))
        # Check environment variable override DL_AUTO_MIGRATE_DDL
        env_auto_migrate = os.environ.get("DL_AUTO_MIGRATE_DDL")
        if env_auto_migrate is not None:
            self.auto_migrate_ddl = env_auto_migrate.strip().lower() in ("1", "true", "yes")
            self.schema_guard_enabled = True
        else:
            self.auto_migrate_ddl = bool(self.schema_guard_cfg.get("auto_migrate_ddl", True))

        self.default_column_type = self.schema_guard_cfg.get("default_column_type", "VARCHAR(4000)")
        self.character_set = self.schema_guard_cfg.get("character_set", "UNICODE")
        self.target_database = self.schema_guard_cfg.get(
            "target_database", self.tpt_config.get("target_db")
        )

        self.db: Optional[TeradataStateDB] = None
        self.schema_actions: Dict[str, str] = {}

        # Cross-Process Teradata Utility Slot Governor
        max_utility_slots = int(self.orch_config.get("max_global_utility_slots", 8))
        loading_base = (
            os.path.dirname(self.loading_zone) if self.source_override else self.loading_zone
        )
        slot_dir = self.orch_config.get("slot_dir") or os.path.join(loading_base, ".utility_slots")
        self.slot_governor = UtilitySlotGovernor(slot_dir=slot_dir, max_slots=max_utility_slots, logger=self.logger)
        self.orch_config["slot_governor"] = self.slot_governor

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

    def check_inflight_batch(self) -> Optional[BatchContext]:
        """
        Inspects LOADING_ZONE for any uncompleted batch (flat .MF or BATCH_* directory)
        left over from an interrupted or crashed run.
        """
        if not os.path.exists(self.loading_zone):
            return None

        # Check for in-flight BATCH_* capsule dir
        capsule_dirs = [
            os.path.join(self.loading_zone, d)
            for d in os.listdir(self.loading_zone)
            if d.startswith("BATCH_") and os.path.isdir(os.path.join(self.loading_zone, d))
        ]
        if capsule_dirs:
            c_dir = capsule_dirs[0]
            self.logger.info(f"In-flight capsule directory detected in LOADING_ZONE: {c_dir}. Resuming execution.")
            # Unpack contents of capsule_dir into loading_zone root
            for item in os.listdir(c_dir):
                shutil.move(os.path.join(c_dir, item), os.path.join(self.loading_zone, item))
            try:
                os.rmdir(c_dir)
            except Exception:
                pass

        # Check for in-flight flat manifest in loading_zone
        in_flight_manifests = [
            os.path.join(self.loading_zone, f)
            for f in os.listdir(self.loading_zone)
            if f.endswith(".MF") and not f.startswith(".")
        ]
        if not in_flight_manifests:
            return None

        manifest_path = in_flight_manifests[0]
        self.logger.info(f"In-flight batch detected in LOADING_ZONE: {manifest_path}. Resuming execution.")

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

        desc_path = os.path.join(self.loading_zone, "batch_descriptor.json")
        descriptor: Optional[BatchDescriptor] = None
        if os.path.exists(desc_path):
            try:
                descriptor = BatchDescriptor.from_json_file(desc_path)
            except Exception:
                pass

        if descriptor:
            entities_workload, validated_files = verify_descriptor_batch(
                self.loading_zone, descriptor, verify_checksum=False
            )
        else:
            manifest_entries = parse_manifest_content(manifest_path)
            entities_workload, validated_files = verify_and_map_batch(
                landing_zone=self.loading_zone,
                manifest_entries=manifest_entries,
                mapping_file=self.mapping_file,
                verify_checksum=False,
                allow_orphan_files=True,
                source=source,
            )

        raw_dir = os.path.join(self.loading_zone, "RAW") if os.path.isdir(os.path.join(self.loading_zone, "RAW")) else None

        return BatchContext(
            batch_id=batch_id,
            source=source,
            extract_ts=extract_ts,
            clean_ts=clean_ts,
            manifest_path=manifest_path,
            manifest_basename=os.path.basename(manifest_path),
            active_zone=self.loading_zone,
            is_in_flight=True,
            entities_workload=entities_workload,
            validated_files=validated_files,
            batch_log_dir=batch_log_dir,
            run_log_file=run_log_file,
            raw_dir=raw_dir,
            descriptor_path=desc_path if os.path.exists(desc_path) else None,
            descriptor=descriptor,
        )

    def prepare_batch_from_queued_item(self, item: QueuedBatchItem) -> BatchContext:
        """
        Transitions a QueuedBatchItem from LANDING_ZONE to LOADING_ZONE,
        sets up logging, and validates file workloads via BatchDescriptor or manifest.
        """
        source = item.source
        extract_ts = item.extract_ts
        batch_id = item.batch_id
        clean_ts = CapsulePathResolver.normalize_timestamp_token(extract_ts)

        batch_log_dir = CapsulePathResolver.get_log_capsule_dir(
            self.logs_dir, source, batch_id, clean_ts, ensure_exists=True
        )
        self.tpt_config["work_dir"] = os.path.join(batch_log_dir, "TPT")
        os.makedirs(self.tpt_config["work_dir"], exist_ok=True)

        run_log_file = add_batch_file_logger(batch_log_dir, source, extract_ts, batch_id)
        self.logger.info(f"Preparing Batch {batch_id} (Source: {source}, Extract: {extract_ts}) [Capsule: {item.is_capsule}]")

        os.makedirs(self.loading_zone, exist_ok=True)

        # Transition files to isolated LOADING_ZONE
        if item.is_capsule and item.capsule_dir:
            self.logger.info(f"Unpacking capsule '{item.capsule_dir}' into isolated '{self.loading_zone}'...")
            for entry in os.listdir(item.capsule_dir):
                src_path = os.path.join(item.capsule_dir, entry)
                dst_path = os.path.join(self.loading_zone, entry)
                if os.path.exists(dst_path):
                    if os.path.isdir(dst_path):
                        shutil.rmtree(dst_path)
                    else:
                        os.remove(dst_path)
                shutil.move(src_path, dst_path)
            try:
                os.rmdir(item.capsule_dir)
            except Exception as exc:
                self.logger.debug(f"Could not remove empty capsule staging dir: {exc}")
        else:
            # Legacy flat files
            self.logger.info(f"Moving legacy flat batch files to isolated '{self.loading_zone}'...")
            shutil.move(item.manifest_path, os.path.join(self.loading_zone, item.manifest_basename))
            if item.data_files:
                for f in item.data_files:
                    src_file = os.path.join(self.landing_zone, f)
                    if os.path.exists(src_file):
                        shutil.move(src_file, os.path.join(self.loading_zone, f))

        manifest_in_loading = os.path.join(self.loading_zone, item.manifest_basename)
        desc_in_loading = os.path.join(self.loading_zone, "batch_descriptor.json")
        raw_in_loading = os.path.join(self.loading_zone, "RAW") if os.path.isdir(os.path.join(self.loading_zone, "RAW")) else None

        # Load descriptor if present
        descriptor = item.descriptor
        if descriptor is None and os.path.exists(desc_in_loading):
            try:
                descriptor = BatchDescriptor.from_json_file(desc_in_loading)
            except Exception:
                pass

        verify_checksum = self.orch_config.get("verify_checksum", True)
        verify_checksum_capsule = self.orch_config.get("verify_checksum_on_queued_capsule", False)

        try:
            if descriptor is not None and not verify_checksum_capsule:
                self.logger.info("Using fast-path O(1) file stat verification via BatchDescriptor.")
                entities_workload, validated_files = verify_descriptor_batch(
                    self.loading_zone, descriptor, verify_checksum=False
                )
            elif descriptor is not None and verify_checksum_capsule:
                self.logger.info("Verifying BatchDescriptor files and checksums.")
                entities_workload, validated_files = verify_descriptor_batch(
                    self.loading_zone, descriptor, verify_checksum=True
                )
            else:
                self.logger.info("Verifying manifest content and entity mappings.")
                manifest_entries = parse_manifest_content(manifest_in_loading)
                allow_orphan = self.orch_config.get("allow_orphan_files", False)
                entities_workload, validated_files = verify_and_map_batch(
                    landing_zone=self.loading_zone,
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
            manifest_path=manifest_in_loading,
            manifest_basename=item.manifest_basename,
            active_zone=self.loading_zone,
            is_in_flight=False,
            entities_workload=entities_workload,
            validated_files=validated_files,
            batch_log_dir=batch_log_dir,
            run_log_file=run_log_file,
            raw_dir=raw_in_loading,
            descriptor_path=desc_in_loading if os.path.exists(desc_in_loading) else None,
            descriptor=descriptor,
        )

    def detect_and_validate_batch(self) -> Optional[BatchContext]:
        """
        Legacy Single-Batch method preserved for unit-test compatibility.
        Detects an in-flight batch or first batch in landing_zone.
        """
        inflight = self.check_inflight_batch()
        if inflight is not None:
            return inflight

        status, items = scan_landing_queue(
            self.landing_zone,
            manifest_pattern=self.manifest_pattern,
            manifest_regex=self.orch_config.get("manifest_regex"),
        )
        if status == "IDLE":
            return None
        if status == "ERROR_MISSING_MANIFEST":
            raise BatchValidationError(f"Found data files in '{self.landing_zone}' without manifest.")
        if status == "ERROR_MULTIPLE_MANIFESTS":
            raise BatchValidationError(f"Multiple flat manifests detected in '{self.landing_zone}'.")
        if not items:
            return None

        return self.prepare_batch_from_queued_item(items[0])

    acquire_guards_and_detect_batch = detect_and_validate_batch

    def transition_to_loading_zone(self, ctx: BatchContext) -> None:
        """Compatibility no-op: files already transitioned during prepare_batch_from_queued_item."""
        self.orch_config["loading_zone"] = self.loading_zone

    def _sync_schema_for_entity(
        self,
        entity: str,
        entity_info: Dict[str, Any],
        source: str,
    ) -> Tuple[bool, int]:
        """
        Executes Just-In-Time Schema Guard synchronization for a single entity
        before TPT load begins. Returns (success, has_meta).
        """
        if not self.schema_guard_enabled:
            return True, 2

        db = self._get_db()
        # Check if database has active connection manager
        if not hasattr(db, "conn_mgr") or db.conn_mgr is None:
            return True, 2

        csv_cols = entity_info.get("header_columns")
        sample_file = entity_info.get("sample_file")
        sample_path = os.path.join(self.loading_zone, sample_file) if sample_file else None
        if not sample_path and entity_info.get("files"):
            sample_path = os.path.join(self.loading_zone, entity_info["files"][0])

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
                )
                if len(res) >= 4:
                    is_identical, action, cols, has_meta = res[:4]
                else:
                    is_identical, action, cols = res[:3]
                    has_meta = 2

                self.schema_actions[entity] = action
                if not is_identical and action == "BLOCKED":
                    self.logger.error(f"[{entity}] Schema drift detected and auto_migrate_ddl is disabled. DDL blocked.")
                    return False, has_meta
                return True, has_meta
        except SchemaSyncError as exc:
            self.logger.error(f"[{entity}] Schema Guard synchronization error: {exc}")
            return False, 0
        except Exception as exc:
            self.logger.error(f"[{entity}] Unexpected Schema Guard error: {exc}")
            return False, 0

    def execute_entity_tasks(self, ctx: BatchContext) -> bool:
        """Registers batch state in Teradata, synchronizes schemas, and executes parallel entity workers."""
        db = self._get_db()
        tasks_list = []
        for entity, info in ctx.entities_workload.items():
            for fname in info["files"]:
                tasks_list.append((entity, fname))

        if not tasks_list and ctx.validated_files:
            msg = (
                f"FATAL: No tasks generated for batch {ctx.batch_id} despite "
                f"{len(ctx.validated_files)} validated data files present in active zone."
            )
            self.logger.critical(msg)
            raise DataLoaderError(msg)

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

        # 1. Just-In-Time Schema Guard pre-synchronization
        entity_has_meta: Dict[str, int] = {}
        for entity in pending_entities:
            entity_info = ctx.entities_workload[entity]
            schema_ok, has_meta = self._sync_schema_for_entity(entity, entity_info, ctx.source)
            if not schema_ok:
                self.logger.error(f"Schema synchronization failed for entity '{entity}'. Aborting batch execution.")
                db.update_task_end(
                    ctx.batch_id,
                    entity,
                    "FAILED",
                    error_msg="Schema Guard drift blocked or DDL synchronization failed.",
                    source=ctx.source,
                )
                return False
            entity_has_meta[entity] = has_meta

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
                        has_meta=entity_has_meta.get(entity, 2),
                        skip_precheck=True,
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
        Verifies 100% task completion in Teradata, executes post-tasks,
        archives deliverables into Batch Capsule, cleans LOADING_ZONE, and updates BATCH_RUNS.
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

        # Post-tasks execution
        post_tasks: List[Dict[str, Any]] = []

        # 1. From optional CLI post-tasks file
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
                    f"Loaded {len(post_tasks)} post-task(s) from external file ({self.post_tasks_file})."
                )
            except Exception as exc:
                self.logger.warning(f"Could not load post-tasks from file {self.post_tasks_file}: {exc}")

        # 2. From orchestrator config (supporting source-specific dict or flat list)
        orch_post_tasks = self.orch_config.get("post_ingestion_tasks", [])
        if isinstance(orch_post_tasks, dict):
            src_tasks = orch_post_tasks.get(ctx.source, [])
            default_tasks = orch_post_tasks.get("default", [])
            post_tasks.extend(src_tasks)
            for dt in default_tasks:
                if dt not in post_tasks:
                    post_tasks.append(dt)
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

        # Update RAW/RECEIPT_*.json with final schema actions if present
        raw_zone = os.path.join(self.loading_zone, "RAW")
        if os.path.isdir(raw_zone):
            receipt_files = glob.glob(os.path.join(raw_zone, f"RECEIPT_{ctx.source}_*_{ctx.batch_id}.json"))
            if not receipt_files:
                receipt_files = glob.glob(os.path.join(raw_zone, "RECEIPT_*.json"))
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

        # Batch Capsule Archiving
        batch_archive_dir = CapsulePathResolver.get_archive_capsule_dir(
            self.archive_zone, ctx.source, ctx.batch_id, ctx.clean_ts, ensure_exists=True
        )
        self.logger.info(f"Archiving processed batch deliverables into Batch Capsule: '{batch_archive_dir}'...")

        # Move manifest
        shutil.move(
            os.path.join(self.loading_zone, ctx.manifest_basename),
            os.path.join(batch_archive_dir, ctx.manifest_basename),
        )

        # Move batch descriptor if present
        if ctx.descriptor_path and os.path.exists(ctx.descriptor_path):
            shutil.move(ctx.descriptor_path, os.path.join(batch_archive_dir, "batch_descriptor.json"))
        elif os.path.exists(os.path.join(self.loading_zone, "batch_descriptor.json")):
            shutil.move(
                os.path.join(self.loading_zone, "batch_descriptor.json"),
                os.path.join(batch_archive_dir, "batch_descriptor.json"),
            )

        # Move RAW vault directory if present
        if os.path.isdir(raw_zone):
            dst_raw = os.path.join(batch_archive_dir, "RAW")
            if os.path.exists(dst_raw):
                shutil.rmtree(dst_raw)
            shutil.move(raw_zone, dst_raw)

        # Move validated payload files
        for f in ctx.validated_files:
            f_path = os.path.join(self.loading_zone, f)
            if os.path.exists(f_path):
                shutil.move(f_path, os.path.join(batch_archive_dir, f))
            marker_path = os.path.join(self.loading_zone, "." + f + ".header_stripped")
            if os.path.exists(marker_path):
                os.remove(marker_path)

        # Clean up any remaining temporary files in loading_zone (preserving active .orchestrator.pid)
        if os.path.exists(self.loading_zone):
            for item in os.listdir(self.loading_zone):
                if item == ".orchestrator.pid":
                    continue
                item_path = os.path.join(self.loading_zone, item)
                try:
                    if os.path.isfile(item_path):
                        os.remove(item_path)
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                except Exception as exc:
                    self.logger.warning(f"Could not clean up temporary item {item_path}: {exc}")

        db.finish_batch(ctx.batch_id, "SUCCESS", source=ctx.source)
        self.logger.info(f"[SUCCESS] Pipeline completed successfully for batch {ctx.batch_id}!")
        detach_batch_file_logger(self.logger)
        return 0

    finalize_and_archive = run_completeness_barrier_and_archive

    def run_all_sources(self) -> int:
        """Discovers active source queues and drains them sequentially with unified slot governance."""
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
        for src in sources_to_run:
            self.logger.info("=" * 70)
            self.logger.info(f">>> STARTING ORCHESTRATION FOR SOURCE: {src}")
            self.logger.info("=" * 70)
            src_service = OrchestratorService(
                config_path=self.config_path,
                source_override=src,
                skip_post_tasks=self.skip_post_tasks,
                post_tasks_file=self.post_tasks_file,
            )
            rc = src_service.run()
            if rc != 0:
                self.logger.critical(
                    f"FATAL: Orchestration failed for source '{src}' with exit code {rc}. Halting sweep (FAIL_STOP)."
                )
                return rc

        self.logger.info("=" * 70)
        self.logger.info(">>> [ALL SOURCES COMPLETE] Multi-source sweep completed with 100% SUCCESS!")
        self.logger.info("=" * 70)
        return 0

    def run(
        self,
        skip_post_tasks: Optional[bool] = None,
        post_tasks_file: Optional[str] = None,
    ) -> int:
        """
        Executes the Multi-Batch FIFO Orchestrator queue lifecycle:
          1. Recovers any in-flight batch in LOADING_ZONE first.
          2. Drains queued Batch Capsules and flat batches from LANDING_ZONE in FIFO order.
          3. Enforces FAIL_STOP semantics on any batch failure.
        """
        if skip_post_tasks is not None:
            self.skip_post_tasks = skip_post_tasks
        if post_tasks_file is not None:
            self.post_tasks_file = post_tasks_file

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
                # 1. Check for interrupted in-flight batch in LOADING_ZONE
                inflight_ctx = self.check_inflight_batch()
                if inflight_ctx is not None:
                    self.logger.info(f"Resuming in-flight batch {inflight_ctx.batch_id} in LOADING_ZONE...")
                    tasks_ok = self.execute_entity_tasks(inflight_ctx)
                    if not tasks_ok:
                        return 1
                    rc = self.run_completeness_barrier_and_archive(inflight_ctx)
                    if rc != 0:
                        return rc

                # 2. Scan LANDING_ZONE for queued capsules or flat batches
                scan_status, queued_items = scan_landing_queue(
                    self.landing_zone,
                    manifest_pattern=self.manifest_pattern,
                    manifest_regex=self.orch_config.get("manifest_regex"),
                )

                if scan_status == "IDLE":
                    if inflight_ctx is None:
                        self.logger.info(f"Landing zone '{self.landing_zone}' is empty. Exiting gracefully with RC 0.")
                    return 0

                if scan_status == "ERROR_MISSING_MANIFEST":
                    msg = f"FATAL: Unmanifested data files found in landing zone '{self.landing_zone}'. Aborting."
                    self.logger.critical(msg)
                    raise BatchValidationError(msg)

                if scan_status == "ERROR_MULTIPLE_MANIFESTS":
                    msg = f"FATAL: Multiple flat manifests detected in landing zone '{self.landing_zone}'. Aborting."
                    self.logger.critical(msg)
                    raise BatchValidationError(msg)

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
                        self.logger.error(f"FAIL_STOP: Completeness barrier failed for batch {ctx.batch_id}. Aborting queue.")
                        return rc

                self.logger.info("--- [QUEUE] All queued batches processed successfully! ---")
                return 0

        except DataLoaderError:
            return 1
        except Exception as exc:
            self.logger.critical(f"FATAL: Unhandled exception in Orchestrator: {exc}")
            return 1
        finally:
            detach_batch_file_logger(self.logger)
            if self.db is not None:
                self.db.close()
