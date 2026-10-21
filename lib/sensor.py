"""
lib/sensor.py - Landing Zone Queue Sensor & Batch Payload Validator

Scans LANDING_ZONE for queued Batch Capsules (BATCH_<CLEAN_TS>_<BATCH_ID>/)
or legacy flat Single-Batch files, enforcing FIFO chronological ordering,
OPS-01 boundary checks, and validating workloads via `dataloader_core`.
"""

from __future__ import annotations

import glob
import logging
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

_COMMON_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "COMMON"))
if _COMMON_DIR not in sys.path:
    sys.path.insert(0, _COMMON_DIR)

from dataloader_core.capsule import BatchDescriptor
from dataloader_core.checksum import compute_file_checksum
from dataloader_core.manifest import parse_manifest_filename, read_manifest_entries
from dataloader_core.mapping import EntityMapper, inspect_file_inner_extension

logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")

DEFAULT_MANIFEST_REGEX = r"^MANIFEST_([A-Za-z0-9_]+)_(\d{8}_\d{6})_([A-Za-z0-9_-]+)\.MF$"


@dataclass
class QueuedBatchItem:
    """Represents a discovered batch item in the landing queue (capsule directory or legacy flat)."""

    batch_id: str
    source: str
    extract_ts: str
    raw_timestamp: str
    manifest_path: str
    manifest_basename: str
    capsule_dir: Optional[str] = None  # None if legacy flat batch
    descriptor_path: Optional[str] = None
    descriptor: Optional[BatchDescriptor] = None
    data_files: Optional[List[str]] = None

    @property
    def is_capsule(self) -> bool:
        return self.capsule_dir is not None


def scan_landing_zone(
    landing_zone: str,
    manifest_pattern: str = "MANIFEST_*.MF",
) -> Tuple[str, Any, List[str]]:
    """
    Legacy helper: Scans the landing zone and strictly enforces Single-Batch rules.
    Returns tuple: (status, manifest_path, data_files)
    status can be:
      - 'IDLE': 0 manifests, 0 data files (normal exit)
      - 'ERROR_MISSING_MANIFEST': 0 manifests, but data files exist
      - 'ERROR_MULTIPLE_MANIFESTS': > 1 manifests found
      - 'OK': exactly 1 manifest found
    """
    if not os.path.exists(landing_zone):
        os.makedirs(landing_zone, exist_ok=True)
        return "IDLE", None, []

    all_items = [f for f in os.listdir(landing_zone) if not f.startswith(".")]

    # Search for manifests matching pattern
    search_path = os.path.join(landing_zone, manifest_pattern)
    found_manifests = glob.glob(search_path)

    # Also catch any .MF files if pattern didn't match directly
    if not found_manifests:
        found_manifests = [os.path.join(landing_zone, f) for f in all_items if f.endswith(".MF")]

    manifest_basenames = set(os.path.basename(m) for m in found_manifests)
    data_files = [f for f in all_items if f not in manifest_basenames and not os.path.isdir(os.path.join(landing_zone, f))]

    if len(found_manifests) == 0:
        if len(data_files) == 0:
            logger.info(f"Landing zone '{landing_zone}' is empty. Nothing to process.")
            return "IDLE", None, []
        else:
            logger.error(
                f"Data files present in '{landing_zone}', but no manifest file found ({len(data_files)} data files detected)."
            )
            return "ERROR_MISSING_MANIFEST", None, data_files

    if len(found_manifests) > 1:
        logger.error(f"Single-Batch violation: Multiple manifests detected in '{landing_zone}': {found_manifests}.")
        return "ERROR_MULTIPLE_MANIFESTS", found_manifests, data_files

    manifest_path = found_manifests[0]
    logger.info(f"Manifest detected: {manifest_path}. Found {len(data_files)} data file(s) in landing zone.")
    return "OK", manifest_path, data_files


def scan_landing_queue(
    landing_zone: str,
    manifest_pattern: str = "MANIFEST_*.MF",
    manifest_regex: Optional[str] = None,
) -> Tuple[str, List[QueuedBatchItem]]:
    """
    Scans landing_zone for 1..N queued Batch Capsules and/or legacy flat manifest batches.
    Sorts all discovered items in ascending chronological FIFO order ((extract_timestamp, batch_id)).

    Returns:
      (status, queued_items)
      status: 'IDLE', 'OK', 'ERROR_MISSING_MANIFEST', 'ERROR_MULTIPLE_MANIFESTS'
    """
    if not os.path.exists(landing_zone):
        os.makedirs(landing_zone, exist_ok=True)
        return "IDLE", []

    all_entries = os.listdir(landing_zone)
    visible_entries = [e for e in all_entries if not e.startswith(".")]

    queued_items: List[QueuedBatchItem] = []
    orphan_files: List[str] = []

    # 1. Discover Queued Capsule Directories: BATCH_*
    for entry in visible_entries:
        entry_path = os.path.join(landing_zone, entry)
        if os.path.isdir(entry_path):
            if entry.startswith("BATCH_"):
                desc_path = os.path.join(entry_path, "batch_descriptor.json")
                descriptor: Optional[BatchDescriptor] = None
                if os.path.exists(desc_path):
                    try:
                        descriptor = BatchDescriptor.from_json_file(desc_path)
                    except Exception as exc:
                        logger.warning(f"Could not parse batch descriptor at '{desc_path}': {exc}")

                # Locate manifest file in capsule directory
                capsule_manifests = glob.glob(os.path.join(entry_path, manifest_pattern))
                if not capsule_manifests:
                    capsule_manifests = [
                        os.path.join(entry_path, f)
                        for f in os.listdir(entry_path)
                        if f.endswith(".MF") and not f.startswith(".")
                    ]

                if not capsule_manifests and descriptor is None:
                    logger.warning(f"Capsule directory '{entry}' has neither manifest nor descriptor; ignoring.")
                    continue

                if capsule_manifests:
                    mf_path = capsule_manifests[0]
                    mf_basename = os.path.basename(mf_path)
                    try:
                        meta = parse_manifest_metadata(mf_path, manifest_regex=manifest_regex)
                        source = meta["source"]
                        extract_ts = meta["extract_timestamp"]
                        batch_id = meta["batch_id"]
                        raw_ts = meta.get("raw_timestamp", extract_ts)
                    except Exception as exc:
                        if descriptor:
                            source = descriptor.source
                            extract_ts = descriptor.extract_timestamp
                            batch_id = descriptor.batch_id
                            raw_ts = descriptor.extract_timestamp_raw
                        else:
                            logger.error(f"Failed parsing manifest metadata in capsule '{entry}': {exc}")
                            continue
                else:
                    mf_basename = f"MANIFEST_{descriptor.source}_{descriptor.extract_timestamp_raw}_{descriptor.batch_id}.MF"
                    mf_path = os.path.join(entry_path, mf_basename)
                    source = descriptor.source
                    extract_ts = descriptor.extract_timestamp
                    batch_id = descriptor.batch_id
                    raw_ts = descriptor.extract_timestamp_raw

                # Data files inside capsule
                capsule_data_files = [
                    f
                    for f in os.listdir(entry_path)
                    if not f.startswith(".")
                    and f != os.path.basename(mf_path)
                    and f != "batch_descriptor.json"
                    and f != "RAW"
                    and os.path.isfile(os.path.join(entry_path, f))
                ]

                queued_items.append(
                    QueuedBatchItem(
                        batch_id=str(batch_id),
                        source=source,
                        extract_ts=extract_ts,
                        raw_timestamp=raw_ts,
                        manifest_path=mf_path,
                        manifest_basename=mf_basename,
                        capsule_dir=entry_path,
                        descriptor_path=desc_path if os.path.exists(desc_path) else None,
                        descriptor=descriptor,
                        data_files=capsule_data_files,
                    )
                )

    # 2. Check for legacy flat batch files at the root of landing_zone
    flat_manifests = glob.glob(os.path.join(landing_zone, manifest_pattern))
    if not flat_manifests:
        flat_manifests = [
            os.path.join(landing_zone, f)
            for f in visible_entries
            if f.endswith(".MF") and os.path.isfile(os.path.join(landing_zone, f))
        ]

    flat_data_files = [
        f
        for f in visible_entries
        if not f.endswith(".MF")
        and not f.endswith(".json")
        and os.path.isfile(os.path.join(landing_zone, f))
    ]

    if flat_manifests:
        if len(flat_manifests) > 1 and not queued_items:
            logger.error(f"Single-Batch violation: Multiple flat manifests detected in '{landing_zone}': {flat_manifests}")
            return "ERROR_MULTIPLE_MANIFESTS", []

        for mf_path in flat_manifests:
            try:
                meta = parse_manifest_metadata(mf_path, manifest_regex=manifest_regex)
                queued_items.append(
                    QueuedBatchItem(
                        batch_id=str(meta["batch_id"]),
                        source=meta["source"],
                        extract_ts=meta["extract_timestamp"],
                        raw_timestamp=meta.get("raw_timestamp", meta["extract_timestamp"]),
                        manifest_path=mf_path,
                        manifest_basename=os.path.basename(mf_path),
                        capsule_dir=None,
                        descriptor_path=None,
                        descriptor=None,
                        data_files=flat_data_files,
                    )
                )
            except Exception as exc:
                logger.error(f"Failed parsing flat manifest '{mf_path}': {exc}")
    elif flat_data_files:
        orphan_files.extend(flat_data_files)

    if orphan_files and not flat_manifests and not queued_items:
        logger.error(f"Data files present in '{landing_zone}', but no manifest or capsule found: {orphan_files}")
        return "ERROR_MISSING_MANIFEST", []

    if not queued_items:
        return "IDLE", []

    # Sort queue in ascending FIFO chronological order
    queued_items.sort(key=lambda item: (item.extract_ts, item.batch_id))
    logger.info(f"Discovered {len(queued_items)} queued batch(es) in '{landing_zone}' (FIFO order).")
    return "OK", queued_items


def parse_manifest_metadata(
    manifest_path: str,
    manifest_regex: Optional[str] = None,
) -> Dict[str, str]:
    """
    Extracts SOURCE, EXTRACT_TIMESTAMP, and BATCH_ID from the manifest filename.
    Delegates to `dataloader_core.manifest.parse_manifest_filename` with `strict=True`.
    """
    meta = parse_manifest_filename(
        manifest_path,
        custom_regex=manifest_regex,
        strict=True,
    )
    return {
        "source": meta.source,
        "extract_timestamp": meta.extract_timestamp_iso,
        "batch_id": meta.batch_id,
        "raw_timestamp": meta.extract_timestamp_raw,
    }


def parse_manifest_content(manifest_path: str) -> List[Dict[str, Optional[str]]]:
    """
    Parses manifest content containing columns: filename, checksum (optional).
    Delegates to `dataloader_core.manifest.read_manifest_entries`.
    """
    entries = read_manifest_entries(manifest_path, require_non_empty=True)
    return [entry.to_dict() for entry in entries]


def compute_checksum(filepath: str, algorithm: str = "auto") -> str:
    """Computes hash (MD5 or SHA256) of a file via `dataloader_core.checksum`."""
    return compute_file_checksum(filepath, algorithm=algorithm)


def verify_and_map_batch(
    landing_zone: str,
    manifest_entries: List[Dict[str, Any]],
    mapping_file: str,
    verify_checksum: bool = True,
    allow_orphan_files: bool = False,
    source: Optional[str] = None,
) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    """
    Validates:
      1. All manifested files exist in landing_zone and have size > 0.
      2. Checksums match (if present and verify_checksum is True).
      3. All files correspond to known entities in entity_mapping.csv for the given source.
      4. OPS-01 Boundary: No unmanifested orphan data files present in landing_zone.
    Returns: (entities_workload, validated_files_list)
    """
    manifested_filenames = set()
    for entry in manifest_entries:
        fname = entry["filename"]
        manifested_filenames.add(fname)
        full_path = os.path.join(landing_zone, fname)

        # 1. Existence check
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"Manifested file not found in landing zone: '{fname}'")

        # 2. Size check
        if os.path.getsize(full_path) == 0:
            raise ValueError(f"Manifested file is empty (0 bytes): '{fname}'")

        # 3. Checksum check
        expected_cs = entry.get("checksum")
        if verify_checksum and expected_cs:
            expected_cs = expected_cs.lower()
            algo = "sha256" if len(expected_cs) == 64 else "md5"
            computed_cs = compute_checksum(full_path, algo)
            if computed_cs != expected_cs:
                raise ValueError(f"Checksum mismatch for '{fname}': expected {expected_cs}, computed {computed_cs}")

    # 4. Orphan check (OPS-01)
    if not allow_orphan_files:
        all_lz_files = [
            f
            for f in os.listdir(landing_zone)
            if not f.startswith(".")
            and not f.endswith(".MF")
            and not f.endswith(".json")
            and f != "RAW"
            and not os.path.isdir(os.path.join(landing_zone, f))
        ]
        orphan_files = [f for f in all_lz_files if f not in manifested_filenames]
        if orphan_files:
            raise ValueError(f"Boundary violation (OPS-01): Unmanifested orphan file(s) found in landing zone: {orphan_files}")

    # 5. Entity mapping check via dataloader_core.mapping.EntityMapper
    mapper = EntityMapper(mapping_file=mapping_file, default_source=source)
    mapper.load_rules(source=source)

    entities_workload: Dict[str, Dict[str, Any]] = {}

    for fname in sorted(manifested_filenames):
        matched_rule = mapper.match_file(fname, file_dir=landing_zone, source=source)
        if not matched_rule:
            if source:
                raise ValueError(
                    f"File '{fname}' does not match any recognized entity signature for source '{source}' in '{mapping_file}'."
                )
            else:
                raise ValueError(f"File '{fname}' does not match any recognized entity signature in '{mapping_file}'.")

        matched_entity = matched_rule.target_table
        if matched_entity not in entities_workload:
            entities_workload[matched_entity] = {
                "target_table": matched_entity,
                "signature": matched_rule.signature,
                "files": [],
                "extension": matched_rule.raw_extension,
                "inner_extension": matched_rule.inner_extension,
                "delimiter": matched_rule.delimiter,
                "load_mode": matched_rule.load_mode,
                "skip_rows": matched_rule.skip_rows,
                "header_mode": matched_rule.header_mode,
            }
        entities_workload[matched_entity]["files"].append(fname)

    return entities_workload, list(manifested_filenames)


def verify_descriptor_batch(
    active_zone: str,
    descriptor: BatchDescriptor,
    verify_checksum: bool = False,
) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    """
    Validates a batch using pre-computed BatchDescriptor:
      1. Verifies each manifested file exists and matches expected filesize_bytes (O(1)).
      2. If verify_checksum is True, computes MD5 and compares.
      3. Constructs entities_workload directly from descriptor.entities with header_columns.
    Returns: (entities_workload, validated_files_list)
    """
    validated_files: List[str] = []
    entities_workload: Dict[str, Dict[str, Any]] = {}

    for target_table, entity_info in descriptor.entities.items():
        raw_files = entity_info.get("files") or entity_info.get("data_files") or []
        if not raw_files:
            continue

        file_names: List[str] = []
        for item in raw_files:
            if isinstance(item, dict):
                fname = item["filename"]
                expected_size = item.get("filesize_bytes")
                expected_md5 = item.get("md5")
            else:
                fname = str(item)
                expected_size = entity_info.get("file_sizes", {}).get(fname)
                expected_md5 = entity_info.get("checksums", {}).get(fname)

            file_names.append(fname)
            fpath = os.path.join(active_zone, fname)
            if not os.path.exists(fpath):
                raise FileNotFoundError(f"Manifested file not found in active zone: '{fname}'")

            actual_size = os.path.getsize(fpath)
            if actual_size == 0:
                raise ValueError(f"Manifested file is empty (0 bytes): '{fname}'")
            if expected_size is not None and actual_size != expected_size:
                raise ValueError(f"File size mismatch for '{fname}': expected {expected_size} bytes, got {actual_size} bytes")

            if verify_checksum and expected_md5:
                computed_cs = compute_checksum(fpath, "md5")
                if computed_cs.lower() != expected_md5.lower():
                    raise ValueError(f"Checksum mismatch for '{fname}': expected {expected_md5}, got {computed_cs}")

            validated_files.append(fname)

        entities_workload[target_table] = {
            "target_table": target_table,
            "signature": entity_info.get("signature", target_table.lower()),
            "files": file_names,
            "extension": entity_info.get("extension", ".csv"),
            "inner_extension": entity_info.get("inner_extension", ".csv"),
            "header_columns": entity_info.get("header_columns", []),
            "sample_file": entity_info.get("sample_file") or (file_names[0] if file_names else None),
            "delimiter": entity_info.get("delimiter", ","),
            "load_mode": entity_info.get("load_mode", "replace"),
            "skip_rows": entity_info.get("skip_rows", 1),
            "header_mode": entity_info.get("header_mode", "first_chunk_only"),
        }

    return entities_workload, validated_files
