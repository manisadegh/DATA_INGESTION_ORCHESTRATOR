"""
lib/sensor.py - Landing Zone Single-Batch Sensor & Payload Validator

Enforces the Single-Batch ingress contract on LANDING_ZONE and delegates
manifest parsing, streaming checksum verification, and 2-pass entity mapping
to `dataloader_core`.
"""

from __future__ import annotations

import glob
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

_COMMON_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "COMMON"))
if _COMMON_DIR not in sys.path:
    sys.path.insert(0, _COMMON_DIR)

from dataloader_core.checksum import compute_file_checksum
from dataloader_core.manifest import parse_manifest_filename, read_manifest_entries
from dataloader_core.mapping import EntityMapper, inspect_file_inner_extension

logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")

DEFAULT_MANIFEST_REGEX = r"^MANIFEST_([A-Za-z0-9_]+)_(\d{8}_\d{6})_([A-Za-z0-9_-]+)\.MF$"


def scan_landing_zone(
    landing_zone: str,
    manifest_pattern: str = "MANIFEST_*.MF",
) -> Tuple[str, Any, List[str]]:
    """
    Scans the landing zone and strictly enforces Single-Batch rules.
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
    data_files = [f for f in all_items if f not in manifest_basenames]

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
        all_lz_files = [f for f in os.listdir(landing_zone) if not f.startswith(".") and not f.endswith(".MF")]
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
            }
        entities_workload[matched_entity]["files"].append(fname)

    return entities_workload, list(manifested_filenames)
