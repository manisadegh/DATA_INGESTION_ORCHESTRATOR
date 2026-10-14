import logging
import glob
import os
import re
import csv
import hashlib
import zipfile
from datetime import datetime

logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")

DEFAULT_MANIFEST_REGEX = r"^MANIFEST_([A-Za-z0-9_]+)_(\d{8}_\d{6})_([A-Za-z0-9_-]+)\.MF$"

def scan_landing_zone(landing_zone, manifest_pattern="MANIFEST_*.MF"):
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

    all_items = [f for f in os.listdir(landing_zone) if not f.startswith('.')]
    
    # Search for manifests matching pattern
    search_path = os.path.join(landing_zone, manifest_pattern)
    found_manifests = glob.glob(search_path)
    
    # Also catch any .MF files if pattern didn't match directly
    if not found_manifests:
        found_manifests = [os.path.join(landing_zone, f) for f in all_items if f.endswith('.MF')]

    manifest_basenames = set(os.path.basename(m) for m in found_manifests)
    data_files = [f for f in all_items if f not in manifest_basenames]

    if len(found_manifests) == 0:
        if len(data_files) == 0:
            logger.info(f"Landing zone '{landing_zone}' is empty. Nothing to process.")
            return "IDLE", None, []
        else:
            logger.error(f"Data files present in '{landing_zone}', but no manifest file found ({len(data_files)} data files detected).")
            return "ERROR_MISSING_MANIFEST", None, data_files

    if len(found_manifests) > 1:
        logger.error(f"Single-Batch violation: Multiple manifests detected in '{landing_zone}': {found_manifests}.")
        return "ERROR_MULTIPLE_MANIFESTS", found_manifests, data_files

    manifest_path = found_manifests[0]
    logger.info(f"Manifest detected: {manifest_path}. Found {len(data_files)} data file(s) in landing zone.")
    return "OK", manifest_path, data_files

def parse_manifest_metadata(manifest_path, manifest_regex=None):
    """
    Extracts SOURCE, EXTRACT_TIMESTAMP, and BATCH_ID from the manifest filename.
    Pattern: MANIFEST_<SOURCE>_<EXTRACT_TIMESTAMP>_<BATCH_ID>.MF
    """
    filename = os.path.basename(manifest_path)
    regex = manifest_regex if manifest_regex else DEFAULT_MANIFEST_REGEX
    
    match = re.match(regex, filename)
    if match:
        source = match.group(1)
        extract_ts_raw = match.group(2)
        batch_id = match.group(3)
        
        # Parse timestamp into standard format 'YYYY-MM-DD HH:MM:SS'
        try:
            dt = datetime.strptime(extract_ts_raw, "%Y%m%d_%H%M%S")
            extract_timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            extract_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
        return {
            "source": source,
            "extract_timestamp": extract_timestamp,
            "batch_id": batch_id,
            "raw_timestamp": extract_ts_raw
        }
        
    # Fallback pattern for backward compatibility: MANIFEST-batch<ID>-.MF
    fallback_match = re.search(r"-batch(\d+)-", filename)
    if fallback_match:
        batch_id = fallback_match.group(1)
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return {
            "source": "BICC_LEGACY",
            "extract_timestamp": now_str,
            "batch_id": batch_id,
            "raw_timestamp": datetime.now().strftime("%Y%m%d_%H%M%S")
        }

    raise ValueError(f"Manifest filename '{filename}' does not adhere to standard naming: MANIFEST_<SOURCE>_<EXTRACT_TIMESTAMP>_<BATCH_ID>.MF")

def parse_manifest_content(manifest_path):
    """
    Parses manifest content containing columns: filename, checksum (optional).
    Supports comma, semicolon, tab delimiters and handles header or raw lines.
    """
    entries = []
    with open(manifest_path, 'r', encoding='utf-8') as f:
        lines = [line.strip() for line in f if line.strip() and not line.startswith('#') and not line.startswith('PLV_KEY=')]

    if not lines:
        raise ValueError(f"Manifest '{manifest_path}' is empty or has no entries.")

    first_line = lines[0].lower()
    has_header = "filename" in first_line

    start_idx = 1 if has_header else 0
    for line in lines[start_idx:]:
        # Determine delimiter
        if ';' in line:
            parts = line.split(';')
        elif '\t' in line:
            parts = line.split('\t')
        elif ',' in line:
            parts = line.split(',')
        else:
            parts = [line]

        filename = parts[0].strip().strip('"').strip("'")
        checksum = parts[1].strip().strip('"').strip("'") if len(parts) > 1 and parts[1].strip() else None

        if filename:
            entries.append({
                "filename": filename,
                "checksum": checksum
            })

    if not entries:
        raise ValueError(f"Manifest '{manifest_path}' contains 0 file entries after parsing.")

    return entries

def compute_checksum(filepath, algorithm="auto"):
    """Computes hash (MD5 or SHA256) of a file."""
    h = hashlib.sha256() if algorithm == "sha256" else hashlib.md5()
    with open(filepath, 'rb') as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest().lower()

def verify_and_map_batch(landing_zone, manifest_entries, mapping_file, verify_checksum=True, allow_orphan_files=False, source=None):
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
        all_lz_files = [f for f in os.listdir(landing_zone) if not f.startswith('.') and not f.endswith('.MF')]
        orphan_files = [f for f in all_lz_files if f not in manifested_filenames]
        if orphan_files:
            raise ValueError(f"Boundary violation (OPS-01): Unmanifested orphan file(s) found in landing zone: {orphan_files}")

    # 5. Entity mapping check (supports SOURCE column and INNER_EXTENSION for .csv vs .pecsv)
    def inspect_file_inner_extension(file_path):
        lower_p = file_path.lower()
        if lower_p.endswith(".zip") and os.path.exists(file_path):
            try:
                with zipfile.ZipFile(file_path, "r") as z:
                    names = [n.lower() for n in z.namelist()]
                    if any(n.endswith(".pecsv") for n in names):
                        return ".pecsv"
                    if any(n.endswith(".csv") for n in names):
                        return ".csv"
                    if names:
                        return os.path.splitext(names[0])[1]
            except Exception as e:
                logger.warning(f"Unable to inspect ZIP archive '{file_path}': {e}")
        return os.path.splitext(file_path)[1].lower()

    mapping_rules = []
    with open(mapping_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            normalized = {k.strip().upper(): v.strip() for k, v in row.items() if k and v}
            tgt = normalized.get("TARGET_TABLE", "")
            sig = normalized.get("FILE_SIGNATURE", "")
            ext = normalized.get("EXTENSION", ".csv")
            inner_ext = normalized.get("INNER_EXTENSION", "").lower()
            if not inner_ext:
                inner_ext = ".csv"
            src = normalized.get("SOURCE", "")

            if tgt and sig:
                mapping_rules.append({
                    "source": src,
                    "target_table": tgt,
                    "signature": sig.lower(),
                    "extension": ext.lower(),
                    "inner_extension": inner_ext
                })

    entities_workload = {}
    
    for fname in sorted(manifested_filenames):
        matched_entity = None
        matched_sig = None
        matched_ext = None
        matched_inner_ext = None
        
        full_fpath = os.path.join(landing_zone, fname)
        actual_inner_ext = inspect_file_inner_extension(full_fpath)

        # 1. First pass: exact match with source, signature, outer extension, and inner_extension
        for rule in mapping_rules:
            rule_src = rule.get("source", "").strip()
            if source and rule_src and rule_src != '*':
                allowed_sources = [s.strip().upper() for s in rule_src.split(',') if s.strip()]
                if source.strip().upper() not in allowed_sources:
                    continue

            fname_lower = fname.lower()
            if rule["signature"] in fname_lower:
                ext_rule = rule["extension"]
                ext_match = not ext_rule or ext_rule == '*' or fname_lower.endswith(ext_rule) or any(fname_lower.endswith(e.strip()) for e in ext_rule.split(','))
                if ext_match:
                    rule_inner = rule.get("inner_extension")
                    if rule_inner and rule_inner == actual_inner_ext:
                        matched_entity = rule["target_table"]
                        matched_sig = rule["signature"]
                        matched_ext = rule["extension"]
                        matched_inner_ext = rule_inner
                        break

        # 2. Fallback pass without inner_extension (backward compatibility)
        if not matched_entity:
            for rule in mapping_rules:
                rule_src = rule.get("source", "").strip()
                if source and rule_src and rule_src != '*':
                    allowed_sources = [s.strip().upper() for s in rule_src.split(',') if s.strip()]
                    if source.strip().upper() not in allowed_sources:
                        continue

                fname_lower = fname.lower()
                if rule["signature"] in fname_lower:
                    ext_rule = rule["extension"]
                    ext_match = not ext_rule or ext_rule == '*' or fname_lower.endswith(ext_rule) or any(fname_lower.endswith(e.strip()) for e in ext_rule.split(','))
                    if ext_match:
                        matched_entity = rule["target_table"]
                        matched_sig = rule["signature"]
                        matched_ext = rule["extension"]
                        matched_inner_ext = actual_inner_ext
                        break

        if not matched_entity:
            if source:
                raise ValueError(f"File '{fname}' does not match any recognized entity signature for source '{source}' in '{mapping_file}'.")
            else:
                raise ValueError(f"File '{fname}' does not match any recognized entity signature in '{mapping_file}'.")

        if matched_entity not in entities_workload:
            entities_workload[matched_entity] = {
                "target_table": matched_entity,
                "signature": matched_sig,
                "files": [],
                "extension": matched_ext,
                "inner_extension": matched_inner_ext
            }
        entities_workload[matched_entity]["files"].append(fname)

    return entities_workload, list(manifested_filenames)
