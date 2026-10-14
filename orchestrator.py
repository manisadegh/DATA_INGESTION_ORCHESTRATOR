import json
import logging
import sys
import os
import shutil
import argparse
import zipfile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from lib.logger import setup_logger, add_batch_file_logger
from lib.db_state import TeradataStateDB
from lib.sensor import (
    scan_landing_zone,
    parse_manifest_metadata,
    parse_manifest_content,
    verify_and_map_batch
)
from lib.tpt_worker import run_tpt_loader
from lib.post_tasks import run_post_tasks

def strip_header_in_place(file_path):
    """
    Removes the header (first line) of a CSV file in-place (Solution A).
    Leaves a marker file to ensure it is never stripped more than once on retries.
    """
    marker = os.path.join(os.path.dirname(file_path), "." + os.path.basename(file_path) + ".header_stripped")
    if os.path.exists(marker):
        return
        
    temp_path = file_path + ".tmp"
    with open(file_path, 'r', encoding='utf-8', errors='replace') as fin, \
         open(temp_path, 'w', encoding='utf-8') as fout:
        _ = fin.readline() # Strip header line
        for line in fin:
            fout.write(line)
            
    os.replace(temp_path, file_path)
    with open(marker, 'w') as f:
        f.write("STRIPPED\n")

def process_entity_task(entity, entity_info, batch_id, tpt_config, orch_config, db):
    """
    Ingests all files for a given entity in the isolated LOADING_ZONE.
    Handles single files vs multi-chunk extracts and header normalization.
    """
    logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")
    loading_zone = orch_config.get('loading_zone', orch_config['landing_zone'])
    files = sorted(entity_info['files'])
    signature = entity_info.get('signature', entity.lower())
    extension = entity_info.get('extension', '.csv')
    
    # Check max retry count for this entity
    pending_tasks = db.get_pending_tasks(batch_id)
    entity_retries = [t[2] for t in pending_tasks if t[0] == entity]
    retry_count = max(entity_retries) if entity_retries else 0
    
    load_mode = tpt_config.get('default_load_mode') or orch_config.get('default_load_mode', 'replace')
    
    # Multi-chunk header normalization (Solution A) & ZIP Decompression
    any_zip = any(f.lower().endswith('.zip') for f in files)
    if any_zip:
        extracted_files = []
        for f in files:
            f_path = os.path.join(loading_zone, f)
            if f.lower().endswith('.zip'):
                with zipfile.ZipFile(f_path, 'r') as z:
                    for m in z.namelist():
                        if m.lower().endswith(('.csv', '.pecsv')):
                            base_m = os.path.basename(m)
                            if not base_m:
                                continue
                            out_p = os.path.join(loading_zone, base_m)
                            if not os.path.exists(out_p):
                                with z.open(m) as zf, open(out_p, 'wb') as out_f:
                                    shutil.copyfileobj(zf, out_f)
                            extracted_files.append(base_m)
            elif f.lower().endswith(('.csv', '.pecsv')):
                extracted_files.append(f)
            else:
                extracted_files.append(f)
        data_files = sorted(list(set(extracted_files)))
        data_ext = os.path.splitext(data_files[0])[1].lower() if data_files else ".csv"
    else:
        data_files = files
        data_ext = os.path.splitext(data_files[0])[1].lower() if data_files else ".csv"

    if len(data_files) == 1:
        # Single file: standard load with SkipRows = 1
        target_spec = data_files[0]
        skip_rows = 1
        skip_rows_every_file = "N"
    else:
        # Multi-chunks: check if files are delimited text (.csv, .pecsv)
        all_delimited = all(f.lower().endswith(('.csv', '.pecsv')) for f in data_files)
        if all_delimited:
            # Strip header from the first chunk in LOADING_ZONE
            chunk_1_path = os.path.join(loading_zone, data_files[0])
            strip_header_in_place(chunk_1_path)
            
            # Wildcard matching all chunks for this entity
            target_spec = f"*{signature}*{data_ext}"
            skip_rows = 0
            skip_rows_every_file = "N"
            logger.info(f"[{entity}] Multi-chunk detected ({len(data_files)} files). First chunk header stripped; loading with SkipRows=0.")
        else:
            # For non-delimited or compressed files, target wildcard with SkipRows=1
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
        skip_rows_every_file=skip_rows_every_file
    )
    
    return success

def expand_env_vars(data):
    """Recursively expands ${VAR} or $VAR environment variables in configuration strings."""
    if isinstance(data, str):
        return os.path.expandvars(data)
    elif isinstance(data, dict):
        return {k: expand_env_vars(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [expand_env_vars(v) for v in data]
    return data

def main():
    # 1. Bootstrap Logger
    logger = setup_logger()
    
    parser = argparse.ArgumentParser(description="Generic Data Ingestion Orchestrator - Single Batch Architecture")
    parser.add_argument("--config", type=str, required=True, help="Path to configuration file")
    args = parser.parse_args()

    # 2. Load Configuration
    try:
        with open(args.config, "r", encoding="utf-8") as f:
            config = expand_env_vars(json.load(f))
    except Exception as e:
        logger.critical(f"Failed to load configuration file {args.config}: {e}")
        sys.exit(1)

    tpt_config = config.get('tpt', {})
    orch_config = config.get('orchestration', {})
    base_dir = os.path.dirname(os.path.abspath(args.config))

    for key in ['landing_zone', 'loading_zone', 'archive_zone', 'logs_dir']:
        if orch_config.get(key) and not os.path.isabs(orch_config[key]):
            orch_config[key] = os.path.normpath(os.path.join(base_dir, orch_config[key]))

    landing_zone = orch_config.get('landing_zone')
    loading_zone = orch_config.get('loading_zone', os.path.normpath(os.path.join(base_dir, '../LOADING_ZONE')))
    archive_zone = orch_config.get('archive_zone', os.path.normpath(os.path.join(base_dir, '../ARCHIVE_ZONE')))
    logs_dir = orch_config.get('logs_dir', os.path.normpath(os.path.join(base_dir, '../LOGS')))
    
    if not landing_zone:
        logger.critical("Configuration error: 'landing_zone' is required.")
        sys.exit(1)

    for key in ['wrapper_script', 'unlock_script']:
        if tpt_config.get(key) and not os.path.isabs(tpt_config[key]):
            tpt_config[key] = os.path.normpath(os.path.join(base_dir, tpt_config[key]))

    mapping_file = orch_config.get('entity_mapping_file', 'entity_mapping.csv')
    if not os.path.isabs(mapping_file):
        mapping_file = os.path.normpath(os.path.join(base_dir, mapping_file))

    # Setup TPT Env vars globally
    tpt_env = os.environ.copy()
    td_config = config.get('teradata', {})
    host = td_config.get('host') or td_config.get('server') or td_config.get('tdpid') or os.environ.get('TDPID')
    if host:
        tpt_env['TDPID'] = host
    pwd = td_config.get('password', '')
    if pwd and not pwd.startswith('ENCRYPTED_PASSWORD'):
        tpt_env['TD_PASSWORD'] = pwd
    elif 'TD_PASSWORD' in tpt_env:
        del tpt_env['TD_PASSWORD']
    env_user = f"{os.environ.get('TDENV', '')}{os.environ.get('SID_ADM_USER', '')}" if os.environ.get('SID_ADM_USER') else os.environ.get('USER')
    user = td_config.get('user') or td_config.get('username') or env_user
    if user:
        tpt_env['SID_ADM_USER'] = user
        tpt_env['USER'] = user
    orch_config['tpt_env'] = tpt_env
    tpt_config['tpt_env'] = tpt_env

    manifest_pattern = orch_config.get('manifest_pattern', 'MANIFEST_*.MF')

    # 3. Check for In-Flight Batch in LOADING_ZONE (Crash Recovery)
    in_flight_manifests = [
        os.path.join(loading_zone, f)
        for f in os.listdir(loading_zone)
        if f.endswith('.MF')
    ] if os.path.exists(loading_zone) else []

    if in_flight_manifests:
        logger.info(f"In-flight batch detected in LOADING_ZONE: {in_flight_manifests[0]}. Resuming execution.")
        manifest_path = in_flight_manifests[0]
        active_zone = loading_zone
        is_in_flight = True
    else:
        active_zone = landing_zone
        is_in_flight = False
        scan_status, manifest_path, data_files = scan_landing_zone(landing_zone, manifest_pattern)
        
        if scan_status == "IDLE":
            logger.info("Landing zone is empty. Exiting gracefully with RC 0.")
            sys.exit(0)
        elif scan_status == "ERROR_MISSING_MANIFEST":
            logger.critical(f"FATAL: Found {len(data_files)} data files in landing zone, but no manifest file. Aborting.")
            sys.exit(1)
        elif scan_status == "ERROR_MULTIPLE_MANIFESTS":
            logger.critical(f"FATAL: Single-Batch rule violation: Multiple manifests detected in landing zone: {manifest_path}. Aborting.")
            sys.exit(1)

    # 4. Phase 1: Parse Manifest Metadata & Validate Payload
    try:
        manifest_meta = parse_manifest_metadata(manifest_path, orch_config.get('manifest_regex'))
    except Exception as e:
        logger.critical(f"Failed to parse manifest filename '{manifest_path}': {e}")
        sys.exit(1)

    source = manifest_meta['source']
    extract_ts = manifest_meta['extract_timestamp']
    batch_id = manifest_meta['batch_id']

    # Initialize dedicated run log file in LOGS/
    run_log_file = add_batch_file_logger(logs_dir, source, extract_ts, batch_id)
    logger.info(f"Processing Batch {batch_id} (Source: {source}, Extract: {extract_ts}) [In-Flight: {is_in_flight}]")

    try:
        manifest_entries = parse_manifest_content(manifest_path)
    except Exception as e:
        logger.critical(f"Failed to parse manifest content for {manifest_path}: {e}")
        sys.exit(1)

    # For in-flight recovery, files have already been verified and headers may have been normalized
    verify_checksum = False if is_in_flight else orch_config.get('verify_checksum', True)
    allow_orphan = orch_config.get('allow_orphan_files', False)

    try:
        entities_workload, validated_files = verify_and_map_batch(
            landing_zone=active_zone,
            manifest_entries=manifest_entries,
            mapping_file=mapping_file,
            verify_checksum=verify_checksum,
            allow_orphan_files=allow_orphan,
            source=source
        )
    except Exception as e:
        logger.critical(f"Batch validation failed for batch {batch_id}: {e}")
        sys.exit(1)

    manifest_basename = os.path.basename(manifest_path)

    # 5. Phase 2: Atomic Transition to LOADING_ZONE (if not already in-flight)
    if not is_in_flight:
        os.makedirs(loading_zone, exist_ok=True)
        logger.info(f"Moving validated batch files from '{landing_zone}' to isolated '{loading_zone}'...")
        
        shutil.move(manifest_path, os.path.join(loading_zone, manifest_basename))
        
        for f in validated_files:
            src_file = os.path.join(landing_zone, f)
            dst_file = os.path.join(loading_zone, f)
            shutil.move(src_file, dst_file)
            
        logger.info(f"Successfully transferred manifest and {len(validated_files)} data files to '{loading_zone}'.")
        
    orch_config['loading_zone'] = loading_zone

    # 6. Phase 3: State Machine Registration (BATCH_RUNS & BATCH_TASKS)
    db = TeradataStateDB(config['teradata'])
    tasks_list = []
    for entity, info in entities_workload.items():
        for fname in info['files']:
            tasks_list.append((entity, fname))

    try:
        db.initialize_batch(batch_id, source, extract_ts, tasks_list)
    except Exception as e:
        logger.critical(f"Database initialization failed for batch {batch_id}: {e}")
        sys.exit(1)

    # 7. Phase 4: High-Performance Concurrent Ingestion by Entity
    pending_tasks = db.get_pending_tasks(batch_id)
    if not pending_tasks:
        logger.info(f"No pending tasks to execute for batch {batch_id}.")
    else:
        pending_entities = sorted(list(set(t[0] for t in pending_tasks)))
        logger.info(f"Found pending tasks for {len(pending_entities)} distinct entities in batch {batch_id}.")
        
        max_workers = orch_config.get('max_concurrent_jobs', 5)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for entity in pending_entities:
                entity_info = entities_workload[entity]
                futures[executor.submit(process_entity_task, entity, entity_info, batch_id, tpt_config, orch_config, db)] = entity
                
            for future in as_completed(futures):
                entity = futures[future]
                try:
                    success = future.result()
                    if not success:
                        logger.error(f"Ingestion for entity '{entity}' reported failure.")
                except Exception as exc:
                    logger.error(f"Worker for entity '{entity}' raised exception: {exc}")

    # 8. Phase 5: Completeness Barrier & Post-Tasks
    expected_total_files = len(validated_files)
    if db.get_batch_summary(batch_id, expected_total_files):
        logger.info(f"[SUCCESS] All {expected_total_files} file tasks for batch {batch_id} ingested successfully.")
        
        # Execute post-tasks
        post_tasks = orch_config.get('post_ingestion_tasks', [])
        os.environ['CURRENT_BATCH_ID'] = str(batch_id)
        os.environ['CURRENT_SOURCE'] = str(source)
        os.environ['CURRENT_MANIFEST'] = os.path.join(loading_zone, manifest_basename)
        
        post_success = run_post_tasks(post_tasks) if post_tasks else True
        if post_success:
            # Move files to ARCHIVE_ZONE
            os.makedirs(archive_zone, exist_ok=True)
            logger.info(f"Archiving processed files from '{loading_zone}' to '{archive_zone}'...")
            
            # Archive manifest
            shutil.move(os.path.join(loading_zone, manifest_basename), os.path.join(archive_zone, manifest_basename))
            # Archive data files & clean markers
            for f in validated_files:
                f_path = os.path.join(loading_zone, f)
                if os.path.exists(f_path):
                    shutil.move(f_path, os.path.join(archive_zone, f))
                marker_path = os.path.join(loading_zone, "." + f + ".header_stripped")
                if os.path.exists(marker_path):
                    os.remove(marker_path)
                    
            # Clean up any remaining temporary extracted files or markers in loading_zone
            if os.path.exists(loading_zone):
                for item in os.listdir(loading_zone):
                    item_path = os.path.join(loading_zone, item)
                    if os.path.isfile(item_path):
                        try:
                            os.remove(item_path)
                        except Exception as e:
                            logger.warning(f"Could not remove temporary file {item_path}: {e}")
                    
            db.finish_batch(batch_id, 'SUCCESS')
            logger.info(f"[SUCCESS] Pipeline completed successfully for batch {batch_id}!")
            sys.exit(0)
        else:
            db.finish_batch(batch_id, 'FAILED')
            logger.error(f"[ERROR] Pipeline failed during post-ingestion tasks for batch {batch_id}.")
            sys.exit(1)
    else:
        db.finish_batch(batch_id, 'FAILED')
        logger.error(f"[ERROR] Completeness barrier failed for batch {batch_id}. Files retained in '{loading_zone}' for recovery.")
        sys.exit(1)

if __name__ == "__main__":
    main()
