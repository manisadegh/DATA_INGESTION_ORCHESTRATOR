# DATA_INGESTION_ORCHESTRATOR - Testing, Validation & QA Guide

## 1. Testing Scope & Objectives

The testing framework for the **DATA_INGESTION_ORCHESTRATOR** verifies the multi-batch FIFO queue contract, Just-In-Time Schema Guard Auto-DDL, fast-path TPT execution (`-C -M 2`), concurrency sentries, crash resilience, and Teradata technical metadata integrity.

### Validation Boundaries
```mermaid
flowchart LR
    subgraph Inputs["Test Vectors"]
        T1["Nominal Multi-Entity Batch"]
        T2["ZIP Archive Batch (.zip / .gz)"]
        T3["Multi-Chunk Extract (Solution A)"]
        T4["Orphan Stray File (OPS-02)"]
        T5["Multi-Batch Queue (1..N FIFO)"]
        T6["Checksum / Size Corruption"]
        T7["Locked Target Table (Error 2652)"]
        T8["In-Flight Crash Resumption"]
        T9["Concurrent Orchestrator Contention"]
        T10["JIT Schema Drift (Replace / Append)"]
        T11["Fast-Path TPT Bypass (-C -M 2)"]
    end

    subgraph Orchestrator["Orchestration Engine"]
        Sentry["Concurrency & Crash Sentry"]
        Queue["FIFO Multi-Batch Sensor"]
        Guard["JIT Schema Guard Auto-DDL"]
        SAS["Loading Zone Isolation SAS"]
        TPT["ThreadPool & Fast-Path TPT"]
        State["Teradata BATCH_RUNS & TASKS"]
    end

    subgraph Assertions["Verified Outcomes"]
        A1["FIFO Chronological Consumption"]
        A2["Chunk 1 Header Stripping (SkipRows=0)"]
        A3["Auto-DDL NOPI Table Sync"]
        A4["Auto-Unlock via unlock_tpt_table.sh"]
        A5["Teradata BATCH_RUNS = SUCCESS + PROCESS_PID"]
        A6["Consolidated Batch Capsule in ARCHIVE_ZONE"]
        A7["Active Wait Loop Collision Avoidance"]
        A8["In-Flight Delta Task Completion"]
    end

    Inputs --> Orchestrator --> Assertions
```

---

## 2. Test Environment & Sandpit Setup

Tests should be executed in an isolated sandpit to avoid locks on active production staging tables:

```bash
export ORCH_TEST_DIR="/sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST"
mkdir -p "${ORCH_TEST_DIR}/LANDING_ZONE"
mkdir -p "${ORCH_TEST_DIR}/LOADING_ZONE"
mkdir -p "${ORCH_TEST_DIR}/ARCHIVE_ZONE"
mkdir -p "${ORCH_TEST_DIR}/LOGS"

cp /sg/local/sid/home/Dsidadm/DATA_LOADER/CONFIG/entity_mapping.csv "${ORCH_TEST_DIR}/"
```

### Test Configuration (`orch_test_config.json`)
```json
{
  "orchestration": {
    "landing_zone": "/sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/LANDING_ZONE",
    "loading_zone": "/sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/LOADING_ZONE",
    "archive_zone": "/sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/ARCHIVE_ZONE",
    "logs_dir": "/sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/LOGS",
    "entity_mapping_file": "/sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/entity_mapping.csv",
    "max_concurrent_jobs": 2,
    "manifest_pattern": "MANIFEST_*.MF",
    "verify_checksum": true,
    "allow_orphan_files": false,
    "auto_migrate_ddl": true,
    "character_set": "UNICODE",
    "concurrency": {
      "poll_interval_seconds": 2,
      "lock_timeout_seconds": 30
    },
    "post_ingestion_tasks": {
      "ORACLE_FUSION": [
        {
          "type": "shell",
          "command": "echo 'Orchestration test completed for batch $CURRENT_BATCH_ID'"
        }
      ]
    }
  },
  "tpt": {
    "wrapper_script": "../TPT_LOADER/load_csv_generic.sh",
    "unlock_script": "../TPT_LOADER/unlock_tpt_table.sh",
    "default_load_mode": "replace",
    "target_db": "${TDENV}SIDFINSTG",
    "work_db": "${TDENV}SIDFINWRK",
    "timeout_seconds": 600
  },
  "teradata": {
    "host": "${TDPID}",
    "user": "${TDENV}${SID_ADM_USER}",
    "technical_database": "${TDENV}SIDFINTCH",
    "logmech": "TD2",
    "connection_pool_size": 4
  }
}
```

---

## 3. Core Test Scenarios & Test Matrix

| Test ID | Scenario | Input Dataset | Expected Outcome | Exit Code |
|---|---|---|---|:---:|
| **TC-ORCH-01** | Nominal Multi-Entity CSV Load | Capsule with Manifest + 2 CSVs | ThreadPool loads entities in parallel; moves capsule to `ARCHIVE_ZONE`. | 0 |
| **TC-ORCH-02** | ZIP Archive Ingestion | Capsule with Manifest + 2 ZIPs | Decompresses in `LOADING_ZONE`, ingests CSV/PECSV, cleans temp files. | 0 |
| **TC-ORCH-03** | Multi-Chunk Header Stripping | Entity with 2 CSV chunks | Strips line 1 from Chunk 1; runs TPT with wildcard and `SkipRows=0`. | 0 |
| **TC-ORCH-04** | OPS-02 Boundary: Orphan File | Capsule + 1 extra unmanifested CSV | Detects unmanifested file; aborts without touching database. | 1 |
| **TC-ORCH-05** | Multi-Batch FIFO Queue Processing | 2 Queued Capsules in `LANDING_ZONE` | Consumes Batch 1001 first, then Batch 1002 in strict FIFO order. | 0 |
| **TC-ORCH-06** | MD5 Checksum / Size Tampering | Modified byte in data file | Checksum/size mismatch raised; aborts before TPT launch. | 1 |
| **TC-ORCH-07** | Reactive Table Lock Recovery | Table locked in FastLoad Paused (2652) | Calls `unlock_tpt_table.sh`, releases lock, retries and succeeds. | 0 |
| **TC-ORCH-08** | In-Flight Crash Resumption | Pre-existing files in `LOADING_ZONE` | Bypasses `LANDING_ZONE`, completes remaining tasks, archives batch. | 0 |
| **TC-ORCH-09** | Concurrency & Active Polling Wait | 2 concurrent Orchestrator executions | Detects `.orchestrator.pid`, enters active polling loop until release. | 0 |
| **TC-ORCH-10** | Hybrid Decoupling (`--skip-post-tasks`) | Batch with `--skip-post-tasks` | Ingests data, skips all post-tasks, logs bypass trace. | 0 |
| **TC-ORCH-11** | Stale PID Cleanup & Dead-PID Recovery | Pre-existing batch with dead PID in lockfile | Cleans dead PID lock, marks dead batch FAILED, resumes safely. | 0 |
| **TC-ORCH-12** | Source Post-Tasks Merge & Skip | `--post-tasks-file` + `--skip-post-tasks` | Merges external source tasks; cleanly skips all tasks when flag passed. | 0 |
| **TC-ORCH-13** | Teradata DB, Storage & PID Audit | Batch 9007 verified via Python auditor | Asserts `PROCESS_PID` is NOT NULL in `BATCH_RUNS`, rows loaded, storage clean. | 0 |
| **TC-ORCH-14** | JIT Schema Guard: Auto-DDL Replace | Extra column added in CSV | Drops and recreates NOPI staging table with `VARCHAR(4000)`, loads data. | 0 |
| **TC-ORCH-15** | JIT Schema Guard: Append Mode Guard | Extra column in CSV with `append` mode | Raises `SchemaSyncError`; blocks destructive DDL on existing table. | 1 |
| **TC-ORCH-16** | Fast-Path TPT Precheck Bypass | Standard load | Passes `-C -M 2` to `load_csv_generic.sh`; skips BTEQ catalog pre-check. | 0 |

> [!TIP]
> The automated Unix test suite in [`DATA_LOADER/TESTS/`](../../TESTS/README.md) validates all these scenarios end-to-end on Teradata Vantage with 100% automated pass verification (`./run_all_tests.sh`).

---

## 4. Detailed Procedures & Assertion Checklists

### Test Case: TC-ORCH-05 (Multi-Batch FIFO Queue Processing)
- **Objective**: Verify that when multiple batch capsules exist in `LANDING_ZONE/<SOURCE>/`, the orchestrator processes them strictly in chronological ascending order.
- **Procedure**:
  1. Stage two Batch Capsules in `LANDING_ZONE/ORACLE_FUSION/`:
     - `BATCH_20261016_090000_1002/` (newer extract)
     - `BATCH_20261016_080000_1001/` (older extract)
  2. Execute Orchestrator:
     ```bash
     python3 orchestrator.py --config orch_test_config.json --source ORACLE_FUSION
     ```
- **Assertions**:
  - Orchestrator log verifies FIFO order:
    ```text
    Discovered 2 queued batches in LANDING_ZONE: ['BATCH_20261016_080000_1001', 'BATCH_20261016_090000_1002']
    Processing batch 1 of 2: BATCH_20261016_080000_1001
    Processing batch 2 of 2: BATCH_20261016_090000_1002
    ```
  - Both capsules are ingested, post-tasks executed, and archived into `ARCHIVE_ZONE/ORACLE_FUSION/2026-10/`.
  - Teradata `BATCH_RUNS` contains 2 rows marked `SUCCESS`.

---

### Test Case: TC-ORCH-14 (JIT Schema Guard: Auto-DDL in Replace Mode)
- **Objective**: Verify that Schema Guard drops and recreates the target staging table when CSV headers drift under `auto_migrate_ddl = true` and `load_mode = 'replace'`.
- **Procedure**:
  1. Create target table `ERP_MSH_USER` with 3 columns: `(USER_ID, USERNAME, EMAIL)`.
  2. Stage a batch whose CSV contains an additional column: `USER_ID,USERNAME,EMAIL,DEPARTMENT`.
  3. Execute Orchestrator.
- **Assertions**:
  - Log asserts:
    ```text
    Schema drift detected on ERP_MSH_USER: missing column ['DEPARTMENT'].
    Auto-DDL enabled in replace mode: dropping and recreating table ERP_MSH_USER.
    ```
  - Teradata `DBC.ColumnsV` shows `ERP_MSH_USER` has all 4 CSV columns as `VARCHAR(4000)` plus `INFA_LAST_REPLICATED` and `INFA_BATCH_ID`.
  - Staging row count matches CSV data rows.

---

### Test Case: TC-ORCH-15 (JIT Schema Guard: Append Mode Guard)
- **Objective**: Verify that Schema Guard blocks destructive DDL when `load_mode = 'append'`.
- **Procedure**:
  1. Configure `default_load_mode = "append"` in `orch_test_config.json`.
  2. Stage a batch with schema drift.
  3. Execute Orchestrator.
- **Assertions**:
  - Exit code is `1`.
  - Log asserts: `SchemaSyncError: Cannot auto-migrate schema in append mode for table ERP_MSH_USER`.
  - Existing table in Teradata is **unmodified**.
  - Pipeline halts with **`FAIL_STOP`**.

---

### Test Case: TC-ORCH-16 (Fast-Path TPT Precheck Bypass)
- **Objective**: Verify that Orchestrator passes `-C -M 2` to `load_csv_generic.sh`.
- **Procedure**:
  1. Execute a standard batch load.
  2. Inspect Orchestrator execution logs.
- **Assertions**:
  - Worker invocation command contains `-C` and `-M 2`:
    ```text
    Executing: ../TPT_LOADER/load_csv_generic.sh -f ... -t ERP_MSH_USER -C -M 2 ...
    ```
  - TPT Loader logs affirm BTEQ check bypass:
    ```text
    Skipping BTEQ pre-check (-C flag provided with replace mode).
    ```

---

### Test Case: TC-ORCH-13 (Teradata DB, Storage & PID Audit via verify_database_and_storage.py)
- **Objective**: Perform automated deep verification of Teradata technical state (`BATCH_RUNS`, `BATCH_TASKS`, `PROCESS_PID` preservation), staging table data, and storage lifecycles.
- **Procedure**:
  ```bash
  # Standard audit (nominal batch with 3 rows)
  python3 ../../TESTS/verify_database_and_storage.py \
      --batch-id "9007" \
      --source "ORACLE_FUSION" \
      --target-table "ERP_MSH_USER" \
      --expected-count 3 \
      --output-file "audit_batch_9007.txt"

  # Multi-batch queue audit (superseded batch in replace mode)
  python3 ../../TESTS/verify_database_and_storage.py \
      --batch-id "9002" \
      --source "ORACLE_FUSION" \
      --target-table "ERP_MSH_USER" \
      --skip-staging-check \
      --output-file "audit_batch_9002.txt"
  ```
- **Assertions**:
  - `BATCH_RUNS`: `STATUS = 'SUCCESS'`, `END_TIME IS NOT NULL`, and `PROCESS_PID` is **NOT NULL** (audit preservation confirmed).
  - `BATCH_TASKS`: All tasks recorded with `STATUS = 'SUCCESS'` and `ERROR_MESSAGE IS NULL`.
  - Storage: `LANDING_ZONE` clean, `LOADING_ZONE` clean, and `ARCHIVE_ZONE` contains complete Batch Capsule with `batch_descriptor.json` and `RAW/RECEIPT_*.json`.
  - Staging Table: Row count matches expected count, `INFA_BATCH_ID` matches batch ID, and `INFA_LAST_REPLICATED` is populated (or skipped cleanly when `--skip-staging-check` is specified).

---

## 5. Automated Regression Test Script

```bash
#!/bin/bash
set -e

ORCH_DIR="/sg/local/sid/home/Dsidadm/DATA_LOADER/PROCESSING_TOOLS/DATA_INGESTION_ORCHESTRATOR"
CONFIG="/sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/orch_test_config.json"

echo "=== Starting Data Ingestion Orchestrator Regression Test Suite ==="

# Test 1: Empty Landing Zone (RC 0 expected)
python3 "${ORCH_DIR}/orchestrator.py" --config "${CONFIG}"
echo "[PASS] Empty landing zone handled gracefully."

# Test 2: Standard Multi-Batch Queue Run
python3 "${ORCH_DIR}/orchestrator.py" --config "${CONFIG}" --source ORACLE_FUSION
echo "[PASS] Queued batch capsules processed and archived."

echo "=== All Orchestrator Tests Passed Successfully ==="
```
