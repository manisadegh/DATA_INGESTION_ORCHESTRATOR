# DATA_INGESTION_ORCHESTRATOR - Testing, Validation & QA Guide

## 1. Testing Scope & Objectives

The testing framework for the **DATA_INGESTION_ORCHESTRATOR** verifies the single-batch contract, concurrency sentries, crash resilience, TPT execution, and Teradata technical metadata integrity.

### Validation Boundaries
```mermaid
flowchart LR
    subgraph Inputs["Test Vectors"]
        T1["Nominal Multi-Entity Batch"]
        T2["ZIP Archive Batch (.zip / .gz)"]
        T3["Multi-Chunk Extract (Solution A)"]
        T4["Orphan Stray File (OPS-01)"]
        T5["Multiple Manifests (OPS-01)"]
        T6["Checksum Corruption"]
        T7["Locked Target Table (Error 2652)"]
        T8["In-Flight Crash Resumption"]
        T9["Concurrent Orchestrator Contention"]
        T10["Stale Dead-PID Lockfile"]
    end

    subgraph Orchestrator["Orchestration Engine"]
        Sentry["Concurrency & Crash Sentry"]
        Gate["Single-Batch Sensor"]
        SAS["Loading Zone Isolation SAS"]
        TPT["ThreadPool & TPT Worker"]
        State["Teradata BATCH_RUNS & TASKS"]
    end

    subgraph Assertions["Verified Outcomes"]
        A1["Atomic Move: LANDING -> LOADING"]
        A2["Chunk 1 Header Stripping (SkipRows=0)"]
        A3["Fatal Abort (RC 1) on OPS-01 / Checksum"]
        A4["Auto-Unlock via unlock_tpt_table.sh"]
        A5["Teradata BATCH_RUNS = SUCCESS + PROCESS_PID"]
        A6["Final Capsule in ARCHIVE_ZONE"]
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
    "concurrency": {
      "poll_interval_seconds": 2,
      "lock_timeout_seconds": 30
    },
    "post_ingestion_tasks": [
      {
        "type": "shell",
        "command": "echo 'Orchestration test completed for batch $CURRENT_BATCH_ID'"
      }
    ]
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
    "logmech": "TD2"
  }
}
```

---

## 3. Core Test Scenarios & Test Matrix

| Test ID | Scenario | Input Dataset | Expected Outcome | Exit Code |
|---|---|---|---|:---:|
| **TC-ORCH-01** | Nominal Multi-Entity CSV Load | Manifest + 2 CSVs | ThreadPool loads both entities in parallel; moves to `ARCHIVE_ZONE`. | 0 |
| **TC-ORCH-02** | ZIP Archive Ingestion | Manifest + 2 ZIPs | Decompresses in `LOADING_ZONE`, ingests CSV/PECSV, cleans temp files. | 0 |
| **TC-ORCH-03** | Multi-Chunk Header Stripping | Entity with 2 CSV chunks | Strips line 1 from Chunk 1; runs TPT with wildcard and `SkipRows=0`. | 0 |
| **TC-ORCH-04** | OPS-01 Boundary: Orphan File | 1 Manifest + 1 extra unmanifested CSV | Detects unmanifested file; aborts without touching database. | 1 |
| **TC-ORCH-05** | OPS-01 Boundary: Multiple Manifests | 2 Manifests in `LANDING_ZONE` | Detects multi-batch violation; aborts immediately. | 1 |
| **TC-ORCH-06** | MD5 Checksum Tampering | Modified byte in data file | Checksum mismatch raised; aborts before TPT launch. | 1 |
| **TC-ORCH-07** | Reactive Table Lock Recovery | Table locked in FastLoad Paused (2652) | Calls `unlock_tpt_table.sh`, releases lock, retries and succeeds. | 0 |
| **TC-ORCH-08** | In-Flight Crash Resumption | Pre-existing files in `LOADING_ZONE` | Bypasses `LANDING_ZONE`, completes remaining tasks, archives batch. | 0 |
| **TC-ORCH-09** | Concurrency & Active Polling Wait | 2 concurrent Orchestrator executions | Detects `.orchestrator.pid`, enters active polling loop until release. | 0 |
| **TC-ORCH-10** | Hybrid Decoupling (`--skip-post-tasks`) | Batch with `--skip-post-tasks` | Ingests data, skips all post-tasks, logs bypass trace. | 0 |
| **TC-ORCH-11** | Stale PID Cleanup & Dead-PID Recovery | Pre-existing batch with dead PID in lockfile | Cleans dead PID lock, marks dead batch FAILED, resumes safely. | 0 |
| **TC-ORCH-12** | Source Post-Tasks Merge & Skip | `--post-tasks-file` + `--skip-post-tasks` | Merges external source tasks; cleanly skips all tasks when flag passed. | 0 |
| **TC-ORCH-13** | Teradata DB, Storage & PID Audit | Batch 9007 verified via Python auditor | Asserts `PROCESS_PID` is NOT NULL in `BATCH_RUNS`, rows loaded, storage clean. | 0 |

> [!TIP]
> The automated Unix test suite in [`DATA_LOADER/TESTS/`](../../TESTS/README.md) validates all these scenarios end-to-end on Teradata Vantage with 100% automated pass verification (`./run_all_tests.sh`).

---

## 4. Detailed Procedures & Assertion Checklists

### Test Case: TC-ORCH-04 (OPS-01 Orphan File Gating)
- **Objective**: Verify that the orchestrator rejects batches if any unmanifested data file is present in `LANDING_ZONE`.
- **Procedure**:
  ```bash
  cp /sg/local/sid/home/Dsidadm/DATA_LOADER/DATA/TEST_BATCH_1/* /sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/LANDING_ZONE/
  echo "orphan_data" > /sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/LANDING_ZONE/unmanifested_stray_file.csv
  python3 orchestrator.py --config /sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/orch_test_config.json
  ```
- **Assertions**:
  - Exit code is `1`.
  - Log asserts: `ValueError: Boundary violation (OPS-01): Unmanifested orphan file(s) found in landing zone`.
  - Target Teradata staging tables were **never modified**.
  - `LOADING_ZONE` remains empty.

---

### Test Case: TC-ORCH-03 (Solution A Multi-Chunk Header Normalization)
- **Objective**: Verify that when an entity has multiple files, Chunk 1 has its header stripped in-place and loads with `SkipRows = 0`.
- **Procedure**:
  1. Stage two files for `ERP_MSH_USER`: `file_userpvo-batch1001-part1.csv` and `file_userpvo-batch1001-part2.csv`.
  2. Include header in `part1.csv`, but no header in `part2.csv`.
  3. Execute Orchestrator.
- **Assertions**:
  - Log asserts: `Multi-chunk detected (2 files). First chunk header stripped; loading with SkipRows=0.`
  - Marker file `file_userpvo-batch1001-part1.csv.header_stripped` was created during execution.
  - TPT executes with `-f "*userpvo*.csv" -S 0`.
  - Staging row count equals total data rows of part1 + part2 (header was not inserted as data).

---

### Test Case: TC-ORCH-07 (Error 2652 Lock Recovery)
- **Objective**: Validate automated unlock and retry when a table is left locked by an interrupted FastLoad.
- **Procedure**:
  1. Deliberately lock the target table by issuing a partial FastLoad session.
  2. Execute Orchestrator against that table.
- **Assertions**:
  - Initial worker attempt logs: `TPT encountered Error 2652 (table locked/paused)`.
  - Orchestrator invokes: `Executing reactive table unlock via unlock_tpt_table.sh`.
  - Retry attempt succeeds with `rc=0`.
  - Database table `BATCH_TASKS` records `RETRY_COUNT = 1` and `STATUS = 'SUCCESS'`.

---

### Test Case: TC-ORCH-08 (In-Flight Crash Resumption)
- **Objective**: Validate that a batch interrupted inside `LOADING_ZONE` resumes safely without re-verifying checksums.
- **Procedure**:
  1. Place an uncompleted standardized manifest and data files directly into `LOADING_ZONE/`.
  2. Execute Orchestrator.
- **Assertions**:
  - Log asserts: `In-flight batch detected in LOADING_ZONE: ... Resuming execution.`
  - Checksum validation is bypassed.
  - Remaining entity tasks are loaded via TPT into Teradata.
  - Completed batch is archived to `ARCHIVE_ZONE/`.

---

### Test Case: TC-ORCH-09 (Process Concurrency & Active Polling Wait)
- **Objective**: Validate that two concurrent Orchestrator executions for the same source do not collide.
- **Procedure**:
  ```bash
  python3 orchestrator.py --config orch_test_config.json &
  PID_1=$!
  sleep 1
  python3 orchestrator.py --config orch_test_config.json
  wait $PID_1
  ```
- **Assertions**:
  - Instance 2 detects `.orchestrator.pid` with PID of Instance 1.
  - Instance 2 logs: `Another Orchestrator run is actively in progress (PID ...). Entering active waiting loop...`
  - Instance 2 polls until Instance 1 finishes, avoiding Teradata session collisions.

---

### Test Case: TC-ORCH-11 (Stale PID Cleanup & Dead-PID Recovery)
- **Objective**: Validate that a dead PID in `.orchestrator.pid` or `BATCH_RUNS` is auto-healed.
- **Procedure**:
  1. Write a dummy non-existent PID (e.g. `999999`) to `LOADING_ZONE/.orchestrator.pid`.
  2. Insert a row in `BATCH_RUNS` with `STATUS = 'RUNNING'` and `PROCESS_PID = 999999`.
  3. Execute Orchestrator.
- **Assertions**:
  - Orchestrator logs: `Batch ... was marked RUNNING in BATCH_RUNS with dead PID 999999. Reclassifying as FAILED.`
  - Dead PID file `.orchestrator.pid` is pruned.
  - Orchestrator writes its own PID and processes the incoming batch cleanly.

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

# Test 2: Standard Single-Batch Load
cp /sg/local/sid/home/Dsidadm/DATA_LOADER/DATA/TEST_BATCH_1/* \
   /sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/LANDING_ZONE/
python3 "${ORCH_DIR}/orchestrator.py" --config "${CONFIG}"
echo "[PASS] Single batch processed and archived."

echo "=== All Orchestrator Tests Passed Successfully ==="
```
