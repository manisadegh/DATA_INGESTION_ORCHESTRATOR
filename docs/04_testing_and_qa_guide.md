# DATA_INGESTION_ORCHESTRATOR - Testing, Validation & QA Guide

## 1. Testing Scope & Objectives

The **DATA_INGESTION_ORCHESTRATOR** test framework verifies the orchestrator's contract enforcement, multi-threaded worker dispatch, archive decompression, header normalization, and database state transitions.

```mermaid
flowchart LR
    subgraph TestVectors["Input Scenarios"]
        T1["Single-Entity CSV"]
        T2["Multi-Entity Concurrency"]
        T3["Compressed ZIP Archive"]
        T4["Multi-Chunk CSV (Solution A)"]
        T5["OPS-01 Orphan File"]
        T6["Multiple Manifests"]
        T7["Corrupted MD5 Checksum"]
        T8["Table Locked (Error 2652)"]
    end

    subgraph OrchestratorEngine["Orchestrator Engine Under Test"]
        Sensor["Sensor & Gate"]
        Decomp["ZIP / GZ Decompressor"]
        Pool["ThreadPool (Workers = N)"]
        Recovery["2652 Lock Recovery"]
    end

    subgraph OutputAssertions["Verified Assertions"]
        A1["OPS-01 Rejection (RC 1)"]
        A2["Multi-Manifest Rejection (RC 1)"]
        A3["Checksum Mismatch Abort (RC 1)"]
        A4["Solution A Header Stripped (Chunk 1)"]
        A5["Teradata BATCH_RUNS = SUCCESS"]
        A6["Files Relocated to ARCHIVE_ZONE"]
    end

    TestVectors --> OrchestratorEngine --> OutputAssertions
```

---

## 2. Test Environment Setup & Prerequisites

Tests should be executed in an isolated sandpit to avoid locks on active production staging tables:

```bash
# Setup sandbox
export TEST_DIR="/sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST"
mkdir -p "${TEST_DIR}/LANDING_ZONE"
mkdir -p "${TEST_DIR}/LOADING_ZONE"
mkdir -p "${TEST_DIR}/ARCHIVE_ZONE"
mkdir -p "${TEST_DIR}/LOGS"
mkdir -p "${TEST_DIR}/CONFIG"

# Copy entity mapping
cp /sg/local/sid/home/Dsidadm/DATA_LOADER/CONFIG/entity_mapping.csv "${TEST_DIR}/CONFIG/"
```

### Test Configuration (`orch_test_config.json`)
```json
{
  "orchestration": {
    "landing_zone": "/sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/LANDING_ZONE",
    "loading_zone": "/sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/LOADING_ZONE",
    "archive_zone": "/sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/ARCHIVE_ZONE",
    "logs_dir": "/sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/LOGS",
    "entity_mapping_file": "/sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/CONFIG/entity_mapping.csv",
    "max_concurrent_entities": 2,
    "max_retries_per_entity": 1,
    "verify_checksum": true,
    "allow_orphan_files": false
  },
  "tpt": {
    "wrapper_script": "../TPT_LOADER/load_csv_generic.sh",
    "unlock_script": "../TPT_LOADER/unlock_tpt_table.sh",
    "default_load_mode": "replace",
    "target_database": "${TDENV}SIDFINSTG",
    "work_database": "${TDENV}SIDFINWRK",
    "logon_user": "${TDENV}${SID_ADM_USER}"
  },
  "teradata": {
    "host": "${TDPID}",
    "user": "${TDENV}${SID_ADM_USER}",
    "technical_database": "${TDENV}SIDFINTCH",
    "logmech": "TD2"
  },
  "post_tasks": [
    {
      "name": "Audit Task",
      "type": "shell",
      "command": "echo 'Orchestration test completed for batch {batch_id}'"
    }
  ]
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
| **TC-ORCH-11** | Stale PID Cleanup & Crash Recovery | Pre-existing batch with dead PID in lockfile | Cleans dead PID lock, resumes in-flight tasks safely. | 0 |
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
  # Stage valid batch into LANDING_ZONE
  cp /sg/local/sid/home/Dsidadm/DATA_LOADER/DATA/TEST_BATCH_1/* /sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/LANDING_ZONE/
  
  # Inject stray unmanifested orphan file
  echo "orphan_data" > /sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/LANDING_ZONE/unmanifested_stray_file.csv
  
  # Execute Orchestrator
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

## 5. Automated Regression Test Script

```bash
#!/bin/bash
set -e

ORCH_DIR="/sg/local/sid/home/Dsidadm/DATA_LOADER/PROCESSING_TOOLS/DATA_INGESTION_ORCHESTRATOR"
CONFIG="/sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/orch_test_config.json"

echo "=== Starting Data Ingestion Orchestrator Regression Test Suite ==="

# Test 1: Idle Run (Empty Landing Zone -> RC 0)
python3 "${ORCH_DIR}/orchestrator.py" --config "${CONFIG}"
echo "[PASS] Idle run completed with RC 0."

# Test 2: Standard Batch Run
cp /sg/local/sid/home/Dsidadm/DATA_LOADER/DATA/TEST_BATCH_12/* \
   /sg/local/sid/home/Dsidadm/DATA_LOADER/ORCH_TEST/LANDING_ZONE/
python3 "${ORCH_DIR}/orchestrator.py" --config "${CONFIG}"
echo "[PASS] Batch 12 ingested and archived."

echo "=== All Orchestrator Tests Completed Successfully ==="
```
