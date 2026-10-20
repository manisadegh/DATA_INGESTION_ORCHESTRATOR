# Data Ingestion Orchestrator

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Platform: Linux / POSIX](https://img.shields.io/badge/platform-Linux%20%7C%20POSIX-lightgrey.svg)](https://www.kernel.org/)
[![Database: Teradata Vantage](https://img.shields.io/badge/database-Teradata%20Vantage%2016.20+-orange.svg)](https://www.teradata.com/)
[![License: Proprietary](https://img.shields.io/badge/license-Proprietary-red.svg)](#)

An enterprise batch-load orchestrator engineered to bridge standardized flat-file manifests with the high-throughput **Generic Teradata Parallel Transporter (TPT) Loader**.

The orchestrator enforces a strict **Single-Batch Architecture** with atomic directory transitions, process-level concurrency locking, pre-flight Teradata state sentries, automated in-flight crash recovery, parallel entity-level worker threads, automated table unlock & retry capabilities (Error 2652), cryptographic checksum validation, and unmanifested file boundary protection.

---

## 📚 Complete Documentation Suite

Comprehensive operational, technical, and testing manuals are available in the [`docs/`](docs/) directory:

- 📖 **[User How-To & Configuration Guide](docs/01_user_how_to_guide.md)**: Command-line syntax, complete `config.json` reference, in-flight recovery runbooks, and Teradata catalog monitoring.
- 📐 **[Functional Specification](docs/02_functional_specification.md)**: Single-batch invariant (OPS-01), concurrency sentries, multi-chunk normalization (Solution A), and state machine lifecycle.
- ⚙️ **[Technical Manual & Function Inventory](docs/03_technical_manual_and_inventory.md)**: Module breakdowns (`sensor`, `db_state`, `tpt_worker`, `post_tasks`), function signatures, and state machine DDL.
- 🧪 **[Testing, Validation & QA Guide](docs/04_testing_and_qa_guide.md)**: Test matrices, orphan file injection, Error 2652 reactive lock recovery tests, concurrency tests, and automated regression harnesses.

---

## Table of Contents

- [Key Architectural Capabilities](#key-architectural-capabilities)
- [Architecture & End-to-End Workflow](#architecture--end-to-end-workflow)
- [Directory Layout & SAS Lifecycle](#directory-layout--sas-lifecycle)
- [Core Invariants & Policies](#core-invariants--policies)
  - [1. Single-Flight Concurrency & Active Polling](#1-single-flight-concurrency--active-polling)
  - [2. Pre-Flight Teradata Sentry & Dead-PID Auto-Healing](#2-pre-flight-teradata-sentry--dead-pid-auto-healing)
  - [3. Automated In-Flight Crash Resumption](#3-automated-in-flight-crash-resumption)
  - [4. Single-Batch Gate (Invariant OPS-01)](#4-single-batch-gate-invariant-ops-01)
  - [5. Zero Unmanifested Orphans (OPS-02)](#5-zero-unmanifested-orphans-ops-02)
  - [6. Multi-Chunk Header Normalization (Solution A)](#6-multi-chunk-header-normalization-solution-a)
  - [7. Reactive Lock Resolution (Error 2652)](#7-reactive-lock-resolution-error-2652)
  - [8. State Machine & PID Audit Trail](#8-state-machine--pid-audit-trail)
- [Configuration Reference (`config.json`)](#configuration-reference-configjson)
- [Entity Mapping Dictionary (`entity_mapping.csv`)](#entity-mapping-dictionary-entity_mappingcsv)
- [Usage & Execution](#usage--execution)
- [Troubleshooting & Exit Codes](#troubleshooting--exit-codes)

---

## Key Architectural Capabilities

- **Strict Single-Batch Gating**: Processes exactly one batch per execution cycle, preventing race conditions, lock collisions, and multi-tenant batch contention.
- **Single-Flight Process Concurrency**: Manages `.orchestrator.pid` in `LOADING_ZONE/<SOURCE>` and enters an active polling wait loop on live PIDs, avoiding concurrent job collision.
- **Pre-Flight Teradata Sentry & Auto-Healing**: Queries `${TDENV}SIDFINTCH.BATCH_RUNS` for running batches before execution. If an active PID is alive, it waits; if dead, it automatically reclassifies the orphaned batch as `FAILED`.
- **Automated In-Flight Crash Resumption**: Interrupted runs inside `LOADING_ZONE` are automatically resumed upon re-invocation, bypassing redundant checksum verification and rerunning only pending/failed tasks.
- **Isolated Processing SAS**: Atomically shifts data payloads from `LANDING_ZONE` to private `LOADING_ZONE`, ensuring that failed ingestions never corrupt the ingress gateway.
- **In-Place Multi-Chunk Normalization (Solution A)**: For multi-chunk extracts, strips line 1 from Chunk 1 in-place and executes TPT using wildcard pattern with `SkipRows = 0`. Single-chunk extracts retain headers with `SkipRows = 1`.
- **Concurrent Worker Pool**: Dispatches independent entity load jobs in parallel via `ThreadPoolExecutor` while honoring configurable concurrency limits (`max_concurrent_jobs`).
- **Enterprise Auto-Healing**: Intercepts Teradata Error 2652 (`Table is locked for FastLoad / MultiLoad`), executes automated table unlocking via `unlock_tpt_table.sh`, and retries without manual intervention.
- **Permanent Process PID Audit**: Stores OS `PROCESS_PID` in `BATCH_RUNS` and permanently retains it upon completion (`SUCCESS` or `FAILED`).
- **Chained Post-Tasks Pipeline**: Executes source-specific (`--post-tasks-file`) and global tasks (e.g. dbt models) only after 100% entity load success.
- **Hybrid Decoupling Support**: Accepts `--skip-post-tasks` to bypass analytical models during bulk re-inits or operational maintenance.

---

## Architecture & End-to-End Workflow

```mermaid
flowchart TD
    subgraph PREFLIGHT["1. Concurrency & Pre-Flight Sentry"]
        DB_CHECK{Active Batch in BATCH_RUNS?}
        DB_CHECK -- Yes (PID Alive) --> WAIT_DB[Active Polling Wait Loop]
        DB_CHECK -- Yes (PID Dead) --> HEAL[Auto-Heal: Set STATUS=FAILED] --> PID_CHECK
        DB_CHECK -- No --> PID_CHECK{Active .orchestrator.pid?}
        PID_CHECK -- Yes --> WAIT_PID[Active Polling Wait Loop]
        PID_CHECK -- No --> INFLIGHT{In-Flight *.MF in LOADING_ZONE?}
    end

    subgraph INGRESS["2. Ingress & Single-Batch Gating"]
        INFLIGHT -- No --> SCAN[Scan LANDING_ZONE/<SOURCE>/]
        SCAN --> GATE{OPS-01 Check: Exactly 1 Manifest & 0 Orphans?}
        GATE -- No --> ABORT[Fatal Abort: RC 1]
        GATE -- Yes --> MD5[Verify MD5 Checksums]
        MD5 --> ATOMIC_MOVE[Atomic Move: LANDING_ZONE -> LOADING_ZONE]
    end

    subgraph ISOLATION["3. SAS Processing & State Init"]
        INFLIGHT -- Yes --> INFLIGHT_RESUME[Resume In-Flight: Bypass Checksum]
        ATOMIC_MOVE --> DECOMP[Decompress Archives .zip / .gz]
        INFLIGHT_RESUME --> DECOMP
        DECOMP --> CHUNKS{Multi-Chunk Delimited?}
        CHUNKS -- Yes --> STRIP[Strip Chunk 1 Header: SkipRows=0]
        CHUNKS -- No --> SINGLE[Retain Header: SkipRows=1]
        STRIP --> DB_INIT[Register BATCH_RUNS & BATCH_TASKS]
        SINGLE --> DB_INIT
    end

    subgraph EXECUTION["4. Parallel Workers & Auto-Healing"]
        DB_INIT --> POOL[ThreadPoolExecutor: Max Concurrent Jobs]
        POOL --> TPT[load_csv_generic.sh]
        TPT -->|Lock Error 2652| UNLOCK[unlock_tpt_table.sh] --> TPT
        TPT -->|Success| TASK_OK[Update BATCH_TASKS = SUCCESS]
    end

    subgraph COMPLETION["5. Barrier & Archiving"]
        TASK_OK --> BARRIER{100% Entities Succeeded?}
        BARRIER -- Yes --> POST[Run Post-Tasks: Source + Global]
        POST --> ARCHIVE[Move Batch to ARCHIVE_ZONE]
        ARCHIVE --> COMMIT[Set BATCH_RUNS = SUCCESS + Preserved PID]
        BARRIER -- No --> FAIL[Mark BATCH_RUNS = FAILED]
    end
```

---

## Configuration Reference (`config.json`)

```json
{
  "teradata": {
    "host": "${TDPID}",
    "user": "${TDENV}${SID_ADM_USER}",
    "logmech": "TD2",
    "technical_database": "${TDENV}SIDFINTCH"
  },
  "tpt": {
    "wrapper_script": "../TPT_LOADER/load_csv_generic.sh",
    "unlock_script": "../TPT_LOADER/unlock_tpt_table.sh",
    "timeout_seconds": 3600,
    "target_db": "${TDENV}SIDFINSTG",
    "work_db": "${TDENV}SIDFINWRK",
    "default_load_mode": "replace",
    "work_dir": "../../LOGS/tpt_work"
  },
  "orchestration": {
    "landing_zone": "../../DATA/LANDING_ZONE",
    "loading_zone": "../../DATA/LOADING_ZONE",
    "archive_zone": "../../DATA/ARCHIVE_ZONE",
    "logs_dir": "../../LOGS",
    "max_concurrent_jobs": 4,
    "manifest_pattern": "MANIFEST_*.MF",
    "manifest_regex": "^MANIFEST_([A-Za-z0-9_]+)_(\\d{8}_\\d{6})_([A-Za-z0-9_-]+)\\.MF$",
    "entity_mapping_file": "../../CONFIG/entity_mapping.csv",
    "verify_checksum": true,
    "allow_orphan_files": false,
    "concurrency": {
      "poll_interval_seconds": 5,
      "lock_timeout_seconds": 7200
    },
    "post_ingestion_tasks": [
      {
        "type": "shell",
        "command": "echo 'Ingestion completed for batch $CURRENT_BATCH_ID from source $CURRENT_SOURCE'"
      }
    ]
  }
}
```

---

## Entity Mapping Dictionary (`entity_mapping.csv`)

```csv
SOURCE,TARGET_TABLE,FILE_SIGNATURE,EXTENSION,INNER_EXTENSION
ORACLE_FUSION,ERP_MSH_INVOICE_HEADER,invoiceheaderextractpvo,".csv,.zip,.gz,.csv.gz",.csv
ORACLE_FUSION,ERP_MSH_INVOICE_HEADER_AK,invoiceheaderextractpvo,".pecsv,.zip,.gz,.pecsv.gz",.pecsv
ORACLE_FUSION,ERP_MSH_USER,userpvo,".csv,.zip,.gz,.csv.gz",.csv
ORACLE_FUSION,ERP_MSH_USER_AK,userpvo,".pecsv,.zip,.gz,.pecsv.gz",.pecsv
```

---

## Usage & Execution

### Standard Batch Ingestion
```bash
python3 orchestrator.py --config config.json
```

### Ingestion with Source Override
```bash
python3 orchestrator.py --config config.json --source ORACLE_FUSION
```

### Ingestion with Post-Tasks Skipped
```bash
python3 orchestrator.py --config config.json --skip-post-tasks
```

### Ingestion with Source Post-Tasks File
```bash
python3 orchestrator.py --config config.json --source ORACLE_FUSION --post-tasks-file /path/to/POST_TASKS.json
```

If `LANDING_ZONE` contains no manifests and no files, the orchestrator exits cleanly with Return Code `0` (`IDLE`).

---

## Troubleshooting & Exit Codes

| Exit Code | Meaning | Action |
| :---: | :--- | :--- |
| `0` | Batch loaded, verified, transformed, and archived with 100% success, or `LANDING_ZONE` is empty (`IDLE`). | Normal operation. |
| `1` | Boundary violation (OPS-01), checksum failure, lock timeout, or unrecoverable worker failure. | Inspect batch log in `LOGS/<SOURCE>/<YYYY-MM>/BATCH_<ID>_<TS>/` and Teradata `BATCH_TASKS` table. |

For detailed failure scenarios, error runbooks, and recovery instructions, consult **[docs/01_user_how_to_guide.md](docs/01_user_how_to_guide.md)**.
