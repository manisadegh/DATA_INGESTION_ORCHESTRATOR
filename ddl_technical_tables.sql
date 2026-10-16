-- ==============================================================================
-- DDL for Orchestrator Technical Tables (State Machine) - Version 2.1
-- Standardized schema for Single-Batch Architecture & Multi-Source Partitions
-- Non-Unique Primary Index on (BATCH_ID, SOURCE) for optimal AMP distribution
-- ==============================================================================

-- 1. BATCH_RUNS Table
-- Tracks the overall status of a complete batch execution per source
CREATE MULTISET TABLE <TECH_DB>.BATCH_RUNS (
    BATCH_ID            VARCHAR(100) NOT NULL,
    SOURCE              VARCHAR(50)  NOT NULL,
    EXTRACT_TIMESTAMP   TIMESTAMP(0) NOT NULL,
    RUN_DATE            DATE FORMAT 'YYYY-MM-DD' NOT NULL,
    START_TIME          TIMESTAMP(0) NOT NULL,
    END_TIME            TIMESTAMP(0),
    STATUS              VARCHAR(30)  NOT NULL,
    PROCESS_PID         INTEGER
)
PRIMARY INDEX (BATCH_ID, SOURCE);

-- Migration statement for existing environments (PID column):
-- ALTER TABLE <TECH_DB>.BATCH_RUNS ADD PROCESS_PID INTEGER;

-- 2. BATCH_TASKS Table
-- Tracks the granular status of each individual file/entity within a batch per source
CREATE MULTISET TABLE <TECH_DB>.BATCH_TASKS (
    BATCH_ID            VARCHAR(100) NOT NULL,
    SOURCE              VARCHAR(50)  NOT NULL,
    EXTRACT_TIMESTAMP   TIMESTAMP(0) NOT NULL,
    ENTITY_NAME         VARCHAR(100) NOT NULL,
    FILE_NAME           VARCHAR(255) NOT NULL,
    STATUS              VARCHAR(30)  NOT NULL,
    START_TIME          TIMESTAMP(0),
    END_TIME            TIMESTAMP(0),
    RETRY_COUNT         INTEGER DEFAULT 0,
    ERROR_MESSAGE       VARCHAR(2000)
)
PRIMARY INDEX (BATCH_ID, SOURCE);
