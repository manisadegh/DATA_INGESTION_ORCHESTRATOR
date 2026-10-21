"""
lib/schema_guard.py - Just-In-Time Schema Guard & Automated NOPI DDL Synchronizer

Executed inside DATA_INGESTION_ORCHESTRATOR immediately prior to entity loading:
compares extracted CSV/PECSV header columns (from `batch_descriptor.json` or
in-memory stream inspection) against Teradata Vantage `DBC.ColumnsV`, and
automatically generates and executes NOPI VARCHAR(4000) table creation/recreation
upon schema drift when `auto_migrate_ddl=True` and `load_mode='replace'`.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Dict, List, Optional, Tuple

_COMMON_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "COMMON"))
if _COMMON_DIR not in sys.path:
    sys.path.insert(0, _COMMON_DIR)

from dataloader_core.db import validate_sql_identifier
from dataloader_core.exceptions import SchemaSyncError
from dataloader_core.mapping import EntityMapper, inspect_file_inner_extension
from dataloader_core.schema_reader import (
    clean_column_name,
    extract_csv_columns,
    select_header_chunk_file,
    validate_column_type,
)

logger = logging.getLogger("DATA_INGESTION_ORCHESTRATOR")

TECHNICAL_AUDIT_COLUMNS = {"INFA_BATCH_ID", "INFA_LAST_REPLICATED"}


def load_entity_mappings(mapping_file: str, filter_source: str = "ORACLE_FUSION") -> List[Dict]:
    """
    Loads entity mapping rules for the specified source from entity_mapping.csv.
    Delegates to `dataloader_core.mapping.EntityMapper`.
    """
    mapper = EntityMapper(mapping_file=mapping_file, default_source=filter_source)
    return [rule.to_dict() for rule in mapper.load_rules(source=filter_source)]


def match_target_table_for_file(
    filename: str,
    mapping_rules: List[Dict],
    file_dir: Optional[str] = None,
) -> Optional[str]:
    """
    Matches a given data filename against loaded mapping rules to resolve the target staging table.
    Delegates to `dataloader_core.mapping.EntityMapper`.
    """
    mapper = EntityMapper()
    matched = mapper.match_file(filename, file_dir=file_dir, rules=mapping_rules)
    return matched.target_table if matched else None


def get_teradata_table_columns(connection, target_db: str, target_table: str) -> Optional[List[str]]:
    """
    Queries DBC.ColumnsV for existing columns in the target table.
    Returns an ordered list of uppercase column names, excluding technical audit columns.
    Returns None if the table does not exist.
    """
    query = """
    SELECT TRIM(ColumnName)
    FROM DBC.ColumnsV
    WHERE DatabaseName = ?
      AND TableName = ?
    ORDER BY ColumnId
    """
    cursor = connection.cursor()
    try:
        cursor.execute(query, [target_db, target_table])
        rows = cursor.fetchall()
        if not rows:
            return None

        td_cols = []
        for r in rows:
            c_name = str(r[0]).strip().upper()
            if c_name not in TECHNICAL_AUDIT_COLUMNS:
                td_cols.append(c_name)

        return td_cols
    except Exception as e:
        logger.warning(f"Error checking DBC.ColumnsV for {target_db}.{target_table}: {e}")
        return None
    finally:
        cursor.close()


def generate_staging_ddl(
    target_db: str,
    target_table: str,
    columns: List[str],
    default_type: str = "VARCHAR(4000)",
    character_set: str = "UNICODE",
) -> str:
    """
    Generates Teradata DDL for a NOPI staging table with all payload columns
    typed as VARCHAR(4000) and standard audit columns appended.
    """
    safe_db = validate_sql_identifier(target_db)
    safe_table = validate_sql_identifier(target_table)
    safe_type = validate_column_type(default_type)
    safe_charset = validate_sql_identifier(character_set).upper()
    safe_cols = [validate_sql_identifier(c) for c in columns]

    cols_sql = [f"    {c} {safe_type} CHARACTER SET {safe_charset} NOT CASESPECIFIC" for c in safe_cols]
    cols_sql.append("    INFA_LAST_REPLICATED TIMESTAMP(0)")
    cols_sql.append(f"    INFA_BATCH_ID VARCHAR(100) CHARACTER SET {safe_charset} NOT CASESPECIFIC")

    cols_joined = ",\n".join(cols_sql)
    ddl = f"""CREATE MULTISET TABLE {safe_db}.{safe_table} ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
{cols_joined}
     )
NO PRIMARY INDEX;"""

    return ddl


def recreate_staging_table(
    connection,
    target_db: str,
    target_table: str,
    columns: List[str],
    default_type: str = "VARCHAR(4000)",
    character_set: str = "UNICODE",
):
    """
    Executes DROP TABLE (if table exists) followed by CREATE MULTISET TABLE ... NO PRIMARY INDEX.
    """
    safe_db = validate_sql_identifier(target_db)
    safe_table = validate_sql_identifier(target_table)
    cursor = connection.cursor()
    ddl = generate_staging_ddl(safe_db, safe_table, columns, default_type, character_set)

    # 1. Drop existing table
    drop_sql = f"DROP TABLE {safe_db}.{safe_table};"
    try:
        cursor.execute(drop_sql)
        logger.info(f"[{safe_table}] Dropped existing table {safe_db}.{safe_table}")
    except Exception as e:
        err_msg = str(e)
        if "3807" in err_msg or "does not exist" in err_msg.lower():
            pass
        else:
            logger.warning(f"[{safe_table}] Notice during DROP TABLE: {e}")

    # 2. Create new table
    try:
        cursor.execute(ddl)
        logger.info(f"[{safe_table}] Successfully created NOPI staging table with {len(columns)} payload columns.")
    finally:
        cursor.close()


def sync_entity_schema_if_drifted(
    connection=None,
    target_db: Optional[str] = None,
    target_table: Optional[str] = None,
    sample_file_path: Optional[str] = None,
    default_type: str = "VARCHAR(4000)",
    character_set: str = "UNICODE",
    auto_migrate_ddl: bool = True,
    load_mode: str = "replace",
    csv_cols: Optional[List[str]] = None,
    **kwargs,
) -> Tuple[bool, str, List[str], int]:
    """
    Compares CSV/PECSV header columns (either pre-extracted in `csv_cols` from
    `batch_descriptor.json` or read from `sample_file_path`) with the Teradata
    staging table via `DBC.ColumnsV`.
    Enforces `auto_migrate_ddl` configuration and blocks destructive `DROP TABLE`
    in `append` mode.
    Returns:
      (is_identical: bool, action: str, columns: List[str], has_meta: int)
    """
    conn = connection or kwargs.get("conn")
    td_db = target_db or kwargs.get("target_database")
    td_table = target_table or kwargs.get("entity") or kwargs.get("table")
    def_type = kwargs.get("default_col_type", default_type)
    mode = kwargs.get("load_mode", load_mode)
    auto_migrate = kwargs.get("auto_migrate_ddl", auto_migrate_ddl)
    cols_in = csv_cols or kwargs.get("csv_cols")
    sample_path = sample_file_path or kwargs.get("sample_path") or kwargs.get("sample_file_path")

    safe_db = validate_sql_identifier(td_db)
    safe_table = validate_sql_identifier(td_table)

    if cols_in is not None and len(cols_in) > 0:
        resolved_cols = [clean_column_name(c) for c in cols_in if clean_column_name(c)]
    elif sample_path:
        resolved_cols = extract_csv_columns(sample_path)
    else:
        raise ValueError(f"[{safe_table}] Either 'csv_cols' or 'sample_file_path' must be provided to Schema Guard.")

    td_cols = get_teradata_table_columns(conn, safe_db, safe_table)

    if td_cols is None:
        if not auto_migrate:
            raise SchemaSyncError(
                f"[{safe_table}] Target table {safe_db}.{safe_table} does not exist and auto_migrate_ddl is disabled."
            )
        logger.info(f"[{safe_table}] Table does not exist in Teradata staging. Creating table...")
        recreate_staging_table(conn, safe_db, safe_table, resolved_cols, def_type, character_set)
        return False, "CREATED", resolved_cols, 2

    if resolved_cols == td_cols:
        logger.info(f"[{safe_table}] Schema is identical ({len(resolved_cols)} columns). No DDL modification needed.")
        return True, "IDENTICAL", resolved_cols, 2

    logger.warning(
        f"[{safe_table}] Schema drift detected!\n"
        f"  File columns ({len(resolved_cols)}): {resolved_cols}\n"
        f"  Table columns ({len(td_cols)}): {td_cols}"
    )
    if not auto_migrate:
        raise SchemaSyncError(
            f"[{safe_table}] Schema drift detected on {safe_db}.{safe_table} "
            f"(file columns: {resolved_cols}, table columns: {td_cols}) and auto_migrate_ddl is disabled."
        )
    if str(mode).strip().lower() == "append":
        raise SchemaSyncError(
            f"[{safe_table}] Schema drift detected on {safe_db}.{safe_table}, "
            f"but destructive DROP TABLE is prohibited when load_mode is 'append'."
        )
    recreate_staging_table(conn, safe_db, safe_table, resolved_cols, def_type, character_set)
    return False, "RECREATED", resolved_cols, 2
