"""
lib/schema_guard.py - Just-In-Time Schema Guard & Automated NOPI DDL Synchronizer

Executed inside DATA_INGESTION_ORCHESTRATOR immediately prior to entity loading:
compares extracted CSV/PECSV header columns (from `batch_descriptor.json` or
in-memory stream inspection) against Teradata Vantage `DBC.ColumnsV`, and
automatically creates missing tables or non-destructively appends new columns
via ALTER TABLE ADD when `auto_migrate_ddl=True`.
Destructive DROP TABLE is strictly prohibited.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from typing import Dict, List, Optional, Tuple

_COMMON_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "COMMON"))
if _COMMON_DIR not in sys.path:
    sys.path.insert(0, _COMMON_DIR)

from dataloader_core.db import validate_sql_identifier
from dataloader_core.exceptions import SchemaDriftError, SchemaSyncError
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
    Matches a raw data filename to its configured Teradata staging target table.
    Delegates to `dataloader_core.mapping.EntityMapper.match_file`.
    """
    mapper = EntityMapper()
    matched = mapper.match_file(filename, file_dir=file_dir)
    return matched.target_table if matched else None


def get_teradata_table_columns(connection, target_db: str, target_table: str) -> Tuple[Optional[List[str]], int]:
    """
    Queries DBC.ColumnsV for existing columns in the target table.
    Returns (td_cols, has_meta):
      td_cols: ordered list of uppercase column names excluding technical audit columns,
               or None if the table does not exist.
      has_meta: count of technical audit columns present (0, 1, or 2).
    """
    query = """
    SELECT TRIM(ColumnName)
    FROM DBC.ColumnsV
    WHERE DatabaseName = ? AND TableName = ?
    ORDER BY ColumnId;
    """
    cursor = connection.cursor()
    try:
        cursor.execute(query, [target_db, target_table])
        rows = cursor.fetchall()
        if not rows:
            return None, 0

        td_cols, audit_present = [], set()
        for r in rows:
            c_name = str(r[0]).strip().upper()
            if c_name in TECHNICAL_AUDIT_COLUMNS:
                audit_present.add(c_name)
            else:
                td_cols.append(c_name)

        return td_cols, len(audit_present)
    except Exception as e:
        logger.warning(f"Error checking DBC.ColumnsV for {target_db}.{target_table}: {e}")
        return None, 0
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


def declared_row_bytes(columns: List[str], default_type: str, character_set: str, has_meta_cols: int = 2) -> int:
    """
    Computes the declared maximum row size in bytes for a table schema.
    Uses width * per_char + 2 bytes per VARCHAR column, plus audit columns.
    """
    match = re.match(r"^VARCHAR\((\d+)\)$", validate_column_type(default_type), re.IGNORECASE)
    width = int(match.group(1)) if match else 4000
    per_char = 2 if character_set.upper() == "UNICODE" else 1
    payload = len(columns) * (width * per_char + 2)          # +2 bytes VARCHAR length indicator
    audit = 10 + (100 * per_char + 2) if has_meta_cols else 0   # TIMESTAMP(0)=10 bytes; VARCHAR(100)
    return payload + audit


def create_staging_table(
    connection,
    target_db: str,
    target_table: str,
    columns: List[str],
    default_type: str = "VARCHAR(4000)",
    character_set: str = "UNICODE",
    max_row_bytes: int = 64000,
):
    """
    Creates a new MULTISET TABLE ... NO PRIMARY INDEX for the target entity.
    Fails if the table already exists.
    """
    safe_db = validate_sql_identifier(target_db)
    safe_table = validate_sql_identifier(target_table)
    
    size = declared_row_bytes(columns, default_type, character_set, has_meta_cols=2)
    if size > max_row_bytes:
        raise SchemaDriftError(
            f"[{safe_table}] Declared row size {size} bytes exceeds max_row_bytes={max_row_bytes} "
            f"for {len(columns)} columns of {default_type} {character_set}. "
            "Create the table manually with appropriate column types, or raise schema_guard.max_row_bytes if the system supports large rows."
        )

    cursor = connection.cursor()
    ddl = generate_staging_ddl(safe_db, safe_table, columns, default_type, character_set)
    try:
        cursor.execute(ddl)
        logger.info(f"[{safe_table}] Successfully created NOPI staging table with {len(columns)} payload columns.")
    finally:
        cursor.close()


def alter_add_columns(
    connection,
    target_db: str,
    target_table: str,
    all_resulting_columns: List[str],
    new_columns: List[str],
    default_type: str,
    character_set: str,
    max_row_bytes: int = 64000,
):
    """
    Appends new columns to an existing staging table via non-destructive ALTER TABLE ... ADD.
    Guards declared row size against max_row_bytes before executing DDL.
    """
    safe_db = validate_sql_identifier(target_db)
    safe_table = validate_sql_identifier(target_table)
    size = declared_row_bytes(all_resulting_columns, default_type, character_set, has_meta_cols=2)
    if size > max_row_bytes:
        raise SchemaDriftError(
            f"[{safe_table}] Declared row size {size} bytes exceeds max_row_bytes={max_row_bytes} "
            f"for {len(all_resulting_columns)} columns of {default_type} {character_set}. "
            "Create the table manually with appropriate column types, or raise schema_guard.max_row_bytes if the system supports large rows."
        )

    safe_type = validate_column_type(default_type)
    safe_cs = validate_sql_identifier(character_set).upper()
    cur = connection.cursor()
    try:
        for col in new_columns:
            c = validate_sql_identifier(col)
            cur.execute(f"ALTER TABLE {safe_db}.{safe_table} ADD {c} {safe_type} CHARACTER SET {safe_cs} NOT CASESPECIFIC")
            logger.info(f"[{safe_table}] Added column {c}")
    finally:
        cur.close()


def sync_entity_schema_if_drifted(
    connection=None,
    target_db: Optional[str] = None,
    target_table: Optional[str] = None,
    sample_file_path: Optional[str] = None,
    default_type: str = "VARCHAR(4000)",
    character_set: str = "UNICODE",
    auto_migrate_ddl: bool = False,
    load_mode: str = "replace",
    csv_cols: Optional[List[str]] = None,
    max_row_bytes: int = 64000,
    **kwargs,
) -> Tuple[bool, str, List[str], int]:
    """
    Compares CSV/PECSV header columns against Teradata Vantage DBC.ColumnsV.
    Performs non-destructive table creation or ALTER TABLE ADD when auto_migrate_ddl is enabled.
    Destructive DROP TABLE is never executed.
    Returns:
      (is_identical: bool, action: str, columns: List[str], has_meta: int)
    """
    conn = connection or kwargs.get("conn")
    td_db = target_db or kwargs.get("td_db") or kwargs.get("target_database")
    td_table = target_table or kwargs.get("td_table") or kwargs.get("entity") or kwargs.get("table")
    def_type = kwargs.get("default_col_type", default_type)
    auto_migrate = kwargs.get("auto_migrate_ddl", auto_migrate_ddl)
    cols_in = csv_cols or kwargs.get("csv_cols")
    sample_path = sample_file_path or kwargs.get("sample_path") or kwargs.get("sample_file_path")
    max_bytes = int(kwargs.get("max_row_bytes", max_row_bytes))

    safe_db = validate_sql_identifier(td_db)
    safe_table = validate_sql_identifier(td_table)

    if cols_in is not None and len(cols_in) > 0:
        resolved_cols = [clean_column_name(c) for c in cols_in if clean_column_name(c)]
    elif sample_path:
        resolved_cols = extract_csv_columns(sample_path)
    else:
        raise ValueError(f"[{safe_table}] Either 'csv_cols' or 'sample_file_path' must be provided to Schema Guard.")

    td_cols, has_meta = get_teradata_table_columns(conn, safe_db, safe_table)

    if td_cols is None:
        if not auto_migrate:
            raise SchemaDriftError(
                f"[{safe_table}] Target table {safe_db}.{safe_table} does not exist and auto_migrate_ddl is disabled."
            )
        logger.info(f"[{safe_table}] Table does not exist in Teradata staging. Creating table...")
        create_staging_table(conn, safe_db, safe_table, resolved_cols, def_type, character_set, max_row_bytes=max_bytes)
        return False, "CREATED", resolved_cols, 2

    if has_meta == 1:
        raise SchemaSyncError(f"[{safe_table}] table has exactly one audit column; expected both or none")

    if resolved_cols == td_cols:
        logger.info(f"[{safe_table}] Schema is identical ({len(resolved_cols)} columns). No DDL modification needed.")
        return True, "IDENTICAL", resolved_cols, has_meta

    is_pure_append = (len(resolved_cols) > len(td_cols) and resolved_cols[:len(td_cols)] == td_cols)
    drift_msg = (f"[{safe_table}] Schema drift on {safe_db}.{safe_table}: "
                 f"file columns ({len(resolved_cols)}) = {resolved_cols}; table payload columns ({len(td_cols)}) = {td_cols}")
    if not auto_migrate:
        raise SchemaDriftError(drift_msg + " — auto_migrate_ddl is disabled; a DBA must align the table")
    if not is_pure_append:
        raise SchemaDriftError(drift_msg + " — only appended new columns can be migrated automatically; a DBA must align the table")

    new_cols = resolved_cols[len(td_cols):]
    logger.warning(f"[{safe_table}] Appending {len(new_cols)} new column(s): {new_cols}")
    alter_add_columns(conn, safe_db, safe_table, resolved_cols, new_cols, def_type, character_set, max_row_bytes=max_bytes)
    return False, "ALTERED", resolved_cols, has_meta
