"""
Load module - Load raw data into staging layer.

This module handles inserting raw data from DETRAN CSV into
staging.detran_vehicle_raw table.
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from typing import Optional, Tuple
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()


def get_db_connection():
    """
    Create database connection from environment variables.
    
    Returns:
        psycopg2 connection object
        
    Raises:
        ValueError: If required environment variables are missing
    """
    from pipeline.utils import get_db_connection_from_env
    return get_db_connection_from_env()


def create_staging_schemas(conn):
    """
    Create staging schema if it doesn't exist.
    
    Args:
        conn: Database connection
    """
    cursor = conn.cursor()
    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()


def _raw_partition_name(report_period: int) -> str:
    """Return partition table name for raw (e.g. detran_vehicle_raw_202501)."""
    return f"detran_vehicle_raw_{report_period}"


def ensure_staging_table_exists(
    report_period: int,
    conn: Optional[psycopg2.extensions.connection] = None,
) -> Tuple[bool, bool]:
    """
    Ensure staging schema, detran_vehicle_raw partitioned table, and the
    partition for report_period exist. Creates them if they don't exist.

    Args:
        report_period: Report period YYYYMM (e.g. 202501)
        conn: Database connection (if None, creates new)

    Returns:
        tuple: (schema_created: bool, table_created: bool)
    """
    from pipeline.utils import get_db_connection_from_env, execute_sql_file
    from pathlib import Path

    if conn is None:
        conn = get_db_connection_from_env()
        close_conn = True
    else:
        close_conn = False

    try:
        cursor = conn.cursor()
        schema_created = False
        table_created = False

        cursor.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name = 'staging'
        """)
        if cursor.fetchone() is None:
            create_staging_schemas(conn)
            schema_created = True

        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'staging'
            AND table_name = 'detran_vehicle_raw'
        """)
        if cursor.fetchone() is None:
            sql_file = Path(__file__).parent.parent / "sql" / "staging" / "02_create_raw_table.sql"
            execute_sql_file(str(sql_file), conn)
            table_created = True

        # Ensure partition for report_period exists
        part_name = _raw_partition_name(report_period)
        cursor.execute("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'staging' AND tablename = %s
        """, (part_name,))
        if cursor.fetchone() is None:
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS staging." + part_name + " PARTITION OF staging.detran_vehicle_raw FOR VALUES IN (%s)",
                (report_period,),
            )
            conn.commit()

        cursor.close()
        return schema_created, table_created

    finally:
        if close_conn:
            conn.close()


def load_raw_data(
    df: pd.DataFrame,
    report_period: int,
    table_name: str = "staging.detran_vehicle_raw",
    source_file: Optional[str] = None,
    conn: Optional[psycopg2.extensions.connection] = None,
) -> int:
    """
    Load raw DataFrame into staging table (partition for report_period).

    Args:
        df: DataFrame with raw data from DETRAN CSV
        report_period: Report period YYYYMM (e.g. 202501)
        table_name: Target table name (parent partitioned table)
        source_file: Path to source CSV file (for metadata)
        conn: Database connection (if None, creates new)

    Returns:
        Number of rows inserted
    """
    import unicodedata
    import re

    if conn is None:
        conn = get_db_connection()
        close_conn = True
    else:
        close_conn = False

    try:
        def normalize_column_name(col_name: str) -> str:
            normalized = col_name.lower()
            normalized = unicodedata.normalize('NFD', normalized)
            normalized = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
            normalized = re.sub(r'[\s\-]+', '_', normalized)
            normalized = re.sub(r'[^a-z0-9_]', '', normalized)
            normalized = re.sub(r'_+', '_', normalized)
            normalized = normalized.strip('_')
            return normalized if normalized else 'coluna_' + str(hash(col_name) % 10000)

        column_mapping = {}
        for excel_col in df.columns:
            column_mapping[excel_col] = normalize_column_name(excel_col)

        df_mapped = df.copy()
        df_mapped.rename(columns=column_mapping, inplace=True)

        source_file_name = Path(source_file).name if source_file else None
        df_mapped['source_file'] = source_file_name
        df_mapped['csv_row'] = range(2, len(df_mapped) + 2)

        sql_columns = ['report_period'] + [col for col in df_mapped.columns if col not in ['id_raw']]

        values = []
        for _, row in df_mapped.iterrows():
            row_values = [report_period]
            for col in df_mapped.columns:
                if col == 'id_raw':
                    continue
                val = row[col]
                if pd.isna(val):
                    row_values.append(None)
                else:
                    row_values.append(str(val).strip() if str(val).strip() else None)
            values.append(tuple(row_values))
        
        # Build INSERT statement template
        columns_str = ', '.join(sql_columns)
        insert_template = f"INSERT INTO {table_name} ({columns_str}) VALUES %s"
        
        # Execute insert using execute_values for batch insertion
        cursor = conn.cursor()
        try:
            execute_values(
                cursor,
                insert_template,
                values,
                page_size=1000  # Insert in batches of 1000
            )
            conn.commit()
            rows_inserted = len(values)  # execute_values doesn't set rowcount, use len of values
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error inserting data: {str(e)}")
        finally:
            cursor.close()
        
        return rows_inserted
        
    finally:
        if close_conn:
            conn.close()


def _norm_partition_name(report_period: int) -> str:
    """Return partition table name for norm (e.g. detran_vehicle_norm_202501)."""
    return f"detran_vehicle_norm_{report_period}"


def truncate_staging_table(
    report_period: int,
    conn: Optional[psycopg2.extensions.connection] = None,
) -> None:
    """
    Truncate only the staging partitions for the given report_period.
    Order: norm partition first (FK to raw), then raw partition.

    Args:
        report_period: Report period YYYYMM (e.g. 202501)
        conn: Database connection (if None, creates new)
    """
    if conn is None:
        conn = get_db_connection()
        close_conn = True
    else:
        close_conn = False

    try:
        cursor = conn.cursor()
        try:
            norm_part = _norm_partition_name(report_period)
            raw_part = _raw_partition_name(report_period)
            cursor.execute(f"TRUNCATE TABLE staging.{norm_part} CASCADE;")
            cursor.execute(f"TRUNCATE TABLE staging.{raw_part} CASCADE;")
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error truncating staging partitions: {str(e)}") from e
        finally:
            cursor.close()
    finally:
        if close_conn:
            conn.close()
